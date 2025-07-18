# worker_main.py
"""
Worker machine main script - replaces main.py for distributed workers.
Run this on worker machines (no database access needed).
"""

import time
import uuid
import socket
import os
import pandas as pd
from typing import Dict, List
from datetime import datetime
from selenium import webdriver

from coordination.github_bridge import GitHubWorkBridge
from configurations import ConfigFactory


class DistributedWorker:
    """
    Worker machine that processes competitions from GitHub work orders.
    """
    
    def __init__(self, repo_url: str, environment: str = "production"):
        """
        Initialize distributed worker.
        
        Args:
            repo_url: GitHub repository URL
            environment: Configuration environment
        """
        self.repo_url = repo_url
        self.environment = environment
        
        # Generate unique worker ID
        self.worker_id = f"{socket.gethostname()}_{os.getpid()}_{uuid.uuid4().hex[:8]}"
        
        # Initialize GitHub bridge
        self.github_bridge = GitHubWorkBridge(repo_url=repo_url)
        
        # Initialize scraping components (but skip database-dependent parts)
        self.config = self._get_scraping_config()
        self._setup_scraping_components()
        
        print(f"🤖 Worker {self.worker_id} initialized")
    
    def _get_scraping_config(self):
        """Get scraping configuration."""
        if self.environment == "production":
            config = ConfigFactory.production()
        elif self.environment == "testing":
            config = ConfigFactory.testing()
        else:
            config = ConfigFactory.development()
            
        config._environment = self.environment
        config.use_vpn = True  # Always use VPN for workers
        return config
    
    def _setup_scraping_components(self):
        """Setup scraping components without database dependencies."""
        from extractors import ClubTableParser
        from vpn_controls import VpnProtectionHandler
        
        # Setup parser
        self.parser = ClubTableParser()
        
        # Setup VPN handler
        self.vpn_handler = VpnProtectionHandler(config=self.config)
        
        print("🔧 Scraping components initialized")
    
    def process_work_order(self, work_order: Dict, driver) -> Dict:
        """
        Process a single work order - scrape competition seasons.
        
        Args:
            work_order: Work order from GitHub
            driver: Selenium WebDriver
            
        Returns:
            Results dictionary
        """
        competition_id = work_order['competition_id']
        competition_url = work_order['competition_url']
        completed_seasons = work_order.get('completed_seasons', [])
        
        #print(f"🎯 Processing competition {competition_id}")
        #print(f"🔗 URL: {competition_url}")
        #print(f"⏭️ Skipping {len(completed_seasons)} completed seasons")
        
        try:
            # Ensure VPN protection
            self.vpn_handler.ensure_vpn_protection()
            
            # Discover seasons
            seasons = self._discover_seasons(driver, competition_url)
            print(f"🔍 Discovered {len(seasons)} total seasons")
            
            # Filter out completed seasons
            pending_seasons = [
                season for season in seasons
                if season['year'] not in completed_seasons
            ]
            print(f"📋 Processing {len(pending_seasons)} pending seasons")
            
            # Process each pending season
            all_club_data = []
            seasons_processed = []
            
            for i, season in enumerate(pending_seasons, 1):
                print(f"📅 Processing season {i}/{len(pending_seasons)}: {season['season_id']}")
                
                try:
                    club_data = self._scrape_season_data(
                        driver, competition_url, competition_id, season
                    )
                    
                    if not club_data.empty:
                        all_club_data.append(club_data)
                        clubs_count = len(club_data)
                        print(f"✅ Season {season['season_id']}: {clubs_count} clubs")
                    else:
                        clubs_count = 0
                        print(f"⚠️ Season {season['season_id']}: No data")
                    
                    seasons_processed.append({
                        'season_id': season['season_id'],
                        'season_year': season['year'],
                        'clubs_scraped': clubs_count,
                        'status': 'completed'
                    })
                    
                    # VPN timing between seasons
                    self.vpn_handler.handle_request_timing("between seasons...")
                    
                except Exception as e:
                    print(f"❌ Error in season {season['season_id']}: {e}")
                    seasons_processed.append({
                        'season_id': season['season_id'],
                        'season_year': season['year'],
                        'clubs_scraped': 0,
                        'status': 'failed',
                        'error': str(e)
                    })
                    continue
            
            # Combine all club data
            if all_club_data:
                combined_df = pd.concat(all_club_data, ignore_index=True)
                club_data_dict = combined_df.to_dict('records')
                total_clubs = len(club_data_dict)
            else:
                club_data_dict = []
                total_clubs = 0
            
            print(f"✅ Completed {competition_id}: {total_clubs} total clubs")
            
            return {
                'seasons_processed': seasons_processed,
                'club_data': club_data_dict,
                'total_clubs_scraped': total_clubs,
                'execution_time_seconds': (datetime.now() - datetime.fromisoformat(work_order['created_at'])).total_seconds()
            }
            
        except Exception as e:
            print(f"❌ Fatal error processing {competition_id}: {e}")
            raise
    
    def _discover_seasons(self, driver, competition_url: str) -> List[Dict]:
        """Discover available seasons for competition."""
        driver.get(competition_url)
        self.vpn_handler.handle_request_timing("page load for season discovery...")
        
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(driver.page_source, "html.parser")
        seasons = self.parser.parse_season_options(soup)
        
        return seasons
    
    def _scrape_season_data(self, driver, base_url: str, competition_id: str, season: Dict) -> pd.DataFrame:
        """Scrape data for a single season."""
        season_year = season['year']
        season_id = season['season_id']
        
        # Construct season URL (copied from ClubOrchestrator logic)
        season_url = self._construct_season_url(base_url, season_year)
        
        # Navigate to season URL
        driver.get(season_url)
        self.vpn_handler.handle_request_timing("season page load...")
        
        # Parse club data
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(driver.page_source, "html.parser")
        
        club_data = self.parser.parse_club_table(
            soup, season_year, season_id, competition_id
        )
        
        return club_data
    
    def _construct_season_url(self, base_url: str, season_year: str) -> str:
        """Construct season URL - copied from ClubOrchestrator."""
        from pipelines.princpal_orchestrator.orchestrator_config import OrchestratorConfig
        
        if season_year == OrchestratorConfig.CURRENT_SEASON_YEAR:
            return base_url
        else:
            separator = (OrchestratorConfig.URL_SEPARATOR_PARAM 
                        if OrchestratorConfig.URL_SEPARATOR_QUERY in base_url 
                        else OrchestratorConfig.URL_SEPARATOR_QUERY)
            
            if base_url.endswith(OrchestratorConfig.URL_PATH_SEPARATOR):
                return f"{base_url}{OrchestratorConfig.HISTORICAL_SEASON_PATH}{separator}{OrchestratorConfig.SEASON_PARAM_PREFIX}{season_year}"
            else:
                return f"{base_url}{OrchestratorConfig.URL_PATH_SEPARATOR}{OrchestratorConfig.HISTORICAL_SEASON_PATH}{separator}{OrchestratorConfig.SEASON_PARAM_PREFIX}{season_year}"
    
    def run_worker_cycle(self, max_work_orders: int = 50):
        """
        Run worker cycle - claim work and process it.
        
        Args:
            max_work_orders: Maximum work orders to process
        """
        print(f"🚀 Starting worker cycle (max {max_work_orders} work orders)")
        
        processed_count = 0
        driver = None
        
        try:
            # Initialize webdriver
            driver = webdriver.Chrome()
            print("🌐 WebDriver initialized")
            
            while processed_count < max_work_orders:
                print(f"\n🔍 Looking for work... ({processed_count}/{max_work_orders} completed)")
                
                # Try to claim work
                work_order = self.github_bridge.claim_available_work(self.worker_id)
                
                if not work_order:
                    print("😴 No work available, waiting 30 seconds...")
                    time.sleep(30)
                    continue
                
                print(f"✅ Claimed work: {work_order['work_id']}")
                
                try:
                    # Process the work order
                    results = self.process_work_order(work_order, driver)
                    
                    # Submit completed work
                    self.github_bridge.submit_completed_work(work_order, results)
                    
                    processed_count += 1
                    print(f"🎉 Completed work: {work_order['work_id']}")
                    
                except Exception as e:
                    # Submit failed work
                    error_msg = f"Processing error: {str(e)}"
                    self.github_bridge.submit_failed_work(work_order, error_msg)
                    print(f"❌ Failed work: {work_order['work_id']} - {error_msg}")
                    continue
                
                # Brief pause between work orders
                time.sleep(10)
            
            print(f"🏁 Worker cycle completed: {processed_count} work orders processed")
            
        except KeyboardInterrupt:
            print("\n⏹️ Worker interrupted by user")
        except Exception as e:
            print(f"❌ Worker error: {e}")
        finally:
            if driver:
                driver.quit()
                print("🌐 WebDriver closed")
    
    def cleanup(self):
        """Cleanup worker resources."""
        if hasattr(self, 'vpn_handler'):
            self.vpn_handler.cleanup()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Distributed Worker")
    parser.add_argument("--repo-url", required=True, help="GitHub repository URL")
    parser.add_argument("--environment", default="production", help="Configuration environment")
    parser.add_argument("--max-work", type=int, default=50, help="Maximum work orders to process")
    
    args = parser.parse_args()
    
    try:
        worker = DistributedWorker(
            repo_url=args.repo_url,
            environment=args.environment
        )
        
        worker.run_worker_cycle(args.max_work)
        
    except KeyboardInterrupt:
        print("\n⏹️ Interrupted by user")
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        if 'worker' in locals():
            worker.cleanup()