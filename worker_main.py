# worker_main_beta.py
"""
Worker machine main script - now uses existing ClubOrchestrator logic.
"""

import time
import uuid
import socket
import os
import subprocess
import signal
import sys
from typing import Dict, Optional
from datetime import datetime
from selenium import webdriver

from coordination.github_bridge import GitHubWorkBridge
from coordination.coordinator import InMemoryProgressTracker
from pipelines.princpal_orchestrator import ClubOrchestrator
from configurations import ConfigFactory

class SleepPreventer:
    """
    Prevents macOS system sleep while program is running.
    Uses caffeinate command to keep system awake.
    """
    
    def __init__(self):
        self.caffeinate_process: Optional[subprocess.Popen] = None
        self.is_active = False
    
    def start_prevention(self):
        """Start preventing system sleep."""
        try:
            # caffeinate -d prevents display sleep, -i prevents idle sleep
            # -s prevents system sleep when on AC power
            self.caffeinate_process = subprocess.Popen([
                'caffeinate', '-d', '-i', '-s'
            ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            
            self.is_active = True
            print("☕ Sleep prevention activated (caffeinate running)")
            
        except FileNotFoundError:
            print("⚠️ caffeinate not found - sleep prevention unavailable")
        except Exception as e:
            print(f"⚠️ Failed to start sleep prevention: {e}")
    
    def stop_prevention(self):
        """Stop preventing system sleep."""
        if self.caffeinate_process and self.is_active:
            try:
                self.caffeinate_process.terminate()
                self.caffeinate_process.wait(timeout=5)
                print("☕ Sleep prevention deactivated")
            except subprocess.TimeoutExpired:
                self.caffeinate_process.kill()
                print("☕ Sleep prevention forcibly stopped")
            except Exception as e:
                print(f"⚠️ Error stopping sleep prevention: {e}")
            finally:
                self.is_active = False
                self.caffeinate_process = None
    
    def __enter__(self):
        self.start_prevention()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop_prevention()

class DistributedWorker:
    """
    Worker machine that processes competitions
    """
    
    def __init__(self, repo_url: str, environment: str = "production"):
        """Initialize distributed worker."""
        self.repo_url = repo_url
        self.environment = environment
        
        # Generate unique worker ID
        self.worker_id = f"{socket.gethostname()}_{os.getpid()}_{uuid.uuid4().hex[:8]}"
        
        # Initialize GitHub bridge
        self.github_bridge = GitHubWorkBridge(repo_url=repo_url)

        # Sleep prevention
        self.sleep_preventer = SleepPreventer()
        
        print(f"🤖 Worker {self.worker_id} initialized")
    
    def process_work_order(self, work_order: Dict, driver) -> Dict:
        """
        Process work order using existing scrape_club_data API.
        """
        competition_id = work_order['competition_id']
        
        print(f"🎯 Processing competition {competition_id}")
        
        try:
            # Create in-memory progress tracker for this work order
            progress_tracker = InMemoryProgressTracker(work_order)
            
            # Get config (your existing code)
            if self.environment == "production":
                config = ConfigFactory.production()
            elif self.environment == "testing":
                config = ConfigFactory.testing()
            else:
                config = ConfigFactory.development()
            
            config._environment = self.environment
            config.use_vpn = True
            config.save_to_database = False
            
            # Create orchestrator with in-memory tracker
            orchestrator = ClubOrchestrator(
                config=config,
                progress_tracker=progress_tracker
            )
            orchestrator._initialize_components()
            
            try:
                # Use the existing method but for specific competition
                competition_data = {
                    'competition_id': competition_id,
                    'competition_url': work_order['competition_url']
                }
                
                # Call existing scraping method (this populates the progress_tracker)
                _ = orchestrator._scrape_competition_club_data(driver, competition_data)
                processed_seasons = progress_tracker.get_processed_seasons()
                all_club_data = progress_tracker.get_all_club_data()
                total_clubs = len(all_club_data)
                
                print(f"✅ Completed {competition_id}: {total_clubs} clubs, {len(processed_seasons)} seasons")
                
                return {
                    'seasons_processed': processed_seasons,
                    'club_data': all_club_data,
                    'total_clubs_scraped': total_clubs,
                    'execution_time_seconds': (datetime.now() - datetime.fromisoformat(work_order['created_at'])).total_seconds()
                }
                
            finally:
                orchestrator.cleanup()
                
        except Exception as e:
            print(f"❌ Error processing {competition_id}: {e}")
            raise

    def _setup_signal_handlers(self):
        """
        Setup signal handlers for graceful shutdown
        """
        def signal_handler(signum, frame):
            print(f"\n⚠️ Received signal {signum}, shutting down gracefully...")
            self.shutdown()
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)   # Ctrl+C
        signal.signal(signal.SIGTERM, signal_handler)  # Termination signal
    
    def shutdown(self):
        """
        Graceful shutdown
        """
        print("🛑 Shutting down worker...")
        self.sleep_preventer.stop_prevention()
    
    def run_worker_cycle(self, max_work_orders: int = 50):
        """
        Run worker cycle
        """
        print(f"🚀 Starting worker cycle (max {max_work_orders} work orders)")

        # Setup signal handlers
        self._setup_signal_handlers()
        
        processed_count = 0
        driver = None

        with self.sleep_preventer:
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
                        if processed_count % 10 == 0:  # Every 10th iteration
                            print(f"💾 System check - Worker still active, sleep prevention: {self.sleep_preventer.is_active}")
                        time.sleep(30)
                        continue
                    
                    print(f"✅ Claimed work: {work_order['work_id']}")
                    
                    try:
                        # Process the work order using existing logic
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
        print("👋 Worker shutdown complete")