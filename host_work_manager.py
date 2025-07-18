# host_work_manager.py
"""
Host machine work manager - creates tasks and processes results.
Run this on the machine with PostgreSQL database.
"""

import time
import pandas as pd
from typing import List, Dict
import json
import os

from coordination.github_bridge import GitHubWorkBridge
from coordination.coordinator import create_work_tracker
from pipelines.princpal_orchestrator import ClubOrchestrator
from database.orchestrators.team_orchestrator import TeamDataOrchestrator


class HostWorkManager:
    """
    Manages work distribution and result processing on host machine.
    """
    
    def __init__(self, environment: str = "production", repo_url: str = None):
        """
        Initialize host work manager.
        
        Args:
            environment: Database environment
            repo_url: GitHub repository URL
        """
        self.environment = environment
        self.github_bridge = GitHubWorkBridge(repo_url=repo_url)
        self.progress_monitor = create_work_tracker(environment)
        
        # Fix: Use correct ClubOrchestrator initialization
        from configurations import ConfigFactory
        if environment == "production":
            config = ConfigFactory.production()
        elif environment == "testing":
            config = ConfigFactory.testing()
        else:
            config = ConfigFactory.development()
        
        config._environment = environment
        self.club_orchestrator = ClubOrchestrator(config=config)
        
        print(f"🏠 Host work manager initialized for {environment}")
    
    def create_work_orders(self) -> int:
        """
        Create work orders for pending competitions.
        
        Returns:
            Number of work orders created
        """
        print("📋 Creating work orders...")

        existing_work = set()
        for folder in ['available', 'claimed', 'completed']:
            folder_path = self.github_bridge.folders[folder]
            for work_file in folder_path.glob('comp_*.json'):
                try:
                    with open(work_file, 'r') as f:
                        work_data = json.load(f)
                        existing_work.add(work_data['competition_id'])
                except:
                    continue
        
        # Get competitions that need work
        competitions = self.club_orchestrator.get_non_cup_competitions()
        competitions = self.club_orchestrator._filter_excluded_competitions(competitions)
        
        work_orders_created = 0
        
        for competition in competitions:
            competition_id = competition['competition_id']

            if competition_id in existing_work:
                continue
            
            # Skip if competition is already completed
            if self.progress_monitor.is_competition_completed(competition_id):
                continue
            
            # Get completed seasons to include in work order
            completed_seasons = self._get_completed_seasons(competition_id)
            
            # Create work order
            work_id = self.github_bridge.create_competition_work_order(
                competition, completed_seasons
            )
            
            work_orders_created += 1
            print(f"📋 Created work order {work_id} for {competition_id}")
        
        print(f"✅ Created {work_orders_created} work orders")
        return work_orders_created
    
    def _get_completed_seasons(self, competition_id: str) -> List[str]:
        """Get list of completed seasons for a competition."""
        try:
            with self.progress_monitor.db_service.transaction() as session:
                from sqlalchemy import text
                result = session.execute(text("""
                    SELECT season_year FROM season_progress 
                    WHERE competition_id = :comp_id AND status = 'completed'
                """), {"comp_id": competition_id})
                
                return [row[0] for row in result.fetchall()]
        except Exception:
            return []
    
    def process_completed_work(self) -> int:
        """
        Process completed work from workers.
        
        Returns:
            Number of completed work items processed
        """
        print("🔄 Processing completed work...")
        
        completed_work_items = self.github_bridge.get_completed_work()
        processed_count = 0
        
        for work_result in completed_work_items:
            try:
                self._process_single_work_result(work_result)
                self.github_bridge.archive_processed_work(work_result)
                processed_count += 1
                
            except Exception as e:
                print(f"❌ Error processing work {work_result.get('work_id', 'unknown')}: {e}")
                continue
        
        if processed_count > 0:
            print(f"✅ Processed {processed_count} completed work items")
        
        return processed_count
    
    def _process_single_work_result(self, work_result: Dict):
        """Process a single completed work result."""
        work_id = work_result['work_id']
        competition_id = work_result['competition_id']
        
        print(f"📊 Processing work result: {work_id}")
        
        # Save club data to database if present
        if 'club_data' in work_result and work_result['club_data']:
            club_df = pd.DataFrame(work_result['club_data'])
            self._save_club_data(club_df)
            print(f"💾 Saved {len(club_df)} clubs to database")
        
        # Update season progress
        if 'seasons_processed' in work_result:
            for season_info in work_result['seasons_processed']:
                season_id = season_info['season_id']
                clubs_saved = season_info.get('clubs_scraped', 0)
                
                # Mark season as completed in progress tracker
                self.progress_monitor.mark_season_completed(
                    competition_id, season_id, clubs_saved
                )
                
                print(f"✅ Marked season {season_id} complete ({clubs_saved} clubs)")
        
        print(f"✅ Finished processing {work_id}")
    
    def _save_club_data(self, club_df: pd.DataFrame):
        """Save club data to database."""
        try:
            team_orchestrator = TeamDataOrchestrator(self.environment)
            result = team_orchestrator.save_clubs(club_df)
            team_orchestrator.cleanup()
            return result
        except Exception as e:
            print(f"❌ Error saving club data: {e}")
            return False

    def _mark_permanently_failed(self, failed_work):
        """Mark competition as permanently failed in database."""
        competition_id = failed_work['competition_id']
        error_message = failed_work['error_message']
        
        # Mark in progress tracker as failed
        try:
            # Use existing progress tracker to mark as failed
            with self.progress_monitor.db_service.transaction() as session:
                from sqlalchemy import text
                session.execute(text("""
                    UPDATE competition_progress 
                    SET status = 'failed', error_message = :error
                    WHERE competition_id = :comp_id
                """), {"comp_id": competition_id, "error": f"Permanent failure: {error_message}"})
                session.commit()
            
            print(f"❌ Permanently failed: {competition_id} - {error_message}")
            
            # Remove from failed folder
            if '_file_path' in failed_work:
                os.remove(failed_work['_file_path'])
                
        except Exception as e:
            print(f"⚠️ Error marking permanent failure: {e}")

    def process_failed_work(self) -> int:
        """Process failed work for retry or permanent failure marking."""
        failed_work_items = self.github_bridge.get_failed_work()
        retried_count = 0
        
        for failed_work in failed_work_items:
            retry_count = failed_work.get('retry_count', 0)
            
            if retry_count < 3:  # Max 3 retries
                self.github_bridge.retry_failed_work(failed_work)
                retried_count += 1
                print(f"🔄 Retrying failed work: {failed_work['work_id']} (attempt {retry_count + 1})")
            else:
                self._mark_permanently_failed(failed_work)
        
        return retried_count
    
    def monitor_work_status(self):
        """Print current work status."""
        github_status = self.github_bridge.get_work_status()
        progress_summary = self.progress_monitor.get_progress_summary()
        
        print("\n📊 WORK STATUS:")
        print(f"Available work orders: {github_status['available']}")
        print(f"Claimed work orders: {github_status['claimed']}") 
        print(f"Completed work orders: {github_status['completed']}")
        print(f"Failed work orders: {github_status['failed']}")
        print(f"Active workers: {progress_summary['active_workers']}")
        print(f"Total clubs saved: {progress_summary['total_clubs_saved']}")
    
    def run_host_cycle(self, max_cycles: int = 100):
        """
        Run host machine cycle - create work orders and process results.
        
        Args:
            max_cycles: Maximum number of cycles to run
        """
        print(f"🚀 Starting host work cycle (max {max_cycles} cycles)")
        
        for cycle in range(max_cycles):
            print(f"\n🔄 Host cycle {cycle + 1}/{max_cycles}")
            
            # Create new work orders
            new_orders = self.create_work_orders()
            
            # Process completed work
            processed = self.process_completed_work()

            _ = self.process_failed_work()
            
            # Show status
            self.monitor_work_status()
            
            # If no new work and nothing to process, we're done
            if new_orders == 0 and processed == 0:
                github_status = self.github_bridge.get_work_status()
                if github_status['claimed'] == 0:  # No active work
                    print("🎉 All work completed!")
                    break
            
            # Wait before next cycle
            print("⏱️ Waiting 30 seconds before next cycle...")
            time.sleep(30)
        
        print("🏁 Host work cycle finished")
    
    def cleanup(self):
        """Cleanup resources."""
        if hasattr(self, 'progress_monitor'):
            self.progress_monitor.db_service.cleanup()
        if hasattr(self, 'club_orchestrator'):
            self.club_orchestrator.cleanup()


if __name__ == "__main__":
    # Example usage
    import argparse
    
    parser = argparse.ArgumentParser(description="Host Work Manager")
    parser.add_argument("--repo-url", help="GitHub repository URL")
    parser.add_argument("--environment", default="production", help="Database environment")
    parser.add_argument("--max-cycles", type=int, default=1500, help="Maximum cycles to run")
    
    args = parser.parse_args()
    
    try:
        manager = HostWorkManager(
            environment=args.environment,
            repo_url=args.repo_url
        )
        
        manager.run_host_cycle(args.max_cycles)
        
    except KeyboardInterrupt:
        print("\n⏹️ Interrupted by user")
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        if 'manager' in locals():
            manager.cleanup()