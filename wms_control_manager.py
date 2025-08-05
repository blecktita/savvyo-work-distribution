# host_work_manager.py
"""
Host machine work manager - creates tasks and processes results.
Run this on the machine with PostgreSQL database.
"""

import json
import os
import time
from typing import Dict, List

import pandas as pd

from coordination.coordinator import create_work_tracker
from coordination.github_bridge import GitHubWorkBridge
from database.orchestrators.team_orchestrator import TeamDataOrchestrator
from pipelines.princpal_orchestrator import ClubOrchestrator


class HostWorkManager:
    """
    Manages work distribution and result processing on host machine.
    """

    def __init__(
        self,
        environment: str = "production",
        repo_url: str = None,
        archive_path: str = None,
    ):
        """
        Initialize host work manager.

        Args:
            environment: Database environment
            repo_url: GitHub repository URL
        """
        self.environment = environment

        if archive_path is None:
            archive_path = f"./work_archive_{environment}"

        self.github_bridge = GitHubWorkBridge(
            repo_url=repo_url, archive_path=archive_path
        )
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
        # DISABLE VPN FOR HOST OPERATIONS
        config.use_vpn = False
        self.club_orchestrator = ClubOrchestrator(config=config)

        print(f"🏠 Host work manager initialized for {environment}")

    def get_archive_statistics(self) -> Dict:
        """Get comprehensive archive statistics."""
        try:
            stats = self.github_bridge.get_archive_statistics()

            print("📊 ARCHIVE STATISTICS")
            print("=" * 25)
            print(f"Total archived: {stats.get('total_archived', 0):,} items")
            print(f"Archive size: {stats.get('total_size_mb', 0):.2f} MB")
            print(f"Files: {stats.get('file_count', 0):,}")
            print(f"Avg file size: {stats.get('average_file_size_kb', 0):.2f} KB")

            return stats
        except Exception as e:
            print(f"⚠️ Error getting archive stats: {e}")
            return {}

    def search_archived_work(
        self, competition_id: str = None, date: str = None, limit: int = 50
    ) -> List[Dict]:
        """Search archived work results."""
        try:
            results = self.github_bridge.retrieve_archived_work(
                competition_id=competition_id, date=date, limit=limit
            )

            print(f"🔍 Found {len(results)} archived work items")
            for result in results[:5]:  # Show first 5
                work_id = result.get("work_id", "unknown")
                comp_id = result.get("competition_id", "unknown")
                status = result.get("status", "unknown")
                print(f"   📄 {work_id} ({comp_id}) - {status}")

            if len(results) > 5:
                print(f"   ... and {len(results) - 5} more")

            return results
        except Exception as e:
            print(f"⚠️ Error searching archives: {e}")
            return []

    def cleanup_old_archives(self, days_to_keep: int = 90):
        """Clean up old archive files."""
        try:
            print(f"🧹 Cleaning up archives older than {days_to_keep} days...")
            result = self.github_bridge.cleanup_old_archives(days_to_keep)

            print(f"✅ Cleanup completed:")
            print(f"   📁 Removed: {result['files_removed']} files")
            print(f"   💾 Freed: {result['size_freed_mb']:.2f} MB")
            print(f"   🗂️ Cleaned: {result['folders_cleaned']} folders")

            if result["errors"]:
                print(f"   ⚠️ {len(result['errors'])} errors occurred")

            return result
        except Exception as e:
            print(f"⚠️ Error during cleanup: {e}")
            return {
                "files_removed": 0,
                "size_freed_mb": 0,
                "folders_cleaned": 0,
                "errors": [str(e)],
            }

    def run_archive_maintenance(self):
        """Run routine archive maintenance."""
        print("🛠️ Running archive maintenance...")

        # Show current stats
        self.get_archive_statistics()

        # Clean up archives older than 90 days (configurable)
        self.cleanup_old_archives(days_to_keep=90)

        print("✅ Archive maintenance completed")

    def create_work_orders(self) -> int:
        """
        Create work orders for pending competitions.
        FIXED: Proper deduplication to prevent duplicate work orders.

        Returns:
            Number of work orders created
        """
        print("📋 Creating work orders...")

        # CRITICAL: Pull latest state from GitHub first
        self.github_bridge._git_pull()

        # Build comprehensive set of existing work (competition_ids that already have work orders)
        existing_work = set()

        print("🔍 Checking for existing work orders...")

        for folder_name in ["available", "claimed", "completed", "failed"]:
            folder_path = self.github_bridge.folders[folder_name]
            work_files = list(folder_path.glob("*.json"))

            for work_file in work_files:
                try:
                    with open(work_file, "r") as f:
                        work_data = json.load(f)
                        competition_id = work_data.get("competition_id")
                        if competition_id:
                            existing_work.add(competition_id)
                            # print(f"  ✅ Found existing work for: {competition_id}")
                except Exception as e:
                    print(f"  ⚠️ Error reading {work_file}: {e}")
                    continue

        print(f"📊 Total existing work orders: {len(existing_work)}")

        # Get all competitions that need work
        print("🔍 Getting competitions from database...")
        competitions = self.club_orchestrator.get_non_cup_competitions()
        competitions = self.club_orchestrator._filter_excluded_competitions(
            competitions
        )
        print(f"📊 Total competitions from database: {len(competitions)}")

        work_orders_created = 0
        skipped_existing = 0
        skipped_completed = 0

        for competition in competitions:
            competition_id = competition["competition_id"]

            # Skip if work order already exists
            if competition_id in existing_work:
                skipped_existing += 1
                # print(f"⏭️ Skipping {competition_id} - work order already exists")
                continue

            # Skip if competition is already completed in database
            if self.progress_monitor.is_competition_completed(competition_id):
                skipped_completed += 1
                # print(f"⏭️ Skipping {competition_id} - already completed in database")
                continue

            # Get completed seasons to include in work order
            completed_seasons = self._get_completed_seasons(competition_id)

            # Create work order
            try:
                work_id = self.github_bridge.create_competition_work_order(
                    competition, completed_seasons
                )

                # Add to existing_work set to prevent duplicates in this batch
                existing_work.add(competition_id)

                work_orders_created += 1
                print(f"📋 Created work order {work_id} for {competition_id}")

            except Exception as e:
                print(f"❌ Failed to create work order for {competition_id}: {e}")
                continue

        print("\n📊 WORK ORDER CREATION SUMMARY:")
        print(f"✅ Created: {work_orders_created} new work orders")
        print(f"⏭️ Skipped (existing): {skipped_existing}")
        print(f"⏭️ Skipped (completed): {skipped_completed}")
        print(f"📊 Total processed: {len(competitions)}")

        return work_orders_created

    def _get_completed_seasons(self, competition_id: str) -> List[str]:
        """Get list of completed seasons for a competition."""
        try:
            with self.progress_monitor.db_service.transaction() as session:
                from sqlalchemy import text

                result = session.execute(
                    text(
                        """
                    SELECT season_year FROM season_progress 
                    WHERE competition_id = :comp_id AND status = 'completed'
                """
                    ),
                    {"comp_id": competition_id},
                )

                return [row[0] for row in result.fetchall()]
        except Exception:
            return []

    def process_completed_work(self) -> int:
        """Process completed work with better error reporting."""
        try:
            print("🔄 Processing completed work...")
            completed_work_items = self.github_bridge.get_completed_work()
            processed_count = 0
            processing_errors = 0

            for work_result in completed_work_items:
                try:
                    self._process_single_work_result(work_result)
                    self.github_bridge.archive_processed_work(work_result)
                    processed_count += 1

                except Exception as e:
                    processing_errors += 1
                    work_id = work_result.get("work_id", "unknown")
                    print(f"❌ Error processing work {work_id}: {e}")
                    continue

            if processed_count > 0:
                print(f"✅ Processed {processed_count} completed work items")

            if processing_errors > 0:
                print(f"⚠️ {processing_errors} processing errors")

            return processed_count

        except Exception as e:
            print(f"💥 CRITICAL ERROR in completed work processing: {e}")
            raise

    def _process_single_work_result(self, work_result: Dict):
        """Process a single completed work result."""
        work_id = work_result["work_id"]
        competition_id = work_result["competition_id"]

        print(f"📊 Processing work result: {work_id}")

        # Save club data to database if present
        if "club_data" in work_result and work_result["club_data"]:
            club_df = pd.DataFrame(work_result["club_data"])
            self._save_club_data(club_df)
            print(f"💾 Saved {len(club_df)} clubs to database")

        # Update season progress
        if "seasons_processed" in work_result:
            for season_info in work_result["seasons_processed"]:
                season_id = season_info["season_id"]
                clubs_saved = season_info.get("clubs_scraped", 0)

                # Mark season as completed in progress tracker
                self.progress_monitor.mark_season_completed(
                    competition_id, season_id, clubs_saved
                )

        self._mark_competition_completed(competition_id)
        print(f"✅ Finished processing {work_id}")

    def _mark_competition_completed(self, competition_id: str):
        """Simple: Mark competition as completed after processing work."""
        try:
            with self.progress_monitor.db_service.transaction() as session:
                from sqlalchemy import text

                # Get competition URL
                url_result = session.execute(
                    text(
                        """
                    SELECT competition_url FROM competitions 
                    WHERE competition_id = :comp_id
                """
                    ),
                    {"comp_id": competition_id},
                )

                url_row = url_result.fetchone()
                comp_url = url_row[0] if url_row else f"PROCESSED_{competition_id}"

                # Upsert competition as completed
                if self.progress_monitor.db_type == "PostgreSQL":
                    session.execute(
                        text(
                            """
                        INSERT INTO competition_progress 
                        (competition_id, competition_url, status, completed_at)
                        VALUES (:comp_id, :url, 'completed', NOW())
                        ON CONFLICT (competition_id) DO UPDATE SET
                            status = 'completed', completed_at = NOW()
                    """
                        ),
                        {"comp_id": competition_id, "url": comp_url},
                    )
                else:  # SQLite
                    session.execute(
                        text(
                            """
                        INSERT OR REPLACE INTO competition_progress 
                        (competition_id, competition_url, status, completed_at)
                        VALUES (:comp_id, :url, 'completed', datetime('now'))
                    """
                        ),
                        {"comp_id": competition_id, "url": comp_url},
                    )

                session.commit()
                print(f"🏆 Marked {competition_id} as completed")

        except Exception as e:
            print(f"⚠️ Error marking competition complete: {e}")

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
        competition_id = failed_work["competition_id"]
        error_message = failed_work["error_message"]

        # Mark in progress tracker as failed
        try:
            # Use existing progress tracker to mark as failed
            with self.progress_monitor.db_service.transaction() as session:
                from sqlalchemy import text

                session.execute(
                    text(
                        """
                    UPDATE competition_progress 
                    SET status = 'failed', error_message = :error
                    WHERE competition_id = :comp_id
                """
                    ),
                    {
                        "comp_id": competition_id,
                        "error": f"Permanent failure: {error_message}",
                    },
                )
                session.commit()

            print(f"❌ Permanently failed: {competition_id} - {error_message}")

            # Remove from failed folder
            if "_file_path" in failed_work:
                os.remove(failed_work["_file_path"])

        except Exception as e:
            print(f"⚠️ Error marking permanent failure: {e}")

    def process_failed_work(self) -> int:
        """Process failed work for retry or permanent failure marking."""
        failed_work_items = self.github_bridge.get_failed_work()
        retried_count = 0

        for failed_work in failed_work_items:
            retry_count = failed_work.get("retry_count", 0)

            if retry_count < 3:  # Max 3 retries
                self.github_bridge.retry_failed_work(failed_work)
                retried_count += 1
                print(
                    f"🔄 Retrying failed work: {failed_work['work_id']} (attempt {retry_count + 1})"
                )
            else:
                self._mark_permanently_failed(failed_work)

        return retried_count

    def monitor_work_status(self):
        """Monitor status with error handling."""
        try:
            github_status = self.github_bridge.get_work_status()
            progress_summary = self.progress_monitor.get_progress_summary()

            print(
                f"\n📊 STATUS: {github_status['available']} available | "
                f"{github_status['claimed']} claimed | {github_status['completed']} completed | "
                f"{github_status['failed']} failed | {progress_summary['total_clubs_saved']} clubs saved"
            )

        except Exception as e:
            print(f"⚠️ Error getting work status: {e}")
            # Try to get partial status
            try:
                github_status = self.github_bridge.get_work_status()
                print(f"📊 Partial status: {github_status}")
            except:
                print("📊 Status unavailable")

    def run_host_cycle(self, max_cycles: int = 100):
        """
        Run host machine cycle with enhanced error visibility and archive management.
        """
        print(f"🚀 Starting host work cycle (max {max_cycles} cycles)")

        # 🆕 ARCHIVE: Show initial archive status
        try:
            if hasattr(self.github_bridge, "get_archive_statistics"):
                stats = self.github_bridge.get_archive_statistics()
                total_archived = stats.get("total_archived", 0)
                archive_size_mb = stats.get("total_size_mb", 0)
                if total_archived > 0:
                    print(
                        f"📦 Archive status: {total_archived:,} items ({archive_size_mb:.1f} MB)"
                    )
        except Exception as e:
            print(f"⚠️ Could not get initial archive stats: {e}")

        # Error tracking
        error_log = []
        total_new_orders = 0
        total_processed = 0
        last_status_time = 0
        last_archive_maintenance = 0  # 🆕 Track last archive maintenance

        for cycle in range(max_cycles):
            cycle_start = time.time()
            cycle_errors = []

            try:
                # Show cycle progress
                if cycle < 5 or cycle % 10 == 0:
                    print(f"\n🔄 Host cycle {cycle + 1}/{max_cycles}")
                elif cycle % 5 == 0:
                    print(".", end="", flush=True)

                # Create new work orders with error tracking
                try:
                    if cycle < 3:
                        new_orders = self.create_work_orders()
                    else:
                        print("📋 Checking for new work...", end="", flush=True)
                        new_orders = self._create_work_orders_quiet()
                        if new_orders > 0:
                            print(f" ✅ Created {new_orders} new orders")
                        else:
                            print(" (no new work)")

                    total_new_orders += new_orders

                except Exception as e:
                    error_msg = f"Work order creation failed: {str(e)}"
                    cycle_errors.append(error_msg)
                    print(f"\n🚨 ERROR in work order creation: {e}")
                    new_orders = 0

                # Process completed work with error tracking
                try:
                    processed = self.process_completed_work()
                    total_processed += processed

                    # 🆕 ARCHIVE: Show archive info when work is processed
                    if processed > 0 and hasattr(
                        self.github_bridge, "get_archive_statistics"
                    ):
                        try:
                            stats = self.github_bridge.get_archive_statistics()
                            recent_activity = stats.get("recent_activity", {})
                            today = time.strftime("%Y-%m-%d")
                            if today in recent_activity:
                                today_stats = recent_activity[today]
                                completed_today = today_stats.get("completed", 0)
                                failed_today = today_stats.get("failed", 0)
                                print(
                                    f"📦 Today's archive: {completed_today} completed, {failed_today} failed"
                                )
                        except:
                            pass  # Don't let archive stats break the flow

                except Exception as e:
                    error_msg = f"Completed work processing failed: {str(e)}"
                    cycle_errors.append(error_msg)
                    print(f"\n🚨 ERROR processing completed work: {e}")
                    processed = 0

                # Process failed work with error tracking
                try:
                    retried = self.process_failed_work()
                except Exception as e:
                    error_msg = f"Failed work processing failed: {str(e)}"
                    cycle_errors.append(error_msg)
                    print(f"\n🚨 ERROR processing failed work: {e}")
                    retried = 0

                # 🆕 ARCHIVE: Periodic archive maintenance (every 25 cycles or 2 hours)
                current_time = time.time()
                time_since_maintenance = current_time - last_archive_maintenance

                if (
                    cycle > 0 and cycle % 25 == 0
                ) or time_since_maintenance > 7200:  # 2 hours
                    try:
                        print("\n🛠️ Running periodic archive maintenance...")

                        # Get current stats
                        if hasattr(self, "get_archive_statistics"):
                            stats = self.get_archive_statistics()

                            # Only clean up if we have a significant archive
                            total_archived = stats.get("total_archived", 0)
                            if total_archived > 100:
                                print(
                                    f"🧹 Archive has {total_archived:,} items, running cleanup..."
                                )
                                cleanup_result = self.cleanup_old_archives(
                                    days_to_keep=90
                                )

                                if cleanup_result["files_removed"] > 0:
                                    print(
                                        f"✅ Freed {cleanup_result['size_freed_mb']:.1f} MB"
                                    )
                            else:
                                print(
                                    f"📦 Archive has {total_archived} items (no cleanup needed)"
                                )

                        last_archive_maintenance = current_time

                    except Exception as e:
                        error_msg = f"Archive maintenance failed: {str(e)}"
                        cycle_errors.append(error_msg)
                        print(f"⚠️ Archive maintenance error: {e}")

                # Monitor work status with error tracking
                try:
                    if (
                        cycle < 3
                        or cycle % 10 == 0
                        or new_orders > 0
                        or processed > 0
                        or retried > 0
                        or current_time - last_status_time > 300
                    ):
                        self.monitor_work_status()
                        last_status_time = current_time

                        if cycle > 0:
                            print(
                                f"📈 Session: {total_new_orders} created, {total_processed} completed"
                            )

                        # Show recent errors if any
                        if error_log:
                            recent_errors = error_log[-3:]  # Last 3 errors
                            print(f"⚠️ Recent errors ({len(error_log)} total):")
                            for i, err in enumerate(recent_errors, 1):
                                print(f"   {i}. {err}")

                except Exception as e:
                    error_msg = f"Status monitoring failed: {str(e)}"
                    cycle_errors.append(error_msg)
                    print(f"\n🚨 ERROR in status monitoring: {e}")

                # Add cycle errors to log
                if cycle_errors:
                    error_log.extend(cycle_errors)
                    print(f"\n⚠️ Cycle {cycle + 1} had {len(cycle_errors)} error(s)")

                # Check for completion
                if new_orders == 0 and processed == 0 and retried == 0:
                    try:
                        github_status = self.github_bridge.get_work_status()
                        if (
                            github_status["claimed"] == 0
                            and github_status["available"] == 0
                        ):
                            print("🎉 All work completed!")
                            break
                        elif cycle > 20:
                            print(
                                f"💤 No activity - waiting 2 minutes before cycle {cycle + 2}..."
                            )
                            time.sleep(120)
                            continue
                    except Exception as e:
                        print(f"🚨 ERROR checking completion status: {e}")

                # Wait between cycles
                if new_orders > 0 or processed > 0:
                    wait_time = 30
                else:
                    wait_time = 60

                cycle_duration = time.time() - cycle_start
                if cycle_duration < wait_time:
                    remaining_wait = wait_time - cycle_duration
                    if cycle < 3 or remaining_wait > 30:
                        print(f"⏱️ Waiting {remaining_wait:.0f}s before next cycle...")
                    time.sleep(remaining_wait)

            except Exception as e:
                # Catch-all for unexpected cycle errors
                error_msg = f"Cycle {cycle + 1} crashed: {str(e)}"
                error_log.append(error_msg)
                print(f"\n💥 CRITICAL ERROR in cycle {cycle + 1}: {e}")
                print("🔄 Attempting to continue...")
                time.sleep(30)  # Brief pause before retry

        # 🆕 ARCHIVE: Final summary with archive statistics
        print("\n🏁 Host work cycle finished")
        print(
            f"📊 Final summary: {total_new_orders} orders created, {total_processed} completed"
        )

        # Show final archive statistics
        try:
            if hasattr(self, "get_archive_statistics"):
                print("\n📦 Final archive statistics:")
                final_stats = self.get_archive_statistics()
            elif hasattr(self.github_bridge, "get_archive_statistics"):
                print("\n📦 Final archive statistics:")
                stats = self.github_bridge.get_archive_statistics()
                print(f"   Total archived: {stats.get('total_archived', 0):,} items")
                print(f"   Archive size: {stats.get('total_size_mb', 0):.2f} MB")
        except Exception as e:
            print(f"⚠️ Could not get final archive stats: {e}")

        if error_log:
            print(f"\n⚠️ ERROR SUMMARY ({len(error_log)} total errors):")
            for i, error in enumerate(error_log[-10:], 1):  # Show last 10 errors
                print(f"   {i}. {error}")
            print("\n💡 Consider investigating if errors persist or increase")
        else:
            print("✅ No errors encountered during execution")

    def _create_work_orders_quiet(self) -> int:
        """Quiet version with better error handling."""
        try:
            # Pull latest state
            self.github_bridge._git_pull()

            # Build existing work set
            existing_work = set()
            for folder_name in ["available", "claimed", "completed", "failed"]:
                folder_path = self.github_bridge.folders[folder_name]
                for work_file in folder_path.glob("comp_*.json"):
                    try:
                        with open(work_file, "r") as f:
                            work_data = json.load(f)
                            competition_id = work_data.get("competition_id")
                            if competition_id:
                                existing_work.add(competition_id)
                    except Exception as e:
                        print(f"\n⚠️ Error reading work file {work_file}: {e}")
                        continue

            # Get competitions
            competitions = self.club_orchestrator.get_non_cup_competitions()
            competitions = self.club_orchestrator._filter_excluded_competitions(
                competitions
            )

            work_orders_created = 0
            creation_errors = 0

            for competition in competitions:
                competition_id = competition["competition_id"]

                # Skip if already exists or completed
                if (
                    competition_id in existing_work
                    or self.progress_monitor.is_competition_completed(competition_id)
                ):
                    continue

                try:
                    completed_seasons = self._get_completed_seasons(competition_id)
                    work_id = self.github_bridge.create_competition_work_order(
                        competition, completed_seasons
                    )
                    existing_work.add(competition_id)
                    work_orders_created += 1
                    print(f"\n📋 Created: {work_id} for {competition_id}")

                except Exception as e:
                    creation_errors += 1
                    print(f"\n❌ Failed creating work order for {competition_id}: {e}")
                    # Continue with other competitions
                    continue

            if creation_errors > 0:
                print(f"\n⚠️ {creation_errors} work order creation errors")

            return work_orders_created

        except Exception as e:
            print(f"\n💥 CRITICAL ERROR in work order creation: {e}")
            raise  # Re-raise to be caught by caller

    def cleanup(self):
        """Cleanup resources."""
        if hasattr(self, "progress_monitor"):
            self.progress_monitor.db_service.cleanup()
        if hasattr(self, "club_orchestrator"):
            self.club_orchestrator.cleanup()


def main():
    """Enhanced main function with archive management options."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Host Work Manager with Archive Support"
    )
    parser.add_argument("--repo-url", help="GitHub repository URL")
    parser.add_argument(
        "--environment", default="production", help="Database environment"
    )
    parser.add_argument(
        "--max-cycles", type=int, default=1500, help="Maximum cycles to run"
    )
    parser.add_argument("--archive-path", help="Custom archive path")

    # Archive management options
    parser.add_argument(
        "--archive-stats", action="store_true", help="Show archive statistics and exit"
    )
    parser.add_argument(
        "--archive-cleanup",
        type=int,
        help="Clean up archives older than N days and exit",
    )
    parser.add_argument(
        "--archive-search", help="Search archives by competition_id and exit"
    )

    args = parser.parse_args()

    try:
        manager = HostWorkManager(
            environment=args.environment,
            repo_url=args.repo_url,
            archive_path=args.archive_path,
        )

        # Handle archive management commands
        if args.archive_stats:
            manager.get_archive_statistics()
            return

        if args.archive_cleanup:
            manager.cleanup_old_archives(days_to_keep=args.archive_cleanup)
            return

        if args.archive_search:
            manager.search_archived_work(competition_id=args.archive_search)
            return

        # Run normal cycle
        manager.run_host_cycle(args.max_cycles)

    except KeyboardInterrupt:
        print("\n⏹️ Interrupted by user")
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        if "manager" in locals():
            manager.cleanup()


if __name__ == "__main__":
    main()
