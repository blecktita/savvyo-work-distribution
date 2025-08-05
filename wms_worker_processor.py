# wms_worker_processor.py
"""
Worker machine main script - now uses existing ClubOrchestrator logic.
"""

import os
import signal
import socket
import subprocess
import sys
import time
import uuid
from datetime import datetime
from typing import Dict, Optional

from selenium import webdriver

from configurations import ConfigFactory
from coordination.coordinator import InMemoryProgressTracker
from coordination.github_bridge import GitHubWorkBridge
from pipelines.princpal_orchestrator import ClubOrchestrator


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
            self.caffeinate_process = subprocess.Popen(
                ["caffeinate", "-d", "-i", "-s"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )

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
        competition_id = work_order["competition_id"]

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
                config=config, progress_tracker=progress_tracker
            )
            orchestrator._initialize_components()

            try:
                # Use the existing method but for specific competition
                competition_data = {
                    "competition_id": competition_id,
                    "competition_url": work_order["competition_url"],
                }

                # Call existing scraping method (this populates the progress_tracker)
                _ = orchestrator._scrape_competition_club_data(driver, competition_data)
                processed_seasons = progress_tracker.get_processed_seasons()
                all_club_data = progress_tracker.get_all_club_data()
                total_clubs = len(all_club_data)

                print(
                    f"✅ Completed {competition_id}: {total_clubs} clubs, {len(processed_seasons)} seasons"
                )

                return {
                    "seasons_processed": processed_seasons,
                    "club_data": all_club_data,
                    "total_clubs_scraped": total_clubs,
                    "execution_time_seconds": (
                        datetime.now()
                        - datetime.fromisoformat(work_order["created_at"])
                    ).total_seconds(),
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

        signal.signal(signal.SIGINT, signal_handler)  # Ctrl+C
        signal.signal(signal.SIGTERM, signal_handler)  # Termination signal

    def shutdown(self):
        """
        Graceful shutdown
        """
        print("🛑 Shutting down worker...")
        self.sleep_preventer.stop_prevention()

    def run_worker_cycle(
        self,
        max_work_orders: int = 50,
        max_consecutive_failures: int = 20,
        max_idle_hours: float = 2.0,
    ):
        """
        Run worker cycle with comprehensive auto-stop mechanisms.

        Args:
            max_work_orders: Maximum work orders to process before stopping
            max_consecutive_failures: Stop after this many consecutive failures (default: 20)
            max_idle_hours: Stop after this many hours with no successful work (default: 2.0)
        """
        print("🚀 Starting worker cycle")
        print(f"   📋 Max work orders: {max_work_orders}")
        print("   🛑 Auto-stop conditions:")
        print(f"      • After {max_consecutive_failures} consecutive failures")
        print(f"      • After {max_idle_hours} hours with no successful work")

        # Setup signal handlers
        self._setup_signal_handlers()

        processed_count = 0
        driver = None
        consecutive_failures = 0
        base_wait_time = 30
        start_time = time.time()
        last_success_time = (
            time.time()
        )  # Track when we last successfully processed work

        with self.sleep_preventer:
            try:
                # Initialize webdriver
                driver = webdriver.Chrome()
                print("🌐 WebDriver initialized")

                while processed_count < max_work_orders:
                    print(
                        f"\n🔍 Looking for work... ({processed_count}/{max_work_orders} completed)"
                    )

                    # Check BOTH auto-stop conditions BEFORE attempting work
                    idle_hours = (time.time() - last_success_time) / 3600

                    if consecutive_failures >= max_consecutive_failures:
                        elapsed_hours = (time.time() - start_time) / 3600
                        print(
                            f"\n🛑 STOPPING: Reached {max_consecutive_failures} consecutive failures"
                        )
                        print(f"   ⏱️ Total runtime: {elapsed_hours:.1f} hours")
                        print(f"   ⏳ Time since last success: {idle_hours:.1f} hours")
                        print(f"   📊 Work completed: {processed_count}")
                        print("   💡 Reason: Consecutive failure limit reached")
                        break

                    elif idle_hours >= max_idle_hours:
                        elapsed_hours = (time.time() - start_time) / 3600
                        print(
                            f"\n🛑 STOPPING: {idle_hours:.1f} hours with no successful work"
                        )
                        print(f"   ⏱️ Total runtime: {elapsed_hours:.1f} hours")
                        print(f"   📊 Work completed: {processed_count}")
                        print(f"   🔄 Consecutive failures: {consecutive_failures}")
                        print("   💡 Reason: Idle time limit reached")
                        break

                    # Try to claim work
                    work_order = self.github_bridge.claim_available_work(self.worker_id)

                    if not work_order:
                        consecutive_failures += 1

                        # Calculate wait time with exponential backoff
                        wait_time = min(
                            base_wait_time * (1.5 ** min(consecutive_failures - 1, 5)),
                            300,
                        )

                        # Enhanced logging with both progress indicators
                        remaining_failure_attempts = (
                            max_consecutive_failures - consecutive_failures
                        )
                        remaining_idle_time = max_idle_hours - idle_hours

                        print(
                            f"😴 No work available, waiting {wait_time:.1f} seconds..."
                        )
                        print(
                            f"   📊 Consecutive failures: {consecutive_failures}/{max_consecutive_failures} ({remaining_failure_attempts} left)"
                        )
                        print(
                            f"   ⏳ Idle time: {idle_hours:.1f}h/{max_idle_hours}h ({remaining_idle_time:.1f}h left)"
                        )

                        # Determine which limit will be hit first
                        if remaining_failure_attempts <= 3:
                            print("   ⚠️ Approaching failure limit!")
                        elif remaining_idle_time <= 0.5:  # 30 minutes
                            print("   ⚠️ Approaching idle time limit!")

                        # System health checks every 5 failures
                        if consecutive_failures % 5 == 0:
                            elapsed_hours = (time.time() - start_time) / 3600
                            print(
                                f"   💾 System check - Runtime: {elapsed_hours:.1f}h, Sleep prevention: {self.sleep_preventer.is_active}"
                            )

                            # Optional: Check repository status
                            try:
                                work_status = self.github_bridge.get_work_status()
                                total_available = sum(work_status.values())
                                print(
                                    f"   📋 Queue status: {total_available} total items - {work_status}"
                                )

                                if total_available == 0:
                                    print(
                                        "   💡 No work in any queue - consider stopping manually if this persists"
                                    )
                            except Exception as e:
                                print(f"   ⚠️ Could not check queue status: {e}")

                        time.sleep(wait_time)
                        continue

                    # SUCCESS! Reset both counters
                    consecutive_failures = 0
                    last_success_time = time.time()  # Update success timestamp
                    print(f"✅ Claimed work: {work_order['work_id']}")

                    try:
                        # Process the work order
                        results = self.process_work_order(work_order, driver)

                        # Submit completed work
                        self.github_bridge.submit_completed_work(work_order, results)

                        processed_count += 1
                        elapsed_hours = (time.time() - start_time) / 3600
                        rate = (
                            processed_count / elapsed_hours if elapsed_hours > 0 else 0
                        )

                        print(f"🎉 Completed work: {work_order['work_id']}")
                        print(
                            f"   📈 Progress: {processed_count}/{max_work_orders} ({rate:.1f} jobs/hour)"
                        )
                        print("   ✨ Consecutive failures reset to 0")

                    except Exception as e:
                        # Submit failed work (this doesn't count as "no work available")
                        error_msg = f"Processing error: {str(e)}"
                        self.github_bridge.submit_failed_work(work_order, error_msg)
                        print(f"❌ Failed work: {work_order['work_id']} - {error_msg}")
                        print(
                            "   💡 Note: Processing failures don't count toward consecutive failure limit"
                        )
                        continue

                    # Brief pause between work orders
                    time.sleep(10)

                # Final comprehensive summary
                elapsed_hours = (time.time() - start_time) / 3600
                idle_hours_final = (time.time() - last_success_time) / 3600
                rate = processed_count / elapsed_hours if elapsed_hours > 0 else 0

                print("\n" + "=" * 60)

                if processed_count >= max_work_orders:
                    print("🏁 Worker cycle completed successfully!")
                    print(f"   🎯 Reached target: {max_work_orders} work orders")
                else:
                    print("🛑 Worker cycle stopped early")
                    if consecutive_failures >= max_consecutive_failures:
                        print(
                            f"   📉 Reason: Consecutive failure limit ({max_consecutive_failures}) reached"
                        )
                    elif idle_hours_final >= max_idle_hours:
                        print(
                            f"   ⏳ Reason: Idle time limit ({max_idle_hours}h) reached"
                        )

                print("\n📊 Final Statistics:")
                print(f"   • Work completed: {processed_count}")
                print(f"   • Total runtime: {elapsed_hours:.1f} hours")
                print(f"   • Time since last success: {idle_hours_final:.1f} hours")
                print(f"   • Average completion rate: {rate:.1f} jobs/hour")
                print(f"   • Final consecutive failures: {consecutive_failures}")

                # Efficiency analysis
                if processed_count > 0:
                    efficiency = (processed_count / max_work_orders) * 100
                    print(f"   • Efficiency: {efficiency:.1f}% of target completed")

                # Recommendations for next run
                print("\n💡 Recommendations:")
                if consecutive_failures >= max_consecutive_failures:
                    print("   • Check if more work has been added to the queue")
                    print(
                        "   • Consider increasing max_consecutive_failures if work is expected"
                    )
                    print("   • Verify network connectivity and repository access")
                elif idle_hours_final >= max_idle_hours:
                    print(
                        "   • Consider increasing max_idle_hours if work comes in batches"
                    )
                    print("   • Check queue status before restarting")
                elif processed_count >= max_work_orders:
                    print(
                        "   • Worker completed successfully - can restart for more work"
                    )

                print("=" * 60)

            except KeyboardInterrupt:
                elapsed_hours = (time.time() - start_time) / 3600
                print(f"\n⏹️ Worker interrupted by user after {elapsed_hours:.1f} hours")
                print(f"📊 Work completed before interruption: {processed_count}")
            except Exception as e:
                elapsed_hours = (time.time() - start_time) / 3600
                print(f"❌ Worker error after {elapsed_hours:.1f} hours: {e}")
                print(f"📊 Work completed before error: {processed_count}")
            finally:
                if driver:
                    driver.quit()
                    print("🌐 WebDriver closed")
                print("💤 Sleep prevention deactivated")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Distributed Worker")
    parser.add_argument("--repo-url", required=True, help="GitHub repository URL")
    parser.add_argument(
        "--environment", default="production", help="Configuration environment"
    )
    parser.add_argument(
        "--max-work", type=int, default=50, help="Maximum work orders to process"
    )

    # NEW: Add parameters for the enhanced auto-stop functionality
    parser.add_argument(
        "--max-failures",
        type=int,
        default=20,
        help="Stop after this many consecutive failures (default: 20)",
    )
    parser.add_argument(
        "--max-idle-hours",
        type=float,
        default=2.0,
        help="Stop after this many hours with no successful work (default: 2.0)",
    )

    args = parser.parse_args()

    try:
        worker = DistributedWorker(repo_url=args.repo_url, environment=args.environment)

        # Enhanced call with all parameters
        worker.run_worker_cycle(
            max_work_orders=args.max_work,
            max_consecutive_failures=args.max_failures,
            max_idle_hours=args.max_idle_hours,
        )

    except KeyboardInterrupt:
        print("\n⏹️ Interrupted by user")
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()  # Show full error details for debugging
    finally:
        print("👋 Worker shutdown complete")
