# match_worker_processor.py
"""
FINAL VERSION: Match Worker
Processes match work orders using temp storage for memory efficiency.
"""

import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, TypedDict

from selenium.webdriver.remote.webdriver import WebDriver

from configurations import ConfigFactory
from coordination.github_bridge import GitHubWorkBridge
from pipelines.princpal_orchestrator.match_orchestrator import MatchOrchestrator


class SleepPreventer:
    """Prevents macOS system sleep."""

    def __init__(self):
        self.caffeinate_process: Optional[subprocess.Popen] = None
        self.is_active = False

    def start_prevention(self):
        try:
            self.caffeinate_process = subprocess.Popen(
                ["caffeinate", "-d", "-i", "-s"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            self.is_active = True
            print("☕ Sleep prevention activated")
        except FileNotFoundError:
            print("⚠️ caffeinate not found")
        except Exception as e:
            print(f"⚠️ Sleep prevention failed: {e}")

    def stop_prevention(self):
        if self.caffeinate_process and self.is_active:
            try:
                self.caffeinate_process.terminate()
                self.caffeinate_process.wait(timeout=5)
                print("☕ Sleep prevention deactivated")
            except subprocess.TimeoutExpired:
                self.caffeinate_process.kill()
            except Exception:
                pass
            finally:
                self.is_active = False
                self.caffeinate_process = None

    def __enter__(self):
        self.start_prevention()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop_prevention()


# ----------------------------------------------
# TypedDicts for work orders and results
# ----------------------------------------------
class WorkOrder(TypedDict):
    competition_id: str  # IDs often have letters & digits
    season_year: int  # numeric year for arithmetic
    work_id: str  # unique identifier, treated as string
    created_at: str  # ISO8601 timestamp, parsed later


class MatchdayData(TypedDict, total=False):
    matches: List[Dict[str, Any]]  # each match is an arbitrary dict


class CompetitionResult(TypedDict):
    competition_id: str
    season_year: int
    matchdays_data: List[MatchdayData]
    total_matches: int
    total_matchdays: int
    execution_time_seconds: float


class MatchDistributedWorker:
    """Worker that processes match data work orders with memory-efficient temp storage."""

    def __init__(self, repo_url: str, environment: str = "production"):
        self.repo_url = repo_url
        self.environment = environment
        self.worker_id = (
            f"match_{socket.gethostname()}_{os.getpid()}_{uuid.uuid4().hex[:8]}"
        )
        self.github_bridge = GitHubWorkBridge(repo_url=repo_url)
        self.sleep_preventer = SleepPreventer()

        print(f"🤖 Match worker {self.worker_id} initialized")

    # ----------------------------------------------
    # Main processing method
    # ----------------------------------------------
    def process_match_work_order(
        self, work_order: WorkOrder, driver: WebDriver
    ) -> CompetitionResult:
        """
        Doctoring
        Process match work order with temp storage for memory efficiency.
        """
        # ----------------------------------------------
        # Extract and validate input
        # ----------------------------------------------
        # ***> competition_id: str since it may include non‐numeric parts <***
        competition_id: str = work_order["competition_id"]
        # ***> season_year: int to allow potential arithmetic <***
        season_year: int = work_order["season_year"]
        print(f"🎯 Processing: {competition_id} {season_year}")

        # ----------------------------------------------
        # Prepare temporary storage
        # ----------------------------------------------
        # ***> temp_dir: Path for filesystem operations <***
        temp_dir: Path = Path(f"./temp_match_data/{work_order['work_id']}")
        temp_dir.mkdir(parents=True, exist_ok=True)

        try:
            # ------------------------------------------
            # Build environment‐specific config
            # ------------------------------------------
            # ***> config: Config from factory methods <***
            if self.environment == "production":
                config: Config = ConfigFactory.production()
            elif self.environment == "testing":
                config: Config = ConfigFactory.testing()
            else:
                config: Config = ConfigFactory.development()

            config._environment = self.environment
            config.use_vpn = True
            config.save_to_database = False  # workers must not write to DB

            # ------------------------------------------
            # Initialize orchestrator
            # ------------------------------------------
            # ***> orchestrator: MatchOrchestrator for processing logic <***
            orchestrator: MatchOrchestrator = MatchOrchestrator(config=config)
            orchestrator._initialize_components()

            try:
                # --------------------------------------
                # Core processing with temp storage
                # --------------------------------------
                # ***> all_matchdays_data: List[MatchdayData] from helper <***
                all_matchdays_data: List[MatchdayData] = (
                    self._process_competition_with_temp_storage(
                        orchestrator, driver, work_order, temp_dir
                    )
                )

                # Compute summary stats
                # ***> total_matches: int via sum of list lengths <***
                total_matches: int = sum(
                    len(md.get("matches", [])) for md in all_matchdays_data
                )
                # ***> total_matchdays: int count of items in list <***
                total_matchdays: int = len(all_matchdays_data)

                # ***> execution_time_seconds: float difference in seconds <***
                created_dt: datetime = datetime.fromisoformat(
                    work_order["created_at"].replace("Z", "+00:00")
                )
                execution_time_seconds: float = (
                    datetime.now() - created_dt
                ).total_seconds()

                return CompetitionResult(
                    {
                        "competition_id": competition_id,
                        "season_year": season_year,
                        "matchdays_data": all_matchdays_data,
                        "total_matches": total_matches,
                        "total_matchdays": total_matchdays,
                        "execution_time_seconds": execution_time_seconds,
                    }
                )

            finally:
                orchestrator.cleanup()

        except Exception as e:
            print(f"❌ Error processing {competition_id}: {e}")
            raise

        finally:
            # ------------------------------------------
            # Clean up temp storage
            # ------------------------------------------
            if temp_dir.exists():
                shutil.rmtree(temp_dir)
                print(f"🧹 Cleaned temp storage: {work_order['work_id']}")

    def _process_competition_with_temp_storage(
        self, orchestrator, driver, work_order, temp_dir
    ):
        """Process competition using MatchOrchestrator with temp file storage."""
        competition = {
            "competition_id": work_order["competition_id"],
            "competition_code": work_order["competition_code"],
            "competition_name": work_order["competition_name"],
            "season_year": work_order["season_year"],
        }

        # Discover and process matchdays with smarter termination
        consecutive_empty = 0
        max_empty = 5  # Allow 5 consecutive empty matchdays for recent seasons
        max_total = 50  # Reduce from 100 for testing
        processed = 0
        next_matchday = 1

        # Adjust termination logic based on season year
        season_int = int(work_order["season_year"])
        if season_int >= 2025:
            max_empty = 2  # 2025+ seasons might not have data yet
            max_total = 10  # Don't waste time on future seasons
        elif season_int >= 2023:
            max_empty = 5  # Recent seasons should have data
            max_total = 40
        else:
            max_empty = 3  # Older seasons, standard logic
            max_total = 50

        while processed < max_total and consecutive_empty < max_empty:
            if not orchestrator.should_continue_scraping:
                break

            try:
                # Build URL for this matchday
                url = f"https://www.transfermarkt.com/{work_order['competition_code']}/spieltag/wettbewerb/{work_order['competition_id']}/saison_id/{work_order['season_year']}/spieltag/{next_matchday}"

                print(f"📅 Processing matchday {next_matchday}")

                # Process single matchday using worker-specific method
                result = self._extract_and_save_matchday_to_temp(
                    orchestrator, driver, url, next_matchday, work_order, temp_dir
                )

                if result == "empty_matchday":
                    consecutive_empty += 1
                    print(
                        f"📭 Empty matchday {next_matchday} ({consecutive_empty}/{max_empty})"
                    )

                    # Smart termination: if it's 2025, expect empty data
                    if season_int >= 2025 and consecutive_empty >= 2:
                        print(
                            f"🛑 Early stop for {season_int}: likely no data available yet"
                        )
                        break

                elif result == "success":
                    consecutive_empty = 0
                    print(f"✅ Saved matchday {next_matchday} to temp storage")

                next_matchday += 1
                processed += 1

                # VPN timing
                orchestrator.vpn_handler.handle_request_timing("matchday processing...")

            except Exception as e:
                print(f"❌ Error processing matchday {next_matchday}: {e}")
                consecutive_empty += 1
                if consecutive_empty >= max_empty:
                    break
                next_matchday += 1
                continue

        # Load all temp files and return complete results
        return self._load_all_temp_files(temp_dir)

    def _extract_and_save_matchday_to_temp(
        self, orchestrator, driver, url, matchday_num, work_order, temp_dir
    ):
        """Extract matchday data and save to temp file (worker-specific method)."""
        try:
            # Use worker-specific extraction method (no database saving)
            matchday_data = self._extract_matchday_data_only(
                orchestrator,
                driver,
                url,
                matchday_num,
                work_order["season_year"],
                work_order["competition_name"],
                work_order["competition_id"],
            )

            if matchday_data:
                # Save to temp file immediately
                temp_file = temp_dir / f"matchday_{matchday_num}.json"
                with open(temp_file, "w", encoding="utf-8") as f:
                    json.dump(matchday_data, f, ensure_ascii=False, indent=2)
                return "success"
            else:
                return "empty_matchday"

        except Exception as e:
            print(f"❌ Error extracting matchday {matchday_num}: {e}")
            return "error"

    def _extract_matchday_data_only(
        self,
        orchestrator,
        driver,
        url,
        matchday,
        season,
        competition_name,
        competition_id,
    ):
        """
        Worker-specific method: Extract matchday data without saving to database.
        This is Option 2 - simpler approach that doesn't modify existing MatchOrchestrator.
        """
        from bs4 import BeautifulSoup

        try:
            # Navigate to matchday page
            driver.get(url)
            orchestrator.vpn_handler.handle_request_timing("matchday extraction...")

            soup = BeautifulSoup(driver.page_source, "html.parser")

            # Use MatchOrchestrator's extractor to get matchday data
            container = orchestrator.matchday_extractor.extract_matchday(
                soup, matchday, season
            )

            if not container:
                return None

            # Set competition info
            container.matchday_info["competition"] = competition_name

            # Check if meaningful data exists
            matches_count = len(container.matches) if container.matches else 0
            teams_count = (
                len(container.league_table.get("teams", []))
                if container.league_table
                else 0
            )

            if matches_count == 0 and teams_count == 0:
                return None

            # Process matches for detailed data (worker-specific logic)
            processed_matches = []
            if container.matches:
                processed_matches = self._process_matches_for_worker(
                    orchestrator, driver, container.matches, season, competition_name
                )

            # Return extracted data structure
            return {
                "matchday_info": {
                    "number": matchday,
                    "season": season,
                    "competition": competition_name,
                    "source_url": url,
                    "total_matches": matches_count,
                },
                "matches": processed_matches,
                "league_table": container.league_table,
                "top_scorers": container.top_scorers,
                "summary": (
                    container.matchday_summary.get("current_matchday", {})
                    if container.matchday_summary
                    else {}
                ),
                "metadata": {
                    "extraction_time": container.metadata.get("extraction_time"),
                    "source": "worker_extraction",
                    "total_matches": matches_count,
                },
            }

        except Exception as e:
            print(f"❌ Error in worker extraction for matchday {matchday}: {e}")
            return None

    def _process_matches_for_worker(
        self, orchestrator, driver, matches, season, competition_name
    ):
        """Process match details for worker (extract without saving to DB)."""
        processed_matches = []

        print(f"🎮 Processing {len(matches)} matches for worker...")

        for i, match in enumerate(matches, 1):
            if not match.match_report_url:
                print(f"⚠️ Match {i}: No report URL, skipping")
                continue

            try:
                print(f"🎯 Extracting match {i}/{len(matches)}: {match.match_id}")

                # Get detailed match data using MatchOrchestrator's extractor
                driver.get(match.match_report_url)
                orchestrator.vpn_handler.handle_request_timing("match extraction...")

                from bs4 import BeautifulSoup

                soup = BeautifulSoup(driver.page_source, "html.parser")

                # Use MatchOrchestrator's match extractor
                detail = orchestrator.match_extractor.extract_from_url(
                    soup, match.match_report_url
                )

                # Convert to dictionary for JSON serialization
                match_data = {
                    "match_info": {
                        "match_id": detail.match_info.match_id,
                        "competition_name": competition_name,
                        "season": season,
                        "date": detail.match_info.date,
                        "time": detail.match_info.time,
                        "venue": detail.match_info.venue,
                        "attendance": detail.match_info.attendance,
                        "referee": detail.match_info.referee,
                        "match_report_url": match.match_report_url,
                        "day_of_week": match.day_of_week,
                    },
                    "home_team": {
                        "team_id": detail.home_team.team_id,
                        "name": detail.home_team.name,
                        "formation": detail.home_team.formation,
                        "manager": detail.home_team.manager,
                    },
                    "away_team": {
                        "team_id": detail.away_team.team_id,
                        "name": detail.away_team.name,
                        "formation": detail.away_team.formation,
                        "manager": detail.away_team.manager,
                    },
                    "score": {
                        "home_final": detail.score.home_final,
                        "away_final": detail.score.away_final,
                        "home_ht": detail.score.home_ht,
                        "away_ht": detail.score.away_ht,
                    },
                    "lineups": {
                        "home_starting": [
                            self._player_to_dict(p) for p in detail.home_lineup
                        ],
                        "away_starting": [
                            self._player_to_dict(p) for p in detail.away_lineup
                        ],
                        "home_subs": [
                            self._player_to_dict(p) for p in detail.home_substitutes
                        ],
                        "away_subs": [
                            self._player_to_dict(p) for p in detail.away_substitutes
                        ],
                    },
                    "events": {
                        "goals": [self._goal_to_dict(g) for g in detail.goals],
                        "cards": [self._card_to_dict(c) for c in detail.cards],
                        "substitutions": [
                            self._substitution_to_dict(s) for s in detail.substitutions
                        ],
                    },
                }

                processed_matches.append(match_data)
                print(f"✅ Extracted match {match.match_id}")

            except Exception as e:
                print(f"❌ Error extracting match {match.match_id}: {e}")
                continue

        return processed_matches

    def _player_to_dict(self, player):
        """Convert Player object to dictionary."""
        if not player:
            return None
        return {
            "player_id": player.player_id,
            "name": player.name,
            "shirt_number": player.shirt_number,
            "position": player.position,
            "is_captain": getattr(player, "is_captain", False),
            "portrait_url": getattr(player, "portrait_url", None),
        }

    def _goal_to_dict(self, goal):
        """Convert Goal object to dictionary."""
        return {
            "minute": goal.minute,
            "extra_time": goal.extra_time,
            "player": self._player_to_dict(goal.player),
            "assist_player": self._player_to_dict(goal.assist_player),
            "goal_type": goal.goal_type,
            "assist_type": goal.assist_type,
            "team_side": goal.team_side,
            "score_after": goal.score_after,
            "season_goal_number": goal.season_goal_number,
            "season_assist_number": goal.season_assist_number,
        }

    def _card_to_dict(self, card):
        """Convert Card object to dictionary."""
        return {
            "minute": card.minute,
            "extra_time": card.extra_time,
            "player": self._player_to_dict(card.player),
            "card_type": card.card_type,
            "reason": card.reason,
            "team_side": card.team_side,
            "season_card_number": card.season_card_number,
        }

    def _substitution_to_dict(self, substitution):
        """Convert Substitution object to dictionary."""
        return {
            "minute": substitution.minute,
            "extra_time": substitution.extra_time,
            "player_out": self._player_to_dict(substitution.player_out),
            "player_in": self._player_to_dict(substitution.player_in),
            "reason": substitution.reason,
            "team_side": substitution.team_side,
        }

    def _load_all_temp_files(self, temp_dir):
        """Load all temp matchday files into final results."""
        all_matchdays = []

        for temp_file in sorted(temp_dir.glob("matchday_*.json")):
            try:
                with open(temp_file, "r") as f:
                    matchday_data = json.load(f)
                    all_matchdays.append(matchday_data)
            except Exception as e:
                print(f"⚠️ Error loading temp file {temp_file}: {e}")
                continue

        return all_matchdays

    def _setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown."""

        def signal_handler(signum, frame):
            print(f"\n⚠️ Received signal {signum}, shutting down...")
            self.shutdown()
            sys.exit(0)

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    def shutdown(self):
        """Graceful shutdown."""
        print("🛑 Shutting down match worker...")
        self.sleep_preventer.stop_prevention()

    def run_match_worker_cycle(
        self,
        max_work_orders: int = 10,
        max_consecutive_failures: int = 15,
        max_idle_hours: float = 2.0,
    ):
        """Run match worker cycle."""
        print("🚀 Starting match worker cycle")
        print(f"📋 Max work orders: {max_work_orders}")
        print(
            f"🛑 Stop after {max_consecutive_failures} failures or {max_idle_hours}h idle"
        )

        self._setup_signal_handlers()

        processed_count = 0
        driver = None
        consecutive_failures = 0
        start_time = time.time()
        last_success_time = time.time()

        with self.sleep_preventer:
            try:
                driver = webdriver.Chrome()
                print("🌐 WebDriver initialized")

                while processed_count < max_work_orders:
                    print(
                        f"\n🔍 Looking for match work... ({processed_count}/{max_work_orders})"
                    )

                    # Check stop conditions
                    idle_hours = (time.time() - last_success_time) / 3600

                    if consecutive_failures >= max_consecutive_failures:
                        print(
                            f"🛑 STOPPING: {max_consecutive_failures} consecutive failures"
                        )
                        break
                    elif idle_hours >= max_idle_hours:
                        print(f"🛑 STOPPING: {idle_hours:.1f}h idle time")
                        break

                    # Try to claim match work
                    work_order = self.github_bridge.claim_available_work(self.worker_id)

                    # Filter for match work only
                    if work_order and work_order.get("work_type") != "match_data":
                        print(
                            f"⏭️ Skipping non-match work: {work_order.get('work_type')}"
                        )
                        continue

                    if not work_order:
                        consecutive_failures += 1
                        wait_time = min(
                            30 * (1.5 ** min(consecutive_failures - 1, 5)), 300
                        )
                        print(f"😴 No match work, waiting {wait_time:.1f}s...")
                        time.sleep(wait_time)
                        continue

                    # Process the work
                    consecutive_failures = 0
                    last_success_time = time.time()
                    print(f"✅ Claimed: {work_order['work_id']}")

                    try:
                        results = self.process_match_work_order(work_order, driver)
                        self.github_bridge.submit_completed_work(work_order, results)
                        processed_count += 1

                        elapsed_hours = (time.time() - start_time) / 3600
                        rate = (
                            processed_count / elapsed_hours if elapsed_hours > 0 else 0
                        )
                        print(f"🎉 Completed: {work_order['work_id']}")
                        print(
                            f"📈 Progress: {processed_count}/{max_work_orders} ({rate:.1f} jobs/hour)"
                        )

                    except Exception as e:
                        error_msg = f"Processing error: {str(e)}"
                        self.github_bridge.submit_failed_work(work_order, error_msg)
                        print(f"❌ Failed: {work_order['work_id']} - {error_msg}")
                        continue

                    time.sleep(10)  # Brief pause between work orders

                # Final summary
                elapsed_hours = (time.time() - start_time) / 3600
                rate = processed_count / elapsed_hours if elapsed_hours > 0 else 0

                print(f"\n🏁 Match worker finished")
                print(f"📊 Completed: {processed_count} competitions")
                print(f"⏱️ Runtime: {elapsed_hours:.1f} hours")
                print(f"📈 Rate: {rate:.1f} competitions/hour")

            except KeyboardInterrupt:
                print(f"\n⏹️ Worker interrupted")
            except Exception as e:
                print(f"❌ Worker error: {e}")
            finally:
                if driver:
                    driver.quit()
                    print("🌐 WebDriver closed")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Match Distributed Worker")
    parser.add_argument("--repo-url", required=True, help="GitHub repository URL")
    parser.add_argument("--environment", default="production", help="Environment")
    parser.add_argument("--max-work", type=int, default=10, help="Max work orders")
    parser.add_argument(
        "--max-failures", type=int, default=15, help="Max consecutive failures"
    )
    parser.add_argument(
        "--max-idle-hours", type=float, default=2.0, help="Max idle hours"
    )

    args = parser.parse_args()

    try:
        worker = MatchDistributedWorker(
            repo_url=args.repo_url, environment=args.environment
        )
        worker.run_match_worker_cycle(
            max_work_orders=args.max_work,
            max_consecutive_failures=args.max_failures,
            max_idle_hours=args.max_idle_hours,
        )
    except KeyboardInterrupt:
        print("\n⏹️ Interrupted")
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        print("👋 Match worker shutdown complete")
