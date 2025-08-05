# pipelines/princpal_orchestrator/club_orchestrator.py
"""
Club-specific orchestrator for scraping operations.
Handles club data extraction across seasons while preserving logic.
"""

import time
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd
from bs4 import BeautifulSoup
from sqlalchemy import text

from configurations import ScraperConfig
from coordination import (
    SmartSeasonTerminator,
    create_season_tester,
    create_work_tracker,
)
from database import TaskStatus, create_database_service
from exceptions import (
    ConfigurationError,
    DatabaseOperationError,
    IPSecurityViolationError,
    VpnRequiredError,
)
from extractors import ClubTableParser
from vpn_controls import VpnProtectionHandler

from .base_orchestrator import BaseOrchestrator
from .orchestrator_config import OrchestratorConfig


class ClubOrchestrator(BaseOrchestrator):
    """
    Orchestrates club data scraping across all seasons for competitions.
    Added smart termination capability
    """

    def __init__(
        self,
        config: Optional[ScraperConfig] = None,
        env_file_path: Optional[str] = None,
        progress_tracker=None,
    ):
        """
        Initialize club scraping orchestrator.

        Args:
            config: Scraper configuration (uses development if None)
            env_file_path: Path to environment file

        Raises:
            VpnRequiredError: If VPN is required but not available
            ConfigurationError: If configuration is invalid
        """
        self._skip_base_vpn_handler = True
        super().__init__(config)

        # ***> Set environment file path with default <***
        self.env_file_path = env_file_path or OrchestratorConfig.DEFAULT_ENV_PATH

        # ***> Initialize scraping control flag <***
        self.should_continue_scraping = True

        self._custom_progress_tracker = progress_tracker

        # ***> Initialize components flag to prevent recursion <***
        self._components_initialized = False

        # ***> Initialize smart termination <***
        self.smart_terminator = SmartSeasonTerminator()

    def _initialize_components(self) -> None:
        """
        Initialize all orchestrator components.
        """
        if self._components_initialized:
            return

        try:
            # ***> Initialize VPN handler with environment file <***
            self.vpn_handler = VpnProtectionHandler(
                config=self.config, env_file_path=self.env_file_path
            )

            # ***> Initialize club-specific components <***
            self.parser = ClubTableParser()

            # ***> Initialize database manager if enabled <***
            self.database_manager = None
            if self.config.save_to_database:
                self._initialize_club_database_manager()

            # ***> Initialize progress tracker <***
            if self._custom_progress_tracker:
                self.monitor_progress = self._custom_progress_tracker
            else:
                self.monitor_progress = create_work_tracker("production")
            self._components_initialized = True

        except Exception as error:
            raise ConfigurationError("Error initializing components: %s" % error)

    def _initialize_club_database_manager(self) -> None:
        """
        Initialize club database manager with error handling.
        """
        try:
            environment = self._get_environment_setting()
            self.database_manager = create_database_service(environment)
        except DatabaseOperationError as error:
            self._handle_database_initialization_error(error)

    def get_non_cup_competitions(self) -> List[Dict[str, str]]:
        """Get non-cup competitions from database."""

        # NEW: Check for custom tracker FIRST (before any initialization)
        if self._custom_progress_tracker:
            # Worker mode - don't need database competitions
            return []

        # Only initialize components if we actually need database access
        if not self._components_initialized:
            self._initialize_components()

        if not self.database_manager:
            return []

        try:
            competitions = self.database_manager.get_non_cup_competitions()
            return competitions
        except DatabaseOperationError:
            return []

    def _try_claim_competition(self, competition: Dict[str, str]) -> bool:
        """
        Try to claim a competition that needs work (NOT completed).

        Returns:
            True if successfully claimed, False if should skip
        """
        competition_id = competition["competition_id"]
        competition_url = competition["competition_url"]

        # STEP 1: Check if competition is worth claiming
        try:
            with self.monitor_progress.db_service.transaction() as session:
                check_result = session.execute(
                    text(
                        """
                    SELECT status, worker_id FROM competition_progress 
                    WHERE competition_id = :comp_id
                """
                    ),
                    {"comp_id": competition_id},
                )

                existing = check_result.fetchone()

                if existing:
                    status, worker_id = existing

                    # NEVER claim completed competitions
                    if status == TaskStatus.COMPLETED.value:
                        print(f"⏭️ Competition {competition_id} already completed")
                        return False

                    # Don't claim if another worker is actively working on it
                    if (
                        status == TaskStatus.IN_PROGRESS.value
                        and worker_id
                        and worker_id != self.monitor_progress.worker_id
                    ):
                        print(
                            f"⏭️ Competition {competition_id} claimed by another worker"
                        )
                        return False

                    # If we already own it, don't claim again
                    if worker_id == self.monitor_progress.worker_id:
                        print(f"   ℹ️ Already processing {competition_id}")
                        return True

                    # CLAIMABLE: pending, failed, or stuck in_progress without worker
                    print(f"   📋 {competition_id} status: {status} → attempting claim")
                else:
                    # New competition - needs to be initialized
                    print(f"   🆕 New competition {competition_id} → attempting claim")

                session.rollback()  # End check transaction
        except Exception as e:
            print(f"   ⚠️ Status check failed for {competition_id}: {str(e)}")
            return False

        # STEP 2: Attempt to claim the competition
        try:
            with self.monitor_progress.db_service.transaction() as session:
                if self.monitor_progress.db_type == "PostgreSQL":
                    result = session.execute(
                        text(
                            """
                        INSERT INTO competition_progress 
                        (competition_id, competition_url, status, worker_id, started_at)
                        VALUES (:comp_id, :url, :status, :worker, :now)
                        ON CONFLICT (competition_id) DO UPDATE SET
                            status = CASE 
                                WHEN competition_progress.status NOT IN ('completed', 'in_progress')
                                    OR (competition_progress.status = 'in_progress' AND competition_progress.worker_id IS NULL)
                                    OR competition_progress.worker_id = :worker
                                THEN :status
                                ELSE competition_progress.status
                            END,
                            worker_id = CASE
                                WHEN competition_progress.status NOT IN ('completed', 'in_progress')
                                    OR (competition_progress.status = 'in_progress' AND competition_progress.worker_id IS NULL)
                                    OR competition_progress.worker_id = :worker
                                THEN :worker
                                ELSE competition_progress.worker_id
                            END,
                            started_at = CASE
                                WHEN competition_progress.status NOT IN ('completed', 'in_progress')
                                    OR (competition_progress.status = 'in_progress' AND competition_progress.worker_id IS NULL)
                                    OR competition_progress.worker_id = :worker
                                THEN :now
                                ELSE competition_progress.started_at
                            END
                        RETURNING worker_id, status
                    """
                        ),
                        {
                            "comp_id": competition_id,
                            "url": competition_url,
                            "status": TaskStatus.IN_PROGRESS.value,
                            "worker": self.monitor_progress.worker_id,
                            "now": datetime.now(),
                        },
                    )

                    result_row = result.fetchone()
                    if result_row:
                        returned_worker, returned_status = result_row
                        success = (
                            returned_worker == self.monitor_progress.worker_id
                            and returned_status != TaskStatus.COMPLETED.value
                        )
                    else:
                        success = False

                else:  # SQLite
                    # First check if it's claimable
                    availability_check = session.execute(
                        text(
                            """
                        SELECT COUNT(*) FROM competition_progress 
                        WHERE competition_id = :comp_id 
                        AND status = 'completed'
                    """
                        ),
                        {"comp_id": competition_id},
                    )

                    is_completed = availability_check.fetchone()[0] > 0

                    if is_completed:
                        return False  # Don't claim completed competitions

                    # Try to claim it
                    session.execute(
                        text(
                            """
                        INSERT OR REPLACE INTO competition_progress 
                        (competition_id, competition_url, status, worker_id, started_at)
                        VALUES (:comp_id, :url, :status, :worker, :now)
                    """
                        ),
                        {
                            "comp_id": competition_id,
                            "url": competition_url,
                            "status": TaskStatus.IN_PROGRESS.value,
                            "worker": self.monitor_progress.worker_id,
                            "now": datetime.now(),
                        },
                    )

                    success = True

                session.commit()

                if success:
                    print(f"✅ Successfully claimed: {competition_id}")
                else:
                    print(f"❌ Failed to claim: {competition_id}")

                return success

        except Exception as e:
            print(f"❌ Exception claiming {competition_id}: {str(e)}")
            return False

    def scrape_all_club_data(self, driver) -> pd.DataFrame:
        """
        Enhanced: Skip completed competitions entirely.
        """
        if not self._components_initialized:
            self._initialize_components()

        try:
            self.vpn_handler.ensure_vpn_protection()
            self.monitor_progress.recover_stuck_seasons()

            # Get all competitions
            competitions = self.get_non_cup_competitions()
            competitions = self._filter_excluded_competitions(competitions)

            if not competitions:
                return pd.DataFrame()

            # ENHANCED: Pre-filter completed competitions
            available_competitions = self._filter_available_competitions(competitions)

            if not available_competitions:
                print("🎉 All competitions already completed!")
                return pd.DataFrame()

            print(
                f"📊 Found {len(available_competitions)} competitions needing work (out of {len(competitions)} total)"
            )

            # Process competitions one at a time
            processed_count = 0
            max_competitions_per_session = 100

            for competition in available_competitions:
                if processed_count >= max_competitions_per_session:
                    print(
                        f"✅ Reached session limit ({max_competitions_per_session} competitions). Stopping."
                    )
                    break

                if not self.should_continue_scraping:
                    break

                # Try to claim this competition
                claimed = self._try_claim_competition(competition)
                if not claimed:
                    continue

                # Process immediately
                try:
                    print(
                        f"🎯 Processing competition {competition['competition_id']} ({processed_count + 1}/{max_competitions_per_session})"
                    )
                    self._scrape_competition_club_data(driver, competition)
                    processed_count += 1

                    self.vpn_handler.handle_request_timing(
                        "competition processing, please wait..."
                    )

                except KeyboardInterrupt:
                    print(f"⏹️ Interrupted! Releasing {competition['competition_id']}")
                    self._release_competition(competition["competition_id"])
                    break
                except VpnRequiredError:
                    print(f"🚨 VPN issue! Releasing {competition['competition_id']}")
                    self._release_competition(competition["competition_id"])
                    raise
                except Exception as e:
                    print(
                        f"❌ Error processing {competition['competition_id']}: {str(e)}"
                    )
                    continue

            print(f"🏁 Session completed! Processed {processed_count} competitions.")
            return pd.DataFrame()

        except (VpnRequiredError, IPSecurityViolationError):
            raise
        except Exception:
            raise

    def _filter_available_competitions(
        self, competitions: List[Dict[str, str]]
    ) -> List[Dict[str, str]]:
        """
        NEW: Filter out completed competitions before attempting to claim.
        """
        available = []

        try:
            with self.monitor_progress.db_service.transaction() as session:
                for competition in competitions:
                    competition_id = competition["competition_id"]

                    # Check completion status
                    result = session.execute(
                        text(
                            """
                        SELECT status FROM competition_progress 
                        WHERE competition_id = :comp_id
                    """
                        ),
                        {"comp_id": competition_id},
                    )

                    row = result.fetchone()

                    if not row:
                        # New competition - available
                        available.append(competition)
                    elif row[0] != TaskStatus.COMPLETED.value:
                        # Not completed - available
                        available.append(competition)
                    # else: completed - skip

        except Exception as e:
            print(f"⚠️ Error filtering competitions: {str(e)}")
            # If filtering fails, return all (safer)
            return competitions

        return available

    def _release_competition(self, competition_id: str):
        """
        NEW: Release a competition back to the pool.
        """
        try:
            with self.monitor_progress.db_service.transaction() as session:
                session.execute(
                    text(
                        """
                    UPDATE competition_progress 
                    SET status = :pending, worker_id = NULL, started_at = NULL
                    WHERE competition_id = :comp_id AND worker_id = :worker
                """
                    ),
                    {
                        "comp_id": competition_id,
                        "pending": TaskStatus.PENDING.value,
                        "worker": self.monitor_progress.worker_id,
                    },
                )
                session.commit()
                print(f"🔄 Released competition {competition_id} back to pool")
        except Exception as e:
            print(f"⚠️ Failed to release {competition_id}: {str(e)}")

    def _filter_excluded_competitions(
        self, competitions: List[Dict[str, str]]
    ) -> List[Dict[str, str]]:
        """
        Filter out excluded competitions.

        Args:
            competitions: List of all competitions

        Returns:
            Filtered list of competitions
        """
        return [
            comp
            for comp in competitions
            if comp["competition_id"] not in OrchestratorConfig.EXCLUDED_COMPETITION_IDS
        ]

    def _get_pending_competitions(
        self, competitions: List[Dict[str, str]]
    ) -> List[Dict[str, str]]:
        """
        Return all non-excluded competitions (claiming happens individually now).
        """
        return competitions

    def _scrape_competition_club_data(
        self, driver, competition: Dict[str, str]
    ) -> pd.DataFrame:
        """
        DEBUG VERSION: Find out why processing isn't happening
        """
        competition_id = competition["competition_id"]
        competition_url = competition["competition_url"]

        print(f"   🔍 Starting _scrape_competition_club_data for {competition_id}")
        print(f"   🔗 URL: {competition_url}")

        # Check 1: Is competition already completed?
        if self.monitor_progress.is_competition_completed(competition_id):
            print(f"   ⏭️ {competition_id} already completed - exiting early")
            return pd.DataFrame()

        try:
            print(f"   📝 Marking competition {competition_id} as started...")

            # REMOVE THIS LINE if it exists - you're claiming in the main loop now
            # self.monitor_progress.mark_competition_started(competition_id, competition_url)

            print(f"   📊 Getting competition status for {competition_id}...")
            competition_status = self.monitor_progress.get_competition_status(
                competition_id
            )
            print(f"   📊 Status: {competition_status}")

            if competition_status != TaskStatus.SEASONS_DISCOVERED.value:
                print(f"   🔍 Need to discover seasons for {competition_id}...")
                seasons = self._discover_seasons(
                    driver, competition_url, competition_id
                )
                print(
                    f"   🔍 Discovery result: {len(seasons) if seasons else 0} seasons found"
                )

                if not seasons:
                    print(f"   ❌ No seasons found for {competition_id} - exiting")
                    return pd.DataFrame()
            else:
                print(f"   ✅ Seasons already discovered for {competition_id}")

            print(f"   📋 Getting pending seasons for {competition_id}...")
            pending_seasons = self.monitor_progress.get_pending_seasons_for_competition(
                competition_id
            )
            print(
                f"   📋 Found {len(pending_seasons) if pending_seasons else 0} pending seasons"
            )

            if not pending_seasons:
                print(f"   ⭕ No pending seasons for {competition_id} - exiting")
                return pd.DataFrame()

            print("   ⏱️ Applying mandatory delay...")
            time.sleep(self.config.vpn.mandatory_delay)

            # Create season tester for smart termination
            print(f"   🧪 Creating season tester for {competition_id}...")
            season_tester = create_season_tester(self, driver)

            # Process seasons with smart termination
            print(f"   🔄 Starting season processing for {competition_id}...")
            consecutive_failures = 0

            for i, season in enumerate(pending_seasons, 1):
                print(
                    f"   📅 Processing season {i}/{len(pending_seasons)}: {season.get('season_id', 'unknown')}"
                )

                if not self.should_continue_scraping:
                    print("   ⏹️ Stop flag set - breaking from season loop")
                    break

                try:
                    result = self._scrape_season_club_data(
                        driver, competition_url, competition_id, season
                    )

                    print(
                        f"   📊 Season result: {'Empty' if result.empty else f'{len(result)} clubs'}"
                    )

                    # Check if season was successful
                    if not result.empty:
                        consecutive_failures = 0
                        self.smart_terminator.mark_season_success(competition_id)
                        print("   ✅ Season success - reset failure counter")
                    else:
                        consecutive_failures += 1
                        print(
                            f"   ⚠️ Empty result for season {season['season_id']} (failure #{consecutive_failures})"
                        )

                        # Trigger smart termination for empty results
                        if (
                            consecutive_failures
                            >= self.smart_terminator.config.failure_threshold
                        ):
                            print("🎯 Triggering smart termination check...")
                            decision = self.smart_terminator.should_continue_processing(
                                competition_id=competition_id,
                                failed_season=season,
                                remaining_seasons=pending_seasons,
                                test_callback=season_tester,
                            )

                            if not decision.should_continue:
                                print(
                                    f"🛑 Smart termination activated for {competition_id} (empty results)"
                                )
                                print(f"   Reason: {decision.reason}")
                                if decision.cutoff_year:
                                    print(
                                        f"   Data cutoff at year: {decision.cutoff_year}"
                                    )

                                self._mark_old_seasons_as_unavailable(
                                    competition_id,
                                    decision.cutoff_year,
                                    pending_seasons,
                                )
                                break
                            else:
                                print(
                                    f"✅ Continuing despite empty results: {decision.reason}"
                                )

                    print("   ⏱️ Applying VPN timing delay...")
                    self.vpn_handler.handle_request_timing(
                        "season processing, please wait..."
                    )

                except Exception as error:
                    consecutive_failures += 1
                    print(
                        f"   ❌ Exception for season {season.get('season_id', 'unknown')}: {str(error)} (failure #{consecutive_failures})"
                    )

                    print("   🎯 Triggering smart termination check for exception...")
                    decision = self.smart_terminator.should_continue_processing(
                        competition_id=competition_id,
                        failed_season=season,
                        remaining_seasons=pending_seasons,
                        test_callback=season_tester,
                    )

                    if not decision.should_continue:
                        print(
                            f"🛑 Smart termination activated for {competition_id} (exceptions)"
                        )
                        print(f"   Reason: {decision.reason}")
                        if decision.cutoff_year:
                            print(f"   Data cutoff at year: {decision.cutoff_year}")

                        self._mark_old_seasons_as_unavailable(
                            competition_id, decision.cutoff_year, pending_seasons
                        )
                        break
                    else:
                        print(f"✅ Continuing despite exceptions: {decision.reason}")

                    continue

            self._check_and_mark_competition_complete(competition_id)
            print(f"   ✅ Finished processing {competition_id}")
            return pd.DataFrame()

        except Exception as e:
            print(f"   ❌ Fatal error in {competition_id}: {str(e)}")
            import traceback

            traceback.print_exc()
            return pd.DataFrame()

    def _mark_old_seasons_as_unavailable(
        self,
        competition_id: str,
        cutoff_year: Optional[str],
        seasons: List[Dict[str, str]],
    ) -> None:
        """
        NEW: Mark old seasons as completed (not failed) to prevent retrying.
        These seasons are unavailable due to data cutoff, not processing errors.
        """
        if not cutoff_year:
            return

        try:
            cutoff_year_int = int(cutoff_year)

            # Use database transaction to mark seasons as completed
            with self.monitor_progress.db_service.transaction() as session:
                for season in seasons:
                    try:
                        season_year_int = int(season["year"])
                        if season_year_int < cutoff_year_int:
                            # Update season status to COMPLETED with explanatory message
                            session.execute(
                                text(
                                    """
                                UPDATE season_progress 
                                SET status = :status, 
                                    completed_at = :now,
                                    clubs_saved = 0,
                                    error_message = :message
                                WHERE competition_id = :comp_id 
                                AND season_id = :season_id
                            """
                                ),
                                {
                                    "status": TaskStatus.COMPLETED.value,
                                    "now": datetime.now(),
                                    "message": f"Smart termination: Data unavailable before {cutoff_year}",
                                    "comp_id": competition_id,
                                    "season_id": season["season_id"],
                                },
                            )

                            print(
                                f"   ✅ Marked {season['season_id']} as completed (data unavailable before {cutoff_year})"
                            )

                    except (ValueError, KeyError):
                        continue

                session.commit()

        except (ValueError, KeyError):
            pass
        except Exception as e:
            print(f"   ⚠️ Error marking old seasons as unavailable: {str(e)}")
        # Don't raise - this is cleanup, not critical

    def _check_and_mark_competition_complete(self, competition_id: str) -> None:
        """
        Check if a competition has no pending seasons left and mark it as completed.
        Should be called after smart termination marks old seasons as unavailable.
        """
        try:
            with self.monitor_progress.db_service.transaction() as session:
                # Check if any seasons are still pending
                pending_check = session.execute(
                    text(
                        """
                    SELECT COUNT(*) FROM season_progress 
                    WHERE competition_id = :comp_id 
                    AND status = :pending_status
                """
                    ),
                    {
                        "comp_id": competition_id,
                        "pending_status": TaskStatus.PENDING.value,
                    },
                )

                pending_count = pending_check.fetchone()[0]

                if pending_count == 0:
                    # No pending seasons left - mark competition as completed
                    session.execute(
                        text(
                            """
                        UPDATE competition_progress 
                        SET status = :status, 
                            completed_at = :now
                        WHERE competition_id = :comp_id
                    """
                        ),
                        {
                            "status": TaskStatus.COMPLETED.value,
                            "now": datetime.now(),
                            "comp_id": competition_id,
                        },
                    )

                    session.commit()
                    print(
                        f"   🎉 Competition {competition_id} marked as completed (no pending seasons)"
                    )

        except Exception as e:
            print(f"   ⚠️ Error checking competition completion: {str(e)}")

    def get_smart_termination_summary(self) -> Dict:
        """
        NEW: Get summary of smart termination decisions.
        """
        if not self._components_initialized:
            self._initialize_components()

        competitions = self.get_non_cup_competitions()
        summary = {}

        for comp in competitions:
            comp_id = comp["competition_id"]
            termination_info = self.smart_terminator.get_termination_summary(comp_id)
            if termination_info:
                summary[comp_id] = termination_info

        return summary

    def _discover_seasons(
        self, driver, competition_url: str, competition_id: Optional[str] = None
    ) -> List[Dict[str, str]]:
        """
        Discover seasons for a competition.

        Args:
            driver: Selenium WebDriver instance
            competition_url: Competition URL
            competition_id: Competition identifier (optional)
                          If provided, will mark seasons as discovered in progress tracking

        Returns:
            List of discovered seasons
        """
        driver.get(competition_url)
        self.vpn_handler.handle_request_timing("please wait for the page to load...")

        # ***> Parse seasons from current page <***
        soup = BeautifulSoup(driver.page_source, "html.parser")
        seasons = self.parser.parse_season_options(soup)

        # Only update progress tracking if competition_id is provided
        if seasons and competition_id:
            self.monitor_progress.mark_seasons_discovered(competition_id, seasons)

        return seasons

    def _scrape_season_club_data(
        self, driver, base_url: str, competition_id: str, season: Dict[str, str]
    ) -> pd.DataFrame:
        """
        Scrape club data for a single season with resume capability.
        Maintains exact original logic flow.
        """
        season_year = season["year"]
        season_id = season["season_id"]

        # ***> Check if already completed <***
        if self.monitor_progress.is_season_completed(competition_id, season_id):
            return pd.DataFrame()

        # ***> Security check <***
        if not self.should_continue_scraping:
            return pd.DataFrame()

        # ***> Wait for VPN rotation if needed <***
        self._wait_for_vpn_rotation()

        # ***> Mark season as started <***
        self.monitor_progress.mark_season_started(competition_id, season_id)

        try:
            # ***> Construct season URL preserving original logic <***
            season_url = self._construct_season_url(base_url, season_year)

            # ***> Navigate to season URL <***
            driver.get(season_url)
            self.vpn_handler.handle_request_timing("season navigation please wait...")

            # ***> Parse club data <***
            self.vpn_handler.handle_request_timing("page load delay...")
            soup = BeautifulSoup(driver.page_source, "html.parser")
            print(
                f"   🔍 DEBUG: Page title: {soup.title.string if soup.title else 'No title'}"
            )
            print(
                f"   🔍 DEBUG: Page contains 'Club' text: {'Club' in soup.get_text()}"
            )

            tables = soup.find_all("table", class_="items")
            print(f"   🔍 DEBUG: Found {len(tables)} tables with class 'items'")

            club_data = self.parser.parse_club_table(
                soup, season_year, season_id, competition_id
            )

            # ***> Save data if available <***
            # ***> Save data if available <***
            if hasattr(self.monitor_progress, "store_season_club_data"):
                # Worker mode - store in memory
                clubs_saved = self.monitor_progress.store_season_club_data(
                    season_id, club_data
                )
            else:
                # Host mode - save to database
                clubs_saved = self._save_club_data(club_data, competition_id, season_id)

            # ***> Mark season as completed <***
            self.monitor_progress.mark_season_completed(
                competition_id, season_id, clubs_saved
            )

            return club_data

        except Exception as error:
            self.monitor_progress.mark_season_failed(
                competition_id, season_id, str(error)
            )
            return pd.DataFrame()

    def _wait_for_vpn_rotation(self) -> None:
        """
        Wait for VPN rotation to complete if in progress.
        """
        if hasattr(self.vpn_handler, "vpn_rotating") and self.vpn_handler.vpn_rotating:
            while self.vpn_handler.vpn_rotating:
                time.sleep(self.config.vpn.mandatory_delay)

    def _construct_season_url(self, base_url: str, season_year: str) -> str:
        """
        Construct season URL based on year.

        Args:
            base_url: Base competition URL
            season_year: Year of the season

        Returns:
            Constructed season URL
        """
        if season_year == OrchestratorConfig.CURRENT_SEASON_YEAR:
            return base_url
        else:
            # ***> Historical season with parameter <***
            separator = (
                OrchestratorConfig.URL_SEPARATOR_PARAM
                if OrchestratorConfig.URL_SEPARATOR_QUERY in base_url
                else OrchestratorConfig.URL_SEPARATOR_QUERY
            )

            if base_url.endswith(OrchestratorConfig.URL_PATH_SEPARATOR):
                return "%s%s%s%s%s" % (
                    base_url,
                    OrchestratorConfig.HISTORICAL_SEASON_PATH,
                    separator,
                    OrchestratorConfig.SEASON_PARAM_PREFIX,
                    season_year,
                )
            else:
                return "%s%s%s%s%s%s" % (
                    base_url,
                    OrchestratorConfig.URL_PATH_SEPARATOR,
                    OrchestratorConfig.HISTORICAL_SEASON_PATH,
                    separator,
                    OrchestratorConfig.SEASON_PARAM_PREFIX,
                    season_year,
                )

    def _save_club_data(
        self, club_data: pd.DataFrame, competition_id: str, season_id: str
    ) -> int:
        """
        Save club data to database using TeamOrchestrator.

        Args:
            club_data: Club data DataFrame
            competition_id: Competition identifier
            season_id: Season identifier

        Returns:
            Number of clubs saved
        """
        clubs_saved = 0

        if not club_data.empty and self.database_manager:
            try:
                # Import here to avoid circular imports
                from database.orchestrators.team_orchestrator import (
                    TeamDataOrchestrator,
                )

                # Create team orchestrator with same environment
                environment = self._get_environment_setting()
                team_orchestrator = TeamDataOrchestrator(environment)

                result = team_orchestrator.save_clubs(club_data)

                if result:
                    clubs_saved = len(club_data)

                # Cleanup
                team_orchestrator.cleanup()

            except DatabaseOperationError:
                pass
            except Exception:
                pass

        return clubs_saved

    def get_security_dashboard(self) -> Dict:
        """
        Get comprehensive security dashboard data.

        Returns:
            Dictionary with all security metrics
        """
        if not self._components_initialized:
            self._initialize_components()

        stats = self.vpn_handler.get_comprehensive_vpn_statistics()
        alerts = self.vpn_handler.security_alerts

        return {
            "security_status": self.vpn_handler.security_status,
            "should_continue": self.should_continue_scraping,
            "comprehensive_stats": stats,
            "recent_alerts": alerts,
            "progress": self.monitor_progress.get_progress_summary(),
        }

    def cleanup(self) -> None:
        """
        Clean up all resources.
        """
        if not self._components_initialized:
            return

        try:
            # ***> Cleanup database connections <***
            if self.database_manager:
                self.database_manager.cleanup()

            # ***> Cleanup VPN connections <***
            if hasattr(self, "vpn_handler"):
                self.vpn_handler.cleanup()

        except Exception:
            pass

    @property
    def database_available(self) -> bool:
        """
        Check if database is available.

        Returns:
            True if database is available and initialized
        """
        if not self._components_initialized:
            self._initialize_components()

        if not self.database_manager:
            return False

        try:
            # Check if database service is initialized and healthy
            return (
                self.database_manager._initialized
                and self.database_manager.db_manager.health_check()
            )
        except Exception:
            return False
