# pipelines/princpal_orchestrator/match_orchestrator.py
"""
Match-specific orchestrator for scraping operations.
Handles matchday and match data extraction with proper completion tracking.
"""

from typing import Dict, List, Optional

import pandas as pd
from bs4 import BeautifulSoup
from sqlalchemy import text

from configurations import ScraperConfig
from database import create_database_service
from database.repositories.league_table_repository import LeagueTableRepository
from database.repositories.match_repository import MatchRepository
from database.repositories.matchday_repository import MatchdayRepository
from database.repositories.top_scorer_repository import TopScorerRepository
from database.schemas import Match
from exceptions import ConfigurationError, IPSecurityViolationError, VpnRequiredError
from extractors.extractor_match import MatchdayExtractor, MatchExtractor
from vpn_controls import VpnProtectionHandler

from .base_orchestrator import BaseOrchestrator
from .orchestrator_config import OrchestratorConfig


class MatchOrchestrator(BaseOrchestrator):
    """
    Orchestrates match data scraping with smart completion detection.
    Key difference from ClubOrchestrator: focuses on matchday completion logic.
    """

    def __init__(
        self,
        config: Optional[ScraperConfig] = None,
        env_file_path: Optional[str] = None,
    ):
        """
        Initialize match scraping orchestrator.
        """
        self._skip_base_vpn_handler = True
        super().__init__(config)

        self.env_file_path = env_file_path or OrchestratorConfig.DEFAULT_ENV_PATH
        self.should_continue_scraping = True
        self._components_initialized = False

    def _initialize_components(self) -> None:
        """
        Initialize all orchestrator components.
        """
        if self._components_initialized:
            return

        try:
            # VPN handler
            self.vpn_handler = VpnProtectionHandler(
                config=self.config, env_file_path=self.env_file_path
            )

            # Match extractors
            self.matchday_extractor = MatchdayExtractor()
            self.match_extractor = MatchExtractor()

            # Database setup
            self.database_manager = None
            if self.config.save_to_database:
                environment = self._get_environment_setting()
                self.database_manager = create_database_service(environment)

                # Initialize repositories
                self.matchday_repo = MatchdayRepository(
                    self.database_manager.db_manager
                )
                self.league_repo = LeagueTableRepository(
                    self.database_manager.db_manager
                )
                self.scorer_repo = TopScorerRepository(self.database_manager.db_manager)
                self.match_repo = MatchRepository(self.database_manager.db_manager)

            self._components_initialized = True

        except Exception as error:
            raise ConfigurationError(f"Error initializing components: {error}")

    def scrape_all_match_data(self, driver) -> pd.DataFrame:
        """
        Main entry point: scrape match data for competitions with smart recovery.
        """
        if not self._components_initialized:
            self._initialize_components()

        try:
            self.vpn_handler.ensure_vpn_protection()

            # Get available competitions
            competitions = self._get_available_competitions()
            if not competitions:
                print("🎉 No competitions need match data scraping!")
                return pd.DataFrame()

            print(f"📊 Found {len(competitions)} competitions needing match data")

            # Process competitions one at a time
            for competition in competitions:
                if not self.should_continue_scraping:
                    break

                print(
                    f"🎯 Processing {competition['competition_code']} {competition['season_year']}"
                )
                self._scrape_competition_matches(driver, competition)

                # VPN timing between competitions
                self.vpn_handler.handle_request_timing("competition processing...")

            return pd.DataFrame()

        except (VpnRequiredError, IPSecurityViolationError):
            raise
        except Exception as e:
            print(f"❌ Error in match data scraping: {e}")
            raise

    def _get_available_competitions(self) -> List[Dict[str, str]]:
        """
        Get competitions that have teams data but may need match data completion.
        """
        if not self.database_manager:
            return []

        try:
            with self.database_manager.db_manager.get_session() as session:
                # Get competitions with team data (season_year <= 2024)
                competitions = session.execute(
                    text(
                        """
                    SELECT DISTINCT c.competition_code,
                           c.competition_id,
                           c.competition_name,
                           t.season_year,
                           CAST(t.season_year AS INTEGER) as season_year_int
                    FROM competitions c
                    JOIN teams t ON c.competition_id = t.competition_id
                    WHERE t.season_year IS NOT NULL
                      AND CAST(t.season_year AS INTEGER) <= 2024
                    ORDER BY season_year_int DESC
                    """
                    )
                ).fetchall()

                return [
                    {
                        "competition_code": comp[0],
                        "competition_id": comp[1],
                        "competition_name": comp[2],
                        "season_year": comp[3],
                    }
                    for comp in competitions
                ]

        except Exception as e:
            print(f"⚠️ Error getting competitions: {e}")
            return []

    def _scrape_competition_matches(self, driver, competition: Dict[str, str]) -> None:
        """
        Scrape all match data for a single competition with smart completion logic.
        """
        competition_id = competition["competition_id"]
        competition_code = competition["competition_code"]
        season_year = competition["season_year"]
        competition_name = competition["competition_name"]

        print(f"   🔍 Processing matches for {competition_code} {season_year}")

        # Step 1: Complete any incomplete existing matchdays first
        incomplete_matchdays = self._get_incomplete_matchdays(
            competition_id, season_year
        )
        if incomplete_matchdays:
            print(
                f"   🔄 Found {len(incomplete_matchdays)} incomplete matchdays to complete first"
            )
            for matchday_info in incomplete_matchdays:
                self._complete_matchday(driver, matchday_info, competition)

        # Step 2: Discover and process new matchdays
        consecutive_empty = 0
        max_empty = 3
        max_total = 50
        processed = 0

        while processed < max_total and consecutive_empty < max_empty:
            if not self.should_continue_scraping:
                break

            try:
                # Get next matchday number to try
                next_matchday = self._get_next_matchday_number(
                    competition_name, season_year
                )

                if next_matchday > 200:  # Safety check
                    print(f"   🛑 Reached unreasonable matchday limit: {next_matchday}")
                    break

                # Build URL and try to scrape
                url = self._build_transfermarkt_url(
                    competition_code, season_year, next_matchday, competition_id
                )
                print(f"   📅 Trying matchday {next_matchday}: {url}")

                result = self._process_single_matchday(
                    driver,
                    url,
                    next_matchday,
                    season_year,
                    competition_name,
                    competition_id,
                )

                if result == "empty_matchday":
                    consecutive_empty += 1
                    print(
                        f"   📭 Empty matchday {next_matchday} ({consecutive_empty}/{max_empty})"
                    )
                elif result == "success":
                    consecutive_empty = 0  # Reset on success
                    print(f"   ✅ Successfully processed matchday {next_matchday}")

                processed += 1

                # VPN timing between matchdays
                self.vpn_handler.handle_request_timing("matchday processing...")

            except Exception as e:
                print(f"   ❌ Error processing matchday: {e}")
                consecutive_empty += 1
                if consecutive_empty >= max_empty:
                    break
                continue

        print(
            f"   ✅ Finished {competition_code} {season_year}: {processed} matchdays processed"
        )

    def _get_incomplete_matchdays(
        self, competition_id: str, season_year: str
    ) -> List[Dict]:
        """
        Find matchdays where total_matches != actual match count.
        """
        try:
            with self.database_manager.db_manager.get_session() as session:
                incomplete = session.execute(
                    text(
                        """
                    SELECT m.matchday_id, m.number, m.total_matches, 
                           m.source_url, COUNT(ma.match_id) as actual_matches
                    FROM matchdays m
                    LEFT JOIN matches ma ON m.matchday_id = ma.matchday_id
                    WHERE m.competition = :comp_code AND m.season = :season
                    GROUP BY m.matchday_id, m.number, m.total_matches, m.source_url
                    HAVING m.total_matches != COUNT(ma.match_id)
                    ORDER BY m.number
                    """
                    ),
                    {"comp_code": competition_id, "season": season_year},
                ).fetchall()

                return [
                    {
                        "matchday_id": row[0],
                        "number": row[1],
                        "total_matches": row[2],
                        "source_url": row[3],
                        "actual_matches": row[4],
                        "missing_matches": row[2] - row[4],
                    }
                    for row in incomplete
                ]

        except Exception as e:
            print(f"   ⚠️ Error checking incomplete matchdays: {e}")
            return []

    def _complete_matchday(
        self, driver, matchday_info: Dict, competition: Dict
    ) -> None:
        """
        Complete an existing matchday that has missing matches.
        """
        matchday_id = matchday_info["matchday_id"]
        number = matchday_info["number"]
        missing = matchday_info["missing_matches"]
        source_url = matchday_info["source_url"]

        print(f"   🔄 Completing matchday {number}: {missing} missing matches")

        try:
            # Re-scrape the matchday page
            driver.get(source_url)
            self.vpn_handler.handle_request_timing("page reload for completion...")

            soup = BeautifulSoup(driver.page_source, "html.parser")
            container = self.matchday_extractor.extract_matchday(
                soup, number, competition["season_year"]
            )

            if not container.matches:
                print(f"   ⚠️ No matches found on re-scrape of matchday {number}")
                return

            # Process only missing matches
            self._process_missing_matches(
                driver, container.matches, matchday_id, competition
            )

            print(f"   ✅ Completed matchday {number}")

        except Exception as e:
            print(f"   ❌ Error completing matchday {number}: {e}")

    def _process_missing_matches(
        self, driver, matches: List, matchday_id: str, competition: Dict
    ) -> None:
        """
        Process only the matches that are missing from database.
        """
        for match in matches:
            if not match.match_report_url:
                continue

            # Check if match already exists
            with self.database_manager.db_manager.get_session() as session:
                existing = (
                    session.query(Match).filter_by(match_id=match.match_id).first()
                )
                if existing:
                    continue  # Skip existing matches

            # Scrape missing match detail
            try:
                print(f"   🎯 Scraping missing match: {match.match_id}")

                driver.get(match.match_report_url)
                self.vpn_handler.handle_request_timing("missing match detail...")

                soup = BeautifulSoup(driver.page_source, "html.parser")
                detail = self.match_extractor.extract_from_url(
                    soup, match.match_report_url
                )

                # Inject context
                detail.match_info.matchday_id = matchday_id
                detail.match_info.season = competition["season_year"]
                detail.match_info.competition = competition["competition_name"]
                detail.match_info.day_of_week = match.day_of_week
                detail.match_info.match_report_url = match.match_report_url

                # Save match
                self.match_repo.upsert(detail)
                print(f"   → Saved missing match {match.match_id}")

            except Exception as e:
                print(f"   ❌ Error processing missing match {match.match_id}: {e}")
                continue

    def _get_next_matchday_number(self, competition_name: str, season_year: str) -> int:
        """
        Get the next matchday number to try.
        Based on highest matchday number in matchdays table.
        """
        try:
            with self.database_manager.db_manager.get_session() as session:
                # Query using competition_name (what's actually stored)
                result = session.execute(
                    text(
                        """
                    SELECT MAX(number) FROM matchdays 
                    WHERE competition = :comp_name AND season = :season
                    """
                    ),
                    {
                        "comp_name": competition_name,
                        "season": season_year,
                    },  # ← Use competition_name
                ).fetchone()

                if result and result[0]:
                    return result[0] + 1
                else:
                    return 1  # Start with matchday 1

        except Exception as e:
            print(f"   ⚠️ Error getting next matchday number: {e}")
            return 1

    def _process_single_matchday(
        self,
        driver,
        url: str,
        matchday: int,
        season: str,
        competition_name: str,
        competition_id: str,
    ) -> str:
        """
        Process a single new matchday.
        Returns: "success", "empty_matchday"
        """
        try:
            # Extract matchday data
            driver.get(url)
            self.vpn_handler.handle_request_timing("matchday page load...")

            soup = BeautifulSoup(driver.page_source, "html.parser")
            container = self.matchday_extractor.extract_matchday(soup, matchday, season)

            if not container:
                return "empty_matchday"

            container.matchday_info["competition"] = competition_name

            # Check if meaningful data exists
            matches_count = len(container.matches) if container.matches else 0
            teams_count = (
                len(container.league_table.get("teams", []))
                if container.league_table
                else 0
            )

            print(
                f"   📊 Matchday {matchday}: {matches_count} matches, {teams_count} teams"
            )

            if matches_count == 0 and teams_count == 0:
                return "empty_matchday"

            # Save all data in transaction
            with self.database_manager.transaction():
                # Save matchday with total_matches count
                summary_data = {}
                if (
                    container.matchday_summary
                    and "current_matchday" in container.matchday_summary
                ):
                    summary_data = container.matchday_summary["current_matchday"]

                # Ensure total_matches is set correctly
                if (
                    not container.matchday_info.get("total_matches")
                    and matches_count > 0
                ):
                    container.matchday_info["total_matches"] = matches_count

                md_id = self.matchday_repo.upsert(
                    container.matchday_info, container.metadata, summary_data
                )

                # Process all matches
                if container.matches:
                    self._process_all_matches(
                        driver, container.matches, md_id, season, competition_name
                    )

                # Save league table and scorers
                if container.league_table and container.league_table.get("teams"):
                    self.league_repo.upsert_entries(
                        md_id, container.league_table["teams"]
                    )

                if container.top_scorers:
                    self.scorer_repo.upsert_scorers(md_id, container.top_scorers)

            return "success"

        except Exception as e:
            print(f"   ❌ Error processing matchday {matchday}: {e}")
            return "empty_matchday"

    def _process_all_matches(
        self,
        driver,
        matches: List,
        matchday_id: str,
        season: str,
        competition_name: str,
    ) -> None:
        """
        Process all matches for a matchday.
        """
        print(f"   🎮 Processing {len(matches)} matches...")

        for i, match in enumerate(matches, 1):
            if not match.match_report_url:
                print(f"   ⚠️ Match {i}: No report URL, skipping")
                continue

            try:
                print(f"   🎯 Match {i}/{len(matches)}: {match.match_id}")

                # Get detailed match data
                driver.get(match.match_report_url)
                self.vpn_handler.handle_request_timing("match detail page...")

                soup = BeautifulSoup(driver.page_source, "html.parser")
                detail = self.match_extractor.extract_from_url(
                    soup, match.match_report_url
                )

                # Inject context
                detail.match_info.matchday_id = matchday_id
                detail.match_info.season = season
                detail.match_info.competition = competition_name
                detail.match_info.day_of_week = match.day_of_week
                detail.match_info.match_report_url = match.match_report_url

                # Save match
                self.match_repo.upsert(detail)
                print(f"   → Saved match {match.match_id}")

            except Exception as e:
                print(f"   ❌ Error processing match {match.match_id}: {e}")
                continue

    def _build_transfermarkt_url(
        self,
        competition_code: str,
        season_year: str,
        matchday: int,
        competition_id: str,
    ) -> str:
        """Build Transfermarkt URL for matchday."""
        return f"https://www.transfermarkt.com/{competition_code}/spieltag/wettbewerb/{competition_id}/saison_id/{season_year}/spieltag/{matchday}"

    def cleanup(self) -> None:
        """Clean up resources."""
        if not self._components_initialized:
            return

        try:
            if self.database_manager:
                self.database_manager.cleanup()

            if hasattr(self, "vpn_handler"):
                self.vpn_handler.cleanup()

        except Exception:
            pass

    @property
    def database_available(self) -> bool:
        """Check if database is available."""
        if not self._components_initialized:
            self._initialize_components()

        if not self.database_manager:
            return False

        try:
            return (
                self.database_manager._initialized
                and self.database_manager.db_manager.health_check()
            )
        except Exception:
            return False
