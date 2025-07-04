# core/scrapers/club_orchestrator.py
"""
Club-specific orchestrator for scraping operations.
Handles club data extraction across seasons while preserving logic.
"""

import time
from typing import Optional, List, Dict
import pandas as pd
from bs4 import BeautifulSoup
from database import TaskStatus
from coordination.work_coordinator import create_distributed_tracker
from configurations import ScraperConfig
from .base_orchestrator import BaseOrchestrator
from .orchestrator_config import OrchestratorConfig
from extractors import ClubTableParser
from database import ClubDatabaseManager
from exceptions import VpnRequiredError, DatabaseOperationError, ConfigurationError, IPSecurityViolationError
from vpn_controls import VpnProtectionHandler


class ClubOrchestrator(BaseOrchestrator):
    """
    Orchestrates club data scraping across all seasons for competitions.
    Maintains exact original logic for club data extraction.
    """

    def __init__(self, config: Optional[ScraperConfig] = None,
                 env_file_path: Optional[str] = None):
        """
        Initialize club scraping orchestrator.

        Args:
            config: Scraper configuration (uses development if None)
            env_file_path: Path to environment file

        Raises:
            VpnRequiredError: If VPN is required but not available
            ConfigurationError: If configuration is invalid
        """
        super().__init__(config)
        
        #***> Set environment file path with default <***
        self.env_file_path = env_file_path or OrchestratorConfig.DEFAULT_ENV_PATH

        #***> Configure IP security settings if not present <***
        self._configure_ip_security_settings()

        #***> Initialize scraping control flag <***
        self.should_continue_scraping = True

        #***> Initialize components flag to prevent recursion <***
        self._components_initialized = False

    def _configure_ip_security_settings(self) -> None:
        """
        Configure IP security settings if not present in config.
        """
        if not hasattr(self.config, 'max_requests_per_ip'):
            self.config.max_requests_per_ip = (
                OrchestratorConfig.DEFAULT_MAX_REQUESTS_PER_IP
            )
        if not hasattr(self.config, 'ip_check_interval'):
            self.config.ip_check_interval = (
                OrchestratorConfig.DEFAULT_IP_CHECK_INTERVAL
            )

    def _initialize_components(self) -> None:
        """
        Initialize all orchestrator components.
        """
        if self._components_initialized:
            return
            
        try:
            #***> Initialize VPN handler with environment file <***
            self.vpn_handler = VpnProtectionHandler(
                config=self.config,
                env_file_path=self.env_file_path
            )
            
            #***> Initialize club-specific components <***
            self.parser = ClubTableParser()

            #***> Initialize database manager if enabled <***
            self.database_manager = None
            if self.config.save_to_database:
                self._initialize_club_database_manager()
            
            #***> Initialize progress tracker <***
            self.progress_tracker = create_distributed_tracker("production")
            
            self._components_initialized = True
            
        except Exception as error:
            raise ConfigurationError("Error initializing components: %s" % error)

    def _initialize_club_database_manager(self) -> None:
        """
        Initialize club database manager with error handling.
        """
        try:
            environment = self._get_environment_setting()
            self.database_manager = ClubDatabaseManager(environment)
        except DatabaseOperationError as error:
            self._handle_database_initialization_error(error)

    def get_non_cup_competitions(self) -> List[Dict[str, str]]:
        """
        Get non-cup competitions from database.

        Returns:
            List of competition dictionaries with id and url
        """
        if not self._components_initialized:
            self._initialize_components()
            
        if not self.database_manager:
            return []

        try:
            competitions = self.database_manager.get_non_cup_competitions()
            return competitions
        except DatabaseOperationError:
            return []

    def scrape_all_club_data(self, driver) -> pd.DataFrame:
        """
        Scrape club data for all competitions across all seasons.
        Maintains exact original logic flow.

        Args:
            driver: Selenium WebDriver instance

        Returns:
            DataFrame with all club data

        Raises:
            VpnRequiredError: If VPN protection fails
        """
        if not self._components_initialized:
            self._initialize_components()

        try:
            self.vpn_handler.ensure_vpn_protection()

            #***> Recover stuck seasons preserving original logic <***
            stuck_count = self.progress_tracker.recover_stuck_seasons()

            #***> Get competitions and apply exclusion filter <***
            competitions = self.get_non_cup_competitions()
            competitions = self._filter_excluded_competitions(competitions)

            if not competitions:
                return pd.DataFrame()

            #***> Filter out completed competitions <***
            pending_competitions = self._get_pending_competitions(competitions)

            if not pending_competitions:
                return pd.DataFrame()

            #***> Process competitions maintaining original logic <***
            for i, competition in enumerate(pending_competitions, 1):
                try:
                    self._scrape_competition_club_data(driver, competition)
                    self.vpn_handler.handle_request_timing(
                        "competition processing, please wait..."
                    )
                except KeyboardInterrupt:
                    break
                except VpnRequiredError:
                    raise
                except Exception:
                    continue
                    
            return pd.DataFrame()
            
        except (VpnRequiredError, IPSecurityViolationError):
            raise
        except Exception:
            raise

    def _filter_excluded_competitions(
        self, 
        competitions: List[Dict[str, str]]
    ) -> List[Dict[str, str]]:
        """
        Filter out excluded competitions.
        
        Args:
            competitions: List of all competitions
            
        Returns:
            Filtered list of competitions
        """
        return [
            comp for comp in competitions
            if comp['competition_id'] not in 
            OrchestratorConfig.EXCLUDED_COMPETITION_IDS
        ]

    def _get_pending_competitions(
        self, 
        competitions: List[Dict[str, str]]
    ) -> List[Dict[str, str]]:
        """
        Get competitions that are not yet completed.
        
        Args:
            competitions: List of all competitions
            
        Returns:
            List of pending competitions
        """
        return [
            comp for comp in competitions
            if not self.progress_tracker.is_competition_completed(
                comp['competition_id']
            )
        ]

    def _scrape_competition_club_data(
            self,
            driver,
            competition: Dict[str, str]
        ) -> pd.DataFrame:
        """
        Scrape club data for all seasons of a single competition.
        Maintains exact original logic flow.

        Args:
            driver: Selenium WebDriver instance
            competition: Competition dictionary with id and url

        Returns:
            DataFrame with club data for all seasons
        """
        competition_id = competition['competition_id']
        competition_url = competition['competition_url']

        #***> Check if competition already completed <***
        if self.progress_tracker.is_competition_completed(competition_id):
            return pd.DataFrame()

        try:
            #***> Mark competition as started <***
            self.progress_tracker.mark_competition_started(
                competition_id, competition_url
            )
            
            #***> Handle season discovery maintaining original logic <***
            competition_status = self.progress_tracker.get_competition_status(
                competition_id
            )

            if competition_status != TaskStatus.SEASONS_DISCOVERED.value:
                seasons = self._discover_seasons(
                    driver, competition_url, competition_id
                )
                if not seasons:
                    return pd.DataFrame()
            
            #***> Process pending seasons <***
            pending_seasons = (
                self.progress_tracker.get_pending_seasons_for_competition(
                    competition_id
                )
            )
            if not pending_seasons:
                return pd.DataFrame()

            time.sleep(OrchestratorConfig.SEASON_PROCESSING_DELAY)

            #***> Process each season maintaining original logic <***
            for season in pending_seasons:
                if not self.should_continue_scraping:
                    break

                try:
                    self._scrape_season_club_data(
                        driver, competition_url, competition_id, season
                    )
                    self.vpn_handler.handle_request_timing(
                        "season processing, please wait..."
                    )
                except Exception:
                    continue

            return pd.DataFrame()
            
        except Exception:
            return pd.DataFrame()

    def _discover_seasons(
        self, 
        driver, 
        competition_url: str, 
        competition_id: str
    ) -> List[Dict[str, str]]:
        """
        Discover seasons for a competition.
        
        Args:
            driver: Selenium WebDriver instance
            competition_url: Competition URL
            competition_id: Competition identifier
            
        Returns:
            List of discovered seasons
        """
        driver.get(competition_url)
        self.vpn_handler.handle_request_timing(
            "please wait for the page to load..."
        )

        #***> Parse seasons from current page <***
        soup = BeautifulSoup(driver.page_source, "html.parser")
        seasons = self.parser.parse_season_options(soup)

        if seasons:
            self.progress_tracker.mark_seasons_discovered(
                competition_id, seasons
            )
        
        return seasons

    def _scrape_season_club_data(
            self,
            driver,
            base_url: str,
            competition_id: str,
            season: Dict[str, str]
        ) -> pd.DataFrame:
        """
        Scrape club data for a single season with resume capability.
        Maintains exact original logic flow.
        """
        season_year = season['year']
        season_id = season['season_id']
        
        #***> Check if already completed <***
        if self.progress_tracker.is_season_completed(competition_id, season_id):
            return pd.DataFrame()
        
        #***> Security check <***
        if not self.should_continue_scraping:
            return pd.DataFrame()

        #***> Wait for VPN rotation if needed <***
        self._wait_for_vpn_rotation()
        
        #***> Mark season as started <***
        self.progress_tracker.mark_season_started(competition_id, season_id)
        
        try:
            #***> Construct season URL preserving original logic <***
            season_url = self._construct_season_url(base_url, season_year)

            #***> Navigate to season URL <***
            driver.get(season_url)
            self.vpn_handler.handle_request_timing(
                "season navigation please wait..."
            )

            #***> Parse club data <***
            time.sleep(OrchestratorConfig.PAGE_LOAD_DELAY)
            soup = BeautifulSoup(driver.page_source, "html.parser")
            club_data = self.parser.parse_club_table(
                soup, season_year, season_id, competition_id
            )

            #***> Save data if available <***
            clubs_saved = self._save_club_data(
                club_data, competition_id, season_id
            )
            
            #***> Mark season as completed <***
            self.progress_tracker.mark_season_completed(
                competition_id, season_id, clubs_saved
            )
            
            return club_data
            
        except Exception as error:
            self.progress_tracker.mark_season_failed(
                competition_id, season_id, str(error)
            )
            return pd.DataFrame()

    def _wait_for_vpn_rotation(self) -> None:
        """
        Wait for VPN rotation to complete if in progress.
        """
        if (hasattr(self.vpn_handler, 'vpn_rotating') and 
            self.vpn_handler.vpn_rotating):
            while self.vpn_handler.vpn_rotating:
                time.sleep(OrchestratorConfig.VPN_ROTATION_WAIT_INTERVAL)

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
            #***> Historical season with parameter <***
            separator = (OrchestratorConfig.URL_SEPARATOR_PARAM 
                        if OrchestratorConfig.URL_SEPARATOR_QUERY in base_url 
                        else OrchestratorConfig.URL_SEPARATOR_QUERY)
            
            if base_url.endswith(OrchestratorConfig.URL_PATH_SEPARATOR):
                return "%s%s%s%s%s" % (
                    base_url,
                    OrchestratorConfig.HISTORICAL_SEASON_PATH,
                    separator,
                    OrchestratorConfig.SEASON_PARAM_PREFIX,
                    season_year
                )
            else:
                return "%s%s%s%s%s%s" % (
                    base_url,
                    OrchestratorConfig.URL_PATH_SEPARATOR,
                    OrchestratorConfig.HISTORICAL_SEASON_PATH,
                    separator,
                    OrchestratorConfig.SEASON_PARAM_PREFIX,
                    season_year
                )

    def _save_club_data(
        self, 
        club_data: pd.DataFrame, 
        competition_id: str, 
        season_id: str
    ) -> int:
        """
        Save club data to database.
        
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
                result = self.database_manager.save_clubs(club_data)
                if result:
                    clubs_saved = len(club_data)
            except DatabaseOperationError:
                pass
            except Exception:
                pass
        
        return clubs_saved

    def force_ip_check(self) -> Dict[str, str]:
        """
        Force an immediate IP rotation check.
        
        Returns:
            Dictionary with rotation results
        """
        if not self._components_initialized:
            self._initialize_components()
            
        return self.vpn_handler.force_ip_rotation_check()

    def get_security_dashboard(self) -> Dict:
        """
        Get comprehensive security dashboard data.
        
        Returns:
            Dictionary with all security metrics
        """
        if not self._components_initialized:
            self._initialize_components()
            
        stats = self.vpn_handler.get_comprehensive_vpn_statistics()
        alerts = self.vpn_handler.get_security_alerts(
            last_hours=OrchestratorConfig.SECURITY_ALERTS_TIMEFRAME_HOURS
        )
        
        return {
            "security_status": self.vpn_handler.security_status,
            "should_continue": self.should_continue_scraping,
            "comprehensive_stats": stats,
            "recent_alerts": alerts,
            "progress": self.progress_tracker.get_progress_summary()
        }

    def save_to_csv(
            self,
            data: pd.DataFrame,
            filename: Optional[str] = None
        ) -> str:
        """
        Save club data to CSV file.

        Args:
            data: DataFrame with club data
            filename: Optional custom filename

        Returns:
            Path to saved CSV file
        """
        if not self._components_initialized:
            self._initialize_components()
            
        if filename is None:
            timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
            filename = "club_data_%s.csv" % timestamp

        return self.file_manager.save_to_csv(data, filename)

    def cleanup(self) -> None:
        """
        Clean up all resources.
        """
        if not self._components_initialized:
            return
            
        try:
            #***> Cleanup database connections <***
            if self.database_manager:
                self.database_manager.cleanup()
            
            #***> Cleanup VPN connections <***
            if hasattr(self, 'vpn_handler'):
                self.vpn_handler.cleanup()
                
        except Exception:
            pass

    @property
    def database_available(self) -> bool:
        """
        Check if database is available.
        
        Returns:
            True if database is available
        """
        if not self._components_initialized:
            self._initialize_components()
        return (self.database_manager is not None and 
                self.database_manager.is_available)