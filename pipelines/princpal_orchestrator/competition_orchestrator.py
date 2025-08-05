# pipelines/principal_orchestrator/competition_orchestrator.py
"""
Competition-specific orchestrator for scraping operations.
Handles competition data extraction while maintaining original logic.
"""

from typing import Optional

import pandas as pd
from bs4 import BeautifulSoup

from configurations.factory import get_config
from configurations.settings_orchestrator import ScraperConfig
from database import create_database_service
from exceptions import DatabaseOperationError, DatabaseServiceError, VpnRequiredError
from extractors import CompetitionTableParser

from .base_orchestrator import BaseOrchestrator


class CompetitionOrchestrator(BaseOrchestrator):
    """
    Orchestrates the complete competition scraping process.
    """

    def __init__(
        self, config: Optional[ScraperConfig] = None, environment: str = "development"
    ):
        """
        Initialize competition scraping orchestrator.

        Args:
            config: Scraper configuration (uses environment default if None)
            environment: Environment name (development, testing, production)

        Raises:
            VpnRequiredError: If VPN is required but not available
            ConfigurationError: If configuration is invalid
        """
        # Load config based on environment if not provided
        if config is None:
            config = get_config(environment)

        super().__init__(config)

        # Store environment for later use
        self.environment = environment

        # ***> Initialize competition-specific components <***
        self._setup_competition_components()

    def _setup_competition_components(self) -> None:
        """
        Initialize components specific to competition scraping.
        """
        # ***> Initialize database service if enabled <***
        self.database_service = None
        if self.config.save_to_database:
            self._initialize_database_service()

    def _initialize_database_service(self) -> None:
        """
        Initialize database service with error handling using new factory.
        """
        try:
            environment = self._get_environment_setting()
            self.database_service = create_database_service(environment)

            # Initialize the service (creates tables if needed)
            self.database_service.initialize()

        except (DatabaseServiceError, DatabaseOperationError) as error:
            self._handle_database_initialization_error(error)

    def _get_environment_setting(self) -> str:
        """
        Get environment setting for database service.

        Returns:
            Environment string for database initialization
        """
        if hasattr(self.config, "_environment") and self.config._environment:
            return self.config._environment
        return self.environment

    def scrape_all_pages(self, driver, entry_url: str) -> pd.DataFrame:
        """
        Scrape all pages of competition data with VPN enforcement.

        Args:
            driver: Selenium WebDriver instance
            entry_url: Starting URL for scraping

        Returns:
            DataFrame with all scraped competition data

        Raises:
            VpnRequiredError: If VPN protection fails
        """

        # ***> Initialize parser and ensure VPN protection <***
        self.parser = CompetitionTableParser(entry_url)
        self.vpn_handler.ensure_vpn_protection()

        # ***> Navigate to starting URL with VPN timing <***
        driver.get(entry_url)
        self.vpn_handler.handle_request_timing(
            "starting to navigate ..."
        )  # this line calls on mandatory delay

        # ***> Initialize collection variables <***
        all_data = []
        page_num = 1
        total_competitions = 0

        # ***> Main scraping loop preserving original logic <***
        while page_num <= self.config.max_pages:
            try:
                # ***> Process current page <***
                page_data = self._scrape_single_page(driver, page_num)

                if not page_data.empty:
                    # ***> Add metadata preserving original logic <***
                    page_data["page_number"] = page_num
                    page_data["scraped_at"] = pd.Timestamp.now()

                    all_data.append(page_data)
                    total_competitions += len(page_data)

                # ***> Navigate to next page <***
                if not self.navigator.navigate_to_next_page(driver, self.vpn_handler):
                    break

                page_num += 1

            except VpnRequiredError:
                raise
            except Exception as error:
                if "timeout" in str(error).lower():
                    break
                continue

        # ***> Combine all data preserving original logic <***
        final_df = (
            pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()
        )

        # ***> Save to database if configured <***
        self._handle_database_save(final_df)

        return final_df

    def _scrape_single_page(self, driver, page_num: int) -> pd.DataFrame:
        """
        Scrape data from a single page.

        Args:
            driver: Selenium WebDriver instance
            page_num: Current page number

        Returns:
            DataFrame with page data
        """
        try:
            # ***> Parse HTML content <***
            soup = BeautifulSoup(driver.page_source, "html.parser")
            page_data = self.parser.parse_competition_table(soup)

            return page_data

        except Exception:
            return pd.DataFrame()

    def _handle_database_save(self, final_df: pd.DataFrame) -> bool:
        """
        Handle database save operation using new service interface.

        Args:
            final_df: Final scraped data

        Returns:
            True if saved to database successfully
        """
        if final_df.empty or not self.database_service:
            return False

        try:
            return self.database_service.add_competitions_bulk(final_df)
        except (DatabaseServiceError, DatabaseOperationError):
            return False

    def _handle_database_initialization_error(self, error: Exception) -> None:
        """
        Handle database initialization errors.

        Args:
            error: Database operation error
        """
        print(
            f"Warning: Database initialization failed in {self.environment} environment: {error}"
        )
        # Don't raise - allow scraper to continue without database

    def cleanup(self) -> None:
        """
        Clean up all resources including database and VPN connections.
        """
        try:
            # ***> Cleanup database service <***
            if self.database_service:
                self.database_service.cleanup()

            # ***> Cleanup VPN connections <***
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
        return self.database_service is not None and self._check_database_health()

    def _check_database_health(self) -> bool:
        """
        Check database health through the service.

        Returns:
            True if database is healthy
        """
        try:
            if not self.database_service:
                return False

            # Use the database manager's health check
            return self.database_service.db_manager.health_check()
        except Exception:
            return False

    def get_database_info(self) -> dict:
        """
        Get database connection information for monitoring.

        Returns:
            Dictionary with database information
        """
        if not self.database_service:
            return {"status": "not_initialized"}

        try:
            info = self.database_service.db_manager.get_connection_info()
            info["service_environment"] = self.environment
            info["service_available"] = self.database_available
            return info
        except Exception as error:
            return {"status": "error", "error": str(error)}
