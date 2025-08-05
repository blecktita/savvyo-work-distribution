# pipelines/run_competition_club_collection.py
"""
Main competition scraper API interface.
"""

from typing import Optional

import pandas as pd

from configurations import ConfigFactory, EnvironmentVariables, ScraperConfig

from .princpal_orchestrator import ClubOrchestrator, CompetitionOrchestrator


class ScrapingConstants:
    """
    Configuration constants for scraping operations.
    """

    # ***> Scraper names <***
    COMPETITION_SCRAPER_NAME = "CompetitionScraper"
    CLUB_SCRAPER_NAME = "ClubScraper"

    # ***> Environment configuration mapping <***
    ENVIRONMENT_CONFIG_MAP = {
        "development": "development",
        "testing": "testing",
        "production": "production",
    }


class BaseScraper:
    """
    Base class for all scraping orchestrators providing common functionality.
    """

    def __init__(
        self,
        orchestrator_class,
        config: Optional[ScraperConfig] = None,
        env_file_path: Optional[EnvironmentVariables] = None,
    ):
        """
        Initialize base scraper with orchestrator setup.

        Args:
            orchestrator_class: Orchestrator class to instantiate
            config: Scraper configuration (uses development if None)
            env_file_path: Environment file path for configuration

        Raises:
            VpnRequiredError: If VPN is required but not available
            ConfigurationError: If configuration is invalid
        """
        self.orchestrator = orchestrator_class(config, env_file_path)

    def cleanup(self) -> None:
        """
        Clean up all allocated resources and connections.
        """
        self.orchestrator.cleanup()

    @property
    def config(self) -> ScraperConfig:
        """
        Get current scraper configuration.

        Returns:
            ScraperConfig: Current configuration object
        """
        return self.orchestrator.config

    @property
    def vpn_protection_active(self) -> bool:
        """
        Check if VPN protection is currently active.

        Returns:
            bool: True if VPN protection is active, False otherwise
        """
        return self.orchestrator.vpn_protection_active

    @property
    def database_available(self) -> bool:
        """
        Check if database connection is available.

        Returns:
            bool: True if database is available, False otherwise
        """
        return self.orchestrator.database_available


class CompetitionScraper(BaseScraper):
    """
    Competition scraper interface maintaining backward compatibility.
    """

    def __init__(self, config: Optional[ScraperConfig] = None):
        """
        Initialize competition scraper with default configuration.

        Args:
            config: Scraper configuration (uses development if None)

        Raises:
            VpnRequiredError: If VPN is required but not available
            ConfigurationError: If configuration is invalid
        """
        super().__init__(orchestrator_class=CompetitionOrchestrator, config=config)

    def scrape_all_pages(self, driver, entry_url: str) -> pd.DataFrame:
        """
        Scrape all pages of competition data from the given entry URL.

        Args:
            driver: Selenium WebDriver instance for web scraping
            entry_url: Starting URL for scraping operation

        Returns:
            pd.DataFrame: DataFrame containing all scraped competition data

        Raises:
            VpnRequiredError: If VPN protection fails during scraping
        """
        return self.orchestrator.scrape_all_pages(driver, entry_url)


class ClubScraper(BaseScraper):
    """
    Club scraper interface maintaining API consistency with CompetitionScraper.
    """

    def __init__(
        self,
        config: Optional[ScraperConfig] = None,
        env_file_path: Optional[EnvironmentVariables] = None,
    ):
        """
        Initialize club scraper with configuration and environment setup.

        Args:
            config: Scraper configuration (uses development if None)
            env_file_path: Environment file path for configuration

        Raises:
            VpnRequiredError: If VPN is required but not available
            ConfigurationError: If configuration is invalid
        """
        super().__init__(
            orchestrator_class=ClubOrchestrator,
            config=config,
            env_file_path=env_file_path,
        )

    def scrape_all_club_data(self, driver) -> pd.DataFrame:
        """
        Scrape comprehensive club data for all competitions across seasons.

        Args:
            driver: Selenium WebDriver instance for web scraping

        Returns:
            pd.DataFrame: DataFrame containing all club data

        Raises:
            VpnRequiredError: If VPN protection fails during scraping
        """
        return self.orchestrator.scrape_all_club_data(driver)


class ScrapingService:
    """
    Service class handling common scraping operations and shared logic.
    """

    @staticmethod
    def get_environment_config(environment: str) -> ScraperConfig:
        """
        Get configuration based on specified environment.

        Args:
            environment: Configuration environment name

        Returns:
            ScraperConfig: Configuration instance for the environment
        """
        config_map = {
            ScrapingConstants.ENVIRONMENT_CONFIG_MAP[
                "development"
            ]: ConfigFactory.development,
            ScrapingConstants.ENVIRONMENT_CONFIG_MAP["testing"]: ConfigFactory.testing,
            ScrapingConstants.ENVIRONMENT_CONFIG_MAP[
                "production"
            ]: ConfigFactory.production,
        }

        config_factory = config_map.get(environment, ConfigFactory.development)
        config = config_factory()
        config._environment = environment
        return config

    @staticmethod
    def setup_scraper_config(
        environment: str, max_pages: Optional[int] = None, use_vpn: bool = True
    ) -> ScraperConfig:
        """
        Setup scraper configuration with optional parameter overrides.

        Args:
            environment: Configuration environment identifier
            max_pages: Maximum pages to scrape (overrides config default)
            use_vpn: Whether to use VPN rotation functionality

        Returns:
            ScraperConfig: Configured scraper configuration instance
        """
        config = ScrapingService.get_environment_config(environment)

        # ***> Override parameters if provided <***
        if max_pages:
            config.max_pages = max_pages

        config.use_vpn = use_vpn
        return config


def _execute_scraping_workflow(
    scraper_class,
    scraper_method: str,
    environment: str,
    use_vpn: bool,
    driver=None,
    entry_url: Optional[str] = None,
    max_pages: Optional[int] = None,
) -> pd.DataFrame:
    """
    Execute common scraping workflow with consistent error handling.

    Args:
        scraper_class: Class to instantiate for scraping operation
        scraper_method: Method name to call on scraper instance
        environment: Configuration environment to use
        use_vpn: Whether to use VPN protection
        driver: Selenium WebDriver instance
        entry_url: Starting URL (required for competition scraping)
        max_pages: Maximum pages to scrape (optional override)

    Returns:
        pd.DataFrame: Scraped data

    Raises:
        VpnRequiredError: If VPN is required but not available
    """
    # ***> Setup configuration with provided parameters <***
    config = ScrapingService.setup_scraper_config(
        environment=environment, max_pages=max_pages, use_vpn=use_vpn
    )

    # ***> Initialize scraper <***
    scraper = scraper_class(config)

    try:
        # ***> Execute scraping based on method requirements <***
        if entry_url:
            # ***> Competition scraping requires entry_url <***
            data = getattr(scraper, scraper_method)(driver, entry_url)
        else:
            # ***> Club scraping doesn't need entry_url <***
            data = getattr(scraper, scraper_method)(driver)

        return data

    finally:
        # ***> Clean up resources regardless of success/failure <***
        scraper.cleanup()


# ***> Convenience functions for backward compatibility <***
def scrape_competitions(
    driver,
    entry_url: str,
    environment: str = "development",
    max_pages: Optional[int] = None,
    use_vpn: bool = True,
) -> pd.DataFrame:
    """
    Convenience function to scrape competitions with STRICT VPN enforcement.

    Args:
        driver: Selenium WebDriver instance
        entry_url: Starting URL for scraping operation
        environment: Configuration environment (development/testing/production)
        max_pages: Maximum number of pages to scrape (overrides config)
        use_vpn: Whether to use VPN rotation (overrides config)

    Returns:
        pd.DataFrame: Scraped competition data

    Raises:
        VpnRequiredError: If VPN is required but not available
    """
    return _execute_scraping_workflow(
        scraper_class=CompetitionScraper,
        scraper_method="scrape_all_pages",
        environment=environment,
        use_vpn=use_vpn,
        driver=driver,
        entry_url=entry_url,
        max_pages=max_pages,
    )


def scrape_competitions_safe_mode(
    driver,
    entry_url: str,
    environment: str = "development",
    max_pages: Optional[int] = None,
) -> pd.DataFrame:
    """
    Convenience function to scrape competitions WITHOUT VPN protection.

    Args:
        driver: Selenium WebDriver instance
        entry_url: Starting URL for scraping operation
        environment: Configuration environment
        max_pages: Maximum number of pages to scrape

    Returns:
        pd.DataFrame: Scraped competition data
    """
    return scrape_competitions(
        driver=driver,
        entry_url=entry_url,
        environment=environment,
        max_pages=max_pages,
        use_vpn=False,
    )


def scrape_club_data(
    driver, environment: str = "development", use_vpn: bool = True
) -> pd.DataFrame:
    """
    Convenience function to scrape club data with VPN enforcement.

    Args:
        driver: Selenium WebDriver instance
        environment: Configuration environment (development/testing/production)
        use_vpn: Whether to use VPN rotation

    Returns:
        pd.DataFrame: Scraped club data

    Raises:
        VpnRequiredError: If VPN is required but not available
    """
    return _execute_scraping_workflow(
        scraper_class=ClubScraper,
        scraper_method="scrape_all_club_data",
        environment=environment,
        use_vpn=use_vpn,
        driver=driver,
    )


def scrape_club_data_safe_mode(
    driver, environment: str = "development"
) -> pd.DataFrame:
    """
    Convenience function to scrape club data WITHOUT VPN protection.

    Args:
        driver: Selenium WebDriver instance
        environment: Configuration environment

    Returns:
        pd.DataFrame: Scraped club data
    """
    return scrape_club_data(driver=driver, environment=environment, use_vpn=False)
