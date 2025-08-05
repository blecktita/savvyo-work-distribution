# extractors/navigation/navigation_config.py
"""
Configuration settings for navigation operations.
Centralizes all navigation-related constants and settings.
"""

from dataclasses import dataclass


@dataclass
class NavigationConfig:
    """
    Configuration class for navigation operations.

    Contains all navigation-related settings and constants
    to avoid hardcoding values throughout the application.
    """

    # Logging configuration
    DEFAULT_LOGGER_NAME: str = "NavigationManager"
    LOG_FILE_PATH: str = "logs/navigation.log"

    # URL display limits
    URL_DISPLAY_LIMIT: int = 80

    # Default page number
    DEFAULT_PAGE_NUMBER: int = 1

    # URL parameter names
    PAGE_PARAMETER: str = "page"

    # Pagination selectors from HTMLConstants
    PAGINATION_CLASS: str = "pagination"
    PAGINATION_ACTIVE_CLASS: str = "active"
    PAGINATION_LINK_CLASS: str = "page-link"
    PAGINATION_NEXT_CLASS: str = "next"

    # Base URL from ScrapingConstants
    BASE_URL: str = "https://example.com"  # This should be set from ScrapingConstants

    @classmethod
    def from_constants(cls, html_constants, scraping_constants):
        """
        Create configuration from existing constants classes.

        Args:
            html_constants: HTMLConstants class instance
            scraping_constants: ScrapingConstants class instance

        Returns:
            NavigationConfig instance with values from constants
        """
        config = cls()
        config.PAGINATION_CLASS = html_constants.PAGINATION_CLASS
        config.PAGINATION_ACTIVE_CLASS = html_constants.PAGINATION_ACTIVE_CLASS
        config.PAGINATION_LINK_CLASS = html_constants.PAGINATION_LINK_CLASS
        config.PAGINATION_NEXT_CLASS = html_constants.PAGINATION_NEXT_CLASS
        config.BASE_URL = scraping_constants.BASE_URL
        return config
