# extractors/navigation/navigation_manager.py
"""
Navigation management for web scraping operations.
Handles page navigation, URL manipulation and pagination.
"""

from typing import Optional

from bs4 import BeautifulSoup

from exceptions import NavigationError, VpnRequiredError
from logger import HTMLConstants, ScrapingConstants

from .navigation_config import NavigationConfig
from .page_number_extractor import PageNumberExtractor
from .pagination_finder import PaginationFinder
from .url_parser import URLParser


class NavigationManager:
    """
    Orchestrates web page navigation operations.

    Coordinates between different components to handle navigation
    while maintaining separation of concerns and testability.
    """

    def __init__(self, logger_name: str = "NavigationManager"):
        """
        Initialize navigation manager with all required components.

        Args:
            logger_name: Name for the logger instance
        """
        # Initialize configuration
        self.config = NavigationConfig.from_constants(HTMLConstants, ScrapingConstants)

        # Initialize logger
        print(f"NavigationManager initialized: {logger_name}")

        # Initialize components
        self.url_parser = URLParser(self.config)
        self.page_extractor = PageNumberExtractor(self.config)
        self.pagination_finder = PaginationFinder(self.config)

    def navigate_to_next_page(self, driver, vpn_handler=None) -> bool:
        """
        Navigate to the next page in pagination.

        Args:
            driver: Selenium WebDriver instance
            vpn_handler: Optional VPN handler for request timing

        Returns:
            True if navigation successful, False if no next page

        Raises:
            NavigationError: If navigation fails due to technical issues
            VpnRequiredError: If VPN protection fails
        """
        try:
            current_url = driver.current_url
            print(
                f"Current URL: {self.url_parser.truncate_url_for_display(current_url)}"
            )

            # Parse current page for pagination
            soup = BeautifulSoup(driver.page_source, "html.parser")
            next_url = self._find_next_page_url(soup, current_url)

            if not next_url:
                print("No next page URL found")
                return False

            # Check if we're trying to go to the same page
            if next_url == current_url:
                print("Next URL is same as current URL")
                return False

            current_page = self.page_extractor.get_current_page_number(soup)
            next_page_num = current_page + 1

            print(
                f"Navigating to page {next_page_num}: {self.url_parser.truncate_url_for_display(next_url)}"
            )

            # Navigate with VPN-aware timing if handler provided
            driver.get(next_url)
            if vpn_handler:
                vpn_handler.handle_request_timing(
                    "navigating to next page, please wait..."
                )

            # Verify navigation worked
            new_url = driver.current_url
            if new_url != current_url:
                print(
                    f"Successfully navigated to: {self.url_parser.truncate_url_for_display(new_url)}"
                )
                return True
            else:
                print("Navigation failed - URL unchanged")
                return False

        except VpnRequiredError:
            # Re-raise VPN errors immediately
            print("VPN protection failure during navigation")
            raise
        except Exception as e:
            error_msg = f"Error navigating to next page: {e}"
            print(error_msg)
            raise NavigationError(error_msg)

    def _find_next_page_url(
        self, soup: BeautifulSoup, current_url: str
    ) -> Optional[str]:
        """
        Find the URL for the next page using multiple strategies.

        Args:
            soup: BeautifulSoup object of current page
            current_url: Current page URL

        Returns:
            Next page URL or None if not found
        """
        # Look for the pagination container
        pagination = self.pagination_finder.find_pagination_container(soup)
        if not pagination:
            return None

        current_page = self.page_extractor.get_current_page_number(soup)
        next_page_num = current_page + 1

        # Strategy 1: Look for specific next page number link
        next_url = self.pagination_finder.find_next_page_by_number(
            pagination, next_page_num
        )
        if next_url:
            return self.url_parser.make_absolute_url(next_url)

        # Strategy 2: Look for next page icon button
        next_url = self.pagination_finder.find_next_page_by_icon(pagination)
        if next_url:
            return self.url_parser.make_absolute_url(next_url)

        # Strategy 3: Construct next page URL manually
        return self.url_parser.construct_next_page_url(current_url, next_page_num)
