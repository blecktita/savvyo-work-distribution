# core/scrapers/components/page_number_extractor.py
"""
Page number extraction utilities.
Handles extracting current page numbers from HTML pagination.
"""

from typing import Optional

from bs4 import BeautifulSoup

from .navigation_config import NavigationConfig


class PageNumberExtractor:
    """
    Extracts page numbers from HTML pagination elements.

    Responsible for finding and parsing current page numbers
    from pagination containers in HTML.
    """

    def __init__(self, config: NavigationConfig):
        """
        Initialize page number extractor with configuration.

        Args:
            config: NavigationConfig instance with pagination settings
        """
        self.config = config

    def get_current_page_number(self, soup: BeautifulSoup) -> int:
        """
        Extract current page number from pagination HTML.

        Args:
            soup: BeautifulSoup object of current page

        Returns:
            Current page number (defaults to 1 if not found)
        """
        try:
            pagination = self._find_pagination_container(soup)
            if not pagination:
                return self.config.DEFAULT_PAGE_NUMBER

            active_page = pagination.find(
                "li", class_=self.config.PAGINATION_ACTIVE_CLASS
            )

            if active_page:
                page_link = active_page.find("a")
                if page_link:
                    page_text = page_link.get_text(strip=True)
                    return int(page_text)

        except (ValueError, AttributeError) as e:
            print(f"Error extracting page number: {e}")

        return self.config.DEFAULT_PAGE_NUMBER

    def _find_pagination_container(
        self, soup: BeautifulSoup
    ) -> Optional[BeautifulSoup]:
        """
        Find the pagination container in the HTML.

        Args:
            soup: BeautifulSoup object of current page

        Returns:
            Pagination container element or None if not found
        """
        pagination = soup.find("ul", class_=self.config.PAGINATION_CLASS)
        if not pagination:
            print("No pagination container found")
        return pagination
