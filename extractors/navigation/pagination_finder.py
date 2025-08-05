# extractors/navigation/pagination_finder.py
"""
Pagination link finding utilities.
Handles finding next page links using different strategies.
"""

from typing import Optional

from bs4 import BeautifulSoup

from .navigation_config import NavigationConfig


class PaginationFinder:
    """
    Finds pagination links using multiple strategies.

    Responsible for locating next page URLs through different
    methods like page number links and navigation icons.
    """

    def __init__(self, config: NavigationConfig):
        """
        Initialize pagination finder with configuration.

        Args:
            config: NavigationConfig instance with pagination settings
        """
        self.config = config

    def find_next_page_by_number(self, pagination, next_page_num: int) -> Optional[str]:
        """
        Find next page URL by searching for specific page number link.

        Args:
            pagination: Pagination container element
            next_page_num: Expected next page number to find

        Returns:
            Next page URL if found, None otherwise
        """
        page_links = pagination.find_all("a", class_=self.config.PAGINATION_LINK_CLASS)

        for link in page_links:
            href = link.get("href", "")
            page_text = link.get_text(strip=True)

            try:
                if int(page_text) == next_page_num:
                    print(f"Found next page link by number: {href}")
                    return href
            except ValueError:
                continue

        return None

    def find_next_page_by_icon(self, pagination) -> Optional[str]:
        """
        Find next page URL by searching for next page icon button.

        Args:
            pagination: Pagination container element

        Returns:
            Next page URL if found, None otherwise
        """
        next_icon = pagination.find("li", class_=self.config.PAGINATION_NEXT_CLASS)

        if next_icon:
            next_link = next_icon.find("a")
            if next_link:
                href = next_link.get("href", "")
                print(f"Found next page icon link: {href}")
                return href

        return None

    def find_pagination_container(self, soup: BeautifulSoup) -> Optional[BeautifulSoup]:
        """
        Find the main pagination container in HTML.

        Args:
            soup: BeautifulSoup object of current page

        Returns:
            Pagination container element or None if not found
        """
        pagination = soup.find("ul", class_=self.config.PAGINATION_CLASS)
        if not pagination:
            print("No pagination container found")
        return pagination
