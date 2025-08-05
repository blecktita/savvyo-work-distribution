# core/scrapers/components/url_parser.py
"""
URL parsing and manipulation utilities.
Handles URL construction and absolute URL conversion.
"""

import re

from .navigation_config import NavigationConfig


class URLParser:
    """
    Handles URL parsing and manipulation operations.

    Responsible for constructing URLs, making them absolute,
    and handling URL parameter manipulation.
    """

    def __init__(self, config: NavigationConfig):
        """
        Initialize URL parser with configuration.

        Args:
            config: NavigationConfig instance with URL settings
        """
        self.config = config

    def construct_next_page_url(self, current_url: str, next_page_num: int) -> str:
        """
        Construct next page URL manually by manipulating parameters.

        Args:
            current_url: Current page URL
            next_page_num: Next page number to construct URL for

        Returns:
            Constructed next page URL
        """
        page_param = self.config.PAGE_PARAMETER

        if f"{page_param}=" in current_url:
            # Replace existing page parameter
            next_url = re.sub(
                rf"{page_param}=\d+", f"{page_param}={next_page_num}", current_url
            )
        else:
            # Add page parameter
            separator = "&" if "?" in current_url else "?"
            next_url = f"{current_url}{separator}{page_param}={next_page_num}"

        return next_url

    def make_absolute_url(self, url: str) -> str:
        """
        Convert relative URL to absolute URL.

        Args:
            url: URL to convert (can be relative or absolute)

        Returns:
            Absolute URL
        """
        if url.startswith("/"):
            return f"{self.config.BASE_URL}{url}"
        return url

    def truncate_url_for_display(self, url: str) -> str:
        """
        Truncate URL for display purposes.

        Args:
            url: URL to truncate

        Returns:
            Truncated URL with ellipsis if needed
        """
        if len(url) > self.config.URL_DISPLAY_LIMIT:
            return f"{url[: self.config.URL_DISPLAY_LIMIT]}..."
        return url
