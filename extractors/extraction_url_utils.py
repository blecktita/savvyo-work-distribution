# core/scrapers/utils/url_utils.py
"""
URL parsing and manipulation utilities.
Handles URL component extraction and modification for competition and club URLs.
"""

import re
from typing import Tuple

from exceptions import ParsingError

from .extraction_config import ExtractionConfig


class URLParser:
    """
    Utility class for parsing and manipulating URLs.

    Provides methods to extract components from competition and club URLs,
    and modify URLs with season information.
    """

    def __init__(self):
        """Initialize URL parser with configuration."""
        self.config = ExtractionConfig()

    def extract_competition_components(self, url: str) -> Tuple[str, str]:
        """
        Extract code and ID from competition URL.

        Args:
            url: Competition URL to parse

        Returns:
            Tuple of (competition_code, competition_id)

        Example:
            url = "https://site.com/premier-league/startseite/wettbewerb/GB1"
            Returns: ("premier-league", "GB1")
        """
        if not isinstance(url, str):
            return "", ""

        try:
            url_parts = [part for part in url.split("/") if part]

            code = ""
            competition_id = ""

            for i, part in enumerate(url_parts):
                if part == "startseite" and i > 0:
                    code = url_parts[i - 1]  # Part before 'startseite'
                elif part == "wettbewerb" and i < len(url_parts) - 1:
                    competition_id = url_parts[i + 1]  # Part after 'wettbewerb'

            return code or "", competition_id or ""

        except Exception as error:
            raise ParsingError(self.config.ERROR_MESSAGES["url_parsing"].format(error))

    def extract_club_id_from_url(self, url: str) -> str:
        """
        Extract club ID from club URL.

        Args:
            url: Club URL to parse

        Returns:
            Club ID string

        Example:
            url = "https://site.com/manchester-city/startseite/verein/281"
            Returns: "281"
        """
        if not isinstance(url, str):
            return ""

        try:
            # Extract from URL pattern: /verein/281/
            match = re.search(self.config.CLUB_ID_PATTERN, url)
            if match:
                return str(match.group(1))

            # Fallback: use URL path as ID
            url_parts = [part for part in url.split("/") if part]
            if url_parts:
                return str(url_parts[0])

            return ""

        except Exception:
            return ""

    def extract_club_code_from_url(self, url: str) -> str:
        """
        Extract club code from club URL.

        Args:
            url: Club URL to parse

        Returns:
            Club code string

        Example:
            url = "https://site.com/manchester-city/startseite/verein/281"
            Returns: "manchester-city"
        """
        if not isinstance(url, str):
            return ""

        try:
            # Extract from URL pattern before /startseite/verein/
            match = re.search(self.config.CLUB_CODE_PATTERN, url)
            if match:
                return str(match.group(1))

            # Alternative: look for pattern before 'startseite'
            url_parts = url.split("/")
            for i, part in enumerate(url_parts):
                if part == "startseite" and i > 0:
                    return str(url_parts[i - 1])

            return ""

        except Exception:
            return ""

    def fix_season_in_url(self, url: str, season_year: str) -> str:
        """
        Fix the season year in the club URL.

        Args:
            url: Original club URL
            season_year: Desired season year (e.g., "2023")

        Returns:
            URL with corrected season parameter

        Example:
            url = "https://site.com/club/startseite/verein/281/saison_id/2022"
            season_year = "2024"
            Returns: "https://site.com/club/startseite/verein/281/saison_id/2024"
        """
        if not isinstance(url, str) or not isinstance(season_year, str):
            return url

        try:
            # Replace existing season if present
            if "saison_id/" in url:
                url = re.sub(
                    self.config.URL_COMPONENT_PATTERN, f"/saison_id/{season_year}", url
                )
            else:
                # Add season parameter if not present
                if url.endswith("/"):
                    url = f"{url}saison_id/{season_year}"
                else:
                    url = f"{url}/saison_id/{season_year}"

            return url

        except Exception:
            return url

    def is_europa_page(self, url: str) -> bool:
        """
        Check if URL is for Europa competitions page.

        Args:
            url: URL to check

        Returns:
            True if this is a Europa page
        """
        return self.config.EUROPA_URL_IDENTIFIER in str(url)

    def is_europa_jugend_page(self, url: str) -> bool:
        """
        Check if URL is for Europa Jugend competitions page.

        Args:
            url: URL to check

        Returns:
            True if this is a Europa Jugend page
        """
        return self.config.EUROPA_JUGEND_URL_IDENTIFIER in str(url)
