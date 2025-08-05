# core/scrapers/html_parser.py
"""
Main HTML parsing module for competition and club data extraction.
Provides simplified interface to the refactored parser components.
"""

from typing import Dict, List

import pandas as pd
from bs4 import BeautifulSoup

from extractors.parsers import ClubTableParser, CompetitionTableParser


class HTMLParser:
    """
    Main HTML parser class providing unified interface to all parsing functionality.
    Acts as a facade to the specialized parser components.
    """

    def __init__(self, entry_url: str = ""):
        """
        Initialize HTML parser with entry URL.

        Args:
            entry_url: URL being parsed (used for parser configuration)
        """
        self.entry_url = entry_url
        self.competition_parser = CompetitionTableParser(entry_url)
        self.club_parser = ClubTableParser()

    def parse_competition_table(self, soup: BeautifulSoup) -> pd.DataFrame:
        """
        Parse competition table from HTML soup.

        Args:
            soup: BeautifulSoup object containing the HTML

        Returns:
            DataFrame with extracted competition data

        Raises:
            ParsingError: If parsing fails critically
        """
        return self.competition_parser.parse_competition_table(soup)

    def parse_club_table(
        self, soup: BeautifulSoup, season_year: str, season_id: str, competition_id: str
    ) -> pd.DataFrame:
        """
        Parse club table from HTML soup.

        Args:
            soup: BeautifulSoup object containing the HTML
            season_year: Season year (e.g., "2024")
            season_id: Season display ID (e.g., "24/25")
            competition_id: Competition identifier

        Returns:
            DataFrame with extracted club data

        Raises:
            ParsingError: If parsing fails critically
        """
        return self.club_parser.parse_club_table(
            soup, season_year, season_id, competition_id
        )

    def parse_season_options(self, soup: BeautifulSoup) -> List[Dict[str, str]]:
        """
        Parse season options from the page.

        Args:
            soup: BeautifulSoup object containing the HTML

        Returns:
            List of season dictionaries
        """
        return self.club_parser.parse_season_options(soup)
