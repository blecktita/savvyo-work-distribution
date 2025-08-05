# extractors/parsers/competition_parser.py

"""
Competition table parser for extracting competition data from HTML tables.
Specialized parser focusing on competition-specific parsing logic.
"""

from typing import Dict, List, Optional

import pandas as pd
from bs4 import BeautifulSoup

from exceptions import ParsingError
from extractors.extractor_competition import CompetitionRowExtractor

from .base_parser import BaseParser


class CompetitionTableParser(BaseParser):
    """
    HTML parser specialized for extracting competition data from tables.
    Inherits common functionality from BaseParser and adds competition-specific logic.
    """

    def __init__(self, entry_url: str):
        """
        Initialize competition parser with URL and row extractor.

        Args:
            entry_url: URL being parsed for competition data
        """
        super().__init__(entry_url)
        self.row_extractor = CompetitionRowExtractor(entry_url)
        self.current_tier: Optional[str] = None

    def parse_competition_table(self, soup: BeautifulSoup) -> pd.DataFrame:
        """
        Parse the competition table from HTML soup.

        Args:
            soup: BeautifulSoup object containing the HTML

        Returns:
            DataFrame with extracted competition data

        Raises:
            ParsingError: If parsing fails critically
        """
        try:
            rows = self._get_all_table_rows(soup)

            if not rows:
                return pd.DataFrame()

            data = self._process_all_rows(rows)
            return pd.DataFrame(data) if data else pd.DataFrame()

        except Exception as e:
            error_msg = f"Failed to parse competition table: {e}"
            raise ParsingError(error_msg)

    def _process_all_rows(self, rows: List) -> List[Dict]:
        """
        Process all table rows and extract competition data.

        Args:
            rows: List of BeautifulSoup row elements

        Returns:
            List of competition data dictionaries
        """
        data = []
        self.current_tier = None  # Reset tier tracking

        for row in rows:
            if self._should_skip_competition_row(row):
                continue

            if self._is_extrarow_tier(row):
                self._update_current_tier(row)
                continue

            competition_data = self._process_data_row(row)
            if competition_data:
                data.append(competition_data)

        return data

    def _should_skip_competition_row(self, row) -> bool:
        """
        Check if competition row should be skipped.

        Args:
            row: BeautifulSoup row element

        Returns:
            True if row should be skipped
        """
        return self._should_skip_header_row(row)

    def _update_current_tier(self, row) -> None:
        """
        Update current tier from tier row.

        Args:
            row: BeautifulSoup row element containing tier info
        """
        tier_text = self._extract_tier_text(row)
        if tier_text:
            self.current_tier = tier_text

    def _process_data_row(self, row) -> Optional[Dict]:
        """
        Process a data row and extract competition information.

        Args:
            row: BeautifulSoup row element

        Returns:
            Competition data dictionary or None if processing fails
        """
        cells = self._get_table_cells(row)

        # Get minimum columns required based on URL
        min_columns_required = self.config.get_min_columns_for_url(self.entry_url)

        # Must have at least minimum required columns
        if not self._has_minimum_columns(cells, min_columns_required):
            return None

        try:
            return self.row_extractor.extract_competition_from_row(
                cells, self.current_tier
            )
        except Exception:
            return None
