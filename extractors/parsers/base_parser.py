# parsers/base_parser.py
"""
Base parser class providing common functionality for all HTML parsers.
Contains shared methods and utilities used by specific parser implementations.
"""

from typing import List, Optional

from bs4 import BeautifulSoup, Tag

from .parser_config import ParserConfig


class BaseParser:
    """
    Base class for all HTML parsers providing common functionality.
    Contains shared methods for row filtering and basic table operations.
    """

    def __init__(self, entry_url: str = ""):
        """
        Initialize base parser with configuration.

        Args:
            entry_url: URL being parsed (used for configuration decisions)
        """
        self.entry_url = entry_url
        self.config = ParserConfig()

    def _should_skip_header_row(self, row: Tag) -> bool:
        """
        Check if row should be skipped because it's a header row.

        Args:
            row: BeautifulSoup row element to check

        Returns:
            True if row contains header elements and should be skipped
        """
        return row.find(self.config.TABLE_HEADER_SELECTOR) is not None

    def _should_skip_footer_row(self, row: Tag) -> bool:
        """
        Check if row should be skipped because it's a footer row.

        Args:
            row: BeautifulSoup row element to check

        Returns:
            True if row has footer classes and should be skipped
        """
        row_classes = row.get("class", [])
        return any(
            skip_class in row_classes for skip_class in self.config.SKIP_ROW_CLASSES
        )

    def _get_table_cells(self, row: Tag) -> List[Tag]:
        """
        Extract all table cells from a row.

        Args:
            row: BeautifulSoup row element

        Returns:
            List of table cell elements
        """
        return row.find_all(self.config.TABLE_CELL_SELECTOR)

    def _get_all_table_rows(self, soup: BeautifulSoup) -> List[Tag]:
        """
        Extract all table rows from HTML soup.

        Args:
            soup: BeautifulSoup object containing the HTML

        Returns:
            List of table row elements
        """
        return soup.find_all(self.config.TABLE_ROW_SELECTOR)

    def _has_minimum_columns(self, cells: List[Tag], min_required: int) -> bool:
        """
        Check if row has minimum required number of columns.

        Args:
            cells: List of table cell elements
            min_required: Minimum number of columns required

        Returns:
            True if row has enough columns
        """
        return len(cells) >= min_required

    def _is_extrarow_tier(self, row: Tag) -> bool:
        """
        Check if row is an extrarow containing tier information.

        Args:
            row: BeautifulSoup row element

        Returns:
            True if row contains extrarow tier information
        """
        extrarow_cell = row.find(
            self.config.TABLE_CELL_SELECTOR, class_=self.config.EXTRAROW_CLASS
        )
        return extrarow_cell is not None

    def _extract_tier_text(self, row: Tag) -> Optional[str]:
        """
        Extract tier text from an extrarow tier element.

        Args:
            row: BeautifulSoup row element containing tier info

        Returns:
            Tier text string or None if not found
        """
        extrarow_cell = row.find(
            self.config.TABLE_CELL_SELECTOR, class_=self.config.EXTRAROW_CLASS
        )
        if extrarow_cell:
            return extrarow_cell.get_text(strip=True)
        return None
