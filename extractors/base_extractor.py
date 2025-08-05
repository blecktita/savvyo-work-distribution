# core/scrapers/extractors/base_extractor.py
"""
Base data extraction utilities with common extraction methods.
Provides reusable extraction functionality for all data types.
"""

import re

from bs4 import Tag

from exceptions import ParsingError

from .extraction_config import ExtractionConfig


class BaseDataExtractor:
    """
    Base class providing common data extraction methods.

    This class contains reusable extraction methods that can be used
    by specialized extractors for different data types.
    """

    def __init__(self):
        """
        Initialize the base extractor with configuration
        """
        self.config = ExtractionConfig()

    def extract_text_from_cell(self, cell: Tag) -> str:
        """
        Extract clean text content from a table cell.

        Args:
            cell: BeautifulSoup Tag containing text data

        Returns:
            Clean text string or empty string if extraction fails
        """
        if not isinstance(cell, Tag):
            return ""

        try:
            return cell.get_text(strip=True)
        except Exception:
            return ""

    def extract_number(self, cell: Tag) -> int:
        """
        Extract integer number from table cell.

        Args:
            cell: BeautifulSoup Tag containing numeric data

        Returns:
            Extracted integer or 0 if extraction fails
        """
        if not isinstance(cell, Tag):
            return 0

        try:
            text = self.extract_text_from_cell(cell)

            if not text or text == "-":
                return 0

            # Remove commas and dots, find first number
            clean_text = text.replace(",", "").replace(".", "")
            match = re.search(self.config.NUMBER_PATTERN, clean_text)
            return int(match.group(1)) if match else 0

        except (ValueError, AttributeError, TypeError):
            return 0

    def extract_float(self, cell: Tag) -> float:
        """
        Extract float number from table cell.

        Args:
            cell: BeautifulSoup Tag containing float data

        Returns:
            Extracted float or 0.0 if extraction fails
        """
        if not isinstance(cell, Tag):
            return 0.0

        try:
            text = self.extract_text_from_cell(cell)

            if not text or text == "-":
                return 0.0

            # Look for decimal number pattern
            match = re.search(self.config.FLOAT_PATTERN, text)
            return float(match.group(1)) if match else 0.0

        except (ValueError, AttributeError, TypeError):
            return 0.0

    def extract_percentage(self, cell: Tag) -> float:
        """
        Extract percentage value from table cell.

        Args:
            cell: BeautifulSoup Tag containing percentage data

        Returns:
            Extracted percentage or 0.0 if extraction fails
        """
        if not isinstance(cell, Tag):
            return 0.0

        try:
            # Check if cell contains a link first
            link = cell.find("a")
            text = (
                link.get_text(strip=True) if link else self.extract_text_from_cell(cell)
            )

            if not text or text == "-":
                return 0.0

            # Look for percentage pattern
            match = re.search(self.config.PERCENTAGE_PATTERN, text)
            return float(match.group(1)) if match else 0.0

        except (ValueError, AttributeError, TypeError):
            return 0.0

    def extract_market_value(self, cell: Tag) -> float:
        """
        Extract market value from table cell and convert to millions.

        Args:
            cell: BeautifulSoup Tag containing market value data

        Returns:
            Market value in millions or 0.0 if extraction fails
        """
        if not isinstance(cell, Tag):
            return 0.0

        try:
            text = self.extract_text_from_cell(cell)

            if not text or text == "-":
                return 0.0

            # Pattern: €565.09m or €11.30bn
            match = re.search(self.config.MARKET_VALUE_PATTERN, text, re.IGNORECASE)

            if not match:
                return 0.0

            value_str = match.group(1).replace(",", "")
            unit = match.group(2).lower() if match.group(2) else "m"

            value = float(value_str)

            # Convert to millions using configured multipliers
            multiplier = self.config.MARKET_VALUE_MULTIPLIERS.get(unit, 1.0)
            return float(value * multiplier)

        except (ValueError, AttributeError, TypeError):
            return 0.0

    def extract_country_from_flag(self, cell: Tag) -> str:
        """
        Extract country from flag image in table cell.

        Args:
            cell: BeautifulSoup Tag containing flag image

        Returns:
            Country name or empty string if not found
        """
        if not isinstance(cell, Tag):
            return ""

        try:
            # Look for img with specific class first
            img = cell.find("img", class_=self.config.FLAGGENRAHMEN_CLASS)
            if not img:
                # Fallback: any img in the cell
                img = cell.find("img")

            if img and isinstance(img, Tag):
                country = str(img.get("title", "")).strip()
                if not country:
                    country = str(img.get("alt", "")).strip()

                if country:
                    return country

            # Final fallback: get text content
            return self.extract_text_from_cell(cell)

        except Exception:
            return ""

    def validate_cell_count(
        self, cells: list, required_count: int, context: str = ""
    ) -> bool:
        """
        Validate that the cell list has the required number of cells.

        Args:
            cells: List of table cells
            required_count: Minimum required number of cells
            context: Context information for error reporting

        Returns:
            True if validation passes

        Raises:
            ParsingError: If validation fails
        """
        if not isinstance(cells, list):
            raise ParsingError(self.config.ERROR_MESSAGES["invalid_cell"])
        if len(cells) < required_count:
            error_msg = (
                f"Insufficient cells in {context}. "
                f"Required: {required_count}, Found: {len(cells)}"
            )
            raise ParsingError(error_msg)

        return True

    def make_absolute_url(self, url: str) -> str:
        """
        Convert relative URL to absolute URL.

        Args:
            url: URL that might be relative

        Returns:
            Absolute URL string
        """
        if not isinstance(url, str):
            return ""

        if url.startswith("/"):
            return f"{self.config.BASE_URL}{url}"

        return url
