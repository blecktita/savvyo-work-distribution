# core/scrapers/extractors/competition_extractor.py
"""
Competition-specific data extraction utilities.
Handles extraction of competition information and competition row data.
"""

from typing import Any, Dict, List, Optional

from bs4 import Tag

from exceptions import ParsingError

from .base_extractor import BaseDataExtractor
from .extraction_config import ColumnMapping
from .extraction_url_utils import URLParser


class CompetitionDataExtractor(BaseDataExtractor):
    """
    Extractor for competition-specific data from HTML elements.

    Handles extraction of competition names, URLs, codes, and IDs
    from table cells containing competition information.
    """

    def __init__(self):
        """Initialize competition extractor with URL parser."""
        super().__init__()
        self.url_parser = URLParser()

    def extract_competition_info(self, cell: Tag) -> Optional[Dict[str, str]]:
        """
        Extract competition name, URL, code, and ID from table cell.

        Args:
            cell: BeautifulSoup Tag containing competition information

        Returns:
            Dictionary with competition info or None if extraction fails

        Raises:
            ParsingError: If cell structure is invalid
        """
        if not isinstance(cell, Tag):
            return None

        try:
            inline_table = cell.find("table", class_=self.config.INLINE_TABLE_CLASS)
            if not inline_table:
                return None

            # Find the competition name link
            name_link = self._find_competition_link(inline_table)
            if not name_link:
                return None

            name = name_link.get_text(strip=True)
            url = name_link.get("href")

            if not name or not url:
                return None

            # Make URL absolute
            absolute_url = self.make_absolute_url(url)

            # Extract competition code and ID from URL
            code, competition_id = self.url_parser.extract_competition_components(
                absolute_url
            )

            # Provide fallbacks for missing components
            if not code:
                code = name.lower().replace(" ", "-")
            if not competition_id:
                competition_id = code

            return {
                "name": name,
                "url": absolute_url,
                "code": code,
                "id": competition_id,
            }

        except Exception as error:
            raise ParsingError(
                self.config.ERROR_MESSAGES["competition_extraction"].format(error)
            )

    def _find_competition_link(self, inline_table: Tag) -> Optional[Tag]:
        """
        Find the competition name link from inline table.

        Args:
            inline_table: BeautifulSoup Tag containing links

        Returns:
            Competition link Tag or None if not found
        """
        # Find ALL links in the inline table
        links = inline_table.find_all("a")

        # Look for the text link (not the image link)
        for link in links:
            href = link.get("href", "")
            text = link.get_text(strip=True)

            # The name link has text and the right href pattern
            if text and self.config.COMPETITION_URL_PATTERN in href:
                return link

        return None


class CompetitionRowExtractor(BaseDataExtractor):
    """
    Extractor for complete competition data from table rows.

    Handles different column mappings based on the page type
    (Europa, Europa Jugend, or other regions).
    """

    def __init__(self, entry_url: str):
        """
        Initialize the row extractor.

        Args:
            entry_url: The URL being scraped to detect column structure
        """
        super().__init__()
        self.url_parser = URLParser()
        self.column_mapping = ColumnMapping()
        self.competition_extractor = CompetitionDataExtractor()

        # Determine page type from URL
        self.is_europa = self.url_parser.is_europa_page(entry_url)
        self.is_europa_jugend = self.url_parser.is_europa_jugend_page(entry_url)

    def extract_competition_from_row(
        self, cells: List[Tag], current_tier: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Extract competition data from table row cells with URL-based column mapping.

        Args:
            cells: List of table cell Tags
            current_tier: Current tier/category being processed

        Returns:
            Dictionary with competition data or None if extraction fails

        Raises:
            ParsingError: If row structure is invalid
        """
        try:
            # Validate cell count based on page type
            required_columns = self._get_required_column_count()
            self.validate_cell_count(cells, required_columns, "competition row")

            # Extract competition info from first cell
            competition_info = self.competition_extractor.extract_competition_info(
                cells[0]
            )
            if not competition_info:
                return None

            # Extract other data based on page type
            extracted_data = self._extract_data_by_page_type(cells)

            # Combine competition info with extracted data
            result = {
                "competition_name": competition_info["name"],
                "competition_url": competition_info["url"],
                "competition_code": competition_info["code"],
                "competition_id": competition_info["id"],
                "competition_type": self.config.DEFAULT_COMPETITION_TYPE,
                "tier": current_tier or self.config.DEFAULT_TIER,
            }
            result.update(extracted_data)

            return result

        except Exception as error:
            raise ParsingError(
                self.config.ERROR_MESSAGES["row_extraction"].format(error)
            )

    def _get_required_column_count(self) -> int:
        """Get the minimum required column count based on page type."""
        if self.is_europa_jugend:
            return self.config.COLUMN_REQUIREMENTS["europa_jugend"]
        elif self.is_europa:
            return self.config.COLUMN_REQUIREMENTS["europa"]
        else:
            return self.config.COLUMN_REQUIREMENTS["other_regions"]

    def _extract_data_by_page_type(self, cells: List[Tag]) -> Dict[str, Any]:
        """
        Extract data based on the page type and column mapping.

        Args:
            cells: List of table cell Tags

        Returns:
            Dictionary with extracted data
        """
        if self.is_europa_jugend:
            return self._extract_europa_jugend_data(cells)
        elif self.is_europa:
            return self._extract_europa_data(cells)
        else:
            return self._extract_other_regions_data(cells)

    def _extract_europa_jugend_data(self, cells: List[Tag]) -> Dict[str, Any]:
        """Extract data for Europa Jugend pages."""
        mapping = self.column_mapping.get_europa_jugend_mapping()

        return {
            "country": self.extract_country_from_flag(cells[mapping["country_flag"]]),
            "number_of_clubs": self.extract_number(cells[mapping["clubs"]]),
            "number_of_players": self.extract_number(cells[mapping["players"]]),
            "average_age_of_players": self.extract_float(cells[mapping["avg_age"]]),
            "percentage_of_foreign_players": self.extract_percentage(
                cells[mapping["foreigners_pct"]]
            ),
            "percentage_game_ratio_of_foreign_players": 0.0,  # Missing column
            "goals_per_match": 0.0,  # Missing column
            "average_market_value": 0.0,  # Missing column
            "total_market_value": self.extract_market_value(
                cells[mapping["total_market_value"]]
            ),
        }

    def _extract_europa_data(self, cells: List[Tag]) -> Dict[str, Any]:
        """Extract data for Europa pages."""
        mapping = self.column_mapping.get_europa_mapping()

        return {
            "country": self.extract_country_from_flag(cells[mapping["country_flag"]]),
            "number_of_clubs": self.extract_number(cells[mapping["clubs"]]),
            "number_of_players": self.extract_number(cells[mapping["players"]]),
            "average_age_of_players": self.extract_float(cells[mapping["avg_age"]]),
            "percentage_of_foreign_players": self.extract_percentage(
                cells[mapping["foreigners_pct"]]
            ),
            "percentage_game_ratio_of_foreign_players": self.extract_percentage(
                cells[mapping["game_ratio_pct"]]
            ),
            "goals_per_match": self.extract_float(cells[mapping["goals_per_match"]]),
            "average_market_value": self.extract_market_value(
                cells[mapping["avg_market_value"]]
            ),
            "total_market_value": self.extract_market_value(
                cells[mapping["total_market_value"]]
            ),
        }

    def _extract_other_regions_data(self, cells: List[Tag]) -> Dict[str, Any]:
        """Extract data for other region pages."""
        mapping = self.column_mapping.get_other_regions_mapping()

        return {
            "country": self.extract_country_from_flag(cells[mapping["country_flag"]]),
            "number_of_clubs": self.extract_number(cells[mapping["clubs"]]),
            "number_of_players": self.extract_number(cells[mapping["players"]]),
            "average_age_of_players": self.extract_float(cells[mapping["avg_age"]]),
            "percentage_of_foreign_players": self.extract_percentage(
                cells[mapping["foreigners_pct"]]
            ),
            "percentage_game_ratio_of_foreign_players": 0.0,  # Missing column
            "goals_per_match": self.extract_float(cells[mapping["goals_per_match"]]),
            "average_market_value": 0.0,  # Missing column
            "total_market_value": self.extract_market_value(
                cells[mapping["total_market_value"]]
            ),
        }
