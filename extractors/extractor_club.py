# core/scrapers/extractors/club_extractor.py
"""
Club-specific data extraction utilities.
Handles extraction of club information and club row data.
"""

from typing import Any, Dict, List, Optional

from bs4 import BeautifulSoup, Tag

from .base_extractor import BaseDataExtractor
from .extraction_config import ColumnMapping
from .extraction_url_utils import URLParser


class ClubDataExtractor(BaseDataExtractor):
    """
    Extractor for club-specific data from HTML elements.

    Handles extraction of club names, URLs, codes, and IDs
    from table cells containing club information.
    """

    def __init__(self):
        """
        Initialize club extractor with URL parser
        """
        super().__init__()
        self.url_parser = URLParser()

    def extract_club_info(
        self, cell: Tag, season_year: str = ""
    ) -> Optional[Dict[str, str]]:
        """
        Extract club name, URL, ID, and code from table cell.

        Args:
            cell: BeautifulSoup Tag containing club information
            season_year: Season year to use in URL (e.g., "2023")

        Returns:
            Dictionary with club info or None if extraction fails
        """
        if not isinstance(cell, Tag):
            return None

        try:
            # Find the club link
            club_link = cell.find("a", href=True)
            if not club_link or self.config.CLUB_URL_PATTERN not in str(
                club_link.get("href", "")
            ):
                return None

            name = club_link.get_text(strip=True)
            url = club_link.get("href")

            if not name or not url:
                return None

            # Ensure string types and make URL absolute
            name = str(name).strip()
            url = str(url).strip()

            if not name or not url:
                return None

            absolute_url = self.make_absolute_url(url)

            # Extract club components from URL
            try:
                club_id = self.url_parser.extract_club_id_from_url(absolute_url)
                club_code = self.url_parser.extract_club_code_from_url(absolute_url)
            except Exception as e:
                print(f"   ⚠️ URL parsing failed for {absolute_url}: {str(e)}")
                # Use fallback values
                club_id = "unknown"
                club_code = "unknown"

            # Fix season in URL if provided
            if season_year:
                try:
                    absolute_url = self.url_parser.fix_season_in_url(
                        absolute_url, str(season_year)
                    )
                except Exception as e:
                    print(f"   ⚠️ Season URL fix failed: {str(e)}")
                    # Continue with original URL

            return {
                "name": name,
                "url": absolute_url,
                "id": str(club_id),
                "code": str(club_code),
            }

        except Exception as error:
            print(f"   ⚠️ Club extraction failed: {str(error)}")
            return None

    def extract_season_options(self, soup: BeautifulSoup) -> List[Dict[str, str]]:
        """
        Extract season options from season select dropdown.

        Args:
            soup: BeautifulSoup object containing the HTML

        Returns:
            List of season dictionaries with 'year' and 'season_id' keys
        """
        if not isinstance(soup, BeautifulSoup):
            return []

        try:
            seasons = []

            # Find the season select dropdown with error handling
            season_select = soup.find("select", {"name": "saison_id"})
            if not season_select:
                print("   ⚠️ No season select dropdown found")
                return seasons

            # Extract all option elements safely
            try:
                options = season_select.find_all("option")
                if not options:
                    print("   ⚠️ No option elements found in season select")
                    return seasons
            except Exception as e:
                print(f"   ⚠️ Failed to find options: {str(e)}")
                return seasons

            for option in options:
                if not isinstance(option, Tag):
                    continue

                try:
                    # Safely extract year and season_id
                    year_value = option.get("value")
                    if not year_value:
                        continue

                    season_text = option.get_text(strip=True)
                    if not season_text:
                        continue

                    year = str(year_value).strip()
                    season_id = str(season_text).strip()

                    if year and season_id:
                        seasons.append({"year": year, "season_id": season_id})

                except Exception as e:
                    print(f"   ⚠️ Failed to extract option data: {str(e)}")
                    continue

            return seasons

        except Exception as error:
            print(f"   ❌ Season extraction failed: {str(error)}")
            return []


class ClubRowExtractor(BaseDataExtractor):
    """
    Extractor for complete club data from table rows.

    Handles extraction of club information along with associated
    statistics like squad size, market values, etc.
    """

    def __init__(self):
        """
        Initialize the club row extractor
        """
        super().__init__()
        self.club_extractor = ClubDataExtractor()
        self.column_mapping = ColumnMapping()

    def extract_club_from_row(
        self, cells: List[Tag], season_year: str, season_id: str, competition_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Extract club data from table row cells.

        Args:
            cells: List of table cell Tags
            season_year: Season year (e.g., "2024")
            season_id: Season display ID (e.g., "24/25")
            competition_id: Competition identifier

        Returns:
            Dictionary with club data or None if extraction fails
        """
        if not isinstance(cells, list) or not cells:
            return None

        # Check if all cells are valid Tags
        if not all(isinstance(cell, Tag) for cell in cells):
            return None

        cell_count = len(cells)

        # Validate minimum cell count
        min_required = 2  # We only need 2 columns minimum for club extraction
        if cell_count < min_required:
            print(
                f"   ❌ Need at least {min_required} columns for club info, got {cell_count}"
            )
            return None

        print(
            f"   ✅ Validation passed: {cell_count} columns available (minimum: {min_required})"
        )

        # Ensure string parameters
        season_year = str(season_year) if season_year else ""
        season_id = str(season_id) if season_id else ""
        competition_id = str(competition_id) if competition_id else ""

        try:
            # Extract club info from first available cells
            club_info = self._find_club_info_in_cells(cells, season_year)
            if not club_info:
                print(f"   ⚠️ No club info found in row for season {season_id}")
                return None

            # Extract additional data using column mapping with error handling
            try:
                mapping = self.column_mapping.get_club_mapping_safe(cell_count)
                extracted_data = self._extract_club_statistics(cells, mapping)
            except Exception as e:
                print(f"   ⚠️ Statistics extraction failed: {str(e)}")
                # Use default values if statistics extraction fails
                extracted_data = {
                    "squad_size": 0,
                    "average_age_of_players": 0.0,
                    "number_of_foreign_players": 0,
                    "average_market_value": 0.0,
                    "total_market_value": 0.0,
                }

            # Combine all data
            return {
                "club_id": str(club_info["id"]),
                "club_name": str(club_info["name"]),
                "club_code": str(club_info["code"]),
                "club_url": str(club_info["url"]),
                "season_year": season_year,
                "season_id": season_id,
                "competition_id": competition_id,
                **extracted_data,
            }

        except Exception as error:
            print(f"   ❌ Row extraction failed for season {season_id}: {str(error)}")
            return None

    def _find_club_info_in_cells(
        self, cells: List[Tag], season_year: str
    ) -> Optional[Dict[str, str]]:
        """
        Find club information in the first available cells.

        Args:
            cells: List of table cells
            season_year: Season year for URL fixing

        Returns:
            Club information dictionary or None if not found
        """
        # Club info is usually in the first two cells
        for i in range(min(2, len(cells))):
            club_info = self.club_extractor.extract_club_info(cells[i], season_year)
            if club_info:
                return club_info

        return None

    def _extract_club_statistics(
        self, cells: List[Tag], mapping: Dict[str, int]
    ) -> Dict[str, Any]:
        """
        Extract club statistics from cells using column mapping.

        Args:
            cells: List of table cells
            mapping: Column mapping configuration

        Returns:
            Dictionary with extracted statistics
        """
        try:
            # Safely extract basic statistics with bounds checking
            squad_size = 0
            average_age = 0.0
            foreign_players = 0

            if len(cells) > mapping.get("squad_size", 999):
                squad_size = self.extract_number(cells[mapping["squad_size"]])

            if len(cells) > mapping.get("average_age", 999):
                average_age = self.extract_float(cells[mapping["average_age"]])

            if len(cells) > mapping.get("foreign_players", 999):
                foreign_players = self.extract_number(cells[mapping["foreign_players"]])

            # Handle optional market value columns with bounds checking
            avg_market_value = 0.0
            total_market_value = 0.0

            # Extract average market value if column exists and is in bounds
            avg_mv_col = mapping.get("avg_market_value", 999)
            if avg_mv_col < len(cells):
                try:
                    avg_market_value = self.extract_market_value(cells[avg_mv_col])
                except Exception:
                    avg_market_value = 0.0

            # Extract total market value if column exists and is in bounds
            total_mv_col = mapping.get("total_market_value", 999)
            if total_mv_col < len(cells):
                try:
                    total_market_value = self.extract_market_value(cells[total_mv_col])
                except Exception:
                    total_market_value = 0.0

            return {
                "squad_size": int(squad_size),
                "average_age_of_players": float(average_age),
                "number_of_foreign_players": int(foreign_players),
                "average_market_value": float(avg_market_value),
                "total_market_value": float(total_market_value),
            }

        except Exception as e:
            print(f"   ⚠️ Statistics extraction error: {str(e)}")
            # Return default values on any error
            return {
                "squad_size": 0,
                "average_age_of_players": 0.0,
                "number_of_foreign_players": 0,
                "average_market_value": 0.0,
                "total_market_value": 0.0,
            }
