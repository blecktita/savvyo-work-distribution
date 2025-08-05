# extractors/extraction_config.py
"""
Configuration module for data extraction utilities.
"""

from typing import Dict


class ExtractionConfig:
    """
    Configuration class for data extraction settings.
    """

    # HTML Constants
    INLINE_TABLE_CLASS = "inline-table"
    FLAGGENRAHMEN_CLASS = "flaggenrahmen"
    COMPETITION_URL_PATTERN = "/startseite/wettbewerb/"
    CLUB_URL_PATTERN = "/startseite/verein/"

    # Base URL
    BASE_URL = "https://www.transfermarkt.com"

    # Regex Patterns
    NUMBER_PATTERN = r"(\d+)"
    FLOAT_PATTERN = r"(\d+\.?\d*)"
    PERCENTAGE_PATTERN = r"(\d+\.?\d*)\s*%"
    MARKET_VALUE_PATTERN = r"€([\d,]+\.?\d*)\s*([kmb]?)"
    CLUB_ID_PATTERN = r"/verein/(\d+)"
    CLUB_CODE_PATTERN = r"/([^/]+)/startseite/verein/"
    URL_COMPONENT_PATTERN = r"/saison_id/\d+"

    # Market Value Multipliers
    MARKET_VALUE_MULTIPLIERS: Dict[str, float] = {
        "k": 0.001,  # thousands to millions
        "m": 1.0,  # millions
        "b": 1000.0,  # billions to millions
        "bn": 1000.0,  # billions to millions
        "": 1.0,  # default to millions
    }

    # Column Requirements by Page Type
    COLUMN_REQUIREMENTS: Dict[str, int] = {
        "europa_jugend": 8,
        "europa": 11,
        "other_regions": 9,
        "club_minimum": 2,
        "club_with_market_values": 7,
        "club_basic": 5,
    }

    # Default Values
    DEFAULT_COMPETITION_TYPE = "league"
    DEFAULT_TIER = "1st Tier"
    DEFAULT_SEASON_YEAR = "2024"

    # URL Identifiers
    EUROPA_URL_IDENTIFIER = "/wettbewerbe/europa/"
    EUROPA_JUGEND_URL_IDENTIFIER = "/wettbewerbe/europaJugend/"

    # Error Messages
    ERROR_MESSAGES: Dict[str, str] = {
        "competition_extraction": "Failed to extract competition info: {}",
        "club_extraction": "Failed to extract club info: {}",
        "row_extraction": "Failed to extract data from row: {}",
        "url_parsing": "Failed to parse URL components: {}",
        "invalid_cell": "Invalid cell structure provided",
        "season_extraction": "Failed to extract season options: {}",
    }


class ColumnMapping:
    """
    Column mapping configurations for different page types
    """

    @staticmethod
    def get_europa_jugend_mapping() -> Dict[str, int]:
        """Get column mapping for Europa Jugend pages."""
        return {
            "competition_info": 0,
            "country_flag": 3,
            "clubs": 4,
            "players": 5,
            "avg_age": 6,
            "foreigners_pct": 7,
            "total_market_value": 9,
        }

    @staticmethod
    def get_europa_mapping() -> Dict[str, int]:
        """Get column mapping for Europa pages."""
        return {
            "competition_info": 0,
            "country_flag": 3,
            "clubs": 4,
            "players": 5,
            "avg_age": 6,
            "foreigners_pct": 7,
            "game_ratio_pct": 8,
            "goals_per_match": 9,
            "forum": 10,
            "avg_market_value": 11,
            "total_market_value": 12,
        }

    @staticmethod
    def get_other_regions_mapping() -> Dict[str, int]:
        """Get column mapping for other region pages."""
        return {
            "competition_info": 0,
            "country_flag": 3,
            "clubs": 4,
            "players": 5,
            "avg_age": 6,
            "foreigners_pct": 7,
            "goals_per_match": 8,
            "forum": 9,
            "total_market_value": 10,
        }

    @staticmethod
    def get_club_mapping() -> Dict[str, int]:
        """Get column mapping for club data extraction."""
        return {
            "club_info_start": 0,
            "club_info_end": 1,
            "squad_size": 2,
            "average_age": 3,
            "foreign_players": 4,
            "avg_market_value": 5,
            "total_market_value": 6,
        }

    @staticmethod
    def get_club_mapping_safe(cell_count: int) -> Dict[str, int]:
        """
        SAFE VERSION: Get column mapping that adapts to available columns.
        This is the new bulletproof version for production use.
        """
        mapping = {
            "club_info_start": 0,
            "club_info_end": 1,
        }

        # Only add mappings for columns that actually exist
        if cell_count >= 3:
            mapping["squad_size"] = 2
        if cell_count >= 4:
            mapping["average_age"] = 3
        if cell_count >= 5:
            mapping["foreign_players"] = 4
        if cell_count >= 6:
            mapping["avg_market_value"] = 5
        if cell_count >= 7:
            mapping["total_market_value"] = 6

        return mapping
