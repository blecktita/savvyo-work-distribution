# extractors/parsers/parser_config.py
"""
Configuration module for HTML parser settings.
Contains all constants and configurable values used across parsers.
"""

from typing import Any, Dict


class ParserConfig:
    """
    Configuration class containing all parser-related constants and settings.
    Centralizes all hardcoded values to improve maintainability.
    """

    # HTML parsing constants
    TABLE_ROW_SELECTOR = "tr"
    TABLE_HEADER_SELECTOR = "th"
    TABLE_CELL_SELECTOR = "td"
    EXTRAROW_CLASS = "extrarow"

    # Column requirements for different URL patterns
    COLUMN_REQUIREMENTS: Dict[str, int] = {
        "/europaJugend/": 8,
        "default": 6,  # Default minimum columns required
    }

    # Club table specific requirements
    CLUB_TABLE_MIN_COLUMNS = 2

    # Table search patterns for club tables
    CLUB_TABLE_SELECTORS = [
        {"tag": "table", "class": "items"},
        {"tag": "div", "class": "responsive-table"},
        {"tag": "div", "class": "grid-view"},
    ]

    # Row skip patterns
    SKIP_ROW_CLASSES = ["foot"]

    @classmethod
    def get_min_columns_for_url(cls, url: str) -> int:
        """
        Get minimum column requirement based on URL pattern.

        Args:
            url: URL to check pattern against

        Returns:
            Minimum number of columns required for the URL
        """
        for pattern, min_cols in cls.COLUMN_REQUIREMENTS.items():
            if pattern != "default" and pattern in url:
                return min_cols
        return cls.COLUMN_REQUIREMENTS["default"]

    @classmethod
    def get_all_config(cls) -> Dict[str, Any]:
        """
        Get all configuration values as a dictionary.

        Returns:
            Dictionary containing all configuration values
        """
        return {
            "table_row_selector": cls.TABLE_ROW_SELECTOR,
            "table_header_selector": cls.TABLE_HEADER_SELECTOR,
            "table_cell_selector": cls.TABLE_CELL_SELECTOR,
            "extrarow_class": cls.EXTRAROW_CLASS,
            "column_requirements": cls.COLUMN_REQUIREMENTS,
            "club_table_min_columns": cls.CLUB_TABLE_MIN_COLUMNS,
            "club_table_selectors": cls.CLUB_TABLE_SELECTORS,
            "skip_row_classes": cls.SKIP_ROW_CLASSES,
        }
