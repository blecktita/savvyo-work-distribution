# logs/constants.py
"""
Constants for logging operations
"""


# HTML parsing constants
class HTMLConstants:
    """
    HTML parsing related constants
    """

    # Table selectors
    TABLE_ROW_SELECTOR = "tr"
    TABLE_HEADER_SELECTOR = "th"
    TABLE_CELL_SELECTOR = "td"

    # CSS classes
    EXTRAROW_CLASS = "extrarow"
    INLINE_TABLE_CLASS = "inline-table"
    FLAGGENRAHMEN_CLASS = "flaggenrahmen"
    PAGINATION_CLASS = "tm-pagination"
    PAGINATION_ITEM_CLASS = "tm-pagination__list-item"
    PAGINATION_ACTIVE_CLASS = "tm-pagination__list-item--active"
    PAGINATION_NEXT_CLASS = "tm-pagination__list-item--icon-next-page"
    PAGINATION_LINK_CLASS = "tm-pagination__link"

    # URL patterns
    COMPETITION_URL_PATTERN = "/startseite/wettbewerb/"

    # Minimum columns required
    MIN_COLUMNS_REQUIRED = 11


class ScrapingConstants:
    """
    Scraping operation constants
    """

    # Base URLs
    BASE_URL = "https://www.transfermarkt.com"

    # File patterns
    CSV_FILENAME_PATTERN = "competitions_{timestamp}.csv"
    TIMESTAMP_FORMAT = "%Y%m%d_%H%M%S"

    # Default values
    DEFAULT_COUNTRY = "Unknown"
    DEFAULT_TIER = "Unknown"
    DEFAULT_COMPETITION_TYPE = "League"

    # Data extraction patterns
    MARKET_VALUE_PATTERN = r"€([\d,]+\.?\d*)(bn|m|k)?"
    PERCENTAGE_PATTERN = r"(\d+\.?\d*)\s*%?"
    NUMBER_PATTERN = r"(\d+)"
    FLOAT_PATTERN = r"(\d+\.?\d*)"

    # Market value multipliers
    MARKET_VALUE_MULTIPLIERS = {
        "bn": 1000,  # Billion to million
        "m": 1,  # Million stays as is
        "k": 0.001,  # Thousand to million
    }


class DatabaseConstants:
    """
    Database operation constants
    """

    # Metadata fields to remove before database save
    METADATA_FIELDS = ["page_number", "scraped_at"]

    # Numeric field defaults
    NUMERIC_DEFAULTS: dict[str, float] = {
        "number_of_clubs": 0,
        "number_of_players": 0,
        "percentage_of_foreign_players": 0.0,
        "average_age_of_players": 0.0,
        "goals_per_match": 0.0,
        "percentage_game_ratio_of_foreign_players": 0.0,
        "average_market_value": 0.0,
        "total_market_value": 0.0,
    }

    # String field defaults
    STRING_DEFAULTS = {
        "competition_type": "",
        "country": "",
        "tier": "",
        "competition_name": "",
        "competition_url": "",
        "competition_code": "",
        "competition_id": "",
    }


class LoggingConstants:
    """
    Logging related constants
    """

    # Log messages
    VPN_ACTIVE_MSG = "VPN PROTECTION: ACTIVE - requests_per_rotation={}"
    VPN_DISABLED_MSG = "VPN PROTECTION: DISABLED"
    VPN_REQUIRED_ERROR_MSG = (
        "CRITICAL: VPN is required but failed to initialize! "
        "Cannot proceed without VPN protection."
    )
    VPN_SECURITY_VIOLATION_MSG = (
        "SECURITY VIOLATION: Cannot make requests without VPN protection! "
        "VPN is required but not active."
    )

    # Performance metric keys
    PERFORMANCE_METRICS = [
        "total_pages",
        "total_competitions",
        "unique_competitions",
        "database_saved",
        "duration",
        "avg_time_per_page",
        "vpn_protection",
        "requests_made",
        "requests_per_rotation",
    ]
