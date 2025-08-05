# core/scrapers/orchestrator_config.py
"""
Configuration constants and settings for orchestrator classes.
Centralizes all hardcoded values for better maintainability.
"""

from typing import Set


class OrchestratorConfig:
    """
    Configuration constants for orchestrator operations.
    """

    # ***> Competition exclusions for club scraping operations <***
    EXCLUDED_COMPETITION_IDS: Set[str] = {
        "GB1",
        "ES1",
        "IT1",
        "L1",
        "FR1",
        "PO1",
        "NL1",
    }

    # ***> File path configurations <***
    DEFAULT_ENV_PATH: str = "utils/vpn_manager/.env"
    ORCHESTRATOR_LOG_PATH: str = "logs/orchestrator.log"
    CLUB_ORCHESTRATOR_LOG_PATH: str = "logs/club_orchestrator.log"

    # ***> IP security default settings <***
    DEFAULT_MAX_REQUESTS_PER_IP: int = 100
    DEFAULT_IP_CHECK_INTERVAL: int = 90

    # ***> URL construction parameters <***
    CURRENT_SEASON_YEAR: str = "2024"
    HISTORICAL_SEASON_PATH: str = "plus/"
    SEASON_PARAM_PREFIX: str = "saison_id="
    URL_SEPARATOR_QUERY: str = "?"
    URL_SEPARATOR_PARAM: str = "&"
    URL_PATH_SEPARATOR: str = "/"

    # ***> Timing and delay configurations <***
    VPN_ROTATION_WAIT_INTERVAL: int = 5
    SEASON_PROCESSING_DELAY: int = 5
    PAGE_LOAD_DELAY: int = 5

    # ***> Dashboard and monitoring settings <***
    SECURITY_ALERTS_TIMEFRAME_HOURS: int = 24
    RECENT_ALERTS_DISPLAY_LIMIT: int = 3

    # ***> String formatting templates <***
    URL_TRUNCATE_LENGTH: int = 50
    URL_DISPLAY_LENGTH: int = 80
    URL_ELLIPSIS: str = "..."

    # ***> Database environment default <***
    DEFAULT_DB_ENVIRONMENT: str = "development"
