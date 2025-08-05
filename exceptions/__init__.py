from .database import (
    CleanupError,
    CompetitionDataError,
    DatabaseConfigurationError,
    DatabaseConnectionError,
    DatabaseOperationError,
    DatabaseQueryError,
    DatabaseServiceError,
    DatabaseServiceInitializationError,
    URLParsingError,
)
from .extractor import CompetitionScrapingError, ConfigurationError, NavigationError
from .parsers import InsufficientDataError, ParsingError, TableNotFoundError
from .security import (
    EmailConfigError,
    EmailSendingError,
    IPDetectionError,
    IPSecurityViolationError,
    RotationTimeoutError,
)
from .vpn import TunnelblickRecoveryError, VpnConnectionError, VpnRequiredError

__all__ = [
    "VpnConnectionError",
    "TunnelblickRecoveryError",
    "DatabaseServiceError",
    "DatabaseServiceInitializationError",
    "DatabaseConnectionError",
    "DatabaseQueryError",
    "CompetitionDataError",
    "URLParsingError",
    "CleanupError",
    "VpnRequiredError",
    "IPSecurityViolationError",
    "IPDetectionError",
    "RotationTimeoutError",
    "EmailConfigError",
    "EmailSendingError",
    "DatabaseOperationError",
    "DatabaseConfigurationError",
    "CompetitionScrapingError",
    "TableNotFoundError",
    "InsufficientDataError",
    "ParsingError",
    "NavigationError",
    "ConfigurationError",
]
