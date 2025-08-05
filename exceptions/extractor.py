class ParsingError(Exception):
    """
    Exception raised when parsing fails.
    """

    pass


class CompetitionScrapingError(Exception):
    """
    Base exception for competition scraping errors
    """

    pass


class VpnRequiredError(CompetitionScrapingError):
    """
    Raised when VPN is required but not available
    """

    pass


class VpnConnectionError(CompetitionScrapingError):
    """
    Raised when VPN connection fails
    """

    pass


class VpnRotationError(CompetitionScrapingError):
    """
    Raised when VPN rotation fails
    """

    pass


class NavigationError(CompetitionScrapingError):
    """
    Raised when page navigation fails
    """

    pass


class DatabaseOperationError(CompetitionScrapingError):
    """
    Raised when database operations fail
    """

    pass


class ConfigurationError(CompetitionScrapingError):
    """
    Raised when configuration is invalid
    """

    pass


class ValidationError(CompetitionScrapingError):
    """
    Raised when data validation fails
    """

    pass
