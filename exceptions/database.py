# pylint: disable=unnecessary-pass
"""
Custom exceptions for database operations.
"""


class DatabaseServiceError(Exception):
    """
    Base class for database service exceptions
    """

    pass


class DatabaseServiceInitializationError(DatabaseServiceError):
    """
    Exception raised when the database service fails to initialize
    """

    pass


class DatabaseConnectionError(DatabaseServiceError):
    """
    Exception raised for errors in the database connection
    """

    pass


class DatabaseQueryError(DatabaseServiceError):
    """
    Exception raised for errors in database queries
    """

    pass


class CompetitionDataError(DatabaseServiceError):
    """
    Exception raised for errors in competition data
    """

    pass


class URLParsingError(DatabaseServiceError):
    """
    Exception raised for errors in URL parsing
    """

    pass


class CleanupError(DatabaseServiceError):
    """
    Exception raised for errors during cleanup operations
    """

    pass


class DatabaseOperationError(Exception):
    """Custom exception for database operation failures."""

    pass


class DatabaseConfigurationError(Exception):
    """Custom exception for database configuration issues."""

    pass
