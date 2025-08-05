# exceptions/parsers.py
"""
Custom exceptions for HTML parser components.
Provides specific error types for better error handling and debugging.
"""


class ParsingError(Exception):
    """
    Exception raised when HTML parsing operations fail.

    This exception is raised when critical parsing errors occur that prevent
    successful data extraction from HTML tables.
    """

    def __init__(self, message: str, original_error: Exception = None):
        """
        Initialize parsing error with message and optional original error.

        Args:
            message: Human-readable error message describing the parsing failure
            original_error: Original exception that caused this parsing error
        """
        super().__init__(message)
        self.original_error = original_error
        self.message = message

    def __str__(self) -> str:
        """
        Return string representation of the parsing error.

        Returns:
            Formatted error message with original error if available
        """
        if self.original_error:
            return f"{self.message} (Original: {self.original_error})"
        return self.message


class TableNotFoundError(ParsingError):
    """
    Exception raised when expected HTML table is not found.

    This exception is raised when parser cannot locate the expected table
    structure in the provided HTML content.
    """

    def __init__(self, table_type: str = "table"):
        """
        Initialize table not found error.

        Args:
            table_type: Type of table that was not found (e.g., "club", "competition")
        """
        message = f"Expected {table_type} table not found in HTML content"
        super().__init__(message)
        self.table_type = table_type


class InsufficientDataError(ParsingError):
    """
    Exception raised when extracted data is insufficient for processing.

    This exception is raised when the parsed data does not meet minimum
    requirements for successful processing.
    """

    def __init__(self, data_type: str, minimum_required: int, actual: int):
        """
        Initialize insufficient data error.

        Args:
            data_type: Type of data that was insufficient (e.g., "columns", "rows")
            minimum_required: Minimum number of items required
            actual: Actual number of items found
        """
        message = (
            f"Insufficient {data_type}: found {actual}, "
            f"minimum required {minimum_required}"
        )
        super().__init__(message)
        self.data_type = data_type
        self.minimum_required = minimum_required
        self.actual = actual
