class IPSecurityViolationError(Exception):
    """
    Raised when IP security policies are violated
    """

    pass


class IPDetectionError(Exception):
    """
    Raised when IP detection fails
    """

    pass


class RotationTimeoutError(Exception):
    """
    Raised when IP rotation times out
    """

    pass


class EmailConfigError(Exception):
    """
    Raised when email configuration fails
    """

    pass


class EmailSendingError(Exception):
    """
    Raised when email sending fails
    """

    pass
