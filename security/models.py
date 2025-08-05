# security/models.py
"""
Data models and enums for IP security system.
"""

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional


class SecurityThreatLevel(Enum):
    """
    Security threat levels for IP monitoring
    """

    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"
    PROTECTED = "PROTECTED"


@dataclass
class IPRotationEvent:
    """
    Data class for IP rotation events
    """

    timestamp: datetime
    old_ip: str
    new_ip: str
    request_count: int
    rotation_forced: bool = False


@dataclass
class SecurityAlert:
    """
    Data class for security alerts
    """

    timestamp: datetime
    threat_level: SecurityThreatLevel
    message: str
    current_ip: str
    request_count: int
    last_rotation: Optional[datetime] = None
