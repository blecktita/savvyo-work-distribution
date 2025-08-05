# configurations/settings_vpn.py
"""
VPN configuration and management settings (everything in seconds)
"""

from dataclasses import dataclass


@dataclass
class VpnConfig:
    """
    VPN configuration settings
    """

    # Core VPN settings
    requests_per_rotation: int = 10
    disconnect_time: float = 8.0
    connect_time: float = 12.0
    request_min_delay: float = 6.0
    request_max_delay: float = 12.0
    request_min_jitter: float = 1.0
    request_max_jitter: float = 5.0
    max_recovery_attempts: int = 3

    # Timing settings
    rotation_variance: float = 0.4
    mandatory_delay: float = 10.0

    @classmethod
    def development(cls) -> "VpnConfig":
        """
        Development VPN settings
        """
        return cls(
            requests_per_rotation=5,
            disconnect_time=3.0,
            connect_time=5.0,
            request_min_delay=1.0,
            request_max_delay=3.0,
            request_min_jitter=0.1,
            request_max_jitter=0.5,
            rotation_variance=0.2,
            mandatory_delay=5.0,
            max_recovery_attempts=2,
        )

    @classmethod
    def testing(cls) -> "VpnConfig":
        """
        Testing VPN settings
        """
        return cls(
            requests_per_rotation=2,
            disconnect_time=0.1,
            connect_time=0.1,
            request_min_delay=0.1,
            request_max_delay=0.2,
            request_min_jitter=0.0,
            request_max_jitter=0.1,
            rotation_variance=0.1,
            mandatory_delay=0.1,
            max_recovery_attempts=1,
        )

    @classmethod
    def production(cls) -> "VpnConfig":
        """
        Production VPN settings
        """
        return cls(
            requests_per_rotation=15,
            disconnect_time=10.0,
            connect_time=15.0,
            request_min_delay=8.0,
            request_max_delay=15.0,
            request_min_jitter=2.0,
            request_max_jitter=8.0,
            rotation_variance=0.5,
            mandatory_delay=12.0,
            max_recovery_attempts=5,
        )
