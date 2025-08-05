# configurations/settings_security.py
"""
Security and IP management configuration.
"""

from dataclasses import dataclass

from .settings_vpn import VpnConfig


@dataclass
class IpSecurityConfig:
    """
    Configuration for IP security settings.
    Inherits key values from VPN configuration.
    """

    vpn_config: VpnConfig
    rotation_check_interval: int = 90
    max_time_per_ip: int = 240
    security_alerts_timeframe_hours: int = 1

    @property
    def max_requests_per_ip(self) -> int:
        """
        Max requests per IP - inherited from VPN requests_per_rotation
        """
        return self.vpn_config.requests_per_rotation

    @property
    def wait_time(self) -> int:
        """
        Wait time - derived from VPN mandatory_delay
        """
        return int(self.vpn_config.mandatory_delay)

    @classmethod
    def from_vpn_config(cls, vpn_config: VpnConfig) -> "IpSecurityConfig":
        """
        Create security config from VPN configuration
        """
        return cls(
            vpn_config=vpn_config,
            rotation_check_interval=90,
            max_time_per_ip=240,
            security_alerts_timeframe_hours=1,
        )

    def __post_init__(self):
        """
        Post-initialization validation
        """
        if self.max_requests_per_ip <= 0:
            raise ValueError("max_requests_per_ip must be greater than 0")
        if self.rotation_check_interval <= 0:
            raise ValueError("rotation_check_interval must be greater than 0")
        if self.wait_time <= 0:
            raise ValueError("wait_time must be greater than 0")
