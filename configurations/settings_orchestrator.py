# configurations/settings_orchestrator.py
"""
Main orchestrator configuration combining all components.
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

from .settings_database import DatabaseConfig
from .settings_security import IpSecurityConfig
from .settings_vpn import VpnConfig


@dataclass
class TransferMarketConfig:
    """
    Transfer market configuration
    """

    competition_base_url: str = "https://www.transfermarkt.com/wettbewerbe/"
    competition_base_url_ending: str = "/wettbewerbe?plus=1"
    continents_name: List[str] = field(
        default_factory=lambda: [
            "amerika",
            "europa",
            "asien",
            "afrika",
            "fifa",
            "europaJugend",
        ]
    )


@dataclass
class ScraperConfig:
    """
    Combined configuration for orchestrator and database operations with VPN support.
    """

    # Core configurations
    transfer_market: TransferMarketConfig = field(default_factory=TransferMarketConfig)
    database: DatabaseConfig = field(
        default_factory=lambda: DatabaseConfig.development()
    )

    # VPN configuration
    use_vpn: bool = False
    vpn: VpnConfig = field(default_factory=lambda: VpnConfig.development())

    # Request settings
    request_delay: float = 6.0
    max_retries: int = 3
    timeout: int = 60

    # Pagination settings
    max_pages: int = 5

    # Output settings
    output_directory: str = "data_savvyo"
    save_to_database: bool = False

    # Validation settings
    min_competitions_per_page: int = 1

    # Logging settings
    log_level: str = "INFO"
    log_file: Optional[str] = None

    # Environment tracking
    _environment: Optional[str] = None

    @property
    def security(self) -> IpSecurityConfig:
        """
        Security configuration derived from VPN settings
        """
        return IpSecurityConfig.from_vpn_config(self.vpn)

    def get_effective_delay(self) -> tuple[float, float]:
        """
        Get the actual delay range based on VPN settings.
        Returns: (min_delay, max_delay)
        """
        if self.use_vpn:
            return (self.vpn.request_min_delay, self.vpn.request_max_delay)
        else:
            return (self.request_delay, self.request_delay)

    def validate(self) -> bool:
        """
        Validate configuration settings
        """
        if self.max_pages <= 0:
            raise ValueError("max_pages must be greater than 0")

        if self.timeout <= 0:
            raise ValueError("timeout must be greater than 0")

        if self.request_delay < 0:
            raise ValueError("request_delay cannot be negative")

        # Validate VPN settings only if VPN is enabled
        if self.use_vpn:
            if self.vpn.requests_per_rotation <= 0:
                raise ValueError("vpn_requests_per_rotation must be greater than 0")

            if self.vpn.disconnect_time <= 0:
                raise ValueError("vpn_disconnect_time must be greater than 0")

            if self.vpn.connect_time <= 0:
                raise ValueError("vpn_connect_time must be greater than 0")

            if self.vpn.request_min_delay < 0:
                raise ValueError("vpn_request_min_delay cannot be negative")

            if self.vpn.request_max_delay < self.vpn.request_min_delay:
                raise ValueError(
                    "vpn_request_max_delay must be >= vpn_request_min_delay"
                )

        # Validate database settings
        if not self.database.database_url:
            raise ValueError("database_url cannot be empty")

        # Validate output directory
        try:
            Path(self.output_directory).mkdir(parents=True, exist_ok=True)
        except Exception as e:
            raise ValueError(
                f"Cannot create output directory {self.output_directory}: {e}"
            )

        return True

    def get_summary(self) -> dict:
        """
        Get a summary of the current configuration
        """
        min_delay, max_delay = self.get_effective_delay()
        avg_delay = (min_delay + max_delay) / 2

        summary = {
            "environment": self._environment or "unknown",
            "scraping": {
                "max_pages": self.max_pages,
                "timeout": self.timeout,
                "max_retries": self.max_retries,
                "effective_delay_range": f"{min_delay}-{max_delay}s",
                "continents": len(self.transfer_market.continents_name),
            },
            "vpn": {"enabled": self.use_vpn, "avg_delay": avg_delay},
            "database": {
                "save_enabled": self.save_to_database,
                "url_type": (
                    self.database.database_url.split("://")[0]
                    if "://" in self.database.database_url
                    else "unknown"
                ),
                "pool_size": self.database.pool_size,
            },
            "output": {"directory": self.output_directory},
        }

        # Add VPN-specific details only if enabled
        if self.use_vpn:
            total_rotation_time = self.vpn.disconnect_time + self.vpn.connect_time
            summary["vpn"].update(
                {
                    "requests_per_rotation": self.vpn.requests_per_rotation,
                    "rotation_time_seconds": total_rotation_time,
                    "estimated_time_per_rotation_cycle": (
                        self.vpn.requests_per_rotation * avg_delay + total_rotation_time
                    ),
                }
            )

        return summary

    def __post_init__(self):
        """
        Post-initialization validation and setup
        """
        try:
            Path(self.output_directory).mkdir(parents=True, exist_ok=True)
        except Exception as e:
            print(
                f"Warning: Could not create output directory {self.output_directory}: {e}"
            )

        if self._environment != "testing":
            try:
                self.validate()
            except Exception as e:
                print(f"Warning: Configuration validation failed: {e}")
