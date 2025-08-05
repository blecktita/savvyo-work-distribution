# configurations/factory.py
"""
Configuration factory for creating environment-specific configurations.
"""

from typing import List, Optional

from .settings_database import DatabaseConfig
from .settings_orchestrator import ScraperConfig, TransferMarketConfig
from .settings_vpn import VpnConfig


class ConfigFactory:
    """
    Factory for creating environment-specific configurations
    """

    @staticmethod
    def development() -> ScraperConfig:
        """
        Development environment configuration
        """
        return ScraperConfig(
            transfer_market=TransferMarketConfig(),
            database=DatabaseConfig.development(),
            use_vpn=True,
            vpn=VpnConfig.development(),
            log_level="DEBUG",
            request_delay=1.0,
            max_pages=5,
            max_retries=3,
            timeout=30,
            save_to_database=True,
            _environment="development",
        )

    @staticmethod
    def testing() -> ScraperConfig:
        """
        Testing environment configuration
        """
        return ScraperConfig(
            transfer_market=TransferMarketConfig(),
            database=DatabaseConfig.testing(),
            use_vpn=False,
            vpn=VpnConfig.testing(),
            log_level="ERROR",
            request_delay=0.1,
            max_pages=2,
            max_retries=2,
            timeout=15,
            save_to_database=False,
            _environment="testing",
        )

    @staticmethod
    def production() -> ScraperConfig:
        """
        Production environment configuration
        """
        return ScraperConfig(
            transfer_market=TransferMarketConfig(),
            database=DatabaseConfig.production(),
            use_vpn=True,
            vpn=VpnConfig.production(),
            log_level="INFO",
            request_delay=6.0,
            max_pages=30,
            max_retries=3,
            timeout=60,
            log_file="logs/scraper.log",
            save_to_database=True,
            _environment="production",
        )

    @staticmethod
    def custom(
        environment: str = "development",
        max_pages: int = 10,
        use_vpn: bool = True,
        requests_per_rotation: int = 10,
        database_url: Optional[str] = None,
        **kwargs,
    ) -> ScraperConfig:
        """
        Create a custom configuration with specified parameters
        """
        # Start with specified environment defaults
        if environment.lower() == "production":
            config = ConfigFactory.production()
        elif environment.lower() == "testing":
            config = ConfigFactory.testing()
        else:
            config = ConfigFactory.development()

        # Apply custom overrides
        config.max_pages = max_pages
        config.use_vpn = use_vpn
        config.vpn.requests_per_rotation = requests_per_rotation
        config._environment = f"custom-{environment}"

        if database_url:
            config.database = DatabaseConfig.from_url(database_url)

        # Apply any additional keyword arguments
        for key, value in kwargs.items():
            if hasattr(config, key):
                setattr(config, key, value)
            elif hasattr(config.vpn, key.replace("vpn_", "")):
                setattr(config.vpn, key.replace("vpn_", ""), value)
            elif hasattr(config.database, key.replace("db_", "")):
                setattr(config.database, key.replace("db_", ""), value)
            elif hasattr(config.transfer_market, key.replace("tm_", "")):
                setattr(config.transfer_market, key.replace("tm_", ""), value)

        return config

    @staticmethod
    def for_colleague(
        colleague_name: str,
        host_ip: str,
        assigned_continents: Optional[List[str]] = None,
        environment: str = "production",
    ) -> ScraperConfig:
        """
        Create configuration for colleague connecting to your database.

        Args:
            colleague_name: Colleague identifier
            host_ip: Your machine's IP address
            assigned_continents: Specific continents to avoid overlap
            environment: Environment to use
        """
        # Get base config
        if environment == "production":
            config = ConfigFactory.production()
        elif environment == "testing":
            config = ConfigFactory.testing()
        else:
            config = ConfigFactory.development()

        # Set colleague database connection
        config.database = DatabaseConfig.for_colleague(host_ip)

        # Work division
        if assigned_continents:
            config.transfer_market.continents_name = assigned_continents

        # Colleague-specific settings
        config._environment = f"{environment}-{colleague_name}"
        config.output_directory = f"data/{colleague_name}"

        # Stagger timing to avoid conflicts
        colleague_hash = hash(colleague_name) % 4
        base_delay = config.vpn.request_min_delay
        config.vpn.request_min_delay = base_delay + (colleague_hash * 2)
        config.vpn.request_max_delay = config.vpn.request_min_delay + 4

        return config


# Essential convenience functions only
def get_config(environment: str = "development") -> ScraperConfig:
    """
    Get configuration for specified environment
    """
    environment = environment.lower()

    if environment == "development":
        return ConfigFactory.development()
    elif environment == "testing":
        return ConfigFactory.testing()
    elif environment == "production":
        return ConfigFactory.production()
    else:
        raise ValueError(f"Unknown environment: {environment}")


def get_colleague_config(
    colleague_name: str, host_ip: str, continents: Optional[List[str]] = None
) -> ScraperConfig:
    """
    Quick setup for colleague with work division
    """
    return ConfigFactory.for_colleague(
        colleague_name=colleague_name,
        host_ip=host_ip,
        assigned_continents=continents,
        environment="production",
    )
