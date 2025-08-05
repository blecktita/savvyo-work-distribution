# pipelines/princpal_orchestrator/base_orchestrator.py
"""
Base orchestrator class with common functionality.
Provides shared methods and initialization logic.
"""

from abc import ABC, abstractmethod
from typing import Optional

from configurations import ConfigFactory, ScraperConfig
from exceptions import ConfigurationError
from extractors.navigation.navigation_manager import NavigationManager
from pipelines.princpal_orchestrator.orchestrator_config import OrchestratorConfig
from vpn_controls import VpnProtectionHandler


class BaseOrchestrator(ABC):
    """
    Abstract base class for orchestrator implementations.
    Provides common initialization and utility methods.
    """

    def __init__(self, config: Optional[ScraperConfig] = None):
        """
        Initialize base orchestrator with configuration validation.

        Args:
            config: Scraper configuration (uses development if None)

        Raises:
            ConfigurationError: If configuration is invalid
        """
        # ***> Initialize configuration using factory pattern <***
        self.config = config or ConfigFactory.development()

        # ***> Validate configuration before proceeding <***
        self._validate_configuration()

        # ***> Set up core components <***
        self._setup_base_components()

    def _validate_configuration(self) -> None:
        """
        Validate the provided configuration.

        Raises:
            ConfigurationError: If configuration is invalid
        """
        try:
            self.config.validate()
        except Exception as error:
            raise ConfigurationError("Invalid configuration: %s" % error)

    def _setup_base_components(self) -> None:
        """
        Initialize base components common to all orchestrators.
        """
        # ***> Initialize VPN protection handler <***
        if not hasattr(self, "_skip_base_vpn_handler"):
            self.vpn_handler = VpnProtectionHandler(self.config)

        # ***> Initialize navigation manager <***
        self.navigator = NavigationManager()

    def _get_environment_setting(self) -> str:
        """
        Get environment setting from config with fallback.

        Returns:
            Environment string for database configuration
        """
        return getattr(
            self.config, "_environment", OrchestratorConfig.DEFAULT_DB_ENVIRONMENT
        )

    def _handle_database_initialization_error(self, error: Exception) -> None:
        """
        Handle database initialization failures gracefully.

        Args:
            error: The database initialization error
        """
        # ***> Database initialization failed, continue without DB <***
        pass

    @abstractmethod
    def cleanup(self) -> None:
        """
        Clean up resources. Must be implemented by subclasses.
        """
        pass

    @property
    def vpn_protection_active(self) -> bool:
        """
        Check if VPN protection is currently active.

        Returns:
            True if VPN protection is active
        """
        return self.vpn_handler.is_active

    @property
    @abstractmethod
    def database_available(self) -> bool:
        """
        Check if database is available. Must be implemented by subclasses.

        Returns:
            True if database is available
        """
        pass
