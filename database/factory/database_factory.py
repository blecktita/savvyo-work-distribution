# database/factory/database_factory.py
"""
Database factory using existing configuration system.
"""

from typing import Dict

from configurations.settings_database import get_database_config
from exceptions import DatabaseServiceError

from ..core.database_manager import DatabaseManager
from ..services.database_service import DatabaseService


class DatabaseFactory:
    """
    Factory class for creating database instances using existing config system.
    """

    _service_instances: Dict[str, DatabaseService] = {}
    _manager_instances: Dict[str, DatabaseManager] = {}

    @classmethod
    def create_database_manager(
        cls, environment: str = "", instance_name: str = "default"
    ) -> DatabaseManager:
        """
        Create or retrieve a database manager instance.

        Args:
            environment: Target environment ('development', 'testing', 'production')
            instance_name: Name for the instance (for caching)

        Returns:
            DatabaseManager instance
        """
        # ***> Use existing configuration system <***
        config = get_database_config(environment)

        cache_key = f"{environment or 'default'}_{instance_name}"

        if cache_key not in cls._manager_instances:
            cls._manager_instances[cache_key] = DatabaseManager(
                database_url=config.database_url, echo=config.echo
            )

        return cls._manager_instances[cache_key]

    @classmethod
    def create_database_service(
        cls, environment: str = "", instance_name: str = "default"
    ) -> DatabaseService:
        """
        Create or retrieve a database service instance.

        Args:
            environment: Target environment
            instance_name: Name for the instance (for caching)

        Returns:
            DatabaseService instance
        """
        cache_key = f"{environment or 'default'}_{instance_name}"

        if cache_key not in cls._service_instances:
            cls._service_instances[cache_key] = DatabaseService(environment)

        return cls._service_instances[cache_key]

    @classmethod
    def clear_instances(cls) -> None:
        """
        Clear all cached database instances.
        """
        # ***> Cleanup services first <***
        for service in cls._service_instances.values():
            try:
                service.cleanup()
            except Exception:
                pass

        cls._service_instances.clear()
        cls._manager_instances.clear()


def create_database_service(environment: str = "") -> DatabaseService:
    """
    Factory function to create database service using existing config.

    Args:
        environment: Environment name ('development', 'testing', 'production')

    Returns:
        DatabaseService: Configured database service
    """
    try:
        service = DatabaseFactory.create_database_service(environment)
        return service
    except Exception as error:
        raise DatabaseServiceError("Could not create database service") from error
