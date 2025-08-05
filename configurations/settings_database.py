# configurations/settings_database.py
"""
Single source of truth for database configuration.
Manages environment-based database selection with clear rules:
- Development: SQLite (local file)
- Production: PostgreSQL (from environment)
- Testing: SQLite (in-memory)
"""

import logging
import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote_plus

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from configurations.settings_base import EnvironmentVariables

# ***> Load environment variables from .env file <***
env_path = EnvironmentVariables.env_file_path
if env_path and Path(env_path).exists():
    load_dotenv(env_path)
    logging.info("Loaded environment from: %s", env_path)
else:
    logging.warning("Environment file not found: %s", env_path)


@dataclass
class DatabaseConfig:
    """
    Database configuration with environment-based selection.
    Provides single source of truth for database connectivity.
    """

    database_url: str
    database_type: str  # ***> sqlite, postgresql <***
    echo: bool = False
    pool_size: int = 5
    max_overflow: int = 10
    pool_timeout: int = 30
    pool_recycle: int = 3600

    @classmethod
    def _build_sqlite_url(cls, environment: str) -> str:
        """
        Build SQLite URL based on environment.
        Development uses file, testing uses memory.
        """
        if environment == "testing":
            return "sqlite:///:memory:"

        # ***> Create data directory if it doesn't exist <***
        data_dir = Path("data")
        data_dir.mkdir(exist_ok=True)

        db_file = data_dir / f"{environment}_database.db"
        return f"sqlite:///{db_file}"

    @classmethod
    def _build_postgres_url(cls) -> str:
        """
        Build PostgreSQL URL from environment variables with proper encoding.
        Only used in production environment.
        """
        user = os.getenv("POSTGRES_USER", "aseathletics_datascience")
        password = os.getenv("POSTGRES_PASSWORD")
        host = os.getenv("POSTGRES_HOST", "localhost")
        port = os.getenv("POSTGRES_PORT", "5432")
        database = os.getenv("POSTGRES_DB", "production_savvyo_db")

        if not password:
            raise ValueError(
                "POSTGRES_PASSWORD not found in environment. "
                "Please set it in .env file for production use."
            )

        encoded_user = quote_plus(user)
        encoded_password = quote_plus(password)

        url = f"postgresql://{encoded_user}:{encoded_password}@{host}:{port}/{database}"

        # ***> Log safe version without password <***
        safe_url = f"postgresql://{user}:***@{host}:{port}/{database}"
        logging.info("PostgreSQL URL configured: %s", safe_url)

        return url

    @classmethod
    def for_environment(cls, environment: str) -> "DatabaseConfig":
        """
        Create database configuration based on environment.
        Single source of truth for environment-based database selection.

        Args:
            environment: 'development', 'testing', or 'production'

        Returns:
            DatabaseConfig with appropriate database type and settings
        """
        environment = environment.lower()

        if environment == "production":
            return cls(
                database_url=cls._build_postgres_url(),
                database_type="postgresql",
                echo=False,
                pool_size=10,
                max_overflow=20,
                pool_timeout=60,
            )
        elif environment == "testing":
            return cls(
                database_url=cls._build_sqlite_url(environment),
                database_type="sqlite",
                echo=False,
                pool_size=1,
            )
        else:  # ***> development (default) <***
            return cls(
                database_url=cls._build_sqlite_url(environment),
                database_type="sqlite",
                echo=True,
                pool_size=3,
            )

    @classmethod
    def development(cls) -> "DatabaseConfig":
        """
        Development configuration - SQLite file-based.
        """
        return cls.for_environment("development")

    @classmethod
    def testing(cls) -> "DatabaseConfig":
        """
        Testing configuration - SQLite in-memory.
        """
        return cls.for_environment("testing")

    @classmethod
    def production(cls) -> "DatabaseConfig":
        """
        Production configuration - PostgreSQL from environment.
        """
        return cls.for_environment("production")

    @classmethod
    def from_url(cls, url: str) -> "DatabaseConfig":
        """
        Create config from custom URL.
        Determines database type from URL scheme.
        """
        db_type = "postgresql" if url.startswith("postgresql://") else "sqlite"
        return cls(database_url=url, database_type=db_type)

    @classmethod
    def for_colleague(cls, host_ip: str) -> "DatabaseConfig":
        """
        Create configuration for colleague connecting to your PostgreSQL database.

        Args:
            host_ip: Your machine's IP address
        """
        user = os.getenv("POSTGRES_USER", "aseathletics_datascience")
        password = os.getenv("POSTGRES_PASSWORD")
        port = os.getenv("POSTGRES_PORT", "5432")
        database = os.getenv("POSTGRES_DB", "production_savvyo_db")

        if not password:
            raise ValueError("POSTGRES_PASSWORD required for colleague connection")

        encoded_user = quote_plus(user)
        encoded_password = quote_plus(password)

        url = f"postgresql://{encoded_user}:{encoded_password}@{host_ip}:{port}/{database}"

        return cls(
            database_url=url, database_type="postgresql", echo=False, pool_size=5
        )

    def is_sqlite(self) -> bool:
        """
        Check if this configuration uses SQLite.
        """
        return self.database_type == "sqlite"

    def is_postgresql(self) -> bool:
        """
        Check if this configuration uses PostgreSQL.
        """
        return self.database_type == "postgresql"

    def validate_connection(self) -> bool:
        """
        Test database connection.
        """
        try:
            engine_kwargs = {"pool_pre_ping": True}

            # ***> SQLite-specific connection arguments <***
            if self.is_sqlite():
                engine_kwargs["connect_args"] = {"check_same_thread": False}

            engine = create_engine(self.database_url, **engine_kwargs)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))

            logging.info("Database connection validated successfully")
            return True

        except Exception as e:
            logging.error("Database connection failed: %s", str(e))
            return False

    def get_connection_info(self) -> dict:
        """
        Get safe connection information for logging.
        """
        info = {
            "database_type": self.database_type,
            "pool_size": self.pool_size,
            "echo": self.echo,
        }

        if self.is_postgresql():
            parts = self.database_url.replace("postgresql://", "").split("@")
            if len(parts) == 2:
                user_pass = parts[0].split(":")[0]
                host_db = parts[1]

                info.update({"user": user_pass, "host_db": host_db})
        else:
            info["database_file"] = self.database_url.split("/")[-1]

        return info


# ***> Configuration constants <***
SUPPORTED_ENVIRONMENTS = ["development", "testing", "production"]
DEFAULT_ENVIRONMENT = "development"


def get_database_config(environment: str = "") -> DatabaseConfig:
    """
    Get database configuration for specified environment.
    Uses environment variable if not specified.

    Args:
        environment: Target environment name

    Returns:
        DatabaseConfig instance for the environment
    """
    if environment is None:
        environment = os.getenv("ENVIRONMENT", DEFAULT_ENVIRONMENT)

    if environment not in SUPPORTED_ENVIRONMENTS:
        logging.warning(
            "Unknown environment '%s', using default '%s'",
            environment,
            DEFAULT_ENVIRONMENT,
        )
        environment = DEFAULT_ENVIRONMENT

    return DatabaseConfig.for_environment(environment)


if __name__ == "__main__":
    # ***> Test script for database configuration <***
    import sys

    test_env = sys.argv[1] if len(sys.argv) > 1 else "development"

    try:
        config = get_database_config(test_env)
        info = config.get_connection_info()

        print(f"Environment: {test_env}")
        print(f"Database Type: {info['database_type']}")
        print(f"Configuration: {info}")

        if config.validate_connection():
            print("✅ Database connection successful!")
        else:
            print("❌ Database connection failed!")

    except Exception as e:
        print(f"❌ Configuration error: {e}")
