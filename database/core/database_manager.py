# database/core/database_manager.py
"""
Core database management and connection handling
"""

from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict

from sqlalchemy import create_engine, event, text
from sqlalchemy.exc import (
    DisconnectionError,
    IntegrityError,
    OperationalError,
    SQLAlchemyError,
    TimeoutError,
)
from sqlalchemy.orm import sessionmaker

from database.base import Base
from exceptions import (
    DatabaseConfigurationError,
    DatabaseConnectionError,
    DatabaseOperationError,
)


class DatabaseManager:
    """
    Core database connection and session management
    Handles: connections, sessions, health checks, table operations
    """

    def __init__(self, database_url: str, echo: bool = False):
        """
        Initialize database manager with connection parameters

        Args:
            database_url: Database connection URL
            echo: Whether to echo SQL queries (for debugging)

        Raises:
            DatabaseConfigurationError: If database URL is invalid
            DatabaseConnectionError: If connection cannot be established
        """
        if not database_url or not isinstance(database_url, str):
            raise DatabaseConfigurationError("Database URL must be a non-empty string")

        self.database_url = database_url
        self.echo = echo
        self.engine = None
        self.SessionLocal = None
        self.db_type = self._detect_database_type()

        try:
            self._initialize_database()
        except Exception as error:
            raise

    def _detect_database_type(self) -> str:
        """
        Detect the database type from the URL

        Returns:
            Database type string ('sqlite', 'postgresql', etc)
        """
        supported_schemes = ("sqlite", "postgresql", "mysql", "oracle")
        for scheme in supported_schemes:
            if self.database_url.startswith(scheme):
                return scheme
        return "unknown"

    def _initialize_database(self) -> None:
        """
        Initialize database engine with database-specific optimizations

        Raises:
            DatabaseConnectionError: If database connection fails
            DatabaseConfigurationError: If database configuration is invalid
        """
        try:
            # ***> Validate database URL format <***
            supported_schemes = ("sqlite", "postgresql", "mysql", "oracle")
            if not self.database_url.startswith(supported_schemes):
                raise DatabaseConfigurationError(
                    f"Unsupported database type in URL: {self.database_url}"
                )

            # ***> Create engine with database-specific settings <***
            if self.db_type == "sqlite":
                self.engine = self._create_sqlite_engine()
            elif self.db_type == "postgresql":
                self.engine = self._create_postgresql_engine()
            else:
                self.engine = self._create_generic_engine()

            # ***> Test connection <***
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))

            # ***> Create session factory <***
            self.SessionLocal = sessionmaker(
                autocommit=False, autoflush=False, bind=self.engine
            )

        except OperationalError as error:
            raise DatabaseConnectionError(f"Failed to connect to database: {error}")
        except SQLAlchemyError as error:
            raise DatabaseConnectionError(f"Database configuration error: {error}")
        except Exception as error:
            raise DatabaseConnectionError(
                f"Unexpected database initialization error: {error}"
            )

    def _create_sqlite_engine(self):
        """
        Create optimized SQLite engine
        """
        engine = create_engine(
            self.database_url,
            echo=self.echo,
            connect_args={"check_same_thread": False},
            pool_pre_ping=True,
            pool_recycle=300,
        )

        # ***> Enable foreign key constraints for SQLite <***
        @event.listens_for(engine, "connect")
        def set_sqlite_pragma(dbapi_connection, connection_record):
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA synchronous=NORMAL")
            cursor.close()

        return engine

    def _create_postgresql_engine(self):
        """
        Create optimized PostgreSQL engine
        """
        return create_engine(
            self.database_url,
            echo=self.echo,
            pool_pre_ping=True,
            pool_recycle=3600,
            pool_size=20,
            max_overflow=30,
            pool_timeout=30,
        )

    def _create_generic_engine(self):
        """
        Create generic engine for other database types
        """
        return create_engine(
            self.database_url, echo=self.echo, pool_pre_ping=True, pool_recycle=3600
        )

    @contextmanager
    def get_session(self):
        """
        Context manager for database sessions with automatic cleanup

        Yields:
            Session: SQLAlchemy session object

        Raises:
            DatabaseConnectionError: If session cannot be created
            DatabaseOperationError: If session operation fails
        """
        if not self.SessionLocal:
            raise DatabaseConnectionError(
                "Database not initialized - SessionLocal is None"
            )

        session = None

        try:
            session = self.SessionLocal()
            yield session
            session.commit()
        except DisconnectionError as error:
            if session:
                session.rollback()
            raise DatabaseConnectionError(
                f"Database connection lost: {error}"
            ) from error
        except IntegrityError as error:
            if session:
                session.rollback()
            raise DatabaseOperationError(
                f"Data integrity violation: {error}"
            ) from error
        except TimeoutError as error:
            if session:
                session.rollback()
            raise DatabaseOperationError(
                f"Database operation timeout: {error}"
            ) from error
        except SQLAlchemyError as error:
            if session:
                session.rollback()
            raise DatabaseOperationError(f"Database session error: {error}") from error
        except Exception as error:
            if session:
                session.rollback()
            raise DatabaseOperationError(
                f"Unexpected session error: {error}"
            ) from error
        finally:
            if session:
                session.close()

    def create_tables(self) -> None:
        """
        Create all tables defined in the models

        Raises:
            DatabaseOperationError: If table creation fails
            DatabaseConnectionError: If database connection is lost
        """
        try:
            # ***> Ensure directory exists for SQLite databases <***
            if self.database_url.startswith(
                "sqlite"
            ) and not self.database_url.endswith(":memory:"):
                self._ensure_sqlite_directory()

            # ***> Create all tables <***
            Base.metadata.create_all(bind=self.engine)
        except OperationalError as error:
            raise DatabaseConnectionError(
                f"Database connection lost during table creation: {error}"
            ) from error
        except SQLAlchemyError as error:
            raise DatabaseOperationError(f"Failed to create tables: {error}") from error
        except Exception as error:
            raise DatabaseOperationError(
                f"Unexpected table creation error: {error}"
            ) from error

    def _ensure_sqlite_directory(self) -> None:
        """
        Ensure directory exists for SQLite database file
        """
        db_path = self.database_url.replace("sqlite:///", "")
        db_dir = Path(db_path).parent
        db_dir.mkdir(parents=True, exist_ok=True)

    def health_check(self) -> bool:
        """
        Check if database connection is healthy

        Returns:
            True if connection is healthy, False otherwise
        """
        try:
            with self.get_session() as session:
                session.execute(text("SELECT 1"))
            return True
        except (DatabaseConnectionError, DatabaseOperationError):
            return False

    def get_connection_info(self) -> Dict[str, Any]:
        """
        Get database connection information for monitoring

        Returns:
            Connection information dictionary
        """
        try:
            # ***> Safely extract database URL without credentials <***
            safe_url = self.database_url
            if "@" in safe_url:
                safe_url = safe_url.split("@")[-1]

            info = {
                "database_type": self.db_type,
                "database_url": safe_url,
                "engine_echo": self.echo,
                "is_connected": self.health_check(),
            }

            if self.engine:
                pool_size = self._get_pool_size_safely()
                checked_out = self._get_checked_out_safely()

                info.update(
                    {
                        "pool_size": pool_size,
                        "checked_out_connections": checked_out,
                        "pool_class": self.engine.pool.__class__.__name__,
                    }
                )

            return info

        except Exception as error:
            return {"error": f"{error}"}

    def _get_pool_size_safely(self) -> str:
        """
        Safely get pool size information
        """
        pool_size = "N/A"
        if self.engine is not None:
            size_attr = getattr(self.engine.pool, "size", None)
            if size_attr is not None:
                pool_size = size_attr() if callable(size_attr) else size_attr
            else:
                pool_size = "Unknown"
        return str(pool_size)

    def _get_checked_out_safely(self) -> str:
        """
        Safely get checked out connections count
        """
        checked_out = "N/A"
        if self.engine is not None:
            checked_out_attr = getattr(self.engine.pool, "checkedout", None)
            if checked_out_attr is not None:
                checked_out = (
                    checked_out_attr()
                    if callable(checked_out_attr)
                    else checked_out_attr
                )
        return str(checked_out)
