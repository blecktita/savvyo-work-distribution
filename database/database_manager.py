# core/database/database_manager.py

"""
Database manager
"""
from contextlib import contextmanager
from typing import List, Optional, Dict, Any
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import (
    SQLAlchemyError,
    IntegrityError,
    OperationalError,
    DisconnectionError,
    TimeoutError,
    InvalidRequestError
)

from exceptions import (DatabaseConfigurationError,
                        DatabaseConnectionError, 
                        DatabaseOperationError)

from .database_models import Base, Competition, Team


class DatabaseManager:
    """
    Centralized database manager.
    """

    def __init__(self, database_url: str, echo: bool = False):
        """
        Initialize database manager with connection parameters.

        Args:
            database_url (str): Database connection URL
            echo (bool): Whether to echo SQL queries (for debugging)

        Raises:
            DatabaseConfigurationError: If database URL is invalid
            DatabaseConnectionError: If connection cannot be established
        """
        if not database_url or not isinstance(database_url, str):
            raise DatabaseConfigurationError(
                "Database URL must be a non-empty string"
            )

        self.database_url = database_url
        self.echo = echo
        self.engine = None
        self.SessionLocal = None

        try:
            self._initialize_database()
        except Exception as error:
            raise

    def _initialize_database(self) -> None:
        """
        Initialize database engine and session factory.

        Raises:
            DatabaseConnectionError: If database connection fails
            DatabaseConfigurationError: If database configuration is invalid
        """
        try:
            # Validate database URL format
            supported_schemes = ('sqlite', 'postgresql', 'mysql', 'oracle')
            if not self.database_url.startswith(supported_schemes):
                raise DatabaseConfigurationError(
                    f"Unsupported database type in URL: {self.database_url}"
                )

            # Create engine with proper settings based on database type
            if self.database_url.startswith('sqlite'):
                self.engine = create_engine(
                    self.database_url,
                    echo=self.echo,
                    connect_args={"check_same_thread": False},
                    pool_pre_ping=True,
                    pool_recycle=300
                )
            else:
                self.engine = create_engine(
                    self.database_url,
                    echo=self.echo,
                    pool_pre_ping=True,
                    pool_recycle=3600
                )

            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))

            # Create session factory
            self.SessionLocal = sessionmaker(
                autocommit=False,
                autoflush=False,
                bind=self.engine
            )
        except OperationalError as error:
            raise DatabaseConnectionError(
                ("Failed to connect to database: %s", str(error))
            )
        except SQLAlchemyError as error:
            raise DatabaseConnectionError(
                ("Database configuration error: %s", str(error))
            )
        except Exception as error:
            raise DatabaseConnectionError(
                ("Unexpected database initialization error: %s", str(error))
            )

    def _get_pool_size_safely(self) -> str:
        """Safely get pool size information."""
        pool_size = 'N/A'
        if self.engine is not None:
            size_attr = getattr(self.engine.pool, 'size', None)
            if size_attr is not None:
                pool_size = size_attr() if callable(size_attr) else size_attr
            else:
                pool_size = 'Unknown'
        return str(pool_size)

    def create_tables(self) -> None:
        """
        Create all tables defined in the models.
        Raises:
            DatabaseOperationError: If table creation fails
            DatabaseConnectionError: If database connection is lost
        """
        try:
            # Ensure directory exists for SQLite databases
            if (self.database_url.startswith('sqlite') and 
                not self.database_url.endswith(':memory:')):
                self._ensure_sqlite_directory()
            # Create all tables
            Base.metadata.create_all(bind=self.engine)
        except OperationalError as error:
            raise DatabaseConnectionError(
                "Database connection lost during table creation: %s" % error
            ) from error
        except SQLAlchemyError as error:
            raise DatabaseOperationError(
                "Failed to create tables: %s" % error
            ) from error
        except Exception as error:
            raise DatabaseOperationError(
                "Unexpected table creation error: %s" % error
            ) from error

    def _ensure_sqlite_directory(self) -> None:
        """
        Ensure directory exists for SQLite database file
        """
        db_path = self.database_url.replace('sqlite:///', '')
        db_dir = Path(db_path).parent
        db_dir.mkdir(parents=True, exist_ok=True)

    def drop_tables(self) -> None:
        """
        Drop all tables. Use with caution!

        Raises:
            DatabaseOperationError: If table dropping fails
            DatabaseConnectionError: If database connection is lost
        """
        try:
            Base.metadata.drop_all(bind=self.engine)
        except OperationalError as error:
            raise DatabaseConnectionError(
                "Database connection lost during table dropping: %s" % error
            ) from error
        except SQLAlchemyError as error:
            raise DatabaseOperationError(
                "Failed to drop tables: %s" % error
            ) from error
        except Exception as error:
            raise DatabaseOperationError(
                "Unexpected table dropping error: %s" % error
            ) from error

    @contextmanager
    def get_session(self):
        """
        Context manager for database sessions with automatic cleanup.

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
                "Database connection lost: %s" % error
            ) from error
        except IntegrityError as error:
            if session:
                session.rollback()
            raise DatabaseOperationError(
                "Data integrity violation: %s" % error
            ) from error
        except TimeoutError as error:
            if session:
                session.rollback()
            raise DatabaseOperationError(
                "Database operation timeout: %s" % error
            ) from error
        except SQLAlchemyError as error:
            if session:
                session.rollback()
            raise DatabaseOperationError(
                "Database session error: %s" % error
            ) from error
        except Exception as error:
            if session:
                session.rollback()
            raise DatabaseOperationError(
                "Unexpected session error: %s" % error
            ) from error
        finally:
            if session:
                session.close()

    def health_check(self) -> bool:
        """
        Check if database connection is healthy.

        Returns:
            bool: True if connection is healthy, False otherwise
        """
        try:
            with self.get_session() as session:
                session.execute(text("SELECT 1"))
            return True
        except (DatabaseConnectionError, DatabaseOperationError) as error:
            return False

    def get_connection_info(self) -> Dict[str, Any]:
        """
        Get database connection information for monitoring.

        Returns:
            Dict[str, Any]: Connection information
        """
        try:
            # Safely extract database URL without credentials
            safe_url = self.database_url
            if '@' in safe_url:
                safe_url = safe_url.split('@')[-1]

            info = {
                "database_url": safe_url,
                "engine_echo": self.echo,
                "is_connected": self.health_check()
            }

            if self.engine:
                pool_size = self._get_pool_size_safely()
                checked_out = self._get_checked_out_safely()
                        
                info.update({
                    "pool_size": pool_size,
                    "checked_out_connections": checked_out,
                    "pool_class": self.engine.pool.__class__.__name__
                })

            return info

        except Exception as error:
            return {"error": "%s" % error}

    def _get_checked_out_safely(self) -> str:
        """
        Safely get checked out connections count
        """
        checked_out = 'N/A'
        if self.engine is not None:
            checked_out_attr = getattr(self.engine.pool, 'checkedout', None)
            if checked_out_attr is not None:
                checked_out = checked_out_attr() if callable(checked_out_attr) else checked_out_attr
        return str(checked_out)


class CompetitionRepository:
    """
    Repository pattern implementation for Competition operations.

    This class handles all CRUD operations for Competition entities
    with proper error handling and logging.
    """

    def __init__(self, db_manager: DatabaseManager):
        """
        Initialize repository with database manager.

        Args:
            db_manager (DatabaseManager): Database manager instance

        Raises:
            ValueError: If db_manager is None or invalid
        """
        if not isinstance(db_manager, DatabaseManager):
            raise ValueError("db_manager must be a DatabaseManager instance")

        self.db_manager = db_manager

    def create(self, competition_data: Dict[str, Any]) -> Competition:
        """
        Create a new competition record.

        Args:
            competition_data (Dict[str, Any]): Competition data dictionary

        Returns:
            Competition: Created competition object

        Raises:
            ValueError: If competition_data is invalid
            IntegrityError: If competition_id already exists
            DatabaseOperationError: For other database errors
        """

        competition_id = competition_data.get('competition_id', 'unknown')

        # Validate input data
        if not competition_data:
            raise ValueError("Competition data cannot be empty")

        if not competition_data.get('competition_id'):
            raise ValueError("Competition ID is required")

        try:
            with self.db_manager.get_session() as session:
                competition = Competition.from_dict(competition_data)
                session.add(competition)
                session.flush()
                session.refresh(competition)

                return competition
        except IntegrityError as error:
            raise DatabaseOperationError(
                f"Competition with ID {competition_id} already exists"
            ) from error
        except InvalidRequestError as error:
            raise ValueError(f"Invalid competition data: {str(error)}")
        except (DatabaseConnectionError, DatabaseOperationError):
            raise
        except SQLAlchemyError as error:
            raise DatabaseOperationError(
                f"Database error creating competition: {str(error)}"
            )

    def get_by_id(self, competition_id: str) -> Optional[Competition]:
        """
        Retrieve competition by ID.

        Args:
            competition_id (str): Competition identifier

        Returns:
            Optional[Competition]: Competition object or None if not found

        Raises:
            ValueError: If competition_id is invalid
            DatabaseOperationError: For database errors
        """

        if not competition_id or not isinstance(competition_id, str):
            raise ValueError("Competition ID must be a non-empty string")

        try:
            with self.db_manager.get_session() as session:
                competition = session.query(Competition).filter(
                    Competition.competition_id == competition_id
                ).first()

                result = (competition.competition_id if competition 
                         else "Not found")

                return competition
        except (DatabaseConnectionError, DatabaseOperationError):
            raise
        except SQLAlchemyError as error:
            raise DatabaseOperationError(
                f"Database error retrieving competition: {str(error)}"
            )

    def get_all(self, active_only: bool = True) -> List[Competition]:
        """
        Retrieve all competitions.

        Args:
            active_only (bool): Whether to return only active competitions

        Returns:
            List[Competition]: List of competition objects

        Raises:
            DatabaseOperationError: For database errors
        """

        try:
            with self.db_manager.get_session() as session:
                query = session.query(Competition)
                if active_only:
                    query = query.filter(Competition.is_active)
                
                competitions = query.all()

                return competitions
        except (DatabaseConnectionError, DatabaseOperationError):
            raise
        except SQLAlchemyError as error:
            raise DatabaseOperationError(
                f"Database error retrieving competitions: {str(error)}"
            )

    def update(self, competition_id: str, 
               update_data: Dict[str, Any]) -> Optional[Competition]:
        """
        Update competition record.

        Args:
            competition_id (str): Competition identifier
            update_data (Dict[str, Any]): Data to update

        Returns:
            Optional[Competition]: Updated competition object or None

        Raises:
            ValueError: If parameters are invalid
            DatabaseOperationError: For database errors
        """
        if not competition_id or not isinstance(competition_id, str):
            raise ValueError("Competition ID must be a non-empty string")

        if not update_data:
            raise ValueError("Update data cannot be empty")

        try:
            with self.db_manager.get_session() as session:
                competition = session.query(Competition).filter(
                    Competition.competition_id == competition_id
                ).first()

                if not competition:
                    return None

                # Update fields
                updated_fields = []
                for key, value in update_data.items():
                    if hasattr(competition, key):
                        old_value = getattr(competition, key)
                        setattr(competition, key, value)
                        updated_fields.append(f"{key}: {old_value} -> {value}")
                    else:
                        pass

                session.flush()
                session.refresh(competition)

                return competition
        except (DatabaseConnectionError, DatabaseOperationError):
            raise
        except SQLAlchemyError as error:
            raise DatabaseOperationError(
                f"Database error updating competition: {str(error)}"
            )

    def delete(self, competition_id: str, soft_delete: bool = True) -> bool:
        """
        Delete competition record.

        Args:
            competition_id (str): Competition identifier
            soft_delete (bool): Whether to soft delete or hard delete

        Returns:
            bool: True if deleted successfully, False if not found

        Raises:
            ValueError: If competition_id is invalid
            DatabaseOperationError: For database errors
        """
        if not competition_id or not isinstance(competition_id, str):
            raise ValueError("Competition ID must be a non-empty string")

        try:
            with self.db_manager.get_session() as session:
                competition = session.query(Competition).filter(
                    Competition.competition_id == competition_id
                ).first()

                if not competition:
                    return False

                if soft_delete:
                    competition.is_active = False  # type: ignore

                return True
        except (DatabaseConnectionError, DatabaseOperationError):
            raise
        except SQLAlchemyError as error:
            raise DatabaseOperationError(
                f"Database error deleting competition: {str(error)}"
            )

    def search_by_name(self, name_pattern: str) -> List[Competition]:
        """
        Search competitions by name pattern.

        Args:
            name_pattern (str): Name pattern to search for

        Returns:
            List[Competition]: List of matching competitions

        Raises:
            ValueError: If name_pattern is invalid
            DatabaseOperationError: For database errors
        """
        if not name_pattern or not isinstance(name_pattern, str):
            raise ValueError("Name pattern must be a non-empty string")

        try:
            with self.db_manager.get_session() as session:
                competitions = session.query(Competition).filter(
                    Competition.competition_name.like(f"%{name_pattern}%"),
                    Competition.is_active
                ).all()

                return competitions
        except (DatabaseConnectionError, DatabaseOperationError):
            raise
        except SQLAlchemyError as error:
            raise DatabaseOperationError(
                f"Database error searching competitions: {str(error)}"
            )

    def get_by_type(self, competition_type: str) -> List[Competition]:
        """
        Get competitions by type.

        Args:
            competition_type (str): Competition type

        Returns:
            List[Competition]: List of competitions of specified type

        Raises:
            ValueError: If competition_type is invalid
            DatabaseOperationError: For database errors
        """
        if not competition_type or not isinstance(competition_type, str):
            raise ValueError("Competition type must be a non-empty string")

        try:
            with self.db_manager.get_session() as session:
                competitions = session.query(Competition).filter(
                    Competition.competition_type == competition_type,
                    Competition.is_active
                ).all()

                return competitions
        except (DatabaseConnectionError, DatabaseOperationError):
            raise
        except SQLAlchemyError as error:
            raise DatabaseOperationError(
                f"Database error retrieving competitions by type: {str(error)}"
            )


class TeamRepository:
    """
    Repository pattern implementation for Team operations.

    This class handles all CRUD operations for Team entities
    with proper error handling and logging.
    """

    def __init__(self, db_manager: DatabaseManager):
        """
        Initialize repository with database manager.

        Args:
            db_manager (DatabaseManager): Database manager instance

        Raises:
            ValueError: If db_manager is None or invalid
        """
        if not isinstance(db_manager, DatabaseManager):
            raise ValueError("db_manager must be a DatabaseManager instance")

        self.db_manager = db_manager

    def get_by_composite_key(
        self,
        club_id: str,
        season_year: str,
        competition_id: str
    ) -> Optional[Team]:
        """
        ✅ NEW: Get team by composite primary key (club_id, season_year, competition_id).
        
        Args:
            club_id: Club identifier
            season_year: Season year
            competition_id: Competition identifier
            
        Returns:
            Team object or None if not found
        """
        if not all([club_id, season_year, competition_id]):
            raise ValueError("All key components (club_id, season_year, competition_id) are required")

        try:
            with self.db_manager.get_session() as session:
                team = session.query(Team).filter(
                    Team.club_id == club_id,
                    Team.season_year == season_year,
                    Team.competition_id == competition_id
                ).first()

                return team
        except (DatabaseConnectionError, DatabaseOperationError):
            raise
        except SQLAlchemyError as error:
            raise DatabaseOperationError(f"Database error retrieving team: {str(error)}")

    def update_by_composite_key(
        self,
        club_id: str,
        season_year: str, 
        competition_id: str,
        update_data: Dict[str, Any]
    ) -> Optional[Team]:
        """
        Update team by composite primary key.
        """
        if not all([club_id, season_year, competition_id]):
            raise ValueError("All key components are required")
        if not update_data:
            raise ValueError("Update data cannot be empty")

        try:
            with self.db_manager.get_session() as session:
                team = session.query(Team).filter(
                    Team.club_id == club_id,
                    Team.season_year == season_year,
                    Team.competition_id == competition_id
                ).first()

                if not team:
                    return None

                # Update fields
                updated_fields = []
                excluded_fields = {'club_id', 'season_year', 'competition_id'}
                
                for key, value in update_data.items():
                    if key in excluded_fields:
                        continue  # Skip primary key fields
                    if hasattr(team, key):
                        old_value = getattr(team, key)
                        setattr(team, key, value)
                        updated_fields.append(f"{key}: {old_value} -> {value}")

                session.flush()
                session.refresh(team)
                return team
        except (DatabaseConnectionError, DatabaseOperationError):
            raise
        except SQLAlchemyError as error:
            raise DatabaseOperationError(f"Database error updating team: {str(error)}")


    def create(self, team_data: Dict[str, Any]) -> Team:
        """
        Create a new team record.

        Args:
            team_data (Dict[str, Any]): Team data dictionary

        Returns:
            Team: Created team object

        Raises:
            ValueError: If team_data is invalid
            IntegrityError: If club_id already exists
            DatabaseOperationError: For other database errors
        """
        club_id = team_data.get('club_id', 'unknown')

        # Validate input data
        if not team_data:
            raise ValueError("Team data cannot be empty")

        required_fields = ['club_id', 'club_name', 'competition_id', 'season_id']
        for field in required_fields:
            if not team_data.get(field):
                raise ValueError(f"{field} is required")

        try:
            with self.db_manager.get_session() as session:
                team = Team.from_dict(team_data)
                session.add(team)
                session.flush()
                session.refresh(team)

                return team
        except IntegrityError as error:
            raise DatabaseOperationError(
                f"Team with ID {club_id} already exists"
            ) from error
        except InvalidRequestError as error:
            raise ValueError(f"Invalid team data: {str(error)}")
        except (DatabaseConnectionError, DatabaseOperationError):
            raise
        except SQLAlchemyError as error:
            raise DatabaseOperationError(
                f"Database error creating team: {str(error)}"
            )

    def get_by_id(self, club_id: str) -> Optional[Team]:
        """
        Retrieve team by club ID.

        Args:
            club_id (str): Club identifier

        Returns:
            Optional[Team]: Team object or None if not found

        Raises:
            ValueError: If club_id is invalid
            DatabaseOperationError: For database errors
        """
        if not club_id or not isinstance(club_id, str):
            raise ValueError("Club ID must be a non-empty string")

        try:
            with self.db_manager.get_session() as session:
                team = session.query(Team).filter(
                    Team.club_id == club_id
                ).first()

                return team
        except (DatabaseConnectionError, DatabaseOperationError):
            raise
        except SQLAlchemyError as error:
            raise DatabaseOperationError(
                f"Database error retrieving team: {str(error)}"
            )

    def get_by_competition(self, competition_id: str, 
                          season_id: Optional[str] = None) -> List[Team]:
        """
        Get teams by competition and optionally by season.

        Args:
            competition_id (str): Competition identifier
            season_id (Optional[str]): Season identifier

        Returns:
            List[Team]: List of teams in the competition/season

        Raises:
            ValueError: If competition_id is invalid
            DatabaseOperationError: For database errors
        """
        if not competition_id or not isinstance(competition_id, str):
            raise ValueError("Competition ID must be a non-empty string")

        try:
            with self.db_manager.get_session() as session:
                query = session.query(Team).filter(
                    Team.competition_id == competition_id,
                    Team.is_active
                )
                
                if season_id:
                    query = query.filter(Team.season_id == season_id)
                
                teams = query.all()

                return teams
        except (DatabaseConnectionError, DatabaseOperationError):
            raise
        except SQLAlchemyError as error:
            raise DatabaseOperationError(
                f"Database error retrieving teams by competition: {str(error)}"
            )

    def update(self, club_id: str, 
               update_data: Dict[str, Any]) -> Optional[Team]:
        """
        Update team record.

        Args:
            club_id (str): Club identifier
            update_data (Dict[str, Any]): Data to update

        Returns:
            Optional[Team]: Updated team object or None if not found

        Raises:
            ValueError: If parameters are invalid
            DatabaseOperationError: For database errors
        """
        if not club_id or not isinstance(club_id, str):
            raise ValueError("Club ID must be a non-empty string")

        if not update_data:
            raise ValueError("Update data cannot be empty")

        try:
            with self.db_manager.get_session() as session:
                team = session.query(Team).filter(
                    Team.club_id == club_id
                ).first()

                if not team:
                    return None

                # Update fields
                updated_fields = []
                for key, value in update_data.items():
                    if hasattr(team, key):
                        old_value = getattr(team, key)
                        setattr(team, key, value)
                        updated_fields.append(f"{key}: {old_value} -> {value}")
                    else:
                        pass

                session.flush()
                session.refresh(team)
                return team
        except (DatabaseConnectionError, DatabaseOperationError):
            raise
        except SQLAlchemyError as error:
            raise DatabaseOperationError(
                f"Database error updating team: {str(error)}"
            )

    def delete(self, club_id: str, soft_delete: bool = True) -> bool:
        """
        Delete team record.

        Args:
            club_id (str): Club identifier
            soft_delete (bool): Whether to soft delete or hard delete

        Returns:
            bool: True if deleted successfully, False if not found

        Raises:
            ValueError: If club_id is invalid
            DatabaseOperationError: For database errors
        """

        if not club_id or not isinstance(club_id, str):
            raise ValueError("Club ID must be a non-empty string")

        try:
            with self.db_manager.get_session() as session:
                team = session.query(Team).filter(
                    Team.club_id == club_id
                ).first()

                if not team:
                    return False

                if soft_delete:
                    team.is_active = False  # type: ignore

                return True
        except (DatabaseConnectionError, DatabaseOperationError):
            raise
        except SQLAlchemyError as error:
            raise DatabaseOperationError(
                f"Database error deleting team: {str(error)}"
            )