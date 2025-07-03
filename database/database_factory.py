# core/database/database_factory.py

"""
Database factory and service layer.
"""

from typing import Optional, Dict, Any, List, Callable
from contextlib import contextmanager
from sqlalchemy import select

from configurations.factory import get_config, DatabaseConfig
from exceptions import DatabaseServiceError

from .database_manager import DatabaseManager, CompetitionRepository, TeamRepository
from .database_models import Competition, Team


class DatabaseFactory:
    """
    Factory class for creating database instances and repositories.
    """

    _instances: Dict[str, DatabaseManager] = {}

    @classmethod
    def create_database_manager(
        cls, config: DatabaseConfig, instance_name: str = "default"
    ) -> DatabaseManager:
        """
        Create or retrieve a database manager instance.

        Args:
            config (DatabaseConfig): Database configuration
            instance_name (str): Name for the instance (for caching)

        Returns:
            DatabaseManager: Database manager instance
        """

        if instance_name not in cls._instances:
            cls._instances[instance_name] = DatabaseManager(
                database_url=config.database_url, echo=config.echo
            )
        
        return cls._instances[instance_name]

    @classmethod
    def create_competition_repository(
        cls, config: DatabaseConfig, instance_name: str = "default"
    ) -> CompetitionRepository:
        """
        Create a competition repository with database manager.

        Args:
            config (DatabaseConfig): Database configuration
            instance_name (str): Name for the database instance

        Returns:
            CompetitionRepository: Competition repository instance
        """
        db_manager = cls.create_database_manager(config, instance_name)
        repository = CompetitionRepository(db_manager)
        return repository

    @classmethod
    def create_team_repository(
        cls, config: DatabaseConfig, instance_name: str = "default"
    ) -> TeamRepository:
        """
        Create a team repository with database manager.

        Args:
            config (DatabaseConfig): Database configuration
            instance_name (str): Name for the database instance

        Returns:
            TeamRepository: Team repository instance
        """
        db_manager = cls.create_database_manager(config, instance_name)
        repository = TeamRepository(db_manager)
        return repository

    @classmethod
    def clear_instances(cls) -> None:
        """
        Clear all cached database instances
        """
        cls._instances.clear()

    @classmethod
    def get_instance(cls, instance_name: str = "default") -> Optional[DatabaseManager]:
        """
        Get a cached database manager instance.

        Args:
            instance_name (str): Name of the instance

        Returns:
            Optional[DatabaseManager]: Database manager instance or None
        """
        return cls._instances.get(instance_name)


class DatabaseService:
    """
    Service class for database operations.
    """

    def __init__(self, config: DatabaseConfig):
        """
        Initialize database service with configuration.

        Args:
            config (DatabaseConfig): Database configuration
        """
        self.config = config
        self.db_manager = DatabaseFactory.create_database_manager(config)
        self.competition_repo = CompetitionRepository(self.db_manager)
        self.team_repo = TeamRepository(self.db_manager)
        self._initialized = False

    def initialize(self, create_tables: bool = True) -> None:
        """
        Initialize the database service.

        Args:
            create_tables (bool): Whether to create tables if they don't exist
        """
        try:
            if create_tables:
                self.db_manager.create_tables()

            # Verify connection
            if not self.db_manager.health_check():
                raise RuntimeError("Database health check failed")

            self._initialized = True
        except Exception as error:
            raise DatabaseServiceError(
                "Failed to initialize database service"
            ) from error

    def cleanup(self) -> None:
        """
        Cleanup database resources
        """
        try:
            if hasattr(self.db_manager, "engine") and self.db_manager.engine:
                self.db_manager.engine.dispose()
            self._initialized = False
        except Exception as error:
            raise DatabaseServiceError("Failed to cleanup database service") from error

    @contextmanager
    def transaction(self):
        """
        Context manager for database transactions
        """
        if not self._initialized:
            self.initialize()

        with self.db_manager.get_session() as session:
            yield session

    def _execute_operation(
        self, 
        operation_name: str, 
        operation_func: Callable, 
        identifier_key: str,
        *args, 
        **kwargs
    ) -> Any:
        """
        Execute operation
        
        Args:
            operation_name: Name of the operation for logging
            operation_func: Function to execute
            identifier_key: Key to extract identifier from args/kwargs (e.g., 'competition_id', 'club_id')
            *args: Positional arguments for operation_func
            **kwargs: Keyword arguments for operation_func
        
        Returns:
            Result of operation_func
            
        Raises:
            DatabaseServiceError: If operation fails
        """
        if not self._initialized:
            self.initialize()
        
        try:
            return operation_func(*args, **kwargs)
        except Exception as error:
            raise DatabaseServiceError(f"Could not {operation_name}") from error

    def _add_entity_impl(self, entity_data: Dict[str, Any], entity_class):
        """
        Generic implementation for adding entities to the database.
        
        Args:
            entity_data: Data dictionary to create the entity
            entity_class: The model class (Competition, Team, etc.)
        
        Returns:
            The created entity instance
        """
        with self.db_manager.get_session() as session:
            entity = entity_class.from_dict(entity_data)
            session.add(entity)
            session.commit()
            session.expunge(entity)
            return entity

    # ========== Competition Operations ==========
    def add_competition(self, competition_data: Dict[str, Any]) -> Competition:
        """
        Add a new competition.

        Args:
            competition_data (Dict[str, Any]): Competition data

        Returns:
            Competition: Created competition object
        """
        return self._execute_competition_operation(
            "add_competition", self._add_competition_impl, competition_data
        )

    def _add_competition_impl(self, competition_data: Dict[str, Any]) -> Competition:
        """
        Implementation for adding competition
        """
        return self._add_entity_impl(competition_data, Competition)

    def _execute_competition_operation(
        self, operation_name: str, operation_func: Callable, *args, **kwargs
    ) -> Any:
        """
        Execute competition operation with error handling
        """
        return self._execute_operation(
            operation_name, operation_func, "competition_id", *args, **kwargs
        )

    def get_competition(self, competition_id: str) -> Optional[Competition]:
        """
        Get competition by ID
        """
        return self._execute_competition_operation(
            "get_competition", self.competition_repo.get_by_id, competition_id
        )

    def list_competitions(self, active_only: bool = True) -> List[Competition]:
        """
        List all competitions
        """
        return self._execute_competition_operation(
            "list_competitions", self.competition_repo.get_all, active_only
        )

    def update_competition(
        self, competition_id: str, update_data: Dict[str, Any]
    ) -> Optional[Competition]:
        """
        Update competition
        """
        return self._execute_competition_operation(
            "update_competition",
            self.competition_repo.update,
            competition_id,
            update_data,
        )

    def delete_competition(self, competition_id: str, soft_delete: bool = True) -> bool:
        """
        Delete competition
        """
        return self._execute_competition_operation(
            "delete_competition",
            self.competition_repo.delete,
            competition_id,
            soft_delete,
        )

    def search_competitions(self, name_pattern: str) -> List[Competition]:
        """
        Search competitions by name
        """
        return self._execute_competition_operation(
            "search_competitions", self.competition_repo.search_by_name, name_pattern
        )

    def get_competitions_by_type(self, competition_type: str) -> List[Competition]:
        """
        Get competitions by type
        """
        return self._execute_competition_operation(
            "get_competitions_by_type",
            self.competition_repo.get_by_type,
            competition_type,
        )

    # ========== Team Operations ==========
    def add_team(self, team_data: Dict[str, Any]) -> Team:
        """
        Add a new team.

        Args:
            team_data (Dict[str, Any]): Team data

        Returns:
            Team: Created team object
        """
        return self._execute_team_operation("add_team", self._add_team_impl, team_data)

    def _add_team_impl(self, team_data: Dict[str, Any]) -> Team:
        """
        Implementation for adding team
        """
        return self._add_entity_impl(team_data, Team)

    def _execute_team_operation(
        self, operation_name: str, operation_func: Callable, *args, **kwargs
    ) -> Any:
        """
        Execute team operation with error handling
        """
        return self._execute_operation(
            operation_name, operation_func, "club_id", *args, **kwargs
        )

    def get_team(self, club_id: str) -> Optional[Team]:
        """
        Get team by club ID
        """
        return self._execute_team_operation(
            "get_team", self.team_repo.get_by_id, club_id
        )

    def get_teams_by_competition(
        self, competition_id: str, season_id: Optional[str] = None
    ) -> List[Team]:
        """
        Get teams by competition and optionally by season
        """
        return self._execute_team_operation(
            "get_teams_by_competition",
            self.team_repo.get_by_competition,
            competition_id,
            season_id,
        )

    def update_team(self, club_id: str, update_data: Dict[str, Any]) -> Optional[Team]:
        """
        Update team
        """
        return self._execute_team_operation(
            "update_team", self.team_repo.update, club_id, update_data
        )

    def delete_team(self, club_id: str, soft_delete: bool = True) -> bool:
        """
        Delete team
        """
        return self._execute_team_operation(
            "delete_team", self.team_repo.delete, club_id, soft_delete
        )

    def get_team_by_season(
        self, club_id: str, season_year: str, competition_id: str
    ) -> Optional[Team]:
        """
        Get team by season and competition.

        Args:
            club_id: Club identifier
            season_year: Season year
            competition_id: Competition identifier

        Returns:
            Team object or None if not found
        """
        return self.team_repo.get_by_composite_key(club_id, season_year, competition_id)

    def update_club(
        self,
        club_id: str,
        season_year: str,
        competition_id: str,
        update_data: Dict[str, Any],
    ) -> Optional[Team]:
        """
        Update club data

        Args:
            club_id: Club identifier
            season_year: Season year
            competition_id: Competition identifier
            update_data: Data to update

        Returns:
            Updated team object or None if not found
        """
        return self.team_repo.update_by_composite_key(
            club_id, season_year, competition_id, update_data
        )

    # ========== Bulk Operations ==========
    def add_teams_bulk(self, teams_data: List[Dict[str, Any]]) -> List[Team]:
        """
        Add multiple teams in bulk.

        Args:
            teams_data (List[Dict[str, Any]]): List of team data dictionaries

        Returns:
            List[Team]: List of created team objects
        """
        if not self._initialized:
            self.initialize()

        teams: List[Team] = []
        success_count = 0
        error_count = 0

        for team_data in teams_data:
            try:
                team = self.add_team(team_data)
                teams.append(team)
                success_count += 1
            except Exception as error:
                error_count += 1
                continue
        return teams

    # ========== Analytics and Reporting ==========
    def get_competition_team_summary(self, competition_id: str) -> Dict[str, Any]:
        """
        Get summary statistics for a competition including team information.

        Args:
            competition_id (str): Competition identifier

        Returns:
            Dict[str, Any]: Summary statistics
        """
        if not self._initialized:
            self.initialize()

        try:
            # Get competition
            competition = self.get_competition(competition_id)
            if not competition:
                return {"error": f"Competition {competition_id} not found"}

            # Get teams in competition
            teams = self.get_teams_by_competition(competition_id)

            # Calculate summary statistics
            summary = {
                "competition_id": competition_id,
                "competition_name": competition.competition_name,
                "competition_country": competition.country,
                "competition_tier": competition.tier,
                "total_teams": len(teams),
                "total_squad_size": sum(team.squad_size for team in teams),
                "average_squad_size": (
                    sum(team.squad_size for team in teams) / len(teams) if teams else 0
                ),
                "total_foreign_players": sum(
                    team.number_of_foreign_players for team in teams
                ),
                "average_team_age": (
                    sum(team.average_age_of_players for team in teams) / len(teams)
                    if teams
                    else 0
                ),
                "total_market_value": sum(team.total_market_value for team in teams),
                "average_team_market_value": (
                    sum(team.total_market_value for team in teams) / len(teams)
                    if teams
                    else 0
                ),
            }
            return summary

        except Exception as error:
            raise DatabaseServiceError(
                "Could not generate competition summary"
            ) from error

    def get_non_cup_competitions(self) -> List[Dict[str, str]]:
        """
        Query competitions table for non-cup competitions.

        Returns:
            List of dictionaries containing competition_id and competition_url

        Raises:
            DatabaseServiceError: If database query fails
        """
        return self._execute_competition_operation(
            "get_non_cup_competitions", self._get_non_cup_competitions_impl
        )

    def _get_non_cup_competitions_impl(self) -> List[Dict[str, str]]:
        """Implementation for getting non-cup competitions."""
        with self.db_manager.get_session() as session:
            # Query competitions where tier does not contain "cup" (case-insensitive)
            stmt = select(
                Competition.competition_id, Competition.competition_url
            ).where(~Competition.tier.ilike("%cup%"))
            result = session.execute(stmt)
            competitions = [
                {
                    "competition_id": row.competition_id,
                    "competition_url": row.competition_url,
                }
                for row in result
            ]
            return competitions


def create_database_service(environment: str = "development") -> DatabaseService:
    """
    function to create database service for different environments.

    Args:
        environment (str): Environment name ('development', 'testing', 'production')

    Returns:
        DatabaseService: Configured database service
    """
    try:
        scraper_config = get_config(environment)
        database_config = scraper_config.database
        service = DatabaseService(database_config)
        return service
    except Exception as error:
        raise DatabaseServiceError("Could not create database service") from error