# database/services/database_service.py
"""
High-level database service layer.
Coordinates between repositories and provides business logic operations.
"""

from contextlib import contextmanager
from typing import Any, Callable, Dict, List, Optional

import pandas as pd

from configurations.settings_database import get_database_config
from database.schemas import Competition, Team
from exceptions import DatabaseServiceError

from ..core.database_manager import DatabaseManager
from ..repositories.competition_repository import CompetitionRepository
from ..repositories.team_repository import TeamRepository


class DatabaseService:
    """
    High-level service for coordinated database operations.
    """

    def __init__(self, environment: str = ""):
        """
        Initialize database service with environment-based configuration.

        Args:
            environment: Target environment ('development', 'testing', 'production')
        """
        self.environment = environment
        self.config = get_database_config(environment)
        self.db_manager = DatabaseManager(self.config.database_url, self.config.echo)
        self.competition_repo = CompetitionRepository(self.db_manager)
        self.team_repo = TeamRepository(self.db_manager)
        self._initialized = False

    def initialize(self, create_tables: bool = True) -> None:
        """
        Initialize the database service.

        Args:
            create_tables: Whether to create tables if they don't exist
        """
        try:
            if create_tables:
                self.db_manager.create_tables()

            # ***> Verify connection <***
            if not self.db_manager.health_check():
                raise RuntimeError("Database health check failed")

            self._initialized = True
        except Exception as error:
            raise DatabaseServiceError(
                "Failed to initialize database service"
            ) from error

    def cleanup(self) -> None:
        """
        Cleanup database resources.
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
        Context manager for database transactions.
        """
        if not self._initialized:
            self.initialize()

        with self.db_manager.get_session() as session:
            yield session

    def _execute_operation(
        self, operation_name: str, operation_func: Callable, *args, **kwargs
    ) -> Any:
        """
        Execute operation with error handling and initialization check.
        """
        if not self._initialized:
            self.initialize()

        try:
            return operation_func(*args, **kwargs)
        except Exception as error:
            raise DatabaseServiceError(f"Could not {operation_name}") from error

    # ***> Competition Operations <***
    def add_competition(self, competition_data: Dict[str, Any]) -> Competition:
        """
        Add a new competition.
        """
        return self._execute_operation(
            "add_competition", self.competition_repo.create, competition_data
        )

    def get_competition(self, competition_id: str) -> Optional[Competition]:
        """
        Get competition by ID.
        """
        return self._execute_operation(
            "get_competition", self.competition_repo.get_by_id, competition_id
        )

    def list_competitions(self, active_only: bool = True) -> List[Competition]:
        """
        List all competitions.
        """
        return self._execute_operation(
            "list_competitions", self.competition_repo.get_all, active_only
        )

    def update_competition(
        self, competition_id: str, update_data: Dict[str, Any]
    ) -> Optional[Competition]:
        """
        Update competition.
        """
        return self._execute_operation(
            "update_competition",
            self.competition_repo.update,
            competition_id,
            update_data,
        )

    def delete_competition(self, competition_id: str, soft_delete: bool = True) -> bool:
        """
        Delete competition.
        """
        return self._execute_operation(
            "delete_competition",
            self.competition_repo.delete,
            competition_id,
            soft_delete,
        )

    def search_competitions(self, name_pattern: str) -> List[Competition]:
        """
        Search competitions by name.
        """
        return self._execute_operation(
            "search_competitions", self.competition_repo.search_by_name, name_pattern
        )

    def get_competitions_by_type(self, competition_type: str) -> List[Competition]:
        """
        Get competitions by type.
        """
        return self._execute_operation(
            "get_competitions_by_type",
            self.competition_repo.get_by_type,
            competition_type,
        )

    def get_non_cup_competitions(self) -> List[Dict[str, str]]:
        """
        Query competitions table for non-cup competitions.
        """
        return self._execute_operation(
            "get_non_cup_competitions", self.competition_repo.get_non_cup_competitions
        )

    # ***> Team Operations <***
    def add_team(self, team_data: Dict[str, Any]) -> Team:
        """
        Add a new team.
        """
        return self._execute_operation("add_team", self.team_repo.create, team_data)

    def get_team(self, club_id: str) -> Optional[Team]:
        """
        Get team by club ID.
        """
        return self._execute_operation("get_team", self.team_repo.get_by_id, club_id)

    def get_teams_by_competition(
        self, competition_id: str, season_id: Optional[str] = None
    ) -> List[Team]:
        """
        Get teams by competition and optionally by season.
        """
        return self._execute_operation(
            "get_teams_by_competition",
            self.team_repo.get_by_competition,
            competition_id,
            season_id,
        )

    def update_team(self, club_id: str, update_data: Dict[str, Any]) -> Optional[Team]:
        """
        Update team.
        """
        return self._execute_operation(
            "update_team", self.team_repo.update, club_id, update_data
        )

    def delete_team(self, club_id: str, soft_delete: bool = True) -> bool:
        """
        Delete team.
        """
        return self._execute_operation(
            "delete_team", self.team_repo.delete, club_id, soft_delete
        )

    def get_team_by_season(
        self, club_id: str, season_year: str, competition_id: str
    ) -> Optional[Team]:
        """
        Get team by season and competition using composite key.
        """
        return self._execute_operation(
            "get_team_by_season",
            self.team_repo.get_by_composite_key,
            club_id,
            season_year,
            competition_id,
        )

    def update_club(
        self,
        club_id: str,
        season_year: str,
        competition_id: str,
        update_data: Dict[str, Any],
    ) -> Optional[Team]:
        """
        Update club data using composite key.
        """
        return self._execute_operation(
            "update_club",
            self.team_repo.update_by_composite_key,
            club_id,
            season_year,
            competition_id,
            update_data,
        )

    # ***> Bulk Competition Operations <***
    def add_competitions_bulk(self, competitions_data: pd.DataFrame) -> bool:
        """
        Add multiple competitions from DataFrame in bulk.

        Args:
            competitions_data: DataFrame with competition data

        Returns:
            True if at least one competition was saved successfully
        """
        if not self._initialized:
            self.initialize()

        if competitions_data.empty:
            return False

        success_count = 0
        error_count = 0
        duplicate_count = 0

        # ***> Metadata fields to exclude from database operations <***
        metadata_fields = ["__index_level_0__", "index", "_metadata", "dataframe_info"]

        # ***> Default values for missing data <***
        numeric_defaults = {
            "number_of_clubs": 0,
            "number_of_players": 0,
            "average_age_of_players": 0.0,
            "percentage_of_foreign_players": 0,
            "percentage_game_ratio_of_foreign_players": 0.0,
            "goals_per_match": 0.0,
            "average_market_value": 0.0,
            "total_market_value": 0.0,
        }

        string_defaults = {
            "competition_id": "",
            "competition_code": "",
            "competition_name": "",
            "competition_url": "",
            "competition_type": "",
            "country": "",
            "tier": "",
        }

        for _, row in competitions_data.iterrows():
            try:
                # ***> Prepare competition data <***
                competition_data = row.to_dict()

                # ***> Handle NaN values with appropriate defaults <***
                for key, value in competition_data.items():
                    if pd.isna(value):
                        if key in numeric_defaults:
                            competition_data[key] = numeric_defaults[key]
                        elif key in string_defaults:
                            competition_data[key] = string_defaults[key]
                        elif isinstance(value, str):
                            competition_data[key] = ""

                # ***> Remove DataFrame-specific metadata fields <***
                for field in metadata_fields:
                    competition_data.pop(field, None)

                # ***> Check if competition already exists <***
                existing = self.get_competition(
                    competition_data.get("competition_id", "")
                )

                if existing:
                    # ***> Update existing competition <***
                    update_data = {
                        k: v
                        for k, v in competition_data.items()
                        if k != "competition_id"
                    }
                    self.update_competition(
                        competition_data["competition_id"], update_data
                    )
                    duplicate_count += 1
                else:
                    # ***> Add new competition <***
                    self.add_competition(competition_data)
                    success_count += 1

            except Exception:
                error_count += 1
                continue

        # Return True if we processed any competitions successfully
        return (success_count + duplicate_count) > 0

    def save_competitions(self, data: pd.DataFrame) -> bool:
        """
        Legacy method for backward compatibility.
        Delegates to add_competitions_bulk.

        Args:
            data: DataFrame with competition data

        Returns:
            True if successfully saved to database
        """
        return self.add_competitions_bulk(data)

    # ***> Bulk Operations <***
    def add_teams_bulk(self, teams_data: List[Dict[str, Any]]) -> List[Team]:
        """
        Add multiple teams in bulk.
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
            except Exception:
                error_count += 1
                continue
        return teams

    # ***> Analytics and Reporting <***
    def get_competition_team_summary(self, competition_id: str) -> Dict[str, Any]:
        """
        Get summary statistics for a competition including team information.
        """
        if not self._initialized:
            self.initialize()

        try:
            # ***> Get competition <***
            competition = self.get_competition(competition_id)
            if not competition:
                return {"error": f"Competition {competition_id} not found"}

            # ***> Get teams in competition <***
            teams = self.get_teams_by_competition(competition_id)

            # ***> Calculate summary statistics <***
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
