# database/repositories/team_repository.py
"""
Team-specific repository implementation.
"""

from typing import Any, Dict, List, Optional

from sqlalchemy.exc import SQLAlchemyError

from database.schemas import Team
from exceptions import DatabaseOperationError

from ..core.database_manager import DatabaseManager
from .base_repository import BaseRepository


class TeamRepository(BaseRepository):
    """
    Repository for Team entity operations.
    """

    def __init__(self, db_manager: DatabaseManager):
        """
        Initialize team repository.

        Args:
            db_manager: Database manager instance
        """
        super().__init__(db_manager, Team)

    def get_by_id(self, club_id: str) -> Optional[Team]:
        """
        Retrieve team by club ID.

        Args:
            club_id: Club identifier

        Returns:
            Team object or None if not found

        Raises:
            ValueError: If club_id is invalid
            DatabaseOperationError: For database errors
        """
        if not club_id or not isinstance(club_id, str):
            raise ValueError("Club ID must be a non-empty string")

        try:
            with self.db_manager.get_session() as session:
                team = session.query(Team).filter(Team.club_id == club_id).first()
                return team
        except SQLAlchemyError as error:
            raise DatabaseOperationError(f"Database error retrieving team: {error}")

    def get_by_composite_key(
        self, club_id: str, season_year: str, competition_id: str
    ) -> Optional[Team]:
        """
        Get team by composite primary key.

        Args:
            club_id: Club identifier
            season_year: Season year
            competition_id: Competition identifier

        Returns:
            Team object or None if not found
        """
        if not all([club_id, season_year, competition_id]):
            raise ValueError(
                "All key components (club_id, season_year, competition_id) are required"
            )

        try:
            with self.db_manager.get_session() as session:
                team = (
                    session.query(Team)
                    .filter(
                        Team.club_id == club_id,
                        Team.season_year == season_year,
                        Team.competition_id == competition_id,
                    )
                    .first()
                )
                return team
        except SQLAlchemyError as error:
            raise DatabaseOperationError(f"Database error retrieving team: {error}")

    def update(self, club_id: str, update_data: Dict[str, Any]) -> Optional[Team]:
        """
        Update team record by club ID.

        Args:
            club_id: Club identifier
            update_data: Data to update

        Returns:
            Updated team object or None if not found

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
                team = session.query(Team).filter(Team.club_id == club_id).first()

                if not team:
                    return None

                # ***> Update fields <***
                for key, value in update_data.items():
                    if hasattr(team, key):
                        setattr(team, key, value)

                session.flush()
                session.refresh(team)
                return team
        except SQLAlchemyError as error:
            raise DatabaseOperationError(f"Database error updating team: {error}")

    def update_by_composite_key(
        self,
        club_id: str,
        season_year: str,
        competition_id: str,
        update_data: Dict[str, Any],
    ) -> Optional[Team]:
        """
        Update team by composite primary key.

        Args:
            club_id: Club identifier
            season_year: Season year
            competition_id: Competition identifier
            update_data: Data to update

        Returns:
            Updated team object or None if not found
        """
        if not all([club_id, season_year, competition_id]):
            raise ValueError("All key components are required")
        if not update_data:
            raise ValueError("Update data cannot be empty")

        try:
            with self.db_manager.get_session() as session:
                team = (
                    session.query(Team)
                    .filter(
                        Team.club_id == club_id,
                        Team.season_year == season_year,
                        Team.competition_id == competition_id,
                    )
                    .first()
                )

                if not team:
                    return None

                # ***> Update fields (exclude primary key fields) <***
                excluded_fields = {"club_id", "season_year", "competition_id"}

                for key, value in update_data.items():
                    if key in excluded_fields:
                        continue
                    if hasattr(team, key):
                        setattr(team, key, value)

                session.flush()
                session.refresh(team)
                return team
        except SQLAlchemyError as error:
            raise DatabaseOperationError(f"Database error updating team: {error}")

    def delete(self, club_id: str, soft_delete: bool = True) -> bool:
        """
        Delete team record by club ID.

        Args:
            club_id: Club identifier
            soft_delete: Whether to soft delete or hard delete

        Returns:
            True if deleted successfully, False if not found

        Raises:
            ValueError: If club_id is invalid
            DatabaseOperationError: For database errors
        """
        if not club_id or not isinstance(club_id, str):
            raise ValueError("Club ID must be a non-empty string")

        try:
            with self.db_manager.get_session() as session:
                team = session.query(Team).filter(Team.club_id == club_id).first()

                if not team:
                    return False

                if soft_delete:
                    team.is_active = False
                else:
                    session.delete(team)

                return True
        except SQLAlchemyError as error:
            raise DatabaseOperationError(f"Database error deleting team: {error}")

    def get_by_competition(
        self, competition_id: str, season_id: Optional[str] = None
    ) -> List[Team]:
        """
        Get teams by competition and optionally by season.

        Args:
            competition_id: Competition identifier
            season_id: Season identifier (optional)

        Returns:
            List of teams in the competition/season

        Raises:
            ValueError: If competition_id is invalid
            DatabaseOperationError: For database errors
        """
        if not competition_id or not isinstance(competition_id, str):
            raise ValueError("Competition ID must be a non-empty string")

        try:
            with self.db_manager.get_session() as session:
                query = session.query(Team).filter(
                    Team.competition_id == competition_id, Team.is_active
                )

                if season_id:
                    query = query.filter(Team.season_id == season_id)

                teams = query.all()
                return teams
        except SQLAlchemyError as error:
            raise DatabaseOperationError(
                f"Database error retrieving teams by competition: {error}"
            )
