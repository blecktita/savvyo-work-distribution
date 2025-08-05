# database/repositories/competition_repository.py
"""
Competition-specific repository implementation.
"""

from typing import Any, Dict, List, Optional

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError

from database.schemas import Competition
from exceptions import DatabaseOperationError

from ..core.database_manager import DatabaseManager
from .base_repository import BaseRepository


class CompetitionRepository(BaseRepository):
    """
    Repository for Competition entity operations.
    """

    def __init__(self, db_manager: DatabaseManager):
        """
        Initialize competition repository.

        Args:
            db_manager: Database manager instance
        """
        super().__init__(db_manager, Competition)

    def get_by_id(self, competition_id: str) -> Optional[Competition]:
        """
        Retrieve competition by ID.

        Args:
            competition_id: Competition identifier

        Returns:
            Competition object or None if not found

        Raises:
            ValueError: If competition_id is invalid
            DatabaseOperationError: For database errors
        """
        if not competition_id or not isinstance(competition_id, str):
            raise ValueError("Competition ID must be a non-empty string")

        try:
            with self.db_manager.get_session() as session:
                competition = (
                    session.query(Competition)
                    .filter(Competition.competition_id == competition_id)
                    .first()
                )
                return competition
        except SQLAlchemyError as error:
            raise DatabaseOperationError(
                f"Database error retrieving competition: {error}"
            )

    def update(
        self, competition_id: str, update_data: Dict[str, Any]
    ) -> Optional[Competition]:
        """
        Update competition record.

        Args:
            competition_id: Competition identifier
            update_data: Data to update

        Returns:
            Updated competition object or None

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
                competition = (
                    session.query(Competition)
                    .filter(Competition.competition_id == competition_id)
                    .first()
                )

                if not competition:
                    return None

                # ***> Update fields <***
                for key, value in update_data.items():
                    if hasattr(competition, key):
                        setattr(competition, key, value)

                session.flush()
                session.refresh(competition)
                return competition
        except SQLAlchemyError as error:
            raise DatabaseOperationError(
                f"Database error updating competition: {error}"
            )

    def delete(self, competition_id: str, soft_delete: bool = True) -> bool:
        """
        Delete competition record.

        Args:
            competition_id: Competition identifier
            soft_delete: Whether to soft delete or hard delete

        Returns:
            True if deleted successfully, False if not found

        Raises:
            ValueError: If competition_id is invalid
            DatabaseOperationError: For database errors
        """
        if not competition_id or not isinstance(competition_id, str):
            raise ValueError("Competition ID must be a non-empty string")

        try:
            with self.db_manager.get_session() as session:
                competition = (
                    session.query(Competition)
                    .filter(Competition.competition_id == competition_id)
                    .first()
                )

                if not competition:
                    return False

                if soft_delete:
                    setattr(competition, "is_active", False)
                else:
                    session.delete(competition)

                return True
        except SQLAlchemyError as error:
            raise DatabaseOperationError(
                f"Database error deleting competition: {error}"
            )

    def search_by_name(self, name_pattern: str) -> List[Competition]:
        """
        Search competitions by name pattern.

        Args:
            name_pattern: Name pattern to search for

        Returns:
            List of matching competitions

        Raises:
            ValueError: If name_pattern is invalid
            DatabaseOperationError: For database errors
        """
        if not name_pattern or not isinstance(name_pattern, str):
            raise ValueError("Name pattern must be a non-empty string")

        try:
            with self.db_manager.get_session() as session:
                competitions = (
                    session.query(Competition)
                    .filter(
                        Competition.competition_name.like(f"%{name_pattern}%"),
                        Competition.is_active,
                    )
                    .all()
                )
                return competitions
        except SQLAlchemyError as error:
            raise DatabaseOperationError(
                f"Database error searching competitions: {error}"
            )

    def get_by_type(self, competition_type: str) -> List[Competition]:
        """
        Get competitions by type.

        Args:
            competition_type: Competition type

        Returns:
            List of competitions of specified type

        Raises:
            ValueError: If competition_type is invalid
            DatabaseOperationError: For database errors
        """
        if not competition_type or not isinstance(competition_type, str):
            raise ValueError("Competition type must be a non-empty string")

        try:
            with self.db_manager.get_session() as session:
                competitions = (
                    session.query(Competition)
                    .filter(
                        Competition.competition_type == competition_type,
                        Competition.is_active,
                    )
                    .all()
                )
                return competitions
        except SQLAlchemyError as error:
            raise DatabaseOperationError(
                f"Database error retrieving competitions by type: {error}"
            )

    def get_non_cup_competitions(self) -> List[Dict[str, str]]:
        """
        Query competitions table for non-cup competitions.

        Returns:
            List of dictionaries containing competition_id and competition_url

        Raises:
            DatabaseOperationError: If database query fails
        """
        try:
            with self.db_manager.get_session() as session:
                # ***> Query competitions where tier does not contain "cup" <***
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
        except SQLAlchemyError as error:
            raise DatabaseOperationError(
                f"Database error getting non-cup competitions: {error}"
            )
