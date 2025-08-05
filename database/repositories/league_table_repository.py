# database/repositories/league_table_repository.py
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from database.schemas import LeagueTableEntry
from exceptions import DatabaseOperationError

from ..core.database_manager import DatabaseManager
from .base_repository import BaseRepository


class LeagueTableRepository(BaseRepository[LeagueTableEntry]):
    """
    CRUD for league table entries
    """

    def __init__(self, db_manager: DatabaseManager):
        super().__init__(db_manager, LeagueTableEntry)

    def get_by_id(self, entity_id: str) -> Optional[LeagueTableEntry]:
        """Get league table entry by ID"""
        try:
            with self.db_manager.get_session() as session:
                return session.query(LeagueTableEntry).get(entity_id)
        except SQLAlchemyError as e:
            raise DatabaseOperationError(
                f"Failed to get league table entry {entity_id}: {e}"
            )

    def update(
        self, entity_id: str, update_data: Dict[str, Any]
    ) -> Optional[LeagueTableEntry]:
        """Update league table entry"""
        try:
            with self.db_manager.get_session() as session:
                entry = session.query(LeagueTableEntry).get(entity_id)
                if entry:
                    for key, value in update_data.items():
                        setattr(entry, key, value)
                    session.flush()
                    session.refresh(entry)
                return entry
        except SQLAlchemyError as e:
            raise DatabaseOperationError(
                f"Failed to update league table entry {entity_id}: {e}"
            )

    def delete(self, entity_id: str, soft_delete: bool = True) -> bool:
        """Delete league table entry"""
        try:
            with self.db_manager.get_session() as session:
                entry = session.query(LeagueTableEntry).get(entity_id)
                if entry:
                    session.delete(
                        entry
                    )  # League table entries are typically hard deleted
                    return True
                return False
        except SQLAlchemyError as e:
            raise DatabaseOperationError(
                f"Failed to delete league table entry {entity_id}: {e}"
            )

    def upsert_entries(self, matchday_id: str, teams: List[Dict[str, Any]]) -> None:
        """
        Insert or update all entries for a matchday.
        Maps scraper fields to database schema fields.
        """
        try:
            with self.db_manager.get_session() as session:
                # Get matchday info for season and competition
                matchday_result = session.execute(
                    text(
                        "SELECT season, competition FROM matchdays WHERE matchday_id = :md_id"
                    ),
                    {"md_id": matchday_id},
                ).first()

                if not matchday_result:
                    raise ValueError(f"Matchday {matchday_id} not found")

                season = matchday_result[0]
                competition = matchday_result[1]

                for t in teams:
                    # Check if team_id exists
                    if "team_id" not in t or not t["team_id"]:
                        raise ValueError(f"team_id missing or empty in team data: {t}")

                    entry = (
                        session.query(LeagueTableEntry)
                        .filter_by(matchday_id=matchday_id, team_id=t["team_id"])
                        .first()
                    )

                    # Map scraper fields to database fields
                    mapped_data = {
                        "matchday_id": matchday_id,
                        "team_id": t["team_id"],
                        "season": season,
                        "competition": competition,
                        "position": t.get("position"),
                        "movement": t.get("movement"),
                        "matches_played": t.get(
                            "matches", 0
                        ),  # scraper uses 'matches', DB expects 'matches_played'
                        "goal_difference": self._parse_goal_difference(
                            t.get("goal_difference")
                        ),
                        "points": t.get("points", 0),
                    }

                    if entry:
                        # Update existing entry
                        for k, v in mapped_data.items():
                            setattr(entry, k, v)
                    else:
                        # Create new entry with generated ID
                        import uuid

                        mapped_data["entry_id"] = str(uuid.uuid4())
                        session.add(LeagueTableEntry(**mapped_data))

        except SQLAlchemyError as e:
            raise DatabaseOperationError(f"League table upsert failed: {e}")

    def _parse_goal_difference(self, goal_diff_str) -> int:
        """Parse goal difference string to integer"""
        if not goal_diff_str:
            return 0

        # Handle strings like "+5", "-3", "0", "16"
        try:
            goal_diff_str = str(goal_diff_str).strip()
            if goal_diff_str.startswith("+"):
                return int(goal_diff_str[1:])
            else:
                return int(goal_diff_str)
        except (ValueError, TypeError):
            return 0
