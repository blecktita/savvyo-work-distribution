# database/repositories/matchday_repository.py
from typing import Any, Dict, Optional

from sqlalchemy.exc import SQLAlchemyError

from database.schemas import Matchday, MatchdaySummary
from exceptions import DatabaseOperationError

from ..core.database_manager import DatabaseManager
from .base_repository import BaseRepository


class MatchdayRepository(BaseRepository[Matchday]):
    """
    CRUD for Matchday and its summary
    """

    def __init__(self, db_manager: DatabaseManager):
        super().__init__(db_manager, Matchday)

    def get_by_id(self, entity_id: str) -> Optional[Matchday]:
        """Get matchday by ID"""
        try:
            with self.db_manager.get_session() as session:
                return session.query(Matchday).get(entity_id)
        except SQLAlchemyError as e:
            raise DatabaseOperationError(f"Failed to get matchday {entity_id}: {e}")

    def update(self, entity_id: str, update_data: Dict[str, Any]) -> Optional[Matchday]:
        """Update matchday record"""
        try:
            with self.db_manager.get_session() as session:
                matchday = session.query(Matchday).get(entity_id)
                if matchday:
                    for key, value in update_data.items():
                        setattr(matchday, key, value)
                    session.flush()
                    session.refresh(matchday)
                return matchday
        except SQLAlchemyError as e:
            raise DatabaseOperationError(f"Failed to update matchday {entity_id}: {e}")

    def delete(self, entity_id: str, soft_delete: bool = True) -> bool:
        """Delete matchday record"""
        try:
            with self.db_manager.get_session() as session:
                matchday = session.query(Matchday).get(entity_id)
                if matchday:
                    if soft_delete and hasattr(matchday, "is_active"):
                        matchday.is_active = False
                    else:
                        session.delete(matchday)
                    return True
                return False
        except SQLAlchemyError as e:
            raise DatabaseOperationError(f"Failed to delete matchday {entity_id}: {e}")

    def upsert(
        self,
        info: Dict[str, Any],
        metadata: Dict[str, Any],
        summary: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Create or update Matchday and its summary in one go.
        Returns matchday_id.
        """
        import uuid

        try:
            with self.db_manager.get_session() as session:
                # Upsert Matchday
                existing_md = (
                    session.query(Matchday)
                    .filter_by(
                        season=info.get("season"),
                        number=info.get("number"),
                        competition=info.get("competition", "Unknown"),
                    )
                    .first()
                )

                if existing_md:
                    matchday_id = existing_md.matchday_id
                    for k, v in info.items():
                        setattr(existing_md, k, v)
                    # Map extraction metadata
                    existing_md.extraction_time = metadata.get("extraction_time")
                    existing_md.source_url = metadata.get(
                        "source_url", metadata.get("url")
                    )
                    existing_md.total_matches = metadata.get(
                        "total_matches", existing_md.total_matches
                    )
                else:
                    matchday_id = str(uuid.uuid4())
                    # Build new Matchday with metadata
                    info_copy = {
                        **info,
                        "matchday_id": matchday_id,
                        "extraction_time": metadata.get("extraction_time"),
                        "source_url": metadata.get("source_url", metadata.get("url")),
                        "total_matches": metadata.get("total_matches"),
                    }
                    session.add(Matchday(**info_copy))

                # Upsert Summary
                raw = summary or {}
                sec_yellow = _parse_int(raw.get("second_yellow_cards"))
                summary_data = {
                    "matchday_id": matchday_id,
                    "season": info["season"],
                    "competition": info["competition"],
                    "matches_count": _parse_int(raw.get("matches")) or 0,
                    "goals": _parse_int(raw.get("goals")) or 0,
                    "own_goals": _parse_int(raw.get("own_goals")) or 0,
                    "yellow_cards": _parse_int(raw.get("yellow_cards")) or 0,
                    "second_yellow_cards": sec_yellow,
                    "red_cards": _parse_int(raw.get("red_cards")) or 0,
                    "total_attendance": _parse_int(raw.get("total_attendance")) or 0,
                    "average_attendance": _parse_int(raw.get("average_attendance"))
                    or 0,
                    "sold_out_matches": _parse_int(raw.get("sold_out_matches")) or 0,
                }
                existing_sum = session.get(MatchdaySummary, matchday_id)
                if existing_sum:
                    for k, v in summary_data.items():
                        setattr(existing_sum, k, v)
                else:
                    session.add(MatchdaySummary(**summary_data))

                return matchday_id

        except SQLAlchemyError as e:
            raise DatabaseOperationError(f"Matchday upsert failed: {e}")


def _parse_int(val: Any, default: Optional[int] = None) -> Optional[int]:
    """
    Safely convert val to int.
    If val is None, non-digit, or '-', return default.
    """
    try:
        # if it's a float string or similar, int() will still work for digits
        return int(val)
    except (TypeError, ValueError):
        return default
