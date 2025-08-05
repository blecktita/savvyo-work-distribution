# database/repositories/top_scorer_repository.py
from typing import Any, Dict, List, Optional

from sqlalchemy.exc import SQLAlchemyError

from database.schemas import TopScorer
from exceptions import DatabaseOperationError

from ..core.database_manager import DatabaseManager
from .base_repository import BaseRepository
from .player_repository import PlayerRepository


class TopScorerRepository(BaseRepository[TopScorer]):
    """
    CRUD for top scorers
    """

    def __init__(self, db_manager: DatabaseManager):
        super().__init__(db_manager, TopScorer)

    def get_by_id(self, entity_id: str) -> Optional[TopScorer]:
        """Get top scorer by ID"""
        try:
            with self.db_manager.get_session() as session:
                return session.query(TopScorer).get(entity_id)
        except SQLAlchemyError as e:
            raise DatabaseOperationError(f"Failed to get top scorer {entity_id}: {e}")

    def update(
        self, entity_id: str, update_data: Dict[str, Any]
    ) -> Optional[TopScorer]:
        """Update top scorer record"""
        try:
            with self.db_manager.get_session() as session:
                scorer = session.query(TopScorer).get(entity_id)
                if scorer:
                    for key, value in update_data.items():
                        setattr(scorer, key, value)
                    session.flush()
                    session.refresh(scorer)
                return scorer
        except SQLAlchemyError as e:
            raise DatabaseOperationError(
                f"Failed to update top scorer {entity_id}: {e}"
            )

    def delete(self, entity_id: str, soft_delete: bool = True) -> bool:
        """Delete top scorer record"""
        try:
            with self.db_manager.get_session() as session:
                scorer = session.query(TopScorer).get(entity_id)
                if scorer:
                    session.delete(scorer)  # Top scorers are typically hard deleted
                    return True
                return False
        except SQLAlchemyError as e:
            raise DatabaseOperationError(
                f"Failed to delete top scorer {entity_id}: {e}"
            )

    def upsert_scorers(self, matchday_id: str, scorers: List[Dict[str, Any]]) -> None:
        """
        Insert or update all scorers for a matchday.
        Maps scraper fields to database schema fields.
        """
        try:
            with self.db_manager.get_session() as session:
                # Get matchday info for season and league
                from sqlalchemy import text

                matchday_result = session.execute(
                    text(
                        "SELECT season, competition FROM matchdays WHERE matchday_id = :md_id"
                    ),
                    {"md_id": matchday_id},
                ).first()

                if not matchday_result:
                    raise ValueError(f"Matchday {matchday_id} not found")

                season = matchday_result[0]
                league = matchday_result[1]

                player_repo = PlayerRepository(self.db_manager)
                for s in scorers:
                    # Extract player_id from profile_url
                    player_id = self._extract_player_id_from_url(
                        s.get("profile_url", "")
                    )

                    if not player_id:
                        print(
                            f"⚠️ Skipping scorer {s.get('name', 'Unknown')} - no player_id found"
                        )
                        continue

                    player_repo.upsert(
                        player_id=player_id,
                        name=s.get("name", "Unknown"),
                        short_name=s.get("short_name"),  # if your scraper provides it
                        portrait_url=s.get("portrait_url"),  # if available
                        is_active=True,
                    )

                    scorer = (
                        session.query(TopScorer)
                        .filter_by(matchday_id=matchday_id, player_id=player_id)
                        .first()
                    )

                    # Map scraper fields to database fields
                    mapped_data = {
                        "matchday_id": matchday_id,
                        "player_id": player_id,
                        "season": season,
                        "league": league,
                        "goals_this_matchday": self._parse_goals(
                            s.get("goals_this_matchday", 0)
                        ),
                        "total_goals": s.get("total_goals", 0),
                    }

                    if scorer:
                        # Update existing scorer
                        for k, v in mapped_data.items():
                            setattr(scorer, k, v)
                    else:
                        # Create new scorer with generated ID
                        import uuid

                        mapped_data["scorer_id"] = str(uuid.uuid4())
                        session.add(TopScorer(**mapped_data))

        except SQLAlchemyError as e:
            raise DatabaseOperationError(f"Top scorers upsert failed: {e}")

    def _extract_player_id_from_url(self, profile_url: str) -> str:
        """Extract player ID from profile URL like /spieler/123456"""
        if not profile_url:
            return ""

        import re

        match = re.search(r"/spieler/(\d+)", profile_url)
        return match.group(1) if match else ""

    def _parse_goals(self, goals_value) -> int:
        """Parse goals value to integer"""
        if goals_value == "-" or goals_value is None:
            return 0
        try:
            return int(goals_value)
        except (ValueError, TypeError):
            return 0
