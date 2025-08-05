# ***> database/repositories/player_repository.py <***
from database.schemas import Player
from exceptions import DatabaseOperationError

from ..core.database_manager import DatabaseManager


class PlayerRepository:
    """
    Upsert a player record so any FK references (lineup, goals, top scorers, etc.)
    are always safe.
    """

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    def upsert(
        self,
        player_id: str,
        name: str = "Unknown",
        short_name: str = None,
        profile_url: str = None,
        portrait_url: str = None,
        is_active: bool = True,
    ):
        """
        Merge a Player into the DB, including optional fields for clarity.
        """
        try:
            with self.db_manager.get_session() as session:
                session.merge(
                    Player(
                        player_id=player_id,
                        name=name,
                        short_name=short_name,
                        profile_url=profile_url,
                        portrait_url=portrait_url,
                        is_active=is_active,
                    )
                )
        except Exception as e:
            raise DatabaseOperationError(f"Failed to upsert player {player_id}: {e}")
