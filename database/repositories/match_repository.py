# database/repositories/match_repository.py
"""
Match-specific repository implementation following existing patterns.
"""
from typing import Dict, Any, List, Optional
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import joinedload

from .base_repository import BaseRepository
from ..core.database_manager import DatabaseManager
from database.match_models import Match, Player, Goal, Card, Substitution, MatchLineup, MatchdayInfo
from exceptions import DatabaseOperationError


class MatchRepository(BaseRepository):
    """
    Repository for Match entity operations following existing patterns.
    """

    def __init__(self, db_manager: DatabaseManager):
        """
        Initialize match repository.

        Args:
            db_manager: Database manager instance
        """
        super().__init__(db_manager, Match)

    def get_by_id(self, match_id: str) -> Optional[Match]:
        """
        Retrieve match by match ID.

        Args:
            match_id: Match identifier

        Returns:
            Match object or None if not found

        Raises:
            ValueError: If match_id is invalid
            DatabaseOperationError: For database errors
        """
        if not match_id or not isinstance(match_id, str):
            raise ValueError("Match ID must be a non-empty string")

        try:
            with self.db_manager.get_session() as session:
                match = session.query(Match).filter(
                    Match.match_id == match_id
                ).first()
                return match
        except SQLAlchemyError as error:
            raise DatabaseOperationError(
                f"Database error retrieving match: {error}"
            )

    def get_match_with_details(self, match_id: str) -> Optional[Match]:
        """
        Get match with all related data (goals, cards, substitutions, lineups).
        
        Args:
            match_id: Match identifier
            
        Returns:
            Match object with all relationships loaded
        """
        if not match_id or not isinstance(match_id, str):
            raise ValueError("Match ID must be a non-empty string")

        try:
            with self.db_manager.get_session() as session:
                match = session.query(Match).options(
                    joinedload(Match.goals),
                    joinedload(Match.cards), 
                    joinedload(Match.substitutions),
                    joinedload(Match.lineups)
                ).filter(Match.match_id == match_id).first()
                if match:
                    session.expunge(match)
                return match
        except SQLAlchemyError as error:
            raise DatabaseOperationError(
                f"Database error retrieving match with details: {error}"
            )

    def get_by_competition_and_season(
        self, 
        competition_id: str, 
        season: str
        ) -> List[Match]:
        """
        Get matches by competition and season.
        
        Args:
            competition_id: Competition identifier
            season: Season identifier
            
        Returns:
            List of matches
        """
        try:
            with self.db_manager.get_session() as session:
                matches = session.query(Match).filter(
                    Match.competition_id == competition_id,
                    Match.season == season,
                    Match.is_active
                ).all()
                return matches
        except SQLAlchemyError as error:
            raise DatabaseOperationError(
                f"Database error retrieving matches: {error}"
            )

    def get_by_matchday(
        self, 
        competition_id: str, 
        season: str, 
        matchday: int
        ) -> List[Match]:
        """
        Get matches by specific matchday.
        
        Args:
            competition_id: Competition identifier
            season: Season identifier
            matchday: Matchday number
            
        Returns:
            List of matches for the matchday
        """
        try:
            with self.db_manager.get_session() as session:
                matches = session.query(Match).filter(
                    Match.competition_id == competition_id,
                    Match.season == season,
                    Match.matchday == matchday,
                    Match.is_active
                ).all()
                return matches
        except SQLAlchemyError as error:
            raise DatabaseOperationError(
                f"Database error retrieving matchday matches: {error}"
            )

    def update(self, match_id: str, update_data: Dict[str, Any]) -> Optional[Match]:
        """
        Update match record by match ID.

        Args:
            match_id: Match identifier
            update_data: Data to update

        Returns:
            Updated match object or None if not found

        Raises:
            ValueError: If parameters are invalid
            DatabaseOperationError: For database errors
        """
        if not match_id or not isinstance(match_id, str):
            raise ValueError("Match ID must be a non-empty string")

        if not update_data:
            raise ValueError("Update data cannot be empty")

        try:
            with self.db_manager.get_session() as session:
                match = session.query(Match).filter(
                    Match.match_id == match_id
                ).first()

                if not match:
                    return None

                # Update fields
                for key, value in update_data.items():
                    if hasattr(match, key):
                        setattr(match, key, value)

                session.flush()
                session.refresh(match)
                return match
        except SQLAlchemyError as error:
            raise DatabaseOperationError(
                f"Database error updating match: {error}"
            )

    def delete(self, match_id: str, soft_delete: bool = True) -> bool:
        """
        Delete match record by match ID.

        Args:
            match_id: Match identifier
            soft_delete: Whether to soft delete or hard delete

        Returns:
            True if deleted successfully, False if not found

        Raises:
            ValueError: If match_id is invalid
            DatabaseOperationError: For database errors
        """
        if not match_id or not isinstance(match_id, str):
            raise ValueError("Match ID must be a non-empty string")

        try:
            with self.db_manager.get_session() as session:
                match = session.query(Match).filter(
                    Match.match_id == match_id
                ).first()

                if not match:
                    return False

                if soft_delete:
                    match.is_active = False
                else:
                    session.delete(match)

                return True
        except SQLAlchemyError as error:
            raise DatabaseOperationError(
                f"Database error deleting match: {error}"
            )


class PlayerRepository(BaseRepository):
    """
    Repository for Player entity operations.
    """

    def __init__(self, db_manager: DatabaseManager):
        super().__init__(db_manager, Player)

    def get_by_id(self, player_id: str) -> Optional[Player]:
        """
        Retrieve player by player ID.
        """
        if not player_id or not isinstance(player_id, str):
            raise ValueError("Player ID must be a non-empty string")

        try:
            with self.db_manager.get_session() as session:
                player = session.query(Player).filter(
                    Player.player_id == player_id
                ).first()
                return player
        except SQLAlchemyError as error:
            raise DatabaseOperationError(
                f"Database error retrieving player: {error}"
            )

    def update(self, player_id: str, update_data: Dict[str, Any]) -> Optional[Player]:
        """
        Update player record.
        """
        if not player_id or not isinstance(player_id, str):
            raise ValueError("Player ID must be a non-empty string")

        if not update_data:
            raise ValueError("Update data cannot be empty")

        try:
            with self.db_manager.get_session() as session:
                player = session.query(Player).filter(
                    Player.player_id == player_id
                ).first()

                if not player:
                    return None

                for key, value in update_data.items():
                    if hasattr(player, key):
                        setattr(player, key, value)

                session.flush()
                session.refresh(player)
                return player
        except SQLAlchemyError as error:
            raise DatabaseOperationError(
                f"Database error updating player: {error}"
            )

    def delete(self, player_id: str, soft_delete: bool = True) -> bool:
        """
        Delete player record.
        """
        if not player_id or not isinstance(player_id, str):
            raise ValueError("Player ID must be a non-empty string")

        try:
            with self.db_manager.get_session() as session:
                player = session.query(Player).filter(
                    Player.player_id == player_id
                ).first()

                if not player:
                    return False

                if soft_delete:
                    player.is_active = False
                else:
                    session.delete(player)

                return True
        except SQLAlchemyError as error:
            raise DatabaseOperationError(
                f"Database error deleting player: {error}"
            )


class MatchdayInfoRepository(BaseRepository):
    """
    Repository for MatchdayInfo entity operations.
    """

    def __init__(self, db_manager: DatabaseManager):
        super().__init__(db_manager, MatchdayInfo)

    def get_by_id(self, matchday_id: int) -> Optional[MatchdayInfo]:
        """
        Retrieve matchday info by ID.
        """
        try:
            with self.db_manager.get_session() as session:
                matchday_info = session.query(MatchdayInfo).filter(
                    MatchdayInfo.id == matchday_id
                ).first()
                return matchday_info
        except SQLAlchemyError as error:
            raise DatabaseOperationError(
                f"Database error retrieving matchday info: {error}"
            )

    def get_by_competition_season_matchday(
        self, 
        competition_id: str, 
        season: str, 
        matchday_number: int
    ) -> Optional[MatchdayInfo]:
        """
        Get matchday info by competition, season, and matchday number.
        """
        try:
            with self.db_manager.get_session() as session:
                matchday_info = session.query(MatchdayInfo).filter(
                    MatchdayInfo.competition_id == competition_id,
                    MatchdayInfo.season == season,
                    MatchdayInfo.matchday_number == matchday_number,
                    MatchdayInfo.is_active
                ).first()
                return matchday_info
        except SQLAlchemyError as error:
            raise DatabaseOperationError(
                f"Database error retrieving matchday info: {error}"
            )

    def update(self, matchday_id: int, update_data: Dict[str, Any]) -> Optional[MatchdayInfo]:
        """
        Update matchday info record.
        """
        if not update_data:
            raise ValueError("Update data cannot be empty")

        try:
            with self.db_manager.get_session() as session:
                matchday_info = session.query(MatchdayInfo).filter(
                    MatchdayInfo.id == matchday_id
                ).first()

                if not matchday_info:
                    return None

                for key, value in update_data.items():
                    if hasattr(matchday_info, key):
                        setattr(matchday_info, key, value)

                session.flush()
                session.refresh(matchday_info)
                return matchday_info
        except SQLAlchemyError as error:
            raise DatabaseOperationError(
                f"Database error updating matchday info: {error}"
            )

    def delete(self, matchday_id: int, soft_delete: bool = True) -> bool:
        """
        Delete matchday info record.
        """
        try:
            with self.db_manager.get_session() as session:
                matchday_info = session.query(MatchdayInfo).filter(
                    MatchdayInfo.id == matchday_id
                ).first()

                if not matchday_info:
                    return False

                if soft_delete:
                    matchday_info.is_active = False
                else:
                    session.delete(matchday_info)

                return True
        except SQLAlchemyError as error:
            raise DatabaseOperationError(
                f"Database error deleting matchday info: {error}"
            )