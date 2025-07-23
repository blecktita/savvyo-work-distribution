# database/services/match_service.py
"""
Match service layer - coordinates between repositories and provides business logic operations.
"""
from typing import Optional, Dict, Any, List
from contextlib import contextmanager

from configurations.settings_database import get_database_config
from exceptions import DatabaseServiceError

from ..core.database_manager import DatabaseManager
from ..repositories.match_repository import MatchRepository, PlayerRepository, MatchdayInfoRepository
from database.match_models import Match, Player, MatchdayInfo

# Import dataclass to SQLAlchemy converter
from database.match_db_adapter import MatchDataConverter

# Import the dataclass models from the scraper
from extractors.extractor_matchday import MatchDetail, MatchdayContainer


class MatchService:
    """
    High-level service for match-related database operations.
    """

    def __init__(self, environment: str = ""):
        """
        Initialize match service with environment-based configuration.

        Args:
            environment: Target environment ('development', 'testing', 'production')
        """
        self.environment = environment
        self.config = get_database_config(environment)
        self.db_manager = DatabaseManager(
            self.config.database_url, 
            self.config.echo
        )
        
        # Initialize repositories
        self.match_repo = MatchRepository(self.db_manager)
        self.player_repo = PlayerRepository(self.db_manager)
        self.matchday_repo = MatchdayInfoRepository(self.db_manager)
        
        # Initialize converter
        self.converter = MatchDataConverter()
        
        self._initialized = False

    def initialize(self, create_tables: bool = True) -> None:
        """
        Initialize the match service.

        Args:
            create_tables: Whether to create tables if they don't exist
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
                "Failed to initialize match service"
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
            raise DatabaseServiceError(
                "Failed to cleanup match service"
            ) from error

    @contextmanager
    def transaction(self):
        """
        Context manager for database transactions.
        """
        if not self._initialized:
            self.initialize()

        with self.db_manager.get_session() as session:
            yield session

    def add_match_complete(self, match_data: MatchDetail) -> bool:
        """
        Add complete match data (match + players + events + lineups).
        
        Args:
            match_data: MatchDetail dataclass from extractor
            
        Returns:
            True if successfully added
        """
        if not self._initialized:
            self.initialize()
        
        try:
            with self.db_manager.get_session() as session:
                # Set session on converter
                self.converter.set_session(session)
                
                # Convert and save all match data
                success = self.converter.convert_and_save_match(match_data)
                
                if success:
                    session.commit()
                    print(f"✅ Added complete match {match_data.match_info.match_id}")
                    return True
                else:
                    session.rollback()
                    print(f"❌ Failed to add match {match_data.match_info.match_id}")
                    return False
                    
        except Exception as error:
            print(f"❌ Error adding match: {error}")
            raise DatabaseServiceError(
                f"Could not add complete match: {error}"
            ) from error

    def update_match_complete(self, match_data: MatchDetail) -> bool:
        """
        Update complete match data.
        
        Args:
            match_data: MatchDetail dataclass from extractor
            
        Returns:
            True if successfully updated
        """
        if not self._initialized:
            self.initialize()
        
        try:
            with self.db_manager.get_session() as session:
                # Set session on converter
                self.converter.set_session(session)
                
                # For updates, we'll delete existing events and re-add them
                # This ensures data consistency
                existing_match = self.match_repo.get_by_id(match_data.match_info.match_id)
                
                if not existing_match:
                    # If match doesn't exist, create it
                    return self.add_match_complete(match_data)
                
                # Convert and save (converter handles updates)
                success = self.converter.convert_and_save_match(match_data)
                
                if success:
                    session.commit()
                    print(f"✅ Updated complete match {match_data.match_info.match_id}")
                    return True
                else:
                    session.rollback()
                    print(f"❌ Failed to update match {match_data.match_info.match_id}")
                    return False
                    
        except Exception as error:
            print(f"❌ Error updating match: {error}")
            raise DatabaseServiceError(
                f"Could not update complete match: {error}"
            ) from error

    def add_matchday_info(self, matchday_data: MatchdayContainer) -> bool:
        """
        Add matchday info from MatchdayContainer.
        
        Args:
            matchday_data: MatchdayContainer dataclass from extractor
            
        Returns:
            True if successfully added
        """
        if not self._initialized:
            self.initialize()
        
        try:
            with self.db_manager.get_session() as session:
                # Set session on converter
                self.converter.set_session(session)
                
                # Convert and save matchday info
                matchday_info = self.converter.create_matchday_info(matchday_data)
                
                if matchday_info:
                    session.add(matchday_info)
                    session.commit()
                    print(f"✅ Added matchday info")
                    return True
                else:
                    print(f"❌ Failed to create matchday info")
                    return False
                    
        except Exception as error:
            print(f"❌ Error adding matchday info: {error}")
            raise DatabaseServiceError(
                f"Could not add matchday info: {error}"
            ) from error

    # Match Operations
    def get_match(self, match_id: str) -> Optional[Match]:
        """
        Get match by ID.
        """
        if not self._initialized:
            self.initialize()
        
        try:
            return self.match_repo.get_by_id(match_id)
        except Exception as error:
            raise DatabaseServiceError(
                f"Could not get match: {error}"
            ) from error

    def get_match_with_details(self, match_id: str) -> Optional[Match]:
        """
        Get match with all related data.
        """
        if not self._initialized:
            self.initialize()
        
        try:
            return self.match_repo.get_match_with_details(match_id)
        except Exception as error:
            raise DatabaseServiceError(
                f"Could not get match with details: {error}"
            ) from error

    def get_matches_by_competition(
        self, 
        competition_id: str, 
        season: str
        ) -> List[Match]:
        """
        Get matches by competition and season.
        """
        if not self._initialized:
            self.initialize()
        
        try:
            return self.match_repo.get_by_competition_and_season(
                competition_id, season
            )
        except Exception as error:
            raise DatabaseServiceError(
                f"Could not get matches by competition: {error}"
            ) from error

    def get_matches_by_matchday(
        self, 
        competition_id: str, 
        season: str, 
        matchday: int
        ) -> List[Match]:
        """
        Get matches by specific matchday.
        """
        if not self._initialized:
            self.initialize()
        
        try:
            return self.match_repo.get_by_matchday(
                competition_id, season, matchday
            )
        except Exception as error:
            raise DatabaseServiceError(
                f"Could not get matches by matchday: {error}"
            ) from error

    def update_match(self, match_id: str, update_data: Dict[str, Any]) -> Optional[Match]:
        """
        Update match.
        """
        if not self._initialized:
            self.initialize()
        
        try:
            return self.match_repo.update(match_id, update_data)
        except Exception as error:
            raise DatabaseServiceError(
                f"Could not update match: {error}"
            ) from error

    def delete_match(self, match_id: str, soft_delete: bool = True) -> bool:
        """
        Delete match.
        """
        if not self._initialized:
            self.initialize()
        
        try:
            return self.match_repo.delete(match_id, soft_delete)
        except Exception as error:
            raise DatabaseServiceError(
                f"Could not delete match: {error}"
            ) from error

    # Player Operations
    def get_player(self, player_id: str) -> Optional[Player]:
        """
        Get player by ID.
        """
        if not self._initialized:
            self.initialize()
        
        try:
            return self.player_repo.get_by_id(player_id)
        except Exception as error:
            raise DatabaseServiceError(
                f"Could not get player: {error}"
            ) from error

    def update_player(self, player_id: str, update_data: Dict[str, Any]) -> Optional[Player]:
        """
        Update player.
        """
        if not self._initialized:
            self.initialize()
        
        try:
            return self.player_repo.update(player_id, update_data)
        except Exception as error:
            raise DatabaseServiceError(
                f"Could not update player: {error}"
            ) from error

    # Matchday Info Operations
    def get_matchday_info(
        self, 
        competition_id: str, 
        season: str, 
        matchday_number: int
        ) -> Optional[MatchdayInfo]:
        """
        Get matchday info by competition, season, and matchday number.
        """
        if not self._initialized:
            self.initialize()
        
        try:
            return self.matchday_repo.get_by_competition_season_matchday(
                competition_id, season, matchday_number
            )
        except Exception as error:
            raise DatabaseServiceError(
                f"Could not get matchday info: {error}"
            ) from error

    # Analytics and Reporting
    def get_match_statistics(
        self, 
        competition_id: str, 
        season: str
        ) -> Dict[str, Any]:
        """
        Get match statistics for a competition/season.
        
        Args:
            competition_id: Competition identifier
            season: Season identifier
            
        Returns:
            Statistics dictionary
        """
        if not self._initialized:
            self.initialize()

        try:
            # Get all matches for competition/season
            matches = self.get_matches_by_competition(competition_id, season)

            if not matches:
                return {"error": f"No matches found for competition {competition_id}, season {season}"}

            # Calculate statistics
            total_matches = len(matches)
            total_goals = sum(
                (match.home_final_score or 0) + (match.away_final_score or 0) 
                for match in matches
            )
            completed_matches = sum(
                1 for match in matches 
                if match.home_final_score is not None and match.away_final_score is not None
            )
            
            stats = {
                "competition_id": competition_id,
                "season": season,
                "total_matches": total_matches,
                "completed_matches": completed_matches,
                "pending_matches": total_matches - completed_matches,
                "total_goals": total_goals,
                "average_goals_per_match": total_goals / completed_matches if completed_matches > 0 else 0,
                "matches_with_home_wins": sum(
                    1 for match in matches 
                    if (match.home_final_score or 0) > (match.away_final_score or 0)
                ),
                "matches_with_away_wins": sum(
                    1 for match in matches 
                    if (match.away_final_score or 0) > (match.home_final_score or 0)
                ),
                "matches_with_draws": sum(
                    1 for match in matches 
                    if (match.home_final_score or 0) == (match.away_final_score or 0)
                    and match.home_final_score is not None
                ),
                "venues": list(set(match.venue for match in matches if match.venue)),
                "total_attendance": sum(
                    match.attendance or 0 for match in matches if match.attendance
                ),
                "matches_with_attendance": sum(
                    1 for match in matches if match.attendance
                ),
            }
            
            # Add average attendance
            if stats["matches_with_attendance"] > 0:
                stats["average_attendance"] = (
                    stats["total_attendance"] / stats["matches_with_attendance"]
                )
            else:
                stats["average_attendance"] = 0

            return stats

        except Exception as error:
            raise DatabaseServiceError(
                "Could not generate match statistics"
            ) from error