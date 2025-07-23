# database/orchestrators/match_orchestrator.py
"""
Match data orchestrator - following existing patterns for football match data operations.
"""
from typing import Dict, Any, List, Optional

from database.factory.database_factory import create_database_service
from database.services.match_service import MatchService
from exceptions import DatabaseOperationError

# Import the dataclass models from the scraper
from extractors.extractor_matchday import MatchDetail, MatchdayContainer


class MatchDataOrchestrator:
    """
    Orchestrator for football match data operations using existing config system.
    """
    
    def __init__(self, environment: Optional[str] = ""):
        """
        Initialize orchestrator using existing configuration system.
        
        Args:
            environment: Environment name ('development', 'testing', 'production')
            
        Raises:
            DatabaseOperationError: If database initialization fails
        """
        self.environment = environment
        self.match_service: Optional[MatchService] = None
        
        try:
            # Use existing database service factory
            self.db_service = create_database_service(environment)
            self.db_service.initialize()
            
            # Create match service
            self.match_service = MatchService(environment)
            self.match_service.initialize()
        except Exception as e:
            error_msg = f"Failed to initialize match data orchestrator: {e}"
            raise DatabaseOperationError(error_msg)

    def save_match_detail(self, match_data: MatchDetail) -> bool:
        """
        Save detailed match data to database.
        
        Args:
            match_data: MatchDetail dataclass from extractor
            
        Returns:
            True if successfully saved to database
            
        Raises:
            DatabaseOperationError: If database save fails critically
        """
        if not self.match_service:
            return False
        
        try:
            # Check if match already exists
            existing = self.match_service.get_match(match_data.match_info.match_id)
            
            if existing:
                print(f"🔄 Updating match {match_data.match_info.match_id}")
                success = self.match_service.update_match_complete(match_data)
            else:
                print(f"✚ Inserting match {match_data.match_info.match_id}")
                success = self.match_service.add_match_complete(match_data)
            
            if success:
                print(f"✅ Successfully saved match {match_data.match_info.match_id}")
            else:
                print(f"❌ Failed to save match {match_data.match_info.match_id}")
            
            return success
            
        except Exception as e:
            error_msg = f"Match database save operation failed: {e}"
            print(f"❌ Error details: {type(e).__name__}: {e}")
            import traceback
            print(traceback.format_exc())
            raise DatabaseOperationError(error_msg)

    def save_matchday_container(self, matchday_data: MatchdayContainer) -> bool:
        """
        Save matchday container data to database.
        
        Args:
            matchday_data: MatchdayContainer dataclass from extractor
            
        Returns:
            True if successfully saved to database
        """
        if not self.match_service:
            return False
        
        try:
            success = self.match_service.add_matchday_info(matchday_data)
            
            if success:
                print("✅ Successfully saved matchday data")
            else:
                print("❌ Failed to save matchday data")
            
            return success
            
        except Exception as e:
            error_msg = f"Matchday database save operation failed: {e}"
            print(f"❌ Error details: {type(e).__name__}: {e}")
            import traceback
            print(traceback.format_exc())
            raise DatabaseOperationError(error_msg)

    def save_matches_bulk(self, matches_data: List[MatchDetail]) -> bool:
        """
        Save multiple match details in bulk.
        
        Args:
            matches_data: List of MatchDetail dataclasses
            
        Returns:
            True if at least one match was saved successfully
        """
        if not self.match_service:
            return False
        
        if not matches_data:
            return False

        success_count = 0
        error_count = 0
        
        for match_data in matches_data:
            try:
                if self.save_match_detail(match_data):
                    success_count += 1
                else:
                    error_count += 1
            except Exception as e:
                print(f"❌ Error saving match {match_data.match_info.match_id}: {e}")
                error_count += 1
                continue
        
        print(f"✅ save_matches_bulk: success {success_count}, errors {error_count}")
        return success_count > 0

    def get_match_by_id(self, match_id: str) -> Optional[Dict[str, Any]]:
        """
        Get match by ID.
        
        Args:
            match_id: Match identifier
            
        Returns:
            Match data as dictionary or None
        """
        if not self.match_service:
            return None
        
        try:
            match = self.match_service.get_match_with_details(match_id)
            if match:
                return match.to_dict()
            return None
        except Exception as e:
            error_msg = f"Error retrieving match {match_id}: {e}"
            raise DatabaseOperationError(error_msg)

    def get_matches_by_competition(
        self, 
        competition_id: str, 
        season: str
    ) -> List[Dict[str, Any]]:
        """
        Get matches by competition and season.
        
        Args:
            competition_id: Competition identifier
            season: Season identifier
            
        Returns:
            List of match data dictionaries
        """
        if not self.match_service:
            return []
        
        try:
            matches = self.match_service.get_matches_by_competition(
                competition_id, season
            )
            return [match.to_dict() for match in matches]
        except Exception as e:
            error_msg = f"Error retrieving matches for competition {competition_id}: {e}"
            raise DatabaseOperationError(error_msg)

    def get_matches_by_matchday(
        self, 
        competition_id: str, 
        season: str, 
        matchday: int
    ) -> List[Dict[str, Any]]:
        """
        Get matches by specific matchday.
        
        Args:
            competition_id: Competition identifier
            season: Season identifier
            matchday: Matchday number
            
        Returns:
            List of match data dictionaries
        """
        if not self.match_service:
            return []
        
        try:
            matches = self.match_service.get_matches_by_matchday(
                competition_id, season, matchday
            )
            return [match.to_dict() for match in matches]
        except Exception as e:
            error_msg = f"Error retrieving matchday {matchday} matches: {e}"
            raise DatabaseOperationError(error_msg)

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
        if not self.match_service:
            return {}
        
        try:
            return self.match_service.get_match_statistics(competition_id, season)
        except Exception as e:
            error_msg = f"Error generating match statistics: {e}"
            raise DatabaseOperationError(error_msg)

    def cleanup(self) -> None:
        """
        Clean up database resources.
        """
        if self.match_service:
            try:
                self.match_service.cleanup()
            except Exception:
                pass
        
        if self.db_service:
            try:
                self.db_service.cleanup()
            except Exception:
                pass

    @property
    def is_available(self) -> bool:
        """
        Check if match service is available.
        """
        return self.match_service is not None and self.db_service is not None