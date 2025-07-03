# core/scrapers/database_manager.py
"""
Database operations manager for competition data.
Handles all database interactions and data persistence.
"""
from typing import Dict, Any, List
import pandas as pd

from .database_factory import create_database_service
from logger import DatabaseConstants
from exceptions import DatabaseOperationError


class CompetitionDatabaseManager:
    """
    Manages database operations for competition data.
    """
    
    def __init__(
        self,
        environment: str, 
        logger_name: str = "CompetitionDatabaseManager"
    ):
        """
        Initialize database manager.
        
        Args:
            environment: Environment name (development/testing/production)
            logger_name: Name for the logger instance
            
        Raises:
            DatabaseOperationError: If database initialization fails
        """
        self.environment = environment
        self.db_service = None
        
        try:
            self.db_service = create_database_service(environment)
            self.db_service.initialize()
        except Exception as e:
            error_msg = f"Failed to initialize database service: {e}"
            raise DatabaseOperationError(error_msg)
    
    def save_competitions(self, data: pd.DataFrame) -> bool:
        """
        Save scraped competition data to database.
        
        Args:
            data: DataFrame with competition data
            
        Returns:
            True if successfully saved to database
            
        Raises:
            DatabaseOperationError: If database save fails critically
        """
        
        if not self.db_service:
            return False
        
        try:
            success_count = 0
            error_count = 0
            duplicate_count = 0
            
            for _, row in data.iterrows():
                try:
                    competition_data = self._prepare_competition_data(row)
                    
                    # Check if competition already exists
                    existing = self.db_service.get_competition(
                        competition_data['competition_id']
                    )
                    
                    if existing:
                        self._update_existing_competition(
                            competition_data, existing
                        )
                        duplicate_count += 1
                    else:
                        self._add_new_competition(competition_data)
                        success_count += 1
                        
                except Exception as e:
                    error_count += 1
                    continue
            
            return (success_count + duplicate_count) > 0
            
        except Exception as e:
            error_msg = f"Database save operation failed: {e}"
            raise DatabaseOperationError(error_msg)
    
    def _prepare_competition_data(self, row: pd.Series) -> Dict[str, Any]:
        """
        Prepare competition data for database storage.
        
        Args:
            row: Pandas Series with competition data
            
        Returns:
            Dictionary with cleaned competition data
        """
        # Convert pandas row to dictionary
        competition_data = row.to_dict()
        
        # Handle NaN values with appropriate defaults
        self._handle_nan_values(competition_data)
        
        # Remove DataFrame-specific metadata fields
        for field in DatabaseConstants.METADATA_FIELDS:
            competition_data.pop(field, None)
        
        return competition_data
    
    def _handle_nan_values(self, data: Dict[str, Any]) -> None:
        """
        Replace NaN values with appropriate defaults.
        
        Args:
            data: Competition data dictionary to clean
        """
        for key, value in data.items():
            if pd.isna(value):
                if key in DatabaseConstants.NUMERIC_DEFAULTS:
                    data[key] = DatabaseConstants.NUMERIC_DEFAULTS[key]
                elif key in DatabaseConstants.STRING_DEFAULTS:
                    data[key] = DatabaseConstants.STRING_DEFAULTS[key]
                elif isinstance(value, str):
                    data[key] = ''
    
    def _update_existing_competition(
        self, 
        competition_data: Dict[str, Any], 
        existing: Any
    ) -> None:
        """
        Update existing competition in database.
        
        Args:
            competition_data: New competition data
            existing: Existing competition record
        """
        update_data = {
            k: v for k, v in competition_data.items() 
            if k != 'competition_id'
        }
        
        if self.db_service is not None:
            self.db_service.update_competition(
                competition_data['competition_id'],
                update_data
            )
    
    def _add_new_competition(self, competition_data: Dict[str, Any]) -> None:
        """
        Add new competition to database.
        
        Args:
            competition_data: Competition data to add
        """
        if self.db_service is not None:
            self.db_service.add_competition(competition_data)
    
    def _log_save_results(
        self,
        start_time: float,
        total_competitions: int,
        success_count: int,
        duplicate_count: int,
        error_count: int
    ) -> None:
        """
        Log database save operation results.
        
        Args:
            start_time: Operation start time
            total_competitions: Total competitions processed
            success_count: Number of new competitions saved
            duplicate_count: Number of existing competitions updated
            error_count: Number of failed saves
        """
        pass
    
    def cleanup(self) -> None:
        """Clean up database resources."""
        if self.db_service:
            try:
                self.db_service.cleanup()
            except Exception as e:
                pass
    
    @property
    def is_available(self) -> bool:
        """Check if database service is available."""
        return self.db_service is not None


class ClubDatabaseManager(CompetitionDatabaseManager):
    """
    Extends CompetitionDatabaseManager to handle club data operations.
    Follows the same patterns as the existing CompetitionDatabaseManager.
    """
    
    def __init__(
        self, 
        environment: str, 
        logger_name: str = "ClubDatabaseManager"
    ):
        """
        Initialize club database manager.
        
        Args:
            environment: Environment name (development/testing/production)
            logger_name: Name for the logger instance
        """
        super().__init__(environment, logger_name)
    
    def save_clubs(self, data: pd.DataFrame) -> bool:
        """
        Save scraped club data to database.
        
        Args:
            data: DataFrame with club data
            
        Returns:
            True if successfully saved to database
            
        Raises:
            DatabaseOperationError: If database save fails critically
        """
        
        if not self.db_service:
            return False
        
        try:
            success_count = 0
            error_count = 0
            duplicate_count = 0
            
            for _, row in data.iterrows():
                try:
                    club_data = self._prepare_club_data(row)
                    
                    # Check if club record already exists for this season
                    existing = self._get_existing_club_record(club_data)
                    
                    if existing:
                        self._update_existing_club(club_data, existing)
                        duplicate_count += 1
                    else:
                        self._add_new_club(club_data)
                        success_count += 1
                        
                except Exception as e:
                    error_count += 1
                    continue
            
            return (success_count + duplicate_count) > 0
            
        except Exception as e:
            error_msg = f"Club database save operation failed: {e}"
            raise DatabaseOperationError(error_msg)
    
    def get_non_cup_competitions(self) -> List[Dict[str, str]]:
        """
        Query competitions table for non-cup competitions
        
        Returns:
            List of dictionaries containing competition_id and competition_url
        """
        if not self.db_service:
            return []
        
        try:
            competitions = self.db_service.get_non_cup_competitions()
            return competitions
            
        except Exception as e:
            error_msg = f"Error querying non-cup competitions: {e}"
            raise DatabaseOperationError(error_msg) from e
    
    def _prepare_club_data(self, row: pd.Series) -> Dict[str, Any]:
        """
        Prepare club data for database storage.
        
        Args:
            row: Pandas Series with club data
            
        Returns:
            Dictionary with cleaned club data
        """
        # Convert pandas row to dictionary
        club_data = row.to_dict()
        
        # Handle NaN values with appropriate defaults
        self._handle_club_nan_values(club_data)
        
        # Remove DataFrame-specific metadata fields
        for field in DatabaseConstants.METADATA_FIELDS:
            club_data.pop(field, None)
        
        return club_data
    
    def _handle_club_nan_values(self, data: Dict[str, Any]) -> None:
        """
        Replace NaN values in club data with appropriate defaults.
        
        Args:
            data: Club data dictionary to clean
        """
        club_numeric_defaults = {
            "squad_size": 0,
            "average_age_of_players": 0.0,
            "number_of_foreign_players": 0,
            "average_market_value": 0.0,
            "total_market_value": 0.0
        }
        
        club_string_defaults = {
            "club_id": "",
            "club_name": "",
            "club_code": "",
            "club_url": "",
            "season_id": "",
            "season_year": "",
            "competition_id": ""
        }
        
        for key, value in data.items():
            if pd.isna(value):
                if key in club_numeric_defaults:
                    data[key] = club_numeric_defaults[key]
                elif key in club_string_defaults:
                    data[key] = club_string_defaults[key]
                elif isinstance(value, str):
                    data[key] = ''
    
    def _get_existing_club_record(self, club_data: Dict[str, Any]) -> Any:
        """
        Check if club record already exists for this season.
        
        Args:
            club_data: Club data dictionary
            
        Returns:
            Existing record or None
        """
        try:
            if self.db_service is not None:
                return self.db_service.get_team_by_season(
                    club_data['club_id'],
                    club_data['season_year'],
                    club_data['competition_id']
                )
            else:
                return None
        except Exception:
            return None
    
    def _update_existing_club(
        self, 
        club_data: Dict[str, Any], 
        existing: Any
    ) -> None:
        """
        Update existing club record in database.
        
        Args:
            club_data: New club data
            existing: Existing club record
        """
        update_data = {
            k: v for k, v in club_data.items() 
            if k not in ['club_id', 'season_year', 'competition_id']
        }
        
        if self.db_service is not None:
            self.db_service.update_club(
                club_data['club_id'],
                club_data['season_year'],
                club_data['competition_id'],
                update_data
            )
    
    def _add_new_club(self, club_data: Dict[str, Any]) -> None:
        """
        Add new club record to database.
        
        Args:
            club_data: Club data to add
        """
        if self.db_service is not None:
            self.db_service.add_team(club_data)
    
    def _log_club_save_results(
        self,
        start_time: float,
        total_clubs: int,
        success_count: int,
        duplicate_count: int,
        error_count: int
    ) -> None:
        """
        Log club database save operation results.
        
        Args:
            start_time: Operation start time
            total_clubs: Total club records processed
            success_count: Number of new club records saved
            duplicate_count: Number of existing club records updated
            error_count: Number of failed saves
        """
        pass