# database/orchestrators/team_orchestrator.py
"""
Team data orchestrator - simplified to work with existing config.
"""

from typing import Any, Dict, List, Optional

import pandas as pd

from exceptions import DatabaseOperationError

from ..factory.database_factory import create_database_service
from ..services.database_service import DatabaseService


class TeamDataOrchestrator:
    """
    Orchestrator for team/club data operations using existing config system.
    """

    # ***> Metadata fields to exclude from database operations <***
    METADATA_FIELDS = [
        "__index_level_0__",
        "index",
        "_metadata",
        "dataframe_info",
        "source_season_id",
    ]

    # ***> Default values for numeric fields <***
    NUMERIC_DEFAULTS = {
        "squad_size": 0,
        "average_age_of_players": 0.0,
        "number_of_foreign_players": 0,
        "average_market_value": 0.0,
        "total_market_value": 0.0,
    }

    # ***> Default values for string fields <***
    STRING_DEFAULTS = {
        "club_id": "",
        "club_name": "",
        "club_code": "",
        "club_url": "",
        "season_id": "",
        "season_year": "",
        "competition_id": "",
    }

    def __init__(self, environment: Optional[str] = ""):
        """
        Initialize orchestrator using existing configuration system.

        Args:
            environment: Environment name ('development', 'testing', 'production')

        Raises:
            DatabaseOperationError: If database initialization fails
        """
        self.environment = environment
        self.db_service: Optional[DatabaseService] = None

        try:
            self.db_service = create_database_service(environment)
            self.db_service.initialize()
        except Exception as e:
            error_msg = f"Failed to initialize database service: {e}"
            raise DatabaseOperationError(error_msg)

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

                    # ***> Check if club record already exists for this season <***
                    existing = self._get_existing_club_record(club_data)

                    if existing:
                        print(
                            f"🔄 Updating club {club_data['club_id']} / {club_data['season_year']}"
                        )
                        self._update_existing_club(club_data, existing)
                        duplicate_count += 1
                    else:
                        print(
                            f"✚ Inserting club {club_data['club_id']} / {club_data['season_year']}"
                        )
                        self._add_new_club(club_data)
                        success_count += 1

                except Exception as e:
                    print(f"❌ Error details: {type(e).__name__}: {e}")
                    print(f"❌ Club data: {club_data}")
                    import traceback

                    print(traceback.format_exc())
                    error_count += 1
                    continue
            print(
                f"✅ save_clubs: success {success_count}, duplicates {duplicate_count}, errors {error_count}"
            )
            return (success_count + duplicate_count) > 0

        except Exception as e:
            error_msg = f"Club database save operation failed: {e}"
            raise DatabaseOperationError(error_msg)

    def get_non_cup_competitions(self) -> List[Dict[str, str]]:
        """
        Query competitions table for non-cup competitions.

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
        # ***> Convert pandas row to dictionary <***
        club_data = row.to_dict()

        # ***> Handle NaN values with appropriate defaults <***
        self._handle_nan_values(club_data)

        # ***> Remove DataFrame-specific metadata fields <***
        for field in self.METADATA_FIELDS:
            club_data.pop(field, None)

        return club_data

    def _handle_nan_values(self, data: Dict[str, Any]) -> None:
        """
        Replace NaN values in club data with appropriate defaults.

        Args:
            data: Club data dictionary to clean
        """
        for key, value in data.items():
            if pd.isna(value):
                if key in self.NUMERIC_DEFAULTS:
                    data[key] = self.NUMERIC_DEFAULTS[key]
                elif key in self.STRING_DEFAULTS:
                    data[key] = self.STRING_DEFAULTS[key]
                elif isinstance(value, str):
                    data[key] = ""

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
                    club_data["club_id"],
                    club_data["season_year"],
                    club_data["competition_id"],
                )
            else:
                return None
        except Exception:
            return None

    def _update_existing_club(self, club_data: Dict[str, Any], existing: Any) -> None:
        """
        Update existing club record in database.

        Args:
            club_data: New club data
            existing: Existing club record
        """
        update_data = {
            k: v
            for k, v in club_data.items()
            if k not in ["club_id", "season_year", "competition_id"]
        }

        if self.db_service is not None:
            self.db_service.update_club(
                club_data["club_id"],
                club_data["season_year"],
                club_data["competition_id"],
                update_data,
            )

    def _add_new_club(self, club_data: Dict[str, Any]) -> None:
        """Add new club record to database."""
        if self.db_service is not None:
            try:
                result = self.db_service.add_team(club_data)
                # Don't access Team object attributes - just confirm success
                if result:
                    print(
                        f"✅ Added team: {club_data['club_id']} / {club_data['season_year']}"
                    )
                else:
                    print(f"❌ Failed to add team: {club_data['club_id']}")
            except Exception as e:
                print(f"❌ Error adding team {club_data['club_id']}: {e}")
                raise

    def cleanup(self) -> None:
        """
        Clean up database resources.
        """
        if self.db_service:
            try:
                self.db_service.cleanup()
            except Exception:
                pass

    @property
    def is_available(self) -> bool:
        """
        Check if database service is available.
        """
        return self.db_service is not None
