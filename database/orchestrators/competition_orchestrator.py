# database/orchestrators/competition_orchestrator.py
"""
Competition data orchestrator - simplified to work with existing config.
"""

from typing import Any, Dict, Optional

import pandas as pd

from exceptions import DatabaseOperationError

from ..factory.database_factory import create_database_service
from ..services.database_service import DatabaseService


class CompetitionDataOrchestrator:
    """
    Orchestrator for competition data operations using existing config system.
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
        "number_of_clubs": 0,
        "number_of_players": 0,
        "average_age_of_players": 0.0,
        "percentage_of_foreign_players": 0,
        "percentage_game_ratio_of_foreign_players": 0.0,
        "goals_per_match": 0.0,
        "average_market_value": 0.0,
        "total_market_value": 0.0,
    }

    # ***> Default values for string fields <***
    STRING_DEFAULTS = {
        "competition_id": "",
        "competition_code": "",
        "competition_name": "",
        "competition_url": "",
        "competition_type": "",
        "country": "",
        "tier": "",
    }

    def __init__(self, environment: Optional[str] = None):
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

                    # ***> Check if competition already exists <***
                    existing = self.db_service.get_competition(
                        competition_data["competition_id"]
                    )

                    if existing:
                        self._update_existing_competition(competition_data, existing)
                        duplicate_count += 1
                    else:
                        self._add_new_competition(competition_data)
                        success_count += 1

                except Exception:
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
        # ***> Convert pandas row to dictionary <***
        competition_data = row.to_dict()

        # ***> Handle NaN values with appropriate defaults <***
        self._handle_nan_values(competition_data)

        # ***> Remove DataFrame-specific metadata fields <***
        for field in self.METADATA_FIELDS:
            competition_data.pop(field, None)

        return competition_data

    def _handle_nan_values(self, data: Dict[str, Any]) -> None:
        """
        Replace NaN values with appropriate defaults for competitions.

        Args:
            data: Competition data dictionary to clean
        """
        for key, value in data.items():
            if pd.isna(value):
                if key in self.NUMERIC_DEFAULTS:
                    data[key] = self.NUMERIC_DEFAULTS[key]
                elif key in self.STRING_DEFAULTS:
                    data[key] = self.STRING_DEFAULTS[key]
                elif isinstance(value, str):
                    data[key] = ""

    def _update_existing_competition(
        self, competition_data: Dict[str, Any], existing: Any
    ) -> None:
        """
        Update existing competition in database.

        Args:
            competition_data: New competition data
            existing: Existing competition record
        """
        update_data = {
            k: v for k, v in competition_data.items() if k != "competition_id"
        }

        if self.db_service is not None:
            self.db_service.update_competition(
                competition_data["competition_id"], update_data
            )

    def _add_new_competition(self, competition_data: Dict[str, Any]) -> None:
        """
        Add new competition to database.

        Args:
            competition_data: Competition data to add
        """
        if self.db_service is not None:
            self.db_service.add_competition(competition_data)

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
