# core/scrapers/orchestrator_utils.py
"""
Utility functions for orchestrator operations.
Provides helper methods for common orchestrator tasks.
"""

import time
from typing import Dict, List, Optional

import pandas as pd

from .orchestrator_config import OrchestratorConfig


class OrchestratorUtils:
    """
    Utility class providing helper methods for orchestrator operations.
    """

    @staticmethod
    def format_url_for_display(url: str, max_length: int = None) -> str:
        """
        Format URL for display by truncating if necessary.

        Args:
            url: URL to format
            max_length: Maximum length before truncation

        Returns:
            Formatted URL string
        """
        if max_length is None:
            max_length = OrchestratorConfig.URL_DISPLAY_LENGTH

        if len(url) <= max_length:
            return url

        return "%s%s" % (url[:max_length], OrchestratorConfig.URL_ELLIPSIS)

    @staticmethod
    def format_url_for_logging(url: str) -> str:
        """
        Format URL for logging by truncating to standard length.

        Args:
            url: URL to format

        Returns:
            Formatted URL string for logging
        """
        return OrchestratorUtils.format_url_for_display(
            url, OrchestratorConfig.URL_TRUNCATE_LENGTH
        )

    @staticmethod
    def calculate_processing_metrics(
        start_time: float, total_items: int, pages_or_competitions: int
    ) -> Dict[str, float]:
        """
        Calculate processing performance metrics.

        Args:
            start_time: Processing start time
            total_items: Total items processed
            pages_or_competitions: Number of pages or competitions processed

        Returns:
            Dictionary containing performance metrics
        """
        duration = time.time() - start_time
        avg_time_per_unit = duration / max(1, pages_or_competitions)
        items_per_second = total_items / max(1, duration)

        return {
            "duration": duration,
            "avg_time_per_unit": avg_time_per_unit,
            "items_per_second": items_per_second,
            "total_items": total_items,
            "total_units": pages_or_competitions,
        }

    @staticmethod
    def add_metadata_to_dataframe(
        data: pd.DataFrame, page_num: int, timestamp: Optional[pd.Timestamp] = None
    ) -> pd.DataFrame:
        """
        Add metadata columns to dataframe.

        Args:
            data: Source dataframe
            page_num: Page number to add
            timestamp: Timestamp to add (uses current if None)

        Returns:
            Dataframe with added metadata
        """
        if timestamp is None:
            timestamp = pd.Timestamp.now()

        data_copy = data.copy()
        data_copy["page_number"] = page_num
        data_copy["scraped_at"] = timestamp

        return data_copy

    @staticmethod
    def is_timeout_error(error: Exception) -> bool:
        """
        Check if an error is a timeout-related error.

        Args:
            error: Exception to check

        Returns:
            True if error appears to be timeout-related
        """
        return "timeout" in str(error).lower()

    @staticmethod
    def combine_dataframes(data_list: List[pd.DataFrame]) -> pd.DataFrame:
        """
        Combine list of dataframes safely.

        Args:
            data_list: List of dataframes to combine

        Returns:
            Combined dataframe or empty dataframe if list is empty
        """
        if not data_list:
            return pd.DataFrame()

        return pd.concat(data_list, ignore_index=True)

    @staticmethod
    def get_unique_count(data: pd.DataFrame, columns: List[str]) -> int:
        """
        Get count of unique rows based on specified columns.

        Args:
            data: Source dataframe
            columns: Columns to use for uniqueness check

        Returns:
            Count of unique rows
        """
        if data.empty:
            return 0

        return len(data.drop_duplicates(columns))

    @staticmethod
    def format_duration(duration_seconds: float) -> str:
        """
        Format duration in seconds to human-readable string.

        Args:
            duration_seconds: Duration in seconds

        Returns:
            Formatted duration string
        """
        return "%.3fs" % duration_seconds

    @staticmethod
    def safe_divide(numerator: float, denominator: float) -> float:
        """
        Safely divide two numbers, avoiding division by zero.

        Args:
            numerator: Number to divide
            denominator: Number to divide by

        Returns:
            Result of division or 0 if denominator is 0
        """
        return numerator / max(1, denominator)

    @staticmethod
    def extract_sample_data(data: pd.DataFrame) -> Optional[Dict[str, str]]:
        """
        Extract sample data from dataframe for logging purposes.

        Args:
            data: Source dataframe

        Returns:
            Dictionary with sample data or None if empty
        """
        if data.empty:
            return None

        sample = data.iloc[0]
        return {
            "competition_name": str(sample.get("competition_name", "N/A")),
            "country": str(sample.get("country", "N/A")),
            "tier": str(sample.get("tier", "N/A")),
        }

    @staticmethod
    def wait_with_interval(
        condition_func, interval: int = None, max_wait: int = 300
    ) -> bool:
        """
        Wait for a condition to be met with specified interval.

        Args:
            condition_func: Function that returns True when condition is met
            interval: Wait interval in seconds
            max_wait: Maximum wait time in seconds

        Returns:
            True if condition was met, False if timeout
        """
        if interval is None:
            interval = OrchestratorConfig.VPN_ROTATION_WAIT_INTERVAL

        waited = 0
        while waited < max_wait:
            if not condition_func():
                return True
            time.sleep(interval)
            waited += interval

        return False

    @staticmethod
    def generate_csv_filename(prefix: str, timestamp: Optional[str] = None) -> str:
        """
        Generate CSV filename with timestamp.

        Args:
            prefix: Filename prefix
            timestamp: Custom timestamp (uses current if None)

        Returns:
            Generated filename
        """
        if timestamp is None:
            timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")

        return "%s_%s.csv" % (prefix, timestamp)
