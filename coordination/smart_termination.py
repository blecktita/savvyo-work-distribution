# coordination/smart_termination.py
"""
Smart early termination system for detecting when historical seasons are empty.
Detects patterns of missing data and avoids wasting time on unavailable seasons.
"""

import random
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Set


@dataclass
class TerminationConfig:
    """Configuration for smart termination behavior."""

    # Trigger sampling after this many consecutive failures
    failure_threshold: int = 3

    # Number of random seasons to sample when pattern detected
    sample_size: int = 5

    # How many years back to sample for availability check
    sample_range_years: int = 15

    # If this many samples fail, stop completely
    stop_threshold: float = 0.8  # 80% failure rate

    # Minimum year to consider (absolute cutoff)
    absolute_minimum_year: int = 1950


@dataclass
class SeasonSample:
    """Represents a sampled season for availability testing."""

    year: str
    season_id: str
    has_data: bool
    clubs_found: int = 0


class SmartTerminationDecision:
    """Decision result from smart termination analysis."""

    def __init__(
        self,
        should_continue: bool,
        reason: str,
        cutoff_year: Optional[str] = None,
        samples_tested: List[SeasonSample] = None,
    ):
        self.should_continue = should_continue
        self.reason = reason
        self.cutoff_year = cutoff_year
        self.samples_tested = samples_tested or []
        self.timestamp = datetime.now()


class SmartSeasonTerminator:
    """
    Detects patterns in season failures and makes intelligent decisions
    about whether to continue processing historical seasons.
    """

    def __init__(self, config: Optional[TerminationConfig] = None):
        """Initialize with configuration."""
        self.config = config or TerminationConfig()
        self.consecutive_failures: Dict[str, int] = {}
        self.failed_seasons: Dict[str, Set[str]] = {}
        self.tested_samples: Dict[str, List[SeasonSample]] = {}

    def should_continue_processing(
        self,
        competition_id: str,
        failed_season: Dict[str, str],
        remaining_seasons: List[Dict[str, str]],
        test_callback,
    ) -> SmartTerminationDecision:
        """
        Decide whether to continue processing seasons based on failure patterns.

        Args:
            competition_id: Competition being processed
            failed_season: Season that just failed (with 'year', 'season_id')
            remaining_seasons: List of remaining seasons to process
            test_callback: Function to test if a season has data
                         Signature: test_callback(competition_id, season) -> (has_data, clubs_count)

        Returns:
            SmartTerminationDecision with recommendation
        """
        # Initialize tracking for this competition
        if competition_id not in self.consecutive_failures:
            self.consecutive_failures[competition_id] = 0
            self.failed_seasons[competition_id] = set()

        # Track this failure
        self.consecutive_failures[competition_id] += 1
        self.failed_seasons[competition_id].add(failed_season["year"])

        # Check if we should trigger sampling
        if self.consecutive_failures[competition_id] >= self.config.failure_threshold:
            return self._analyze_with_sampling(
                competition_id, failed_season, remaining_seasons, test_callback
            )

        # Not enough failures yet, continue
        return SmartTerminationDecision(
            should_continue=True,
            reason=f"Only {self.consecutive_failures[competition_id]} consecutive failures, continuing",
        )

    def mark_season_success(self, competition_id: str):
        """Reset failure counter on successful season."""
        if competition_id in self.consecutive_failures:
            self.consecutive_failures[competition_id] = 0

    def _analyze_with_sampling(
        self,
        competition_id: str,
        failed_season: Dict[str, str],
        remaining_seasons: List[Dict[str, str]],
        test_callback,
    ) -> SmartTerminationDecision:
        """
        Perform sampling analysis to determine if older seasons have data.
        """
        print(
            f"🔍 Pattern detected for {competition_id}: {self.consecutive_failures[competition_id]} consecutive failures"
        )
        print(
            f"   Failed season: {failed_season['season_id']} ({failed_season['year']})"
        )

        # Get samples to test
        sample_seasons = self._generate_sample_seasons(failed_season, remaining_seasons)

        if not sample_seasons:
            return SmartTerminationDecision(
                should_continue=False,
                reason="No valid seasons to sample",
                cutoff_year=failed_season["year"],
            )

        print(f"🎯 Testing {len(sample_seasons)} random historical seasons...")

        # Test each sample
        tested_samples = []
        for sample in sample_seasons:
            print(f"   Testing {sample.season_id} ({sample.year})...", end=" ")

            try:
                has_data, clubs_count = test_callback(competition_id, sample.__dict__)
                sample.has_data = has_data
                sample.clubs_found = clubs_count

                status = "✅ HAS DATA" if has_data else "❌ EMPTY"
                clubs_info = f"({clubs_count} clubs)" if clubs_count > 0 else ""
                print(f"{status} {clubs_info}")

            except Exception as e:
                sample.has_data = False
                sample.clubs_found = 0
                print(f"❌ ERROR: {str(e)[:50]}...")

            tested_samples.append(sample)

        # Store results
        self.tested_samples[competition_id] = tested_samples

        # Analyze results
        return self._make_termination_decision(
            competition_id, failed_season, tested_samples
        )

    def _generate_sample_seasons(
        self, failed_season: Dict[str, str], remaining_seasons: List[Dict[str, str]]
    ) -> List[SeasonSample]:
        """
        Generate a list of seasons to sample for data availability.
        """
        try:
            failed_year = int(failed_season["year"])
        except (ValueError, KeyError):
            return []

        # Create pool of years to sample from
        min_year = max(
            failed_year - self.config.sample_range_years,
            self.config.absolute_minimum_year,
        )

        # Get available years from remaining seasons
        available_years = set()
        year_to_season = {}

        for season in remaining_seasons:
            try:
                year = int(season["year"])
                if min_year <= year < failed_year:
                    available_years.add(year)
                    year_to_season[year] = season
            except (ValueError, KeyError):
                continue

        if not available_years:
            return []

        # Randomly sample years
        sample_count = min(self.config.sample_size, len(available_years))
        sampled_years = random.sample(list(available_years), sample_count)

        # Convert to SeasonSample objects
        samples = []
        for year in sorted(sampled_years):
            season = year_to_season[year]
            samples.append(
                SeasonSample(
                    year=season["year"],
                    season_id=season["season_id"],
                    has_data=False,  # Will be tested
                )
            )

        return samples

    def _make_termination_decision(
        self,
        competition_id: str,
        failed_season: Dict[str, str],
        tested_samples: List[SeasonSample],
    ) -> SmartTerminationDecision:
        """
        Make termination decision based on sample results.
        """
        if not tested_samples:
            return SmartTerminationDecision(
                should_continue=False,
                reason="No samples could be tested",
                cutoff_year=failed_season["year"],
            )

        # Calculate failure rate
        failed_samples = sum(1 for s in tested_samples if not s.has_data)
        failure_rate = failed_samples / len(tested_samples)

        print(f"\n📊 Sampling Results for {competition_id}:")
        print(f"   • Samples tested: {len(tested_samples)}")
        print(f"   • Failed samples: {failed_samples}")
        print(f"   • Failure rate: {failure_rate:.1%}")

        # Decision logic
        if failure_rate >= self.config.stop_threshold:
            # High failure rate - stop processing
            successful_samples = [s for s in tested_samples if s.has_data]
            if successful_samples:
                # Find the latest year with data
                latest_successful = max(successful_samples, key=lambda s: int(s.year))
                cutoff_year = str(int(latest_successful.year) + 1)
                reason = f"High failure rate ({failure_rate:.1%}), but found data until {latest_successful.year}"
            else:
                cutoff_year = failed_season["year"]
                reason = (
                    f"High failure rate ({failure_rate:.1%}), no data found in samples"
                )

            print(f"🛑 STOPPING: {reason}")
            print(f"   Setting cutoff at year {cutoff_year}")

            return SmartTerminationDecision(
                should_continue=False,
                reason=reason,
                cutoff_year=cutoff_year,
                samples_tested=tested_samples,
            )
        else:
            # Low failure rate - continue with caution
            successful_count = len(tested_samples) - failed_samples
            reason = f"Moderate failure rate ({failure_rate:.1%}), found {successful_count} seasons with data"

            print(f"✅ CONTINUING: {reason}")
            print("   Will continue processing but expect gaps")

            return SmartTerminationDecision(
                should_continue=True, reason=reason, samples_tested=tested_samples
            )

    def get_termination_summary(self, competition_id: str) -> Dict:
        """Get summary of termination analysis for a competition."""
        if competition_id not in self.tested_samples:
            return {}

        samples = self.tested_samples[competition_id]
        return {
            "total_samples": len(samples),
            "successful_samples": sum(1 for s in samples if s.has_data),
            "failed_samples": sum(1 for s in samples if not s.has_data),
            "failure_rate": (
                sum(1 for s in samples if not s.has_data) / len(samples)
                if samples
                else 0
            ),
            "consecutive_failures": self.consecutive_failures.get(competition_id, 0),
            "samples_by_year": {
                s.year: {"has_data": s.has_data, "clubs": s.clubs_found}
                for s in samples
            },
        }


# Example integration function for your existing code
def create_season_tester(club_orchestrator, driver):
    """
    Create a season testing callback for the ClubOrchestrator.

    Args:
        club_orchestrator: Your ClubOrchestrator instance
        driver: Selenium WebDriver

    Returns:
        Function that can test if a season has data
    """

    def test_season_has_data(competition_id: str, season: Dict[str, str]) -> tuple:
        """
        Test if a season has club data available.

        Returns:
            (has_data: bool, clubs_count: int)
        """
        try:
            # Use your existing _scrape_season_club_data logic but without saving
            season_year = season["year"]
            season_id = season["season_id"]

            # Get competition URL (you might need to modify this)
            competitions = club_orchestrator.get_non_cup_competitions()
            competition_url = None
            for comp in competitions:
                if comp["competition_id"] == competition_id:
                    competition_url = comp["competition_url"]
                    break

            if not competition_url:
                return False, 0

            # Construct season URL (using your existing logic)
            season_url = club_orchestrator._construct_season_url(
                competition_url, season_year
            )

            # Quick test navigation
            driver.get(season_url)
            club_orchestrator.vpn_handler.handle_request_timing("quick test...")

            # Parse just to check if table exists
            from bs4 import BeautifulSoup

            soup = BeautifulSoup(driver.page_source, "html.parser")
            club_data = club_orchestrator.parser.parse_club_table(
                soup, season_year, season_id, competition_id
            )

            has_data = not club_data.empty
            clubs_count = len(club_data) if has_data else 0

            return has_data, clubs_count

        except Exception:
            return False, 0

    return test_season_has_data
