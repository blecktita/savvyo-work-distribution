# pipelines/run_match_collection.py
"""
Main match scraper API interface.
Follows the same pattern as club collection but simplified.
"""

from typing import Optional

import pandas as pd

from configurations import ScraperConfig

from .princpal_orchestrator.match_orchestrator import MatchOrchestrator
from .run_competition_club_collection import BaseScraper, _execute_scraping_workflow


class MatchScraper(BaseScraper):
    """
    Match scraper interface maintaining API consistency.
    """

    def __init__(
        self,
        config: Optional[ScraperConfig] = None,
        env_file_path: Optional[str] = None,
    ):
        """
        Initialize match scraper.
        """
        super().__init__(
            orchestrator_class=MatchOrchestrator,
            config=config,
            env_file_path=env_file_path,
        )

    def scrape_all_match_data(self, driver) -> pd.DataFrame:
        """
        Scrape comprehensive match data with smart completion detection.
        """
        return self.orchestrator.scrape_all_match_data(driver)


# ***> Convenience functions <***
def scrape_match_data(
    driver, environment: str = "development", use_vpn: bool = True
) -> pd.DataFrame:
    """
    Scrape match data with VPN protection.
    """
    return _execute_scraping_workflow(
        scraper_class=MatchScraper,
        scraper_method="scrape_all_match_data",
        environment=environment,
        use_vpn=use_vpn,
        driver=driver,
    )


def scrape_match_data_safe_mode(
    driver, environment: str = "development"
) -> pd.DataFrame:
    """
    Scrape match data WITHOUT VPN protection.
    """
    return scrape_match_data(driver=driver, environment=environment, use_vpn=False)
