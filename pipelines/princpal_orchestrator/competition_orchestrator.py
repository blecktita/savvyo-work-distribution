# core/scrapers/competition_orchestrator.py
"""
Competition-specific orchestrator for scraping operations.
Handles competition data extraction while maintaining original logic.
"""

import time
from typing import Optional
import pandas as pd
from bs4 import BeautifulSoup

from .base_orchestrator import BaseOrchestrator
from .orchestrator_config import OrchestratorConfig
from .html_parser import CompetitionTableParser
from .database_manager import DatabaseManager
from .exceptions import VpnRequiredError, DatabaseOperationError
from utils.configurations import ScraperConfig


class CompetitionOrchestrator(BaseOrchestrator):
    """
    Orchestrates the complete competition scraping process.
    Maintains exact original logic flow for competition data extraction.
    """

    def __init__(self, config: Optional[ScraperConfig] = None):
        """
        Initialize competition scraping orchestrator.

        Args:
            config: Scraper configuration (uses development if None)
 
        Raises:
            VpnRequiredError: If VPN is required but not available
            ConfigurationError: If configuration is invalid
        """
        super().__init__(config)
        
        #***> Initialize competition-specific components <***
        self._setup_competition_components()

    def _setup_competition_components(self) -> None:
        """
        Initialize components specific to competition scraping.
        """
        #***> Initialize database manager if enabled <***
        self.database_manager = None
        if self.config.save_to_database:
            self._initialize_database_manager()

    def _initialize_database_manager(self) -> None:
        """
        Initialize database manager with error handling.
        """
        try:
            environment = self._get_environment_setting()
            self.database_manager = DatabaseManager(environment)
        except DatabaseOperationError as error:
            self._handle_database_initialization_error(error)

    def scrape_all_pages(self, driver, entry_url: str) -> pd.DataFrame:
        """
        Scrape all pages of competition data with VPN enforcement.
        Maintains the exact original logic flow.
        
        Args:
            driver: Selenium WebDriver instance
            entry_url: Starting URL for scraping
            
        Returns:
            DataFrame with all scraped competition data
            
        Raises:
            VpnRequiredError: If VPN protection fails
        """
        start_time = time.time()

        #***> Initialize parser and ensure VPN protection <***
        self.parser = CompetitionTableParser(entry_url)
        self.vpn_handler.ensure_vpn_protection()
        
        #***> Navigate to starting URL with VPN timing <***
        driver.get(entry_url)
        self.vpn_handler.handle_request_timing("starting to navigate ...")
        
        #***> Initialize collection variables <***
        all_data = []
        page_num = 1
        total_competitions = 0
        
        #***> Main scraping loop preserving original logic <***
        while page_num <= self.config.max_pages:
            
            try:
                #***> Process current page <***
                page_data = self._scrape_single_page(driver, page_num)
                
                if not page_data.empty:
                    #***> Add metadata preserving original logic <***
                    page_data['page_number'] = page_num
                    page_data['scraped_at'] = pd.Timestamp.now()
                    
                    all_data.append(page_data)
                    total_competitions += len(page_data)
                
                #***> Navigate to next page <***
                if not self.navigator.navigate_to_next_page(
                    driver, self.vpn_handler
                ):
                    break
                
                page_num += 1
                
            except VpnRequiredError:
                raise
            except Exception as error:
                if "timeout" in str(error).lower():
                    break
                continue
        
        #***> Combine all data preserving original logic <***
        final_df = (pd.concat(all_data, ignore_index=True) 
                   if all_data else pd.DataFrame())
        
        #***> Save to database if configured <***
        self._handle_database_save(final_df)
        
        return final_df

    def _scrape_single_page(self, driver, page_num: int) -> pd.DataFrame:
        """
        Scrape data from a single page.
        
        Args:
            driver: Selenium WebDriver instance
            page_num: Current page number
            
        Returns:
            DataFrame with page data
        """
        try:
            #***> Parse HTML content <***
            soup = BeautifulSoup(driver.page_source, "html.parser")
            page_data = self.parser.parse_competition_table(soup)
            
            return page_data
            
        except Exception:
            return pd.DataFrame()

    def _handle_database_save(self, final_df: pd.DataFrame) -> bool:
        """
        Handle database save operation.
        
        Args:
            final_df: Final scraped data
            
        Returns:
            True if saved to database successfully
        """
        if final_df.empty or not self.database_manager:
            return False
        
        try:
            return self.database_manager.save_competitions(final_df)
        except DatabaseOperationError:
            return False

    def save_to_csv(
            self, 
            data: pd.DataFrame, 
            filename: Optional[str] = None
        ) -> str:
        """
        Save scraped data to CSV file.
        
        Args:
            data: DataFrame with competition data
            filename: Optional custom filename
            
        Returns:
            Path to saved CSV file
        """
        return self.file_manager.save_to_csv(data, filename)

    def cleanup(self) -> None:
        """
        Clean up all resources including database and VPN connections.
        """
        try:
            #***> Cleanup database service <***
            if self.database_manager:
                self.database_manager.cleanup()
            
            #***> Cleanup VPN connections <***
            self.vpn_handler.cleanup()
            
        except Exception:
            pass

    @property
    def database_available(self) -> bool:
        """
        Check if database is available.
        
        Returns:
            True if database is available
        """
        return (self.database_manager is not None and
                self.database_manager.is_available)