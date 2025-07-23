# extractors/parsers/club_parser.py
"""
Club table parser for extracting club data from HTML tables.
Specialized parser focusing on club-specific parsing logic and table finding.
"""

from typing import Optional, List, Dict
import pandas as pd
from bs4 import BeautifulSoup, Tag

from .base_parser import BaseParser
from extractors.extractor_club import ClubRowExtractor, ClubDataExtractor


class ClubTableParser(BaseParser):
    """
    HTML parser specialized for extracting club data from tables.
    Inherits common functionality from BaseParser and adds club-specific logic.
    """
    
    def __init__(self):
        """
        Initialize club parser with extractors for club data and season options.
        """
        super().__init__()
        self.row_extractor = ClubRowExtractor()
        self.club_extractor = ClubDataExtractor()
    
    def parse_club_table(
        self, 
        soup: BeautifulSoup, 
        season_year: str,
        season_id: str,
        competition_id: str
    ) -> pd.DataFrame:
        """
        Parse the club table from HTML soup.
        
        Args:
            soup: BeautifulSoup object containing the HTML
            season_year: Season year (e.g., "2024")
            season_id: Season display ID (e.g., "24/25") 
            competition_id: Competition identifier
            
        Returns:
            DataFrame with extracted club data
            
        Raises:
            ParsingError: If parsing fails critically
        """
        try:
            table = self._find_club_table(soup)
            
            if not table:
                return pd.DataFrame()
            
            rows = self._get_table_rows_from_table(table)
            data = self._process_all_club_rows(
                rows, season_year, season_id, competition_id
            )
            
            return pd.DataFrame(data) if data else pd.DataFrame()
            
        except Exception as e:
            print(f"   ❌ Parser exception for season {season_id}: {str(e)}")
            return pd.DataFrame()
    
    def parse_season_options(self, soup: BeautifulSoup) -> List[Dict[str, str]]:
        """
        Parse season options from the page.
        
        Args:
            soup: BeautifulSoup object containing the HTML
            
        Returns:
            List of season dictionaries
        """
        try:
            return self.club_extractor.extract_season_options(soup)
        except Exception:
            return []
    
    def _find_club_table(self, soup: BeautifulSoup) -> Optional[Tag]:
        """
        Find the main table containing club data using multiple search strategies.
        
        Args:
            soup: BeautifulSoup object
            
        Returns:
            Table element or None if not found
        """
        # Try each table selector pattern from config
        for selector in self.config.CLUB_TABLE_SELECTORS:
            if selector["tag"] == "table":
                table = soup.find(selector["tag"], class_=selector["class"])
                if table:
                    return table
            else:
                # For div containers, find table inside
                container = soup.find(selector["tag"], class_=selector["class"])
                if container:
                    table = container.find('table')
                    if table:
                        return table
        
        return None
    
    def _get_table_rows_from_table(self, table: Tag) -> List[Tag]:
        """
        Extract table rows from a table element, checking tbody first.
        
        Args:
            table: Table element to extract rows from
            
        Returns:
            List of table row elements
        """
        # Find tbody or use table directly
        tbody = table.find('tbody') or table
        return tbody.find_all('tr')
    
    def _process_all_club_rows(
        self, 
        rows: List[Tag],
        season_year: str,
        season_id: str,
        competition_id: str
    ) -> List[Dict]:
        """
        Process all club table rows and extract club data.
        
        Args:
            rows: List of BeautifulSoup row elements
            season_year: Season year
            season_id: Season display ID
            competition_id: Competition identifier
            
        Returns:
            List of club data dictionaries
        """
        data = []
        
        for row in rows:
            if self._should_skip_club_row(row):
                continue
            
            club_data = self._process_club_row(
                row, season_year, season_id, competition_id
            )
            if club_data:
                data.append(club_data)
        
        return data
    
    def _should_skip_club_row(self, row: Tag) -> bool:
        """
        Check if club row should be skipped (header/footer rows).
        
        Args:
            row: BeautifulSoup row element
            
        Returns:
            True if row should be skipped
        """
        return (self._should_skip_header_row(row) or 
                self._should_skip_footer_row(row))
    
    def _process_club_row(
        self, 
        row: Tag,
        season_year: str,
        season_id: str, 
        competition_id: str
    ) -> Optional[Dict]:
        """
        Process a club data row and extract club information.
        
        Args:
            row: BeautifulSoup row element
            season_year: Season year
            season_id: Season display ID
            competition_id: Competition identifier
            
        Returns:
            Club data dictionary or None if processing fails
        """
        cells = self._get_table_cells(row)
        
        # Need at least minimum columns for club data
        if not self._has_minimum_columns(
            cells, self.config.CLUB_TABLE_MIN_COLUMNS
        ):
            return None
        
        try:
            return self.row_extractor.extract_club_from_row(
                cells, season_year, season_id, competition_id
            )
        except Exception:
            return None