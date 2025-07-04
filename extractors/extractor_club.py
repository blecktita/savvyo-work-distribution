# core/scrapers/extractors/club_extractor.py
"""
Club-specific data extraction utilities.
Handles extraction of club information and club row data.
"""

from typing import Optional, Dict, Any, List
from bs4 import Tag, BeautifulSoup

from .base_extractor import BaseDataExtractor
from .extraction_config import ColumnMapping
from .extraction_url_utils import URLParser
from exceptions import ParsingError


class ClubDataExtractor(BaseDataExtractor):
    """
    Extractor for club-specific data from HTML elements.
    
    Handles extraction of club names, URLs, codes, and IDs
    from table cells containing club information.
    """
    
    def __init__(self):
        """
        Initialize club extractor with URL parser
        """
        super().__init__()
        self.url_parser = URLParser()
    
    def extract_club_info(
        self, 
        cell: Tag, 
        season_year: str = ""
    ) -> Optional[Dict[str, str]]:
        """
        Extract club name, URL, ID, and code from table cell.
        
        Args:
            cell: BeautifulSoup Tag containing club information
            season_year: Season year to use in URL (e.g., "2023")
            
        Returns:
            Dictionary with club info or None if extraction fails
        """
        if not isinstance(cell, Tag):
            return None
        
        try:
            # Find the club link
            club_link = cell.find('a', href=True)
            if not club_link or self.config.CLUB_URL_PATTERN not in str(
                club_link.get('href', '')
            ):
                return None
            
            name = club_link.get_text(strip=True)
            url = club_link.get('href')
            
            if not name or not url:
                return None
            
            # Ensure string types and make URL absolute
            name = str(name)
            url = str(url)
            absolute_url = self.make_absolute_url(url)
            
            # Extract club components from URL
            club_id = self.url_parser.extract_club_id_from_url(absolute_url)
            club_code = self.url_parser.extract_club_code_from_url(absolute_url)
            
            # Fix season in URL if provided
            if season_year:
                absolute_url = self.url_parser.fix_season_in_url(
                    absolute_url, str(season_year)
                )
            
            return {
                'name': name,
                'url': absolute_url,
                'id': str(club_id),
                'code': club_code
            }
            
        except Exception as error:
            raise ParsingError(
                self.config.ERROR_MESSAGES['club_extraction'].format(error)
            )
    
    def extract_season_options(self, soup: BeautifulSoup) -> List[Dict[str, str]]:
        """
        Extract season options from season select dropdown.
        
        Args:
            soup: BeautifulSoup object containing the HTML
            
        Returns:
            List of season dictionaries with 'year' and 'season_id' keys
        """
        if not isinstance(soup, BeautifulSoup):
            return []
        
        try:
            seasons = []
            
            # Find the season select dropdown
            season_select = soup.find('select', {'name': 'saison_id'})
            if not season_select:
                return seasons
            
            # Extract all option elements
            options = season_select.find_all('option')
            
            for option in options:
                if not isinstance(option, Tag):
                    continue
                
                year = str(option.get('value', '')).strip()
                season_id = str(option.get_text(strip=True))
                
                if year and season_id:
                    seasons.append({
                        'year': year,
                        'season_id': season_id
                    })
            
            return seasons
            
        except Exception as error:
            raise ParsingError(
                self.config.ERROR_MESSAGES['season_extraction'].format(error)
            )


class ClubRowExtractor(BaseDataExtractor):
    """
    Extractor for complete club data from table rows.
    
    Handles extraction of club information along with associated
    statistics like squad size, market values, etc.
    """
    
    def __init__(self):
        """
        Initialize the club row extractor
        """
        super().__init__()
        self.club_extractor = ClubDataExtractor()
        self.column_mapping = ColumnMapping()
    
    def extract_club_from_row(
        self,
        cells: List[Tag],
        season_year: str,
        season_id: str,
        competition_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Extract club data from table row cells.
        
        Args:
            cells: List of table cell Tags
            season_year: Season year (e.g., "2024")
            season_id: Season display ID (e.g., "24/25")
            competition_id: Competition identifier
            
        Returns:
            Dictionary with club data or None if extraction fails
        """
        if not isinstance(cells, list) or not all(
            isinstance(cell, Tag) for cell in cells
        ):
            return None
        
        # Validate minimum cell count
        self.validate_cell_count(
            cells, 
            self.config.COLUMN_REQUIREMENTS['club_minimum'], 
            "club row"
        )
        
        # Ensure string parameters
        season_year = str(season_year) if season_year else ""
        season_id = str(season_id) if season_id else ""
        competition_id = str(competition_id) if competition_id else ""
        
        try:
            # Extract club info from first available cells
            club_info = self._find_club_info_in_cells(cells, season_year)
            if not club_info:
                return None
            
            # Extract additional data using column mapping
            mapping = self.column_mapping.get_club_mapping()
            extracted_data = self._extract_club_statistics(cells, mapping)
            
            # Combine all data
            return {
                'club_id': str(club_info['id']),
                'club_name': str(club_info['name']),
                'club_code': str(club_info['code']),
                'club_url': str(club_info['url']),
                'season_year': season_year,
                'season_id': season_id,
                'competition_id': competition_id,
                **extracted_data
            }
            
        except Exception as error:
            raise ParsingError(
                self.config.ERROR_MESSAGES['row_extraction'].format(error)
            )
    
    def _find_club_info_in_cells(
        self, 
        cells: List[Tag], 
        season_year: str
    ) -> Optional[Dict[str, str]]:
        """
        Find club information in the first available cells.
        
        Args:
            cells: List of table cells
            season_year: Season year for URL fixing
            
        Returns:
            Club information dictionary or None if not found
        """
        # Club info is usually in the first two cells
        for i in range(min(2, len(cells))):
            club_info = self.club_extractor.extract_club_info(
                cells[i], season_year
            )
            if club_info:
                return club_info
        
        return None
    
    def _extract_club_statistics(
        self, 
        cells: List[Tag], 
        mapping: Dict[str, int]
    ) -> Dict[str, Any]:
        """
        Extract club statistics from cells using column mapping.
        
        Args:
            cells: List of table cells
            mapping: Column mapping configuration
            
        Returns:
            Dictionary with extracted statistics
        """
        # Extract basic statistics
        squad_size = self.extract_number(cells[mapping['squad_size']])
        average_age = self.extract_float(cells[mapping['average_age']])
        foreign_players = self.extract_number(cells[mapping['foreign_players']])
        
        # Handle optional market value columns
        avg_market_value = 0.0
        total_market_value = 0.0
        
        # Extract average market value if column exists
        if (len(cells) > mapping['avg_market_value'] and 
            mapping['avg_market_value'] < len(cells)):
            avg_market_value = self.extract_market_value(
                cells[mapping['avg_market_value']]
            )
        
        # Extract total market value if column exists
        if (len(cells) > mapping['total_market_value'] and 
            mapping['total_market_value'] < len(cells)):
            total_market_value = self.extract_market_value(
                cells[mapping['total_market_value']]
            )
        
        return {
            'squad_size': int(squad_size),
            'average_age_of_players': float(average_age),
            'number_of_foreign_players': int(foreign_players),
            'average_market_value': float(avg_market_value),
            'total_market_value': float(total_market_value)
        }