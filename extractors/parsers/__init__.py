from .parser_config import ParserConfig
from .base_parser import BaseParser
from .competition_parser import CompetitionTableParser
from .club_parser import ClubTableParser
from .html_parser import HTMLParser

__all__ = [
    'ParserConfig',
    'BaseParser',
    'CompetitionTableParser',
    'ClubTableParser',
    'HTMLParser'
    ]