from .base_parser import BaseParser
from .club_parser import ClubTableParser
from .competition_parser import CompetitionTableParser
from .html_parser import HTMLParser
from .parser_config import ParserConfig

__all__ = [
    "ParserConfig",
    "BaseParser",
    "CompetitionTableParser",
    "ClubTableParser",
    "HTMLParser",
]
