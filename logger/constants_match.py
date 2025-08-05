from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class MatchContextual:
    """
    Contextual match info
    """

    match_id: str
    home_team: Dict[str, Any]
    away_team: Dict[str, Any]
    final_score: Dict[str, Any]
    match_report_url: str
    date: Optional[str] = None
    time: Optional[str] = None
    day_of_week: Optional[str] = None
    referee: Optional[Dict[str, str]] = None
    attendance: Optional[int] = None
    community_predictions: Optional[Dict[str, float]] = None
    match_events: Optional[List[Dict[str, Any]]] = None


@dataclass
class MatchdayContainer:
    """
    Complete matchday context
    """

    matchday_info: Dict[str, Any]
    league_table: Optional[Dict[str, Any]] = None
    top_scorers: Optional[List[Dict[str, Any]]] = None
    matchday_summary: Optional[Dict[str, Any]] = None
    matches: Optional[List[MatchContextual]] = None
    metadata: Optional[List[Dict[str, Any]]] = None


class MatchItems:
    # HTML Elements
    BRANCH = "table"
    CONTAINER = "box"
    DIV = "div"
    TREE = "tr"
    TREE_DATE = "td"
    TREE_DATE_SPAN = ("td", {"colspan": "5"})
    LINK = "a"
    SPAN = "span"
    CELL = "td"
    IMAGE = "img"

    # Classes / selectors
    TABLE = "table-grosse-schrift"
    FOOTER_CLASS = "footer"
    PRED_ROW_CLASS = "tm-user-tendenz"
    HOME_BAR_CLASS = "bar-sieg"
    AWAY_BAR_CLASS = "bar-niederlage"
    EVENT_ROW_CLASS = "spieltagsansicht-aktionen"
    HIDE_FOR_SMALL_CLASS = "hide-for-small"
    SHOW_FOR_SMALL_CLASS = "show-for-small"

    # HREF substrings
    DATE_HREF_KEYWORD = "waspassiertheute"

    # Text markers / CSS classes
    REFEREE_LABEL = "Referee:"
    ATTENDANCE_ICON_CLASS = "icon-zuschauer-zahl"
    GOAL_ICON = "icon-tor-formation"
    YELLOW_CARD_CLASS = "sb-gelb"
    RED_CARD_CLASS = "sb-rot"

    # Attribute keys
    TITLE_ATTR = "title"
    HREF_ATTR = "href"
    SRC_ATTR = "src"
    CLASS_ATTR = "class"

    # CSS classes & compound selectors
    POSITION_SPAN = ("span", {"class": "tabellenplatz"})

    # Regex patterns
    POSITION_REGEX = r"\((\d+)\.\)"
    MINUTE_REGEX = r"\d+"
    TIME_REGEX = r"(\d{1,2}:\d{2})\s*(AM|PM)?"
    ATTENDANCE_REGEX = r"([\d.,]+)"
    MATCH_ID_REGEX = r"/spielbericht/(\d+)"
    PERCENT_REGEX = r"([\d.]+)\s*%"

    # Cell indexes
    HOME_IDX = 0
    SCORE_IDX = 4
    AWAY_IDX = 7
    MIN_CELL_COUNT = 9

    # Event cell indexes
    EVENT_LEFT_TEAM_IDX = 0
    EVENT_MINUTE_HOME_IDX = 1
    EVENT_SCORE_IDX = 2
    EVENT_MINUTE_AWAY_IDX = 3
    EVENT_RIGHT_TEAM_IDX = 4
    MIN_EVENT_CELLS = 5

    # URL/text markers
    REPORT_KEYWORD = "spielbericht"

    # Titles for prediction spans
    HOME_TITLE = "Win for"
    DRAW_TITLE = "Draws:"

    # Event types
    GOAL_TYPE = "goal"
    YELLOW_CARD_TYPE = "yellow_card"
    RED_CARD_TYPE = "red_card"

    # Team types
    HOME_TEAM = "home"
    AWAY_TEAM = "away"

    # Text constants
    SCORE_SEPARATOR = ":"
    PM_INDICATOR = "PM"
    NOON_HOUR = "12"
    EMPTY_STRING = ""

    # Days of the week
    DAYS_OF_WEEK = [
        "Sunday",
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
    ]

    # Time constants
    HOURS_IN_HALF_DAY = 12

    # Date format patterns
    INPUT_DATE_FORMAT = "%B %d, %Y"
    OUTPUT_DATE_FORMAT = "%Y-%m-%d"

    # Default values
    DEFAULT_PERCENTAGE = 0.0

    # Prediction keys
    HOME_WIN_PERCENTAGE = "home_win_percentage"
    DRAW_PERCENTAGE = "draw_percentage"
    AWAY_WIN_PERCENTAGE = "away_win_percentage"

    # League table constants
    CONTENT_BOX_HEADLINE = "content-box-headline"
    TBODY = "tbody"
    H2 = "h2"

    # League table text markers
    TABLE_TEXT = "table"
    PREMIER_LEAGUE_TEXT = "premier league"
    GOALSCORER_TEXT = "goalscorer"
    SUMMARY_TEXT = "summary"
    FC_SUFFIX = " FC"
    FC_PREFIX = "FC "
    DASH = "-"

    # League table regex patterns
    SEASON_REGEX = r"(\d{2}/\d{2})"
    POSITION_NUMBER_REGEX = r"(\d+)"

    # Movement indicators
    GREEN_ARROW = "green-arrow-ten"
    RED_ARROW = "red-arrow-ten"
    GREY_BLOCK = "grey-block-ten"

    # Movement types
    MOVEMENT_UP = "up"
    MOVEMENT_DOWN = "down"
    MOVEMENT_SAME = "same"

    # League table cell indexes
    POSITION_CELL_IDX = 0
    LOGO_CELL_IDX = 1
    TEAM_CELL_IDX = 2
    MATCHES_CELL_IDX = 3
    GOAL_DIFF_CELL_IDX = 4
    POINTS_CELL_IDX = 5
    MIN_TABLE_CELLS = 6

    # Top scorers cell indexes
    PLAYER_CELL_IDX = 0
    CLUB_CELL_IDX = 1
    MATCHDAY_GOALS_CELL_IDX = 2
    TOTAL_GOALS_CELL_IDX = 3
    MIN_SCORER_CELLS = 4

    # Summary cell indexes for current matchday
    SUMMARY_MATCHES_IDX = 0
    SUMMARY_GOALS_IDX = 1
    SUMMARY_OWN_GOALS_IDX = 2
    SUMMARY_YELLOW_CARDS_IDX = 3
    SUMMARY_SECOND_YELLOW_IDX = 4
    SUMMARY_RED_CARDS_IDX = 5
    SUMMARY_TOTAL_ATTENDANCE_IDX = 7
    SUMMARY_AVG_ATTENDANCE_IDX = 8
    SUMMARY_SOLD_OUT_IDX = 9
    MIN_SUMMARY_CELLS = 9

    # Default numeric values
    DEFAULT_ZERO = 0

    # Text replacements
    DOT_SEPARATOR = "."
    COMMA_SEPARATOR = ","


class JobStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"


class JobType(Enum):
    MATCHDAY_SCRAPE = "matchday_scrape"
    MATCH_DETAIL_SCRAPE = "match_detail_scrape"


@dataclass
class Config:
    """System configuration"""

    # Database
    database_url: str = "postgresql://scraper:password@localhost:5433/football_scraper"
    redis_url: str = "redis://localhost:6379/0"

    # Storage
    base_output_dir: str = "data_savvyo"

    # Processing
    max_workers: int = 2  # Conservative for testing
    delay_between_requests: float = 18.0  # Respectful delays
    max_retries: int = 2

    # Logging
    log_level: str = "INFO"


@dataclass
class Player:
    player_id: str
    name: str
    shirt_number: Optional[int] = None
    position: Optional[str] = None
    is_captain: bool = False
    portrait_url: Optional[str] = None


@dataclass
class Team:
    team_id: str
    name: str
    short_name: Optional[str] = None
    logo_url: Optional[str] = None
    league_position: Optional[int] = None
    formation: Optional[str] = None
    manager: Optional[str] = None


@dataclass
class Goal:
    minute: int
    extra_time: Optional[int] = None
    player: Optional[Player] = None
    assist_player: Optional[Player] = None
    goal_type: Optional[str] = None
    assist_type: Optional[str] = None
    team_side: str = ""
    score_after: Optional[Tuple[int, int]] = None
    season_goal_number: Optional[int] = None
    season_assist_number: Optional[int] = None


@dataclass
class Card:
    minute: int
    extra_time: Optional[int] = None
    player: Optional[Player] = None
    card_type: str = ""
    reason: Optional[str] = None
    team_side: str = ""
    season_card_number: Optional[int] = None


@dataclass
class Substitution:
    minute: int
    extra_time: Optional[int] = None
    player_out: Optional[Player] = None
    player_in: Optional[Player] = None
    reason: Optional[str] = None
    team_side: str = ""


@dataclass
class MatchInfo:
    match_id: str
    competition_name: str
    competition_id: Optional[str] = None
    competition_logo: Optional[str] = None
    matchday: Optional[int] = None
    season: Optional[str] = None
    date: Optional[str] = None
    time: Optional[str] = None
    venue: Optional[str] = None
    attendance: Optional[int] = None
    referee: Optional[str] = None
    referee_id: Optional[str] = None


@dataclass
class Score:
    home_final: int
    away_final: int
    home_ht: Optional[int] = None
    away_ht: Optional[int] = None


@dataclass
class MatchDetail:
    """Detailed match data from notebook snippet 2"""

    match_info: MatchInfo
    home_team: Team
    away_team: Team
    score: Score
    home_lineup: List[Player]
    away_lineup: List[Player]
    home_substitutes: List[Player]
    away_substitutes: List[Player]
    goals: List[Goal]
    cards: List[Card]
    substitutions: List[Substitution]
    extraction_metadata: Dict[str, Any]

    # ----------------------------------------------
    # Delegate attribute lookups to match_info
    # ----------------------------------------------
    def __getattr__(self, name: str) -> Any:
        """If an attribute isn’t found on MatchDetail, try match_info."""
        if hasattr(self.match_info, name):
            return getattr(self.match_info, name)
        raise AttributeError(
            "{cls!r} has no attribute {attr!r}".format(
                cls=self.__class__.__name__, attr=name
            )
        )

    @property
    def matchday_number(self) -> Any:
        # alias the ordinal matchday into the “_number” name
        return getattr(self.match_info, "matchday", None)
