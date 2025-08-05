# database/schemas/__init__.py

# Import Competition and Team FIRST to establish the relationship
from .competition_schema import Competition
from .team_schema import Team  # Move this right after Competition

from .match_schema import (
    Card,
    CommunityPrediction,
    Goal,
    LeagueTableEntry,
    Lineup,
    Match,
)
from .match_schema import MatchCompetition as m_competition
from .match_schema import (
    Matchday,
    MatchdaySummary,
    MatchTeam,
    Player,
    Referee,
    Substitution,
)
from .match_schema import TeamInMatch as m_team
from .match_schema import TeamSide, TopScorer

from .progress_schema import CompetitionProgress, SeasonProgress, TaskStatus

__all__ = [
    "Competition",
    "Team",
    "TaskStatus",
    "CompetitionProgress",
    "SeasonProgress",
    "Match",
    "Matchday",
    "MatchdaySummary",
    "Referee",
    "Player",
    "Card",
    "Goal",
    "Substitution",
    "MatchTeam",
    "CommunityPrediction",
    "TopScorer",
    "Lineup",
    "LeagueTableEntry",
    "m_competition",
    "m_team",
    "TeamSide",
]