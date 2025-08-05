# database/schemas/__init__.py

# Import Competition and Team FIRST to establish the relationship
from .competition_schema import Competition

# THEN import match_schema models
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

# Finally import progress schema
from .progress_schema import CompetitionProgress, SeasonProgress, TaskStatus
from .team_schema import Team

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
