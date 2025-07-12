# database/database_models.py
"""
Database models for competitions and teams
"""
from typing import Dict, Any
from enum import Enum

from sqlalchemy import (
    Column, String, Integer, Float, DateTime, Boolean, Text, ForeignKey,
    PrimaryKeyConstraint, Index
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func


# Create declarative base
Base = declarative_base()

class Competition(Base):
    """
    Competition model with relevant metadata.

    Note: competition_id, competition_code, and competition_name are derived from
    competition_url using URL parsing utilities.
    """
    __tablename__ = 'competitions'

    # Primary key and identifiers (derived from URL)
    competition_id = Column(
        String(50), primary_key=True, nullable=False,
        doc="Unique identifier extracted from competition URL"
    )
    competition_code = Column(
        String(255), nullable=False,
        doc="URL-friendly code extracted from competition URL"
    )
    competition_name = Column(
        String(255), nullable=False,
        doc="Human-readable name derived from competition code"
    )
    competition_url = Column(
        Text, nullable=False, unique=True,
        doc="Source URL from which other fields are derived"
    )
    competition_type = Column(
        String(100), nullable=True,
        doc="Type/category of the competition"
    )

    # Geographic and tier information
    country = Column(
        String(100), nullable=True,
        doc="Country where the competition is held"
    )
    tier = Column(
        String(100), nullable=True,
        doc="Competition tier/level (e.g., 'First Tier', 'Second Tier')"
    )

    # Statistical data
    number_of_clubs = Column(
        Integer, default=0, nullable=False,
        doc="Total number of clubs participating"
    )
    number_of_players = Column(
        Integer, default=0, nullable=False,
        doc="Total number of players across all clubs"
    )
    average_age_of_players: Column[float] = Column(
        Float, default=0.0, nullable=False,
        doc="Average age of all players in competition"
    )
    percentage_of_foreign_players: Column[int] = Column(
        Integer, default=0, nullable=False,
        doc="Percentage of foreign players (0-100)"
    )
    percentage_game_ratio_of_foreign_players: Column[float] = Column(
        Float, default=0.0, nullable=False,
        doc="Game time ratio for foreign players"
    )
    goals_per_match: Column[float] = Column(
        Float, default=0.0, nullable=False,
        doc="Average goals scored per match"
    )

    # Market value data
    average_market_value: Column[float] = Column(
        Float, default=0.0, nullable=False,
        doc="Average market value per player"
    )
    total_market_value: Column[float] = Column(
        Float, default=0.0, nullable=False,
        doc="Total market value of all players"
    )

    # Metadata
    created_at = Column(
        DateTime, default=func.now(), nullable=False,
        doc="Timestamp when record was created"
    )
    updated_at = Column(
        DateTime, default=func.now(), onupdate=func.now(), nullable=False,
        doc="Timestamp when record was last updated"
    )
    is_active = Column(
        Boolean, default=True, nullable=False,
        doc="Whether the competition is currently active"
    )

    # Relationships
    teams = relationship("Team", back_populates="competition", lazy="dynamic")

    def __repr__(self) -> str:
        """String representation of Competition object."""
        return (
            f"<Competition(id='{self.competition_id}', "
            f"name='{self.competition_name}', country='{self.country}')>"
        )

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert Competition object to dictionary.

        Returns:
            Dict[str, Any]: Dictionary representation of the competition
        """
        return {
            'competition_id': self.competition_id,
            'competition_code': self.competition_code,
            'competition_name': self.competition_name,
            'competition_url': self.competition_url,
            'competition_type': self.competition_type,
            'country': self.country,
            'tier': self.tier,
            'number_of_clubs': self.number_of_clubs,
            'number_of_players': self.number_of_players,
            'average_age_of_players': self.average_age_of_players,
            'percentage_of_foreign_players': self.percentage_of_foreign_players,
            'percentage_game_ratio_of_foreign_players':
                self.percentage_game_ratio_of_foreign_players,
            'goals_per_match': self.goals_per_match,
            'average_market_value': self.average_market_value,
            'total_market_value': self.total_market_value,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
            'is_active': self.is_active
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Competition':
        """
        Create Competition object from dictionary.

        Args:
            data (Dict[str, Any]): Dictionary containing competition data

        Returns:
            Competition: New Competition instance
        """
        return cls(**data)

class Team(Base):
    """
    Team/Club model representing individual teams within competitions.

    Each team belongs to a specific competition and season, containing
    squad and market value information.
    """
    __tablename__ = 'teams'

    # Primary key and identifiers
    club_id = Column(
        String(50), nullable=False,
        doc="Unique identifier for the club/team"
    )
    club_name = Column(
        String(255), nullable=False,
        doc="Official name of the club/team"
    )
    club_code = Column(
        String(255), nullable=False,
        doc="URL-friendly code extracted from club URL (e.g., 'manchester-city')"
    )
    club_url = Column(
        Text, nullable=False,
        doc="Source URL for the club/team page"
    )
    season_id = Column(
        String(50), nullable=False,
        doc="Season identifier (e.g., '2023-24', '2024-25')"
    )
    season_year = Column(
        String(50), nullable=False,
        doc="Season year for URL construction (e.g., '2023', '2024')"
    )

    # Foreign key relationship to Competition
    competition_id = Column(
        String(50), ForeignKey('competitions.competition_id'), nullable=False,
        doc="Reference to the competition this team participates in"
    )

    # Squad information
    squad_size = Column(
        Integer, default=0, nullable=False,
        doc="Total number of players in the squad"
    )
    average_age_of_players: Column[float] = Column(
        Float, default=0.0, nullable=False,
        doc="Average age of all players in the team"
    )
    number_of_foreign_players = Column(
        Integer, default=0, nullable=False,
        doc="Total number of foreign players in the squad"
    )

    # Market value data
    average_market_value: Column[float] = Column(
        Float, default=0.0, nullable=False,
        doc="Average market value per player in the team"
    )
    total_market_value: Column[float] = Column(
        Float, default=0.0, nullable=False,
        doc="Total market value of all players in the team"
    )

    # Metadata
    created_at = Column(
        DateTime, default=func.now(), nullable=False,
        doc="Timestamp when record was created"
    )
    updated_at = Column(
        DateTime, default=func.now(), onupdate=func.now(), nullable=False,
        doc="Timestamp when record was last updated"
    )
    is_active = Column(
        Boolean, default=True, nullable=False,
        doc="Whether the team is currently active"
    )

    __table_args__ = (
        PrimaryKeyConstraint('club_id', 'season_year', 'competition_id'),
    )

    # Relationships
    competition = relationship("Competition", back_populates="teams")

    def __repr__(self) -> str:
        """String representation of Team object."""
        return (
            f"<Team(id='{self.club_id}', name='{self.club_name}', "
            f"code='{self.club_code}', competition='{self.competition_id}', "
            f"season='{self.season_id}')>"
        )

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert Team object to dictionary.

        Returns:
            Dict[str, Any]: Dictionary representation of the team
        """
        return {
            'club_id': self.club_id,
            'club_name': self.club_name,
            'club_code': self.club_code,
            'club_url': self.club_url,
            'season_id': self.season_id,
            'season_year': self.season_year,
            'competition_id': self.competition_id,
            'squad_size': self.squad_size,
            'average_age_of_players': self.average_age_of_players,
            'number_of_foreign_players': self.number_of_foreign_players,
            'average_market_value': self.average_market_value,
            'total_market_value': self.total_market_value,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
            'is_active': self.is_active
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Team':
        """
        Create Team object from dictionary.

        Args:
            data (Dict[str, Any]): Dictionary containing team data

        Returns:
            Team: New Team instance
        """
        return cls(**data)

    @property
    def foreign_players_percentage(self) -> float:
        """
        Calculate percentage of foreign players in the squad.

        Returns:
            float: Percentage of foreign players (0.0-100.0)
        """
        squad_size = object.__getattribute__(self, 'squad_size')
        num_foreign = object.__getattribute__(self, 'number_of_foreign_players')
        if not squad_size:
            return 0.0
        return float(num_foreign) / float(squad_size) * 100.0

class TaskStatus(Enum):
    """
    Enum representing various task statuses for progress tracking.
    """
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    SEASONS_DISCOVERED = "seasons_discovered"

class CompetitionProgress(Base):
    """
    Competition progress tracking - matches distributed_progress_tracker.py tables.
    This model corresponds to the 'competition_progress' table created by your tracker.
    """
    __tablename__ = 'competition_progress'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    competition_id = Column(String(100), unique=True, nullable=False)
    competition_url = Column(Text, nullable=False)
    status = Column(String(50), nullable=False)
    seasons_discovered = Column(Integer, default=0)
    worker_id = Column(String(100), nullable=True)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    error_message = Column(Text, nullable=True)
    retry_count = Column(Integer, default=0)
    
    def __repr__(self) -> str:
        """
        String representation of CompetitionProgress object.
        """
        return (
            f"<CompetitionProgress(id='{self.competition_id}', "
            f"status='{self.status}', worker='{self.worker_id}')>"
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert CompetitionProgress object to dictionary.
        """
        return {
            'id': self.id,
            'competition_id': self.competition_id,
            'competition_url': self.competition_url,
            'status': self.status,
            'seasons_discovered': self.seasons_discovered,
            'worker_id': self.worker_id,
            'started_at': self.started_at,
            'completed_at': self.completed_at,
            'error_message': self.error_message,
            'retry_count': self.retry_count
        }

class SeasonProgress(Base):
    """
    Season progress tracking - matches distributed_progress_tracker.py tables.
    This model corresponds to the 'season_progress' table created by your tracker.
    """
    __tablename__ = 'season_progress'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    competition_id = Column(String(100), nullable=False)
    season_id = Column(String(100), nullable=False)
    season_year = Column(String(100), nullable=False)
    status = Column(String(50), nullable=False)
    worker_id = Column(String(100), nullable=True)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    error_message = Column(Text, nullable=True)
    retry_count = Column(Integer, default=0)
    clubs_saved = Column(Integer, default=0)
    
    __table_args__ = (
        #***> Unique constraint matching your tracker's SQL <***
        Index('unique_competition_season', 'competition_id', 'season_id', unique=True),
    )
    
    def __repr__(self) -> str:
        """
        String representation of SeasonProgress object.
        """
        return (
            f"<SeasonProgress(competition='{self.competition_id}', "
            f"season='{self.season_id}', status='{self.status}', "
            f"worker='{self.worker_id}')>"
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert SeasonProgress object to dictionary.
        """
        return {
            'id': self.id,
            'competition_id': self.competition_id,
            'season_id': self.season_id,
            'season_year': self.season_year,
            'status': self.status,
            'worker_id': self.worker_id,
            'started_at': self.started_at,
            'completed_at': self.completed_at,
            'error_message': self.error_message,
            'retry_count': self.retry_count,
            'clubs_saved': self.clubs_saved
        }