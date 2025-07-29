# database/schemas/competition_schema.py
"""
Database models for competitions and teams
"""
from typing import Dict, Any

from sqlalchemy import (
    Column, String, Integer, Float, DateTime, Boolean, Text
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