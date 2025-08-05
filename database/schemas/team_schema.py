# database/schemas/team_schema.py
"""
Database models for competitions and teams
"""

from typing import Any, Dict

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    PrimaryKeyConstraint,
    String,
    Text,
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from database.base import Base


class Team(Base):
    """
    Team/Club model representing individual teams within competitions.

    Each team belongs to a specific competition and season, containing
    squad and market value information.
    """

    __tablename__ = "teams"

    # Primary key and identifiers
    club_id = Column(
        String(50), nullable=False, doc="Unique identifier for the club/team"
    )
    club_name = Column(
        String(255), nullable=False, doc="Official name of the club/team"
    )
    club_code = Column(
        String(255),
        nullable=False,
        doc="URL-friendly code extracted from club URL (e.g., 'manchester-city')",
    )
    club_url = Column(Text, nullable=False, doc="Source URL for the club/team page")
    season_id = Column(
        String(50), nullable=False, doc="Season identifier (e.g., '2023-24', '2024-25')"
    )
    season_year = Column(
        String(50),
        nullable=False,
        doc="Season year for URL construction (e.g., '2023', '2024')",
    )

    # Foreign key relationship to Competition
    competition_id = Column(
        String(50),
        ForeignKey("competitions.competition_id"),
        nullable=False,
        doc="Reference to the competition this team participates in",
    )

    # Squad information
    squad_size = Column(
        Integer, default=0, nullable=False, doc="Total number of players in the squad"
    )
    average_age_of_players: Column[float] = Column(
        Float, default=0.0, nullable=False, doc="Average age of all players in the team"
    )
    number_of_foreign_players = Column(
        Integer,
        default=0,
        nullable=False,
        doc="Total number of foreign players in the squad",
    )

    # Market value data
    average_market_value: Column[float] = Column(
        Float,
        default=0.0,
        nullable=False,
        doc="Average market value per player in the team",
    )
    total_market_value: Column[float] = Column(
        Float,
        default=0.0,
        nullable=False,
        doc="Total market value of all players in the team",
    )

    # Metadata
    created_at = Column(
        DateTime,
        default=func.now(),
        nullable=False,
        doc="Timestamp when record was created",
    )
    updated_at = Column(
        DateTime,
        default=func.now(),
        onupdate=func.now(),
        nullable=False,
        doc="Timestamp when record was last updated",
    )
    is_active = Column(
        Boolean,
        default=True,
        nullable=False,
        doc="Whether the team is currently active",
    )

    __table_args__ = (PrimaryKeyConstraint("club_id", "season_year", "competition_id"),)

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
            "club_id": self.club_id,
            "club_name": self.club_name,
            "club_code": self.club_code,
            "club_url": self.club_url,
            "season_id": self.season_id,
            "season_year": self.season_year,
            "competition_id": self.competition_id,
            "squad_size": self.squad_size,
            "average_age_of_players": self.average_age_of_players,
            "number_of_foreign_players": self.number_of_foreign_players,
            "average_market_value": self.average_market_value,
            "total_market_value": self.total_market_value,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "is_active": self.is_active,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Team":
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
        squad_size = object.__getattribute__(self, "squad_size")
        num_foreign = object.__getattribute__(self, "number_of_foreign_players")
        if not squad_size:
            return 0.0
        return float(num_foreign) / float(squad_size) * 100.0
