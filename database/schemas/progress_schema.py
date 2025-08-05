# database/schemas/progress_schema.py
"""
Schema for tracking competition progress in distributed systems.
This module defines the database models for tracking the progress of competitions
and seasons, including task statuses and worker assignments.
"""

from enum import Enum
from typing import Any, Dict

from sqlalchemy import Column, DateTime, Index, Integer, String, Text

from database.base import Base


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

    __tablename__ = "competition_progress"

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
            "id": self.id,
            "competition_id": self.competition_id,
            "competition_url": self.competition_url,
            "status": self.status,
            "seasons_discovered": self.seasons_discovered,
            "worker_id": self.worker_id,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "error_message": self.error_message,
            "retry_count": self.retry_count,
        }


class SeasonProgress(Base):
    """
    Season progress tracking - matches distributed_progress_tracker.py tables.
    This model corresponds to the 'season_progress' table created by your tracker.
    """

    __tablename__ = "season_progress"

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
        # ***> Unique constraint matching your tracker's SQL <***
        Index("unique_competition_season", "competition_id", "season_id", unique=True),
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
            "id": self.id,
            "competition_id": self.competition_id,
            "season_id": self.season_id,
            "season_year": self.season_year,
            "status": self.status,
            "worker_id": self.worker_id,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "error_message": self.error_message,
            "retry_count": self.retry_count,
            "clubs_saved": self.clubs_saved,
        }
