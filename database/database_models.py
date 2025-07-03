# core/database/database_models.py

"""
Database models for competitions and teams
"""
import sqlite3
from typing import Dict, Any, List, Optional
from datetime import datetime
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

class WorkTask(Base):
    """
    Work queue table for distributed task processing.
    Replaces the old SQLite progress tracking system.
    """
    __tablename__ = 'work_queue'
    
    # Task identification
    task_id = Column(
        String(100), primary_key=True,
        doc="Unique task identifier (e.g., 'competition:GB1', 'season:GB1:2023')"
    )
    task_type = Column(
        String(50), nullable=False,
        doc="Type of task: 'competition', 'season', 'team', etc."
    )
    competition_id = Column(
        String(50), nullable=False,
        doc="Competition this task relates to"
    )
    season_id = Column(
        String(50), nullable=True,
        doc="Season ID for season-level tasks"
    )
    
    # Task status and assignment
    status = Column(
        String(20), default=TaskStatus.PENDING.value, nullable=False,
        doc="Current task status - preserves original pipeline values"
    )
    worker_id = Column(
        String(100), nullable=True,
        doc="ID of worker that claimed this task"
    )
    priority = Column(
        Integer, default=0, nullable=False,
        doc="Task priority (higher = more important)"
    )
    
    # Timing information
    created_at = Column(
        DateTime, default=func.now(), nullable=False,
        doc="When task was created"
    )
    claimed_at = Column(
        DateTime, nullable=True,
        doc="When task was claimed by a worker"
    )
    completed_at = Column(
        DateTime, nullable=True,
        doc="When task was completed"
    )
    heartbeat = Column(
        DateTime, nullable=True,
        doc="Last heartbeat from worker (for detecting stale tasks)"
    )
    
    # Results and error handling
    error_message = Column(
        String(500), nullable=True,
        doc="Error message if task failed"
    )
    retry_count = Column(
        Integer, default=0, nullable=False,
        doc="Number of times this task has been retried"
    )
    result_data = Column(
        Text, nullable=True,
        doc="JSON string containing task results"
    )

    # Indexes for efficient querying
    __table_args__ = (
        Index('idx_worktask_status_priority', 'status', 'priority'),
        Index('idx_worktask_worker', 'worker_id'),
        Index('idx_worktask_competition', 'competition_id'),
        Index('idx_worktask_type', 'task_type'),
        Index('idx_worktask_heartbeat', 'heartbeat'),
    )

    def __repr__(self) -> str:
        """String representation of WorkTask object."""
        return (
            f"<WorkTask(id='{self.task_id}', type='{self.task_type}', "
            f"status='{self.status}', worker='{self.worker_id}')>"
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert WorkTask object to dictionary."""
        return {
            'task_id': self.task_id,
            'task_type': self.task_type,
            'competition_id': self.competition_id,
            'season_id': self.season_id,
            'status': self.status,
            'worker_id': self.worker_id,
            'priority': self.priority,
            'created_at': self.created_at,
            'claimed_at': self.claimed_at,
            'completed_at': self.completed_at,
            'heartbeat': self.heartbeat,
            'error_message': self.error_message,
            'retry_count': self.retry_count,
            'result_data': self.result_data
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'WorkTask':
        """Create WorkTask object from dictionary."""
        return cls(**data)


class ProgressTracker:
    """
    Progress tracker for scraping competitions and seasons.
    This class tracks the progress of scraping competitions and their seasons,
    including status updates, retries, and completion checks.
    It uses SQLite for persistent storage of progress data.
    """

    def __init__(self, db_path: str = None):
        """Initialize with warning about deprecation"""
        import warnings
        warnings.warn(
            "ProgressTracker is deprecated. Use WorkTask and DynamicWorkQueue instead.",
            DeprecationWarning,
            stacklevel=2
        )
        
        # For now, we'll maintain SQLite compatibility
        # but recommend migration to PostgreSQL WorkTask system
        if db_path:
            self.db_path = db_path
            self.init_database()
        else:
            print("⚠️ ProgressTracker: Consider migrating to WorkTask system for better performance")
        self.db_path = db_path or 'progress_tracker.db'

    def init_database(self):
        """Initialize progress tracking tables"""
        with sqlite3.connect(self.db_path) as conn:
            # Competition-level tracking
            conn.execute("""
                CREATE TABLE IF NOT EXISTS competition_progress (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    competition_id TEXT UNIQUE NOT NULL,
                    competition_url TEXT NOT NULL,
                    status TEXT NOT NULL,
                    seasons_discovered INTEGER DEFAULT 0,
                    started_at TIMESTAMP,
                    completed_at TIMESTAMP,
                    error_message TEXT,
                    retry_count INTEGER DEFAULT 0
                )
            """)

            # Season-level tracking (same as before)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS season_progress (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    competition_id TEXT NOT NULL,
                    season_id TEXT NOT NULL,
                    season_year TEXT NOT NULL,
                    status TEXT NOT NULL,
                    started_at TIMESTAMP,
                    completed_at TIMESTAMP,
                    error_message TEXT,
                    retry_count INTEGER DEFAULT 0,
                    clubs_saved INTEGER DEFAULT 0,
                    UNIQUE(competition_id, season_id)
                )
            """)
            conn.commit()

    def is_competition_completed(self, competition_id: str) -> bool:
        """Check if entire competition is completed"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                SELECT status FROM competition_progress 
                WHERE competition_id = ?
            """, (competition_id,))
            result = cursor.fetchone()
            return result and result[0] == TaskStatus.COMPLETED.value

    def mark_competition_started(self, competition_id: str, competition_url: str):
        """Mark competition as started"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO competition_progress 
                (competition_id, competition_url, status, started_at)
                VALUES (?, ?, ?, ?)
            """, (competition_id, competition_url, TaskStatus.IN_PROGRESS.value, datetime.now()))
            conn.commit()

    def mark_seasons_discovered(self, competition_id: str, seasons: List[Dict]):
        """Mark that seasons have been discovered and initialize season tracking"""
        with sqlite3.connect(self.db_path) as conn:
            # Update competition status
            conn.execute("""
                UPDATE competition_progress 
                SET status = ?, seasons_discovered = ?
                WHERE competition_id = ?
            """, (TaskStatus.SEASONS_DISCOVERED.value, len(seasons), competition_id))

            # Initialize season tracking
            for season in seasons:
                conn.execute("""
                    INSERT OR IGNORE INTO season_progress 
                    (competition_id, season_id, season_year, status)
                    VALUES (?, ?, ?, ?)
                """, (competition_id, season['season_id'], season['year'], TaskStatus.PENDING.value))

            conn.commit()

    def get_pending_seasons_for_competition(self, competition_id: str) -> List[Dict]:
        """Get pending seasons for a specific competition"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                SELECT season_id, season_year, retry_count
                FROM season_progress 
                WHERE competition_id = ? 
                AND status IN (?, ?) 
                AND retry_count < 3
                ORDER BY season_year DESC
            """, (competition_id, TaskStatus.PENDING.value, TaskStatus.FAILED.value))

            return [
                {
                    'season_id': row[0],
                    'year': row[1],
                    'retry_count': row[2]
                }
                for row in cursor.fetchall()
            ]

    def mark_season_started(self, competition_id: str, season_id: str):
        """Mark season as started"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                UPDATE season_progress 
                SET status = ?, started_at = ?
                WHERE competition_id = ? AND season_id = ?
            """, (TaskStatus.IN_PROGRESS.value, datetime.now(), competition_id, season_id))
            conn.commit()

    def mark_season_completed(self, competition_id: str, season_id: str, clubs_saved: int = 0):
        """Mark season as completed"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                UPDATE season_progress 
                SET status = ?, completed_at = ?, clubs_saved = ?
                WHERE competition_id = ? AND season_id = ?
            """, (TaskStatus.COMPLETED.value, datetime.now(), clubs_saved, competition_id, season_id))
            conn.commit()

            # Check if all seasons for this competition are done
            self._check_competition_completion(competition_id)

    def mark_season_failed(self, competition_id: str, season_id: str, error_message: str):
        """Mark season as failed"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                UPDATE season_progress 
                SET status = ?, error_message = ?, retry_count = retry_count + 1
                WHERE competition_id = ? AND season_id = ?
            """, (TaskStatus.FAILED.value, error_message, competition_id, season_id))
            conn.commit()

    def _check_competition_completion(self, competition_id: str):
        """Check if all seasons are completed and mark competition as completed"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                SELECT COUNT(*) as total,
                       SUM(CASE WHEN status = ? THEN 1 ELSE 0 END) as completed
                FROM season_progress 
                WHERE competition_id = ?
            """, (TaskStatus.COMPLETED.value, competition_id))

            total, completed = cursor.fetchone()

            if total > 0 and total == completed:
                conn.execute("""
                    UPDATE competition_progress 
                    SET status = ?, completed_at = ?
                    WHERE competition_id = ?
                """, (TaskStatus.COMPLETED.value, datetime.now(), competition_id))
                conn.commit()

    def is_season_completed(self, competition_id: str, season_id: str) -> bool:
        """Check if specific season is completed"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                SELECT status FROM season_progress 
                WHERE competition_id = ? AND season_id = ?
            """, (competition_id, season_id))
            result = cursor.fetchone()
            return result and result[0] == TaskStatus.COMPLETED.value

    def get_competition_status(self, competition_id: str) -> Optional[str]:
        """Get current status of a competition"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                SELECT status FROM competition_progress 
                WHERE competition_id = ?
            """, (competition_id,))
            result = cursor.fetchone()
            return result[0] if result else None

    def get_progress_summary(self) -> Dict:
        """Get overall progress summary"""
        with sqlite3.connect(self.db_path) as conn:
            # Competition summary
            cursor = conn.execute("""
                SELECT status, COUNT(*) as count
                FROM competition_progress 
                GROUP BY status
            """)
            comp_summary = {row[0]: row[1] for row in cursor.fetchall()}

            # Season summary
            cursor = conn.execute("""
                SELECT status, COUNT(*) as count, COALESCE(SUM(clubs_saved), 0) as total_clubs
                FROM season_progress 
                GROUP BY status
            """)
            season_data = cursor.fetchall()
            season_summary = {row[0]: {'count': row[1], 'clubs_saved': row[2]} for row in season_data}

            total_competitions = sum(comp_summary.values())
            completed_competitions = comp_summary.get(TaskStatus.COMPLETED.value, 0)

            total_seasons = sum(data['count'] for data in season_summary.values())
            completed_seasons = season_summary.get(TaskStatus.COMPLETED.value, {}).get('count', 0)
            total_clubs_saved = sum(data['clubs_saved'] for data in season_summary.values())

            return {
                'competitions': {
                    'total': total_competitions,
                    'completed': completed_competitions,
                    'completion_percentage': (completed_competitions / total_competitions * 100) if total_competitions > 0 else 0
                },
                'seasons': {
                    'total': total_seasons,
                    'completed': completed_seasons,
                    'pending': season_summary.get(TaskStatus.PENDING.value, {}).get('count', 0),
                    'failed': season_summary.get(TaskStatus.FAILED.value, {}).get('count', 0),
                    'completion_percentage': (completed_seasons / total_seasons * 100) if total_seasons > 0 else 0
                },
                'total_clubs_saved': total_clubs_saved
            }

    def show_detailed_progress(self):
        """Show detailed progress information"""
        summary = self.get_progress_summary()
        print("\n📊 DETAILED SCRAPING PROGRESS:")
        print(f"Competitions: {summary['competitions']['completed']}/{summary['competitions']['total']} "
            f"({summary['competitions']['completion_percentage']:.1f}%)")
        print(f"Seasons: {summary['seasons']['completed']}/{summary['seasons']['total']} "
            f"({summary['seasons']['completion_percentage']:.1f}%)")
        print(f"Total clubs saved: {summary['total_clubs_saved']}")

        # Show failed seasons
        if summary['seasons']['failed'] > 0:
            print(f"\n❌ FAILED SEASONS ({summary['seasons']['failed']}):")
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("""
                    SELECT competition_id, season_year, error_message, retry_count
                    FROM season_progress 
                    WHERE status = 'failed'
                    ORDER BY retry_count DESC
                """)
                for row in cursor.fetchall():
                    print(f"   • {row[0]} - {row[1]} (retries: {row[3]}): {row[2][:100]}...")

    def reset_failed_seasons(self) -> int:
        """Reset failed seasons to allow retrying"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                UPDATE season_progress 
                SET status = 'pending', retry_count = 0 
                WHERE status = 'failed'
            """)
            count = cursor.rowcount
            conn.commit()
        print(f"✅ Reset {count} failed seasons to pending status")
        return count

    def clear_all_progress(self) -> bool:
        """Clear all progress (start fresh) - USE WITH CAUTION"""
        confirmation = input("⚠️ This will delete ALL progress. Type 'YES' to confirm: ")
        if confirmation == 'YES':
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("DELETE FROM competition_progress")
                conn.execute("DELETE FROM season_progress")
                conn.commit()
            print("✅ All progress cleared - starting fresh")
            return True
        else:
            print("❌ Operation cancelled")
            return False

    def get_failed_seasons(self) -> List[Dict]:
        """Get detailed information about failed seasons"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                SELECT competition_id, season_id, season_year, error_message, retry_count
                FROM season_progress 
                WHERE status = 'failed'
                ORDER BY retry_count DESC, competition_id, season_year
            """)
            return [
                {
                    'competition_id': row[0],
                    'season_id': row[1],
                    'season_year': row[2],
                    'error_message': row[3],
                    'retry_count': row[4]
                }
                for row in cursor.fetchall()
            ]

    def reset_specific_competition(self, competition_id: str) -> int:
        """Reset all progress for a specific competition"""
        with sqlite3.connect(self.db_path) as conn:
            # Reset competition
            conn.execute("""
                UPDATE competition_progress 
                SET status = 'pending'
                WHERE competition_id = ?
            """, (competition_id,))

            # Reset all seasons for this competition
            cursor = conn.execute("""
                UPDATE season_progress 
                SET status = 'pending', retry_count = 0
                WHERE competition_id = ?
            """, (competition_id,))
            count = cursor.rowcount
            conn.commit()

        print(f"✅ Reset {count} seasons for competition {competition_id}")
        return count
    
    def recover_stuck_seasons(self):
        """Automatically recover seasons stuck in 'in_progress' status"""
        with sqlite3.connect(self.db_path) as conn:
            # Find all stuck seasons
            cursor = conn.execute("""
                SELECT competition_id, season_id, season_year, started_at
                FROM season_progress 
                WHERE status = 'in_progress'
            """)

            stuck_seasons = cursor.fetchall()

            if stuck_seasons:
                print(f"🔄 Found {len(stuck_seasons)} stuck seasons - resetting to pending:")

                for comp, season, year, started_at in stuck_seasons:
                    print(f"   • {comp} - {year} (started: {started_at})")

                # Reset all stuck seasons to pending
                cursor = conn.execute("""
                    UPDATE season_progress 
                    SET status = 'pending', retry_count = 0, error_message = NULL
                    WHERE status = 'in_progress'
                """)

                reset_count = cursor.rowcount
                conn.commit()

                print(f"✅ Reset {reset_count} stuck seasons to 'pending' - they will be re-scraped")
                return reset_count
            else:
                print("✅ No stuck seasons found")
                return 0
