# coordination/coordinator.py
"""
Distributed Progress Tracker - Works with both PostgreSQL and SQLite
"""

import os
import socket
import time
import uuid
from typing import List, Dict, Optional
from datetime import datetime, timedelta
from sqlalchemy import text

from database import TaskStatus
from database import create_database_service


class ProgressMonitor:
    """
    Distributed progress tracker for managing competition and season progress across multiple workers.
    Works with both PostgreSQL (production) and SQLite (development/testing).
    """
    
    def __init__(self, db_path: str = None, environment: str = "production"):
        """
        Initialize distributed tracker with environment-aware database selection.
        """
        #***> Generate unique worker ID <***
        self.worker_id = f"{socket.gethostname()}_{os.getpid()}_{uuid.uuid4().hex[:8]}"
        
        #***> Database connection - respects environment configuration <***
        self.environment = environment
        self.db_service = create_database_service(environment)
        self.db_service.initialize()
        
        #***> Determine database type for SQL compatibility <***
        self.db_type = self._get_database_type()
        
        
        #***> Create progress tables (database-specific) <***
        self._create_progress_tables()
        
        #***> Auto-recovery <***
        self._last_recovery = time.time()
        
        print(f"🌐 tracker initialized: {self.worker_id} (using {self.db_type})")
    
    def _get_database_type(self) -> str:
        """
        Determine database type from configuration.
        """
        db_url = self.db_service.config.database_url
        if db_url.startswith('postgresql://'):
            return 'PostgreSQL'
        elif db_url.startswith('sqlite://'):
            return 'SQLite'
        else:
            return 'Unknown'
    
    def _create_progress_tables(self):
        """
        Create progress tracking tables - database-specific SQL.
        """
        with self.db_service.transaction() as session:
            if self.db_type == 'PostgreSQL':
                self._create_postgresql_tables(session)
            else:  #***> SQLite <***
                self._create_sqlite_tables(session)
    
    def _create_postgresql_tables(self, session):
        """
        Create tables using PostgreSQL syntax.
        """
        #***> Competition progress table <***
        session.execute(text("""
            CREATE TABLE IF NOT EXISTS competition_progress (
                id SERIAL PRIMARY KEY,
                competition_id VARCHAR(100) UNIQUE NOT NULL,
                competition_url TEXT NOT NULL,
                status VARCHAR(50) NOT NULL,
                seasons_discovered INTEGER DEFAULT 0,
                worker_id VARCHAR(100),
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                error_message TEXT,
                retry_count INTEGER DEFAULT 0
            )
        """))
        
        #***> Season progress table <***
        session.execute(text("""
            CREATE TABLE IF NOT EXISTS season_progress (
                id SERIAL PRIMARY KEY,
                competition_id VARCHAR(100) NOT NULL,
                season_id VARCHAR(100) NOT NULL,
                season_year VARCHAR(100) NOT NULL,
                status VARCHAR(50) NOT NULL,
                worker_id VARCHAR(100),
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                error_message TEXT,
                retry_count INTEGER DEFAULT 0,
                clubs_saved INTEGER DEFAULT 0,
                UNIQUE(competition_id, season_id)
            )
        """))
        
        session.commit()
    
    def _create_sqlite_tables(self, session):
        """
        Create tables using SQLite syntax.
        """
        #***> Competition progress table <***
        session.execute(text("""
            CREATE TABLE IF NOT EXISTS competition_progress (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                competition_id VARCHAR(100) UNIQUE NOT NULL,
                competition_url TEXT NOT NULL,
                status VARCHAR(50) NOT NULL,
                seasons_discovered INTEGER DEFAULT 0,
                worker_id VARCHAR(100),
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                error_message TEXT,
                retry_count INTEGER DEFAULT 0
            )
        """))
        
        #***> Season progress table <***
        session.execute(text("""
            CREATE TABLE IF NOT EXISTS season_progress (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                competition_id VARCHAR(100) NOT NULL,
                season_id VARCHAR(100) NOT NULL,
                season_year VARCHAR(100) NOT NULL,
                status VARCHAR(50) NOT NULL,
                worker_id VARCHAR(100),
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                error_message TEXT,
                retry_count INTEGER DEFAULT 0,
                clubs_saved INTEGER DEFAULT 0,
                UNIQUE(competition_id, season_id)
            )
        """))
        
        session.commit()
    
    def _auto_recover_if_needed(self):
        """
        Automatically recover stuck tasks periodically.
        """
        if time.time() - self._last_recovery > 60:  #***> Every minute <***
            self.recover_stuck_seasons()
            self._last_recovery = time.time()
    
    def is_competition_completed(self, competition_id: str) -> bool:
        """
        Check if entire competition is completed.
        """
        self._auto_recover_if_needed()
        
        with self.db_service.transaction() as session:
            result = session.execute(text("""
                SELECT status FROM competition_progress 
                WHERE competition_id = :comp_id
            """), {"comp_id": competition_id})
            
            row = result.fetchone()
            return bool(row and row[0] == TaskStatus.COMPLETED.value)
    
    def mark_competition_started(self, competition_id: str, competition_url: str):
        """
        Mark competition as started - WITH AUTOMATIC CLAIMING (database-specific).
        """
        with self.db_service.transaction() as session:
            if self.db_type == 'PostgreSQL':
                self._mark_competition_started_postgresql(session, competition_id, competition_url)
            else:  #***> SQLite <***
                self._mark_competition_started_sqlite(session, competition_id, competition_url)
    
    def _mark_competition_started_postgresql(self, session, competition_id: str, competition_url: str):
        """
        PostgreSQL version with ON CONFLICT.
        """
        result = session.execute(text("""
            INSERT INTO competition_progress 
            (competition_id, competition_url, status, worker_id, started_at)
            VALUES (:comp_id, :url, :status, :worker, :now)
            ON CONFLICT (competition_id) DO UPDATE SET
                status = CASE 
                    WHEN competition_progress.status = 'pending' OR competition_progress.status IS NULL
                    THEN :status
                    ELSE competition_progress.status
                END,
                worker_id = CASE
                    WHEN competition_progress.status = 'pending' OR competition_progress.status IS NULL
                    THEN :worker
                    ELSE competition_progress.worker_id
                END,
                started_at = CASE
                    WHEN competition_progress.status = 'pending' OR competition_progress.status IS NULL
                    THEN :now
                    ELSE competition_progress.started_at
                END
            RETURNING worker_id
        """), {
            "comp_id": competition_id,
            "url": competition_url,
            "status": TaskStatus.IN_PROGRESS.value,
            "worker": self.worker_id,
            "now": datetime.now()
        })
        
        claimed_worker = result.fetchone()
        if claimed_worker and claimed_worker[0] == self.worker_id:
            print(f"✅ Claimed competition: {competition_id}")
        else:
            print(f"⏭️  Competition {competition_id} already claimed by another worker")
    
    def _mark_competition_started_sqlite(self, session, competition_id: str, competition_url: str):
        """
        SQLite version with INSERT OR REPLACE.
        """
        try:
            #***> Try to insert new competition <***
            session.execute(text("""
                INSERT INTO competition_progress 
                (competition_id, competition_url, status, worker_id, started_at)
                VALUES (:comp_id, :url, :status, :worker, :now)
            """), {
                "comp_id": competition_id,
                "url": competition_url,
                "status": TaskStatus.IN_PROGRESS.value,
                "worker": self.worker_id,
                "now": datetime.now()
            })
            print(f"✅ Claimed competition: {competition_id}")
        except Exception:
            #***> Competition already exists, try to claim it <***
            result = session.execute(text("""
                UPDATE competition_progress 
                SET status = :status, worker_id = :worker, started_at = :now
                WHERE competition_id = :comp_id 
                AND (status = 'pending' OR status IS NULL)
            """), {
                "comp_id": competition_id,
                "status": TaskStatus.IN_PROGRESS.value,
                "worker": self.worker_id,
                "now": datetime.now()
            })
            
            rows = result.fetchall()
            count = len(rows)
            
            if count > 0:
                print(f"✅ Claimed competition: {competition_id}")
            else:
                print(f"⏭️  Competition {competition_id} already claimed by another worker")
    
    def mark_seasons_discovered(self, competition_id: str, seasons: List[Dict]):
        """
        Mark that seasons have been discovered and initialize season tracking.
        """
        with self.db_service.transaction() as session:
            #***> Update competition status (only if we own it) <***
            session.execute(text("""
                UPDATE competition_progress 
                SET status = :status, seasons_discovered = :count
                WHERE competition_id = :comp_id AND worker_id = :worker
            """), {
                "status": TaskStatus.SEASONS_DISCOVERED.value,
                "count": len(seasons),
                "comp_id": competition_id,
                "worker": self.worker_id
            })
            
            #***> Initialize season tracking <***
            for season in seasons:
                if self.db_type == 'PostgreSQL':
                    session.execute(text("""
                        INSERT INTO season_progress 
                        (competition_id, season_id, season_year, status)
                        VALUES (:comp_id, :season_id, :year, :status)
                        ON CONFLICT (competition_id, season_id) DO NOTHING
                    """), {
                        "comp_id": competition_id,
                        "season_id": season['season_id'],
                        "year": season['year'],
                        "status": TaskStatus.PENDING.value
                    })
                else:  #***> SQLite <***
                    session.execute(text("""
                        INSERT OR IGNORE INTO season_progress 
                        (competition_id, season_id, season_year, status)
                        VALUES (:comp_id, :season_id, :year, :status)
                    """), {
                        "comp_id": competition_id,
                        "season_id": season['season_id'],
                        "year": season['year'],
                        "status": TaskStatus.PENDING.value
                    })
            
            session.commit()
            print(f"📋 Discovered {len(seasons)} seasons for {competition_id}")
    
    def get_pending_seasons_for_competition(self, competition_id: str) -> List[Dict]:
        """
        Get pending seasons for a specific competition - WITH AUTOMATIC CLAIMING.
        """
        with self.db_service.transaction() as session:
            #***> Find and claim available seasons atomically <***
            result = session.execute(text("""
                UPDATE season_progress 
                SET status = :in_progress, worker_id = :worker, started_at = :now
                WHERE competition_id = :comp_id 
                AND status IN (:pending, :failed)
                AND retry_count < 3
                AND (worker_id IS NULL OR worker_id = :worker)
                RETURNING season_id, season_year, retry_count
            """), {
                "comp_id": competition_id,
                "in_progress": TaskStatus.IN_PROGRESS.value,
                "pending": TaskStatus.PENDING.value,
                "failed": TaskStatus.FAILED.value,
                "worker": self.worker_id,
                "now": datetime.now()
            })
            
            claimed_seasons = [
                {
                    'season_id': row[0],
                    'year': row[1],
                    'retry_count': row[2]
                }
                for row in result.fetchall()
            ]
            
            if claimed_seasons:
                print(f"✅ Claimed {len(claimed_seasons)} seasons for {competition_id}")
            
            return claimed_seasons
    
    def mark_season_started(self, competition_id: str, season_id: str):
        """
        Mark season as started (already done in get_pending_seasons_for_competition).
        """
        #***> This is now handled automatically in get_pending_seasons_for_competition <***
        #***> But we keep this method for compatibility <***
        pass
    
    def mark_season_completed(self, competition_id: str, season_id: str, clubs_saved: int = 0):
        """
        Mark season as completed.
        """
        with self.db_service.transaction() as session:
            session.execute(text("""
                UPDATE season_progress 
                SET status = :status, completed_at = :now, clubs_saved = :clubs
                WHERE competition_id = :comp_id AND season_id = :season_id AND worker_id = :worker
            """), {
                "status": TaskStatus.COMPLETED.value,
                "now": datetime.now(),
                "clubs": clubs_saved,
                "comp_id": competition_id,
                "season_id": season_id,
                "worker": self.worker_id
            })
            
            #***> Check if all seasons for this competition are done <***
            self._check_competition_completion(competition_id, session)
            
            print(f"✅ Completed season {season_id} for {competition_id} ({clubs_saved} clubs)")
    
    def mark_season_failed(self, competition_id: str, season_id: str, error_message: str):
        """
        Mark season as failed - WITH AUTOMATIC REDISTRIBUTION.
        """
        with self.db_service.transaction() as session:
            session.execute(text("""
                UPDATE season_progress 
                SET status = CASE 
                    WHEN retry_count >= 2 THEN :failed
                    ELSE :pending
                END,
                error_message = :error,
                retry_count = retry_count + 1,
                worker_id = CASE
                    WHEN retry_count >= 2 THEN worker_id
                    ELSE NULL
                END
                WHERE competition_id = :comp_id AND season_id = :season_id AND worker_id = :worker
            """), {
                "failed": TaskStatus.FAILED.value,
                "pending": TaskStatus.PENDING.value,
                "error": error_message,
                "comp_id": competition_id,
                "season_id": season_id,
                "worker": self.worker_id
            })
            
            print(f"❌ Failed season {season_id} for {competition_id}: {error_message}")
    
    def _check_competition_completion(self, competition_id: str, session):
        """
        Check if all seasons are completed and mark competition as completed.
        """
        result = session.execute(text("""
            SELECT COUNT(*) as total,
                   SUM(CASE WHEN status = :completed THEN 1 ELSE 0 END) as completed_count
            FROM season_progress 
            WHERE competition_id = :comp_id
        """), {
            "completed": TaskStatus.COMPLETED.value,
            "comp_id": competition_id
        })
        
        total, completed_count = result.fetchone()
        
        if total > 0 and total == completed_count:
            session.execute(text("""
                UPDATE competition_progress 
                SET status = :status, completed_at = :now
                WHERE competition_id = :comp_id
            """), {
                "status": TaskStatus.COMPLETED.value,
                "now": datetime.now(),
                "comp_id": competition_id
            })
            print(f"🏆 Competition {competition_id} completed!")
    
    def is_season_completed(self, competition_id: str, season_id: str) -> bool:
        """
        Check if specific season is completed.
        """
        with self.db_service.transaction() as session:
            result = session.execute(text("""
                SELECT status FROM season_progress 
                WHERE competition_id = :comp_id AND season_id = :season_id
            """), {
                "comp_id": competition_id,
                "season_id": season_id
            })
            
            row = result.fetchone()
            return bool(row and row[0] == TaskStatus.COMPLETED.value)
    
    def get_competition_status(self, competition_id: str) -> Optional[str]:
        """
        Get current status of a competition.
        """
        with self.db_service.transaction() as session:
            result = session.execute(text("""
                SELECT status FROM competition_progress 
                WHERE competition_id = :comp_id
            """), {"comp_id": competition_id})
            
            row = result.fetchone()
            return row[0] if row else None
    
    def get_progress_summary(self) -> Dict:
        """
        Get overall progress summary.
        """
        with self.db_service.transaction() as session:
            #***> Competition summary <***
            comp_result = session.execute(text("""
                SELECT status, COUNT(*) as count
                FROM competition_progress 
                GROUP BY status
            """))
            comp_summary = {row[0]: row[1] for row in comp_result.fetchall()}
            
            #***> Season summary <***
            season_result = session.execute(text("""
                SELECT status, COUNT(*) as count, COALESCE(SUM(clubs_saved), 0) as total_clubs
                FROM season_progress 
                GROUP BY status
            """))
            season_data = season_result.fetchall()
            season_summary = {row[0]: {'count': row[1], 'clubs_saved': row[2]} for row in season_data}
            
            #***> Active workers <***
            worker_result = session.execute(text("""
                SELECT COUNT(DISTINCT worker_id) as active_workers
                FROM season_progress 
                WHERE status = :in_progress AND worker_id IS NOT NULL
            """), {"in_progress": TaskStatus.IN_PROGRESS.value})
            
            row = worker_result.fetchone()
            active_workers = row[0] if row is not None else 0
            
            total_competitions = sum(comp_summary.values()) if comp_summary else 0
            completed_competitions = comp_summary.get(TaskStatus.COMPLETED.value, 0)
            
            total_seasons = sum(data['count'] for data in season_summary.values()) if season_summary else 0
            completed_seasons = season_summary.get(TaskStatus.COMPLETED.value, {}).get('count', 0)
            total_clubs_saved = sum(data['clubs_saved'] for data in season_summary.values()) if season_summary else 0
            
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
                'total_clubs_saved': total_clubs_saved,
                'active_workers': active_workers,
                'my_worker_id': self.worker_id,
                'database_type': self.db_type
            }
    
    def show_detailed_progress(self):
        """
        Show detailed progress information.
        """
        summary = self.get_progress_summary()
        print("\n📊 DISTRIBUTED SCRAPING PROGRESS:")
        print("Database: %s", summary['database_type'])
        print("Active Workers: %d (I am: %s)", summary['active_workers'], summary['my_worker_id'][-8:])
        print("Competitions: %d/%d (%.1f%%)", 
              summary['competitions']['completed'],
              summary['competitions']['total'],
              summary['competitions']['completion_percentage'])
        print("Seasons: %d/%d (%.1f%%)",
              summary['seasons']['completed'], 
              summary['seasons']['total'],
              summary['seasons']['completion_percentage'])
        print("Total clubs saved: %d", summary['total_clubs_saved'])
        
        #***> Show failed seasons <***
        if summary['seasons']['failed'] > 0:
            print("\n❌ FAILED SEASONS (%d):", summary['seasons']['failed'])
            with self.db_service.transaction() as session:
                result = session.execute(text("""
                    SELECT competition_id, season_year, error_message, retry_count
                    FROM season_progress 
                    WHERE status = :failed
                    ORDER BY retry_count DESC
                    LIMIT 10
                """), {"failed": TaskStatus.FAILED.value})
                
                for row in result.fetchall():
                    error_msg = row[2][:100] if row[2] else 'No error message'
                    print("   • %s - %s (retries: %d): %s...", 
                          row[0], row[1], row[3], error_msg)
    
    def reset_failed_seasons(self) -> int:
        """
        Reset failed seasons to allow retrying.
        """
        with self.db_service.transaction() as session:
            result = session.execute(text("""
                UPDATE season_progress 
                SET status = :pending, retry_count = 0, worker_id = NULL, error_message = NULL
                WHERE status = :failed
            """), {
                "pending": TaskStatus.PENDING.value,
                "failed": TaskStatus.FAILED.value
            })
            
            rows = result.fetchall()
            count = len(rows)
            session.commit()
            
        print("✅ Reset %d failed seasons to pending status", count)
        return count
    
    def recover_stuck_seasons(self) -> int:
        """
        Automatically recover seasons stuck in 'in_progress' status.
        """
        threshold = datetime.now() - timedelta(minutes=30)
        
        with self.db_service.transaction() as session:
            #***> Find stuck seasons (in progress for more than 30 minutes) <***
            result = session.execute(text("""
                UPDATE season_progress 
                SET status = :pending, worker_id = NULL, retry_count = retry_count + 1,
                    error_message = 'Recovered from stuck worker'
                WHERE status = :in_progress 
                AND started_at < :threshold
                AND retry_count < 3
                RETURNING competition_id, season_year
            """), {
                "pending": TaskStatus.PENDING.value,
                "in_progress": TaskStatus.IN_PROGRESS.value,
                "threshold": threshold
            })
            
            rows = result.fetchall()
            count = len(rows)
            
            if count > 0:
                print("🔄 Recovered %d stuck seasons:", count)
                for comp_id, season_year in rows:
                    print("   • %s - %s", comp_id, season_year)
                print("✅ These seasons are now available for other workers")
            
            return count
    
    def clear_all_progress(self) -> bool:
        """
        Clear all progress (start fresh) - USE WITH CAUTION.
        """
        confirmation = input("⚠️ This will delete ALL distributed progress. Type 'YES' to confirm: ")
        if confirmation == 'YES':
            with self.db_service.transaction() as session:
                session.execute(text("DELETE FROM competition_progress"))
                session.execute(text("DELETE FROM season_progress"))
                session.commit()
            print("✅ All distributed progress cleared - starting fresh")
            return True
        else:
            print("❌ Operation cancelled")
            return False
    
    def get_failed_seasons(self) -> List[Dict]:
        """
        Get detailed information about failed seasons.
        """
        with self.db_service.transaction() as session:
            result = session.execute(text("""
                SELECT competition_id, season_id, season_year, error_message, retry_count
                FROM season_progress 
                WHERE status = :failed
                ORDER BY retry_count DESC, competition_id, season_year
            """), {"failed": TaskStatus.FAILED.value})
            
            return [
                {
                    'competition_id': row[0],
                    'season_id': row[1],
                    'season_year': row[2],
                    'error_message': row[3],
                    'retry_count': row[4]
                }
                for row in result.fetchall()
            ]
    
    def reset_specific_competition(self, competition_id: str) -> int:
        """
        Reset all progress for a specific competition.
        """
        with self.db_service.transaction() as session:
            #***> Reset competition <***
            session.execute(text("""
                UPDATE competition_progress 
                SET status = :pending, worker_id = NULL
                WHERE competition_id = :comp_id
            """), {
                "pending": TaskStatus.PENDING.value,
                "comp_id": competition_id
            })
            
            #***> Reset all seasons for this competition <***
            result = session.execute(text("""
                UPDATE season_progress 
                SET status = :pending, retry_count = 0, worker_id = NULL, error_message = NULL
                WHERE competition_id = :comp_id
            """), {
                "pending": TaskStatus.PENDING.value,
                "comp_id": competition_id
            })
            
            rows = result.fetchall()
            count = len(rows)
            session.commit()
        
        print("✅ Reset %d seasons for competition %s", count, competition_id)
        return count


#***> Simple factory function <***
def create_work_tracker(environment: str = "production") -> ProgressMonitor:
    """
    Create distributed progress tracker for specified environment.
    
    Args:
        environment: 'development' (SQLite), 'testing' (SQLite), 'production' (PostgreSQL)
    
    Returns:
        ProgressMonitor instance using the correct database type
    """
    return ProgressMonitor(environment=environment)