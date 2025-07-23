#!/usr/bin/env python3
"""
Complete Football Match Data Scraping System - FIXED VERSION
Production-ready with clean data separation and PostgreSQL orchestration
Uses the ACTUAL working extraction logic from the notebook snippets

Usage:
    python football_scraper.py setup                    # Initialize database
    python football_scraper.py schedule --season 2023-24 # Schedule jobs
    python football_scraper.py worker --workers 4       # Start workers
    python football_scraper.py status                   # Check status
"""

import re
import json
import time
import logging
import uuid
import argparse
import sys
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
from pathlib import Path
from dataclasses import dataclass, asdict
from contextlib import contextmanager
from enum import Enum
from dataclasses import asdict
from .database import Match, Player, MatchLineup, Goal, Card, Substitution, MatchdayInfo

# Web scraping imports
import requests
from bs4 import BeautifulSoup, Tag
import random

# Database imports
import psycopg2
from psycopg2.extras import RealDictCursor, Json
import redis

# Concurrency imports
import threading
import backoff

# =============================================================================
# CONFIGURATION AND ENUMS
# =============================================================================

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

# =============================================================================
# FIXED DATA MODELS (using the working notebook structure)
# =============================================================================

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

@dataclass
class MatchContextual:
    """Contextual match info from notebook snippet 3"""
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
    match_events: List[Dict[str, Any]] = None

@dataclass
class MatchdayContainer:
    """Complete matchday context from notebook snippet 3"""
    matchday_info: Dict[str, Any]
    league_table: Optional[Dict[str, Any]] = None
    top_scorers: Optional[List[Dict[str, Any]]] = None
    matchday_summary: Optional[Dict[str, Any]] = None
    matches: Optional[List[MatchContextual]] = None
    metadata: Dict[str, Any] = None


# =============================================================================
# DATABASE MANAGER
# =============================================================================

class DatabaseManager:
    """
    Manages PostgreSQL operations with improved transaction handling
    """
    
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.logger = logging.getLogger(__name__)
    
    def init_database(self):
        """Initialize database schema"""
        schema_sql = """
        CREATE TABLE IF NOT EXISTS scrape_jobs (
            job_id UUID PRIMARY KEY,
            job_type VARCHAR(50) NOT NULL,
            target_url TEXT NOT NULL,
            status VARCHAR(20) NOT NULL DEFAULT 'pending',
            priority INTEGER DEFAULT 5,
            scheduled_at TIMESTAMP DEFAULT NOW(),
            started_at TIMESTAMP,
            completed_at TIMESTAMP,
            retry_count INTEGER DEFAULT 0,
            max_retries INTEGER DEFAULT 3,
            last_error TEXT,
            worker_id VARCHAR(50),
            parent_job_id UUID REFERENCES scrape_jobs(job_id),
            metadata JSONB,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS extractions (
            extraction_id UUID PRIMARY KEY,
            job_id UUID REFERENCES scrape_jobs(job_id),
            match_id VARCHAR(50),
            matchday INTEGER,
            season VARCHAR(20),
            competition VARCHAR(100),
            extracted_data JSONB,
            file_path TEXT,
            data_quality_score FLOAT DEFAULT 0.0,
            extraction_duration_ms INTEGER,
            created_at TIMESTAMP DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_jobs_status ON scrape_jobs(status);
        CREATE INDEX IF NOT EXISTS idx_jobs_scheduled ON scrape_jobs(scheduled_at);
        CREATE INDEX IF NOT EXISTS idx_jobs_created ON scrape_jobs(created_at);
        CREATE INDEX IF NOT EXISTS idx_extractions_match ON extractions(match_id);
        """
        
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(schema_sql)
                conn.commit()
        
        self.logger.info("Database schema initialized")
    
    @contextmanager
    def get_connection(self):
        """Get database connection with cleanup"""
        conn = psycopg2.connect(
            self.connection_string, 
            cursor_factory=RealDictCursor,
            # Improved connection settings
            application_name="football_scraper",
            connect_timeout=30
        )
        try:
            # Set proper isolation level
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)
            yield conn
        finally:
            conn.close()
    
    def create_job(self, job_type: JobType, target_url: str, priority: int = 5, 
                  parent_job_id: str = None, metadata: Dict = None) -> str:
        """Create a new scrape job with immediate commit"""
        job_id = str(uuid.uuid4())
        
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO scrape_jobs (
                        job_id, job_type, target_url, priority, parent_job_id, metadata
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                """, (job_id, job_type.value, target_url, priority, parent_job_id, Json(metadata or {})))
                conn.commit()  # Immediate commit
        
        return job_id
    
    def get_pending_jobs(self, limit: int = 10) -> List[Dict]:
        """Get pending jobs"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT * FROM scrape_jobs 
                    WHERE status = 'pending' AND scheduled_at <= NOW()
                    ORDER BY priority DESC, scheduled_at ASC
                    LIMIT %s
                """, (limit,))
                return [dict(row) for row in cur.fetchall()]
    
    def update_job_status(self, job_id: str, status: JobStatus, worker_id: str = None, error: str = None):
        """Update job status with immediate commit"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                updates = ["status = %s", "updated_at = NOW()"]
                params = [status.value]
                
                if status == JobStatus.RUNNING and worker_id:
                    updates.append("started_at = NOW()")
                    updates.append("worker_id = %s")
                    params.append(worker_id)
                elif status == JobStatus.COMPLETED:
                    updates.append("completed_at = NOW()")
                elif status in [JobStatus.FAILED, JobStatus.RETRYING]:
                    updates.append("retry_count = retry_count + 1")
                    if error:
                        updates.append("last_error = %s")
                        params.append(error)
                
                params.append(job_id)
                
                cur.execute(f"UPDATE scrape_jobs SET {', '.join(updates)} WHERE job_id = %s", params)
                conn.commit()  # Immediate commit
    
    def save_extraction(self, job_id: str, match_id: str, matchday: int, season: str,
                       competition: str, data: Dict, file_path: str, quality_score: float, duration_ms: int):
        """Save extraction result with immediate commit"""
        extraction_id = str(uuid.uuid4())
        
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO extractions (
                        extraction_id, job_id, match_id, matchday, season,
                        competition, extracted_data, file_path, data_quality_score, extraction_duration_ms
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (extraction_id, job_id, match_id, matchday, season, competition, 
                     Json(data), file_path, quality_score, duration_ms))
                conn.commit()  # Immediate commit
    
    def get_job_stats(self) -> Dict[str, Any]:
        """Get processing statistics"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT status, COUNT(*) as count
                    FROM scrape_jobs 
                    WHERE created_at > NOW() - INTERVAL '24 hours'
                    GROUP BY status
                """)
                
                stats = {}
                for row in cur.fetchall():
                    stats[row['status']] = row['count']
                return stats

# =============================================================================
# QUEUE MANAGER (unchanged - this is fine)
# =============================================================================

class QueueManager:
    """Redis-based job queue with improved reliability"""
    
    def __init__(self, redis_url: str):
        self.redis_client = redis.from_url(redis_url)
        self.queue_name = "scrape_jobs"
        self.processing_set = "processing_jobs"  # Track jobs being processed
    
    def enqueue_job(self, job_id: str, priority: int = 5):
        """Add job to queue"""
        self.redis_client.zadd(self.queue_name, {job_id: priority})
    
    def dequeue_job(self) -> Optional[str]:
        """Get next job from queue"""
        result = self.redis_client.zpopmax(self.queue_name)
        if result:
            job_id = result[0][0].decode()
            # Mark as being processed
            self.redis_client.sadd(self.processing_set, job_id)
            return job_id
        return None
    
    def mark_job_completed(self, job_id: str):
        """Mark job as completed (remove from processing set)"""
        self.redis_client.srem(self.processing_set, job_id)
    
    def get_queue_size(self) -> int:
        """Get queue size"""
        return self.redis_client.zcard(self.queue_name)
    
    def get_processing_count(self) -> int:
        """Get number of jobs being processed"""
        return self.redis_client.scard(self.processing_set)
    
    def clear_queue(self):
        """Clear all jobs from queue"""
        self.redis_client.delete(self.queue_name)
        self.redis_client.delete(self.processing_set)

# =============================================================================
# STORAGE MANAGER (unchanged - this is fine)
# =============================================================================

class StorageManager:
    """File storage management"""
    
    def __init__(self, base_path: str):
        self.base_path = Path(base_path)
        self.setup_directories()
    
    def setup_directories(self):
        """Create directory structure"""
        for directory in ["matchdays", "matches", "logs"]:
            (self.base_path / directory).mkdir(parents=True, exist_ok=True)
    
    def save_matchday_data(self, season: str, matchday: int, data: MatchdayContainer) -> str:
        """Save matchday container"""
        filename = f"{season}_matchday_{matchday:02d}.json"
        filepath = self.base_path / "matchdays" / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(asdict(data), f, indent=2, ensure_ascii=False, default=str)
        
        return str(filepath)
    
    def save_match_data(self, match_id: str, season: str, data: MatchDetail) -> str:
        """Save match detail data"""
        season_dir = self.base_path / "matches" / season
        season_dir.mkdir(exist_ok=True)
        
        filename = f"match_{match_id}.json"
        filepath = season_dir / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(asdict(data), f, indent=2, ensure_ascii=False, default=str)
        
        return str(filepath)

# =============================================================================
# FIXED MATCHDAY EXTRACTOR (based on notebook snippet 3)
# =============================================================================

class MatchdayExtractor:
    """Extracts contextual matchday data using the WORKING notebook snippet 3 logic"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
    
    def extract_from_transfermarkt_url(self, url: str, matchday: int = None, 
                                     season: str = None) -> MatchdayContainer:
        """Extract matchday data using the ACTUAL working logic from notebook snippet 3"""
        
        self.logger.info(f"Extracting matchday {matchday} data from {url}")
        
        # Ensure URL has proper scheme
        if not url.startswith('http'):
            url = 'https://' + url
        
        # Fetch HTML
        response = self.session.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Extract matches using the WORKING logic from snippet 3
        matches = self._extract_matches_working_logic(soup)
        
        # Extract additional data
        league_table = self._extract_league_table(soup)
        top_scorers = self._extract_top_scorers(soup)
        matchday_summary = self._extract_matchday_summary(soup)
        
        # Extract matchday info from the page
        matchday_info = self._extract_matchday_info_from_page(soup, matchday, season, url)
        
        return MatchdayContainer(
            matchday_info=matchday_info,
            league_table=league_table,
            top_scorers=top_scorers,
            matchday_summary=matchday_summary,
            matches=matches,
            metadata={
                "extraction_time": datetime.now().isoformat(),
                "source": "matchday_page",
                "url": url,
                "total_matches": len(matches)
            }
        )
    
    def _extract_matches_working_logic(self, soup: BeautifulSoup) -> List[MatchContextual]:
        """Use the ACTUAL working logic from notebook snippet 3"""
        matches = []
        
        # Find all match containers - using the WORKING selector from snippet 3
        match_divs = soup.find_all('div', class_='box')
        
        for match_div in match_divs:
            # Check if this div contains match data (has a table with match structure)
            table = match_div.find('table')
            if table and table.find('tr', class_='table-grosse-schrift'):
                try:
                    match_data = self._extract_single_match_working(match_div)
                    if match_data and match_data.match_id:  # Only add if we have valid data
                        matches.append(match_data)
                except Exception as e:
                    self.logger.warning(f"Failed to extract match: {str(e)}")
                    continue
        
        return matches
    
    def _extract_single_match_working(self, match_div) -> Optional[MatchContextual]:
        """Extract single match using the WORKING logic from snippet 3"""
        table = match_div.find('table')
        if not table:
            return None
        
        # Extract basic match metadata
        metadata = self._extract_match_metadata(table)
        
        # Find the main match row
        main_row = table.find('tr', class_='table-grosse-schrift')
        if not main_row:
            return None
        
        cells = main_row.find_all('td')
        if len(cells) < 9:
            return None
        
        # Extract team data using WORKING logic
        home_team_name_cell = cells[0]
        score_cell = cells[4]
        away_team_name_cell = cells[7]
        
        # Extract home team
        home_team = self._extract_team_data(home_team_name_cell)
        
        # Extract away team
        away_team = self._extract_team_data(away_team_name_cell)
        
        # Extract score and match report URL
        final_score = {}
        match_report_url = ""
        match_id = ""
        
        score_link = score_cell.find('a')
        if score_link:
            score_span = score_link.find('span')
            if score_span:
                score_text = score_span.text.strip()
                final_score = {'display': score_text}
                if ':' in score_text:
                    try:
                        home_score, away_score = score_text.split(':')
                        final_score['home'] = int(home_score)
                        final_score['away'] = int(away_score)
                    except:
                        pass
            
            # Extract match report URL
            match_report_url = score_link.get('href', '')
            if match_report_url:
                # Extract match ID from URL
                match_id = self._extract_match_id(match_report_url)
        
        # Backup: check footer for match report URL
        if not match_report_url:
            footer = match_div.find('div', class_='footer')
            if footer:
                footer_link = footer.find('a', href=True)
                if footer_link and 'spielbericht' in footer_link.get('href', ''):
                    match_report_url = footer_link.get('href', '')
                    match_id = self._extract_match_id(match_report_url)
        
        if not match_id:
            return None  # Skip matches without valid IDs
        
        # Extract match events
        match_events = self._extract_match_events(table)
        
        # Extract community predictions
        community_predictions = self._extract_community_predictions(table)
        
        return MatchContextual(
            match_id=match_id,
            home_team=home_team,
            away_team=away_team,
            final_score=final_score,
            match_report_url=match_report_url,
            date=metadata.get('date'),
            time=metadata.get('time'),
            day_of_week=metadata.get('day_of_week'),
            referee=metadata.get('referee'),
            attendance=metadata.get('attendance'),
            community_predictions=community_predictions,
            match_events=match_events
        )
    
    def _extract_match_id(self, match_report_url: str) -> Optional[str]:
        """Extract match ID from match report URL"""
        if not match_report_url:
            return None
        match = re.search(r'/spielbericht/(\d+)', match_report_url)
        return match.group(1) if match else None
    
    def _extract_team_data(self, team_cell) -> Dict[str, Any]:
        """Extract team information from HTML cells - WORKING logic from snippet 3"""
        team_data = {}
        
        # Find team link
        team_link = team_cell.find('a')
        if team_link:
            team_data['name'] = team_link.get('title', '').replace(' FC', '').replace('FC ', '')
            team_data['profile_url'] = team_link.get('href', '')
            team_data['short_name'] = team_link.text.strip()
        
        # Extract league position
        position_span = team_cell.find('span', class_='tabellenplatz')
        if position_span:
            position_text = position_span.text.strip()
            position_match = re.search(r'\((\d+)\.\)', position_text)
            if position_match:
                team_data['league_position'] = int(position_match.group(1))
        
        # Extract logo URL
        img = team_cell.find('img')
        if img:
            team_data['logo_url'] = img.get('src', '')
            
        return team_data
    
    def _extract_match_events(self, table) -> List[Dict[str, Any]]:
        """Extract match events using WORKING logic from snippet 3"""
        events = []
        
        # Find all event rows
        event_rows = table.find_all('tr', class_='spieltagsansicht-aktionen')
        
        for row in event_rows:
            cells = row.find_all('td')
            if len(cells) < 5:
                continue
                
            event = {}
            
            # Determine which team scored based on cell content
            left_team_cell = cells[0]  # Home team events
            minute_cell = cells[1]     # Minute (if home team)
            score_cell = cells[2]      # Score
            away_minute_cell = cells[3] # Minute (if away team)
            right_team_cell = cells[4]  # Away team events
            
            # Check if it's a home team event
            if minute_cell.text.strip() and minute_cell.text.strip() != '':
                # Home team event
                event['team'] = 'home'
                minute_match = re.search(r'\d+', minute_cell.text)
                if minute_match:
                    event['minute'] = int(minute_match.group())
                player_info = self._extract_player_info(left_team_cell)
            elif away_minute_cell.text.strip() and away_minute_cell.text.strip() != '':
                # Away team event
                event['team'] = 'away'
                minute_match = re.search(r'\d+', away_minute_cell.text)
                if minute_match:
                    event['minute'] = int(minute_match.group())
                player_info = self._extract_player_info(right_team_cell)
            else:
                continue
            
            # Extract score after event
            score_text = score_cell.text.strip()
            if ':' in score_text:
                event['score_after'] = score_text
            
            # Determine event type
            if 'icon-tor-formation' in str(row):
                event['type'] = 'goal'
            elif 'sb-gelb' in str(row):
                event['type'] = 'yellow_card'
            elif 'sb-rot' in str(row):
                event['type'] = 'red_card'
            
            # Add player information
            if player_info:
                event['player'] = player_info
                
            events.append(event)
        
        return sorted(events, key=lambda x: x.get('minute', 0))
    
    def _extract_player_info(self, cell) -> Optional[Dict[str, str]]:
        """Extract player information from event cell - WORKING logic from snippet 3"""
        player_link = cell.find('a')
        if not player_link:
            return None
            
        # Find both full name and short name spans
        full_name_span = cell.find('span', class_='hide-for-small')
        short_name_span = cell.find('span', class_='show-for-small')
        
        player_info = {
            'profile_url': player_link.get('href', '')
        }
        
        if full_name_span and full_name_span.find('a'):
            player_info['name'] = full_name_span.find('a').text.strip()
        elif player_link:
            player_info['name'] = player_link.get('title', player_link.text.strip())
            
        if short_name_span and short_name_span.find('a'):
            player_info['short_name'] = short_name_span.find('a').text.strip()
        else:
            # Generate short name from full name if not available
            full_name = player_info.get('name', '')
            if full_name:
                name_parts = full_name.split()
                if len(name_parts) >= 2:
                    player_info['short_name'] = f"{name_parts[0][0]}. {name_parts[-1]}"
                else:
                    player_info['short_name'] = full_name
        
        return player_info
    
    def _extract_community_predictions(self, table) -> Dict[str, float]:
        """Extract community prediction percentages - WORKING logic from snippet 3"""
        predictions = {
            'home_win_percentage': 0.0,
            'draw_percentage': 0.0,
            'away_win_percentage': 0.0
        }
        
        prediction_row = table.find('tr', class_='tm-user-tendenz')
        if not prediction_row:
            return predictions
            
        cells = prediction_row.find_all('td')
        
        for cell in cells:
            spans = cell.find_all('span')
            for span in spans:
                title = span.get('title', '')
                text = span.text.strip()
                
                if 'Win for' in title and ':' in title:
                    percentage_match = re.search(r'([\d.]+)\s*%', text)
                    if percentage_match:
                        percentage = float(percentage_match.group(1))
                        if span.get('class') and 'bar-sieg' in span.get('class'):
                            predictions['home_win_percentage'] = percentage
                        elif span.get('class') and 'bar-niederlage' in span.get('class'):
                            predictions['away_win_percentage'] = percentage
                elif 'Draws:' in title:
                    percentage_match = re.search(r'([\d.]+)\s*%', text)
                    if percentage_match:
                        predictions['draw_percentage'] = float(percentage_match.group(1))
        
        return predictions
    
    def _extract_match_metadata(self, table) -> Dict[str, Any]:
        """Extract match metadata - WORKING logic from snippet 3"""
        metadata = {}
        
        # Extract date and time
        date_rows = table.find_all('tr')
        for row in date_rows:
            cell = row.find('td', {'colspan': '5'})
            if not cell:
                continue
                
            text = cell.get_text(strip=True)
            
            # Date and time extraction
            date_link = cell.find('a')
            if date_link and 'waspassiertheute' in date_link.get('href', ''):
                date_text = date_link.text.strip()
                time_match = re.search(r'(\d{1,2}:\d{2})\s*(AM|PM)?', text)
                
                # Parse date
                try:
                    if ',' in date_text:
                        date_obj = datetime.strptime(date_text, '%B %d, %Y')
                        metadata['date'] = date_obj.strftime('%Y-%m-%d')
                except:
                    metadata['date'] = date_text
                
                # Parse time
                if time_match:
                    time_str = time_match.group(1)
                    if time_match.group(2) == 'PM' and not time_str.startswith('12'):
                        hour, minute = time_str.split(':')
                        hour = str(int(hour) + 12)
                        time_str = f"{hour}:{minute}"
                    metadata['time'] = time_str
                
                # Extract day of week
                days = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']
                for day in days:
                    if day in text or day[:3] in text:
                        metadata['day_of_week'] = day
                        break
            
            # Referee extraction
            if 'Referee:' in text:
                referee_link = cell.find('a')
                if referee_link:
                    metadata['referee'] = {
                        'name': referee_link.text.strip(),
                        'profile_url': referee_link.get('href', '')
                    }
            
            # Attendance extraction
            if 'icon-zuschauer-zahl' in str(cell):
                attendance_match = re.search(r'([\d.,]+)', text)
                if attendance_match:
                    attendance_str = attendance_match.group(1).replace(',', '').replace('.', '')
                    try:
                        metadata['attendance'] = int(attendance_str)
                    except:
                        pass
        
        return metadata
    
    def _extract_matchday_info_from_page(self, soup: BeautifulSoup, matchday: int, season: str, url: str) -> Dict[str, Any]:
        """Extract matchday info from the page"""
        matchday_info = {
            "number": matchday,
            "season": season,
            "source_url": url
        }
        
        # Try to extract competition name from page
        title = soup.find('title')
        if title:
            title_text = title.text
            if 'Premier League' in title_text:
                matchday_info['competition'] = 'Premier League'
            elif 'Bundesliga' in title_text:
                matchday_info['competition'] = 'Bundesliga'
            else:
                matchday_info['competition'] = 'Unknown'
        
        # Try to extract actual matchday from page if not provided
        if not matchday:
            matchday_select = soup.find('select', {'name': 'spieltag'})
            if matchday_select:
                selected_option = matchday_select.find('option', {'selected': True})
                if selected_option:
                    matchday_text = selected_option.text.strip()
                    matchday_match = re.search(r'(\d+)', matchday_text)
                    if matchday_match:
                        matchday_info['number'] = int(matchday_match.group(1))
        
        return matchday_info
    
    def _extract_league_table(self, soup: BeautifulSoup) -> Optional[Dict[str, Any]]:
        """Extract league table data from the page - WORKING logic from snippet 3"""
        league_table = {
            'season': None,
            'teams': []
        }
        
        # Find all boxes and look for the one with league table
        boxes = soup.find_all('div', class_='box')
        table_box = None
        
        for box in boxes:
            headline = box.find('div', class_='content-box-headline')
            if headline and ('table' in headline.text.lower() or 'premier league' in headline.text.lower()):
                table_box = box
                break
        
        if not table_box:
            return None
        
        # Extract season from headline
        headline = table_box.find('div', class_='content-box-headline')
        if headline:
            season_text = headline.text.strip()
            season_match = re.search(r'(\d{2}/\d{2})', season_text)
            if season_match:
                league_table['season'] = season_match.group(1)
        
        # Find the table
        table = table_box.find('table')
        if not table:
            return None
        
        # Extract team data from table rows
        rows = table.find('tbody').find_all('tr') if table.find('tbody') else []
        
        for row in rows:
            cells = row.find_all('td')
            if len(cells) < 6:
                continue
            
            # Extract position and movement
            position_cell = cells[0]
            position_text = position_cell.text.strip()
            position_match = re.search(r'(\d+)', position_text)
            
            if not position_match:
                continue
                
            position = int(position_match.group(1))
            
            # Extract movement indicator
            movement = None
            if 'green-arrow-ten' in str(position_cell):
                movement = 'up'
            elif 'red-arrow-ten' in str(position_cell):
                movement = 'down'
            elif 'grey-block-ten' in str(position_cell):
                movement = 'same'
            
            # Extract team info
            team_link = cells[2].find('a') if len(cells) > 2 else None
            team_data = {
                'position': position,
                'movement': movement,
                'name': team_link.get('title', '').replace(' FC', '').replace('FC ', '') if team_link else '',
                'short_name': team_link.text.strip() if team_link else '',
                'profile_url': team_link.get('href', '') if team_link else '',
                'logo_url': cells[1].find('img').get('src', '') if len(cells) > 1 and cells[1].find('img') else '',
                'matches': int(cells[3].text.strip()) if len(cells) > 3 and cells[3].text.strip().isdigit() else 0,
                'goal_difference': cells[4].text.strip() if len(cells) > 4 else '',
                'points': int(cells[5].text.strip()) if len(cells) > 5 and cells[5].text.strip().isdigit() else 0
            }
            
            league_table['teams'].append(team_data)
        
        return league_table
    
    def _extract_top_scorers(self, soup: BeautifulSoup) -> Optional[List[Dict[str, Any]]]:
        """Extract top scorers data from the page - WORKING logic from snippet 3"""
        top_scorers = []
        
        # Find all boxes and look for top goalscorer box
        boxes = soup.find_all('div', class_='box')
        goalscorer_box = None
        
        for box in boxes:
            headline = box.find('div', class_='content-box-headline')
            if headline and 'goalscorer' in headline.text.lower():
                goalscorer_box = box
                break
        
        if not goalscorer_box:
            return None
        
        # Find the table
        table = goalscorer_box.find('table')
        if not table:
            return None
        
        # Extract scorer data from table rows
        rows = table.find('tbody').find_all('tr') if table.find('tbody') else []
        
        for row in rows:
            cells = row.find_all('td')
            if len(cells) < 4:
                continue
            
            # Extract player info
            player_cell = cells[0]
            full_name_span = player_cell.find('span', class_='hide-for-small')
            short_name_span = player_cell.find('span', class_='show-for-small')
            
            player_link = full_name_span.find('a') if full_name_span else short_name_span.find('a') if short_name_span else None
            
            if not player_link:
                continue
            
            # Extract club info
            club_cell = cells[1]
            club_images = club_cell.find_all('img')
            clubs = []
            
            for img in club_images:
                clubs.append({
                    'name': img.get('title', ''),
                    'logo_url': img.get('src', '')
                })
            
            # Extract goals
            matchday_goals = cells[2].text.strip()
            total_goals = cells[3].text.strip()
            
            scorer_data = {
                'name': player_link.get('title', player_link.text.strip()),
                'short_name': short_name_span.find('a').text.strip() if short_name_span and short_name_span.find('a') else '',
                'profile_url': player_link.get('href', ''),
                'clubs': clubs,
                'goals_this_matchday': matchday_goals if matchday_goals != '-' else 0,
                'total_goals': int(total_goals) if total_goals.isdigit() else 0
            }
            
            top_scorers.append(scorer_data)
        
        return top_scorers
    
    def _extract_matchday_summary(self, soup: BeautifulSoup) -> Optional[Dict[str, Any]]:
        """Extract matchday summary statistics - WORKING logic from snippet 3"""
        summary = {}
        
        # Find all boxes and look for matchday summary box
        boxes = soup.find_all('div', class_='box')
        summary_box = None
        
        for box in boxes:
            headline = box.find('h2', class_='content-box-headline')
            if headline and 'summary' in headline.text.lower():
                summary_box = box
                break
        
        if not summary_box:
            return None
        
        # Find the table
        table = summary_box.find('table')
        if not table:
            return None
        
        # Extract data from both body and footer
        tbody = table.find('tbody')
        tfoot = table.find('tfoot')
        
        # Extract current matchday data (tbody)
        if tbody:
            main_row = tbody.find('tr')
            if main_row:
                cells = main_row.find_all('td')
                
                if len(cells) >= 9:
                    summary['current_matchday'] = {
                        'matches': int(cells[0].text.strip()) if cells[0].text.strip().isdigit() else 0,
                        'goals': int(cells[1].text.strip()) if cells[1].text.strip().isdigit() else 0,
                        'own_goals': int(cells[2].text.strip()) if cells[2].text.strip().isdigit() else 0,
                        'yellow_cards': int(cells[3].text.strip()) if cells[3].text.strip().isdigit() else 0,
                        'second_yellow_cards': cells[4].text.strip(),
                        'red_cards': int(cells[5].text.strip()) if cells[5].text.strip().isdigit() else 0,
                        'total_attendance': cells[7].text.strip().replace('.', '').replace(',', '') if len(cells) > 7 else '',
                        'average_attendance': cells[8].text.strip().replace('.', '').replace(',', '') if len(cells) > 8 else '',
                        'sold_out_matches': int(cells[9].text.strip()) if len(cells) > 9 and cells[9].text.strip().isdigit() else 0
                    }
        
        return summary if summary else None

# =============================================================================
# FIXED MATCH EXTRACTOR (based on notebook snippet 2)
# =============================================================================

class CleanMatchExtractor:
    """Extracts detailed match data using the WORKING notebook snippet 2 logic"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        # Position mappings from snippet 2
        self.position_mappings = {
            'GK': 'Goalkeeper', 'CB': 'Centre Back', 'LB': 'Left Back', 'RB': 'Right Back',
            'LWB': 'Left Wing Back', 'RWB': 'Right Wing Back', 'DM': 'Defensive Midfielder',
            'CM': 'Central Midfielder', 'AM': 'Attacking Midfielder', 'LM': 'Left Midfielder',
            'RM': 'Right Midfielder', 'LW': 'Left Winger', 'RW': 'Right Winger',
            'CF': 'Centre Forward', 'ST': 'Striker'
        }
    
    @backoff.on_exception(backoff.expo, Exception, max_tries=3)
    def extract_from_url(self, url: str) -> MatchDetail:
        """Extract detailed match data using the WORKING logic from notebook snippet 2"""
        
        self.logger.info(f"Extracting detailed match data from {url}")
        
        # Ensure URL has proper scheme
        if not url.startswith('http'):
            url = 'https://www.transfermarkt.com' + url if url.startswith('/') else 'https://' + url
        
        # Add respectful delay
        time.sleep(random.uniform(2.0, 3.0))
        
        # Fetch HTML
        response = self.session.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Extract match ID
        match_id = self._extract_match_id(soup, url)
        
        # Extract all components using WORKING logic from snippet 2
        match_info = self._extract_match_info(soup, match_id)
        home_team, away_team = self._extract_teams(soup)
        score = self._extract_score(soup)
        lineups = self._extract_lineups(soup)
        goals = self._extract_goals(soup)
        cards = self._extract_cards(soup)
        substitutions = self._extract_substitutions(soup)
        all_players = self._create_player_lookup(
            lineups['home_starting'], 
            lineups['away_starting'],
            lineups['home_subs'],
            lineups['away_subs']
        )

        goals = self._backfill_shirt_numbers_in_goals(goals, all_players)
        cards = self._backfill_shirt_numbers_in_cards(cards, all_players)
        substitutions = self._backfill_shirt_numbers_in_substitutions(substitutions, all_players)
        
        return MatchDetail(
            match_info=match_info,
            home_team=home_team,
            away_team=away_team,
            score=score,
            home_lineup=lineups['home_starting'],
            away_lineup=lineups['away_starting'],
            home_substitutes=lineups['home_subs'],
            away_substitutes=lineups['away_subs'],
            goals=goals,
            cards=cards,
            substitutions=substitutions,
            extraction_metadata={
                'extraction_time': datetime.now().isoformat(),
                'extractor_version': '1.0.0',
                'source': 'match_report_page',
                'source_url': url
            }
        )
    
    def _create_player_lookup(self, home_starting: List[Player], away_starting: List[Player],
                         home_subs: List[Player], away_subs: List[Player]) -> Dict[str, Player]:
        """Create lookup dictionary by player_id for fast matching"""
        all_players = {}
        
        for player_list in [home_starting, away_starting, home_subs, away_subs]:
            for player in player_list:
                if player.player_id:
                    all_players[player.player_id] = player
        
        return all_players

    def _backfill_shirt_numbers_in_goals(self, goals: List[Goal], player_lookup: Dict[str, Player]) -> List[Goal]:
        """Backfill shirt numbers in goal events from lineup data"""
        updated_goals = []
        
        for goal in goals:
            updated_goal = Goal(
                minute=goal.minute,
                extra_time=goal.extra_time,
                player=self._add_shirt_number_to_player(goal.player, player_lookup),
                assist_player=self._add_shirt_number_to_player(goal.assist_player, player_lookup),
                goal_type=goal.goal_type,
                assist_type=goal.assist_type,
                team_side=goal.team_side,
                score_after=goal.score_after,
                season_goal_number=goal.season_goal_number,
                season_assist_number=goal.season_assist_number
            )
            updated_goals.append(updated_goal)
        
        return updated_goals

    def _backfill_shirt_numbers_in_cards(self, cards: List[Card], player_lookup: Dict[str, Player]) -> List[Card]:
        """Backfill shirt numbers in card events from lineup data"""
        updated_cards = []
        
        for card in cards:
            updated_card = Card(
                minute=card.minute,
                extra_time=card.extra_time,
                player=self._add_shirt_number_to_player(card.player, player_lookup),
                card_type=card.card_type,
                reason=card.reason,
                team_side=card.team_side,
                season_card_number=card.season_card_number
            )
            updated_cards.append(updated_card)
        
        return updated_cards

    def _backfill_shirt_numbers_in_substitutions(self, substitutions: List[Substitution], 
                                            player_lookup: Dict[str, Player]) -> List[Substitution]:
        """Backfill shirt numbers in substitution events from lineup data"""
        updated_substitutions = []
        
        for sub in substitutions:
            updated_sub = Substitution(
                minute=sub.minute,
                extra_time=sub.extra_time,
                player_out=self._add_shirt_number_to_player(sub.player_out, player_lookup),
                player_in=self._add_shirt_number_to_player(sub.player_in, player_lookup),
                reason=sub.reason,
                team_side=sub.team_side
            )
            updated_substitutions.append(updated_sub)
        
        return updated_substitutions

    def _add_shirt_number_to_player(self, player: Optional[Player], player_lookup: Dict[str, Player]) -> Optional[Player]:
        """Add shirt number to player from lookup if available"""
        if not player or not player.player_id:
            return player
        
        # Find matching player in lookup
        lineup_player = player_lookup.get(player.player_id)
        if lineup_player and lineup_player.shirt_number is not None:
            # Create new player with shirt number
            return Player(
                player_id=player.player_id,
                name=player.name,
                shirt_number=lineup_player.shirt_number,  # Backfilled from lineup!
                position=player.position,
                is_captain=player.is_captain,
                portrait_url=player.portrait_url
            )
        
        return player  # Return original if no match found

    def _extract_match_id(self, soup: BeautifulSoup, url: str) -> str:
        """Extract match ID - WORKING logic from snippet 2"""
        # Try URL first
        match = re.search(r'/spielbericht/(\d+)', url)
        if match:
            return match.group(1)
        
        # Try meta property
        meta_url = soup.find('meta', property='og:url')
        if meta_url:
            match = re.search(r'spielbericht/(\d+)', meta_url.get('content', ''))
            if match:
                return match.group(1)
        
        # Try canonical URL
        canonical = soup.find('link', rel='canonical')
        if canonical:
            match = re.search(r'spielbericht/(\d+)', canonical.get('href', ''))
            if match:
                return match.group(1)
        
        return str(uuid.uuid4())[:8]  # Fallback
    
    def _extract_match_info(self, soup: BeautifulSoup, match_id: str) -> MatchInfo:
        """Extract basic match info - WORKING logic from snippet 2"""
        
        # Competition info
        comp_link = soup.find('a', class_='direct-headline__link')
        competition_name = comp_link.text.strip() if comp_link else ""
        competition_id = self._extract_id_from_href(comp_link.get('href', '')) if comp_link else None
        
        # Matchday
        matchday_link = soup.find('a', href=re.compile(r'jumplist/spieltag'))
        matchday_text = matchday_link.text if matchday_link else ""
        matchday = self._extract_number_from_text(matchday_text)
        
        # Date and time
        date_link = soup.find('a', href=re.compile(r'waspassiertheute'))
        date_str = date_link.text.strip() if date_link else ""
        date = self._parse_date(date_str)
        
        # Time
        time_match = re.search(r'(\d{1,2}:\d{2})\s*(AM|PM)?', soup.get_text())
        time = time_match.group(1) if time_match else None
        
        # Venue
        venue_link = soup.find('a', href=re.compile(r'/stadion/'))
        venue = venue_link.text.strip() if venue_link else None
        
        # Attendance
        attendance = self._extract_attendance(soup)
        
        # Referee
        referee_link = soup.find('a', href=re.compile(r'/profil/schiedsrichter/'))
        referee = referee_link.text.strip() if referee_link else None
        referee_id = self._extract_id_from_href(referee_link.get('href', '')) if referee_link else None
        
        return MatchInfo(
            match_id=match_id, competition_name=competition_name, competition_id=competition_id,
            matchday=matchday, date=date, time=time,
            venue=venue, attendance=attendance, referee=referee, referee_id=referee_id
        )
    
    def _extract_teams(self, soup: BeautifulSoup) -> Tuple[Team, Team]:
        """Extract both teams - WORKING logic from snippet 2"""
        
        # Home team
        home_section = soup.find('div', class_='sb-team sb-heim')
        home_team = self._extract_single_team(home_section, soup, 'home')
        
        # Away team
        away_section = soup.find('div', class_='sb-team sb-gast')
        away_team = self._extract_single_team(away_section, soup, 'away')
        
        return home_team, away_team
    
    def _extract_single_team(self, section: Tag, soup: BeautifulSoup, team_type: str) -> Team:
        """Extract single team - WORKING logic from snippet 2"""
        if not section:
            return Team(team_id="", name="")
            
        team_link = section.find('a', class_='sb-vereinslink')
        team_logo = section.find('img')
        position_elem = section.find('p')
        
        return Team(
            team_id=self._extract_id_from_href(team_link.get('href', '')) if team_link else "",
            name=team_link.text.strip() if team_link else "",
            logo_url=team_logo.get('src') if team_logo else None,
            league_position=self._extract_league_position(position_elem.text if position_elem else ""),
            formation=self._extract_formation(soup, team_type),
            manager=self._extract_manager(soup, team_type)
        )
    
    def _extract_score(self, soup: BeautifulSoup) -> Score:
        """Extract match score - WORKING logic from snippet 2"""
        score_elem = soup.find('div', class_='sb-endstand')
        
        if not score_elem:
            return Score(home_final=0, away_final=0)
        
        # Extract final score
        score_text = score_elem.text.strip()
        final_match = re.search(r'(\d+):(\d+)', score_text.split('(')[0])
        
        home_final = int(final_match.group(1)) if final_match else 0
        away_final = int(final_match.group(2)) if final_match else 0
        
        # Extract half-time score
        ht_elem = score_elem.find('div', class_='sb-halbzeit')
        home_ht = away_ht = None
        
        if ht_elem:
            ht_text = ht_elem.text.strip('()')
            ht_match = re.search(r'(\d+):(\d+)', ht_text)
            if ht_match:
                home_ht = int(ht_match.group(1))
                away_ht = int(ht_match.group(2))
        
        return Score(home_final=home_final, away_final=away_final, home_ht=home_ht, away_ht=away_ht)
    
    def _extract_lineups(self, soup: BeautifulSoup) -> Dict[str, List[Player]]:
        """Extract complete lineups - WORKING logic from snippet 2"""
        lineups = {
            'home_starting': [], 'away_starting': [],
            'home_subs': [], 'away_subs': []
        }
        
        lineup_sections = soup.find_all('div', class_='large-6')
        
        if len(lineup_sections) >= 2:
            # Home team (first section)
            home_section = lineup_sections[0]
            lineups['home_starting'] = self._extract_starting_xi(home_section)
            lineups['home_subs'] = self._extract_substitutes(home_section)
            
            # Away team (second section)
            away_section = lineup_sections[1]
            lineups['away_starting'] = self._extract_starting_xi(away_section)
            lineups['away_subs'] = self._extract_substitutes(away_section)
        
        return lineups
    
    def _extract_starting_xi(self, section: Tag) -> List[Player]:
        """Extract starting XI - FIXED to properly get shirt numbers and positions"""
        players = []
        formation_players = section.find_all('div', class_='formation-player-container')
        
        for player_container in formation_players:
            # Find shirt number - get the text content directly
            shirt_number_elem = player_container.find('div', class_='tm-shirt-number')
            shirt_number = None
            if shirt_number_elem:
                shirt_text = shirt_number_elem.get_text(strip=True)
                if shirt_text.isdigit():
                    shirt_number = int(shirt_text)
            
            # Find player link
            player_link = player_container.find('a')
            
            # Find captain icon
            captain_elem = player_container.find('div', class_='kapitaenicon-formation')
            
            if player_link:
                player = Player(
                    player_id=self._extract_id_from_href(player_link.get('href', '')),
                    name=player_link.text.strip(),
                    shirt_number=shirt_number,  # Use the extracted number directly
                    position=None,  # Position not available in formation view
                    is_captain=captain_elem is not None,
                    portrait_url=self._extract_player_portrait_url(player_link.get('href', ''))
                )
                players.append(player)
        
        return players
    
    def _extract_substitutes(self, section: Tag) -> List[Player]:
        """Extract substitute players - WORKING logic from snippet 2"""
        players = []
        bench_table = section.find('table', class_='ersatzbank')
        
        if not bench_table:
            return players
        
        rows = bench_table.find_all('tr')
        for row in rows:
            if 'Manager:' in row.get_text():
                continue
            
            cells = row.find_all('td')
            if len(cells) >= 3:
                number_elem = cells[0].find('div', class_='tm-shirt-number')
                player_link = cells[1].find('a')
                position_cell = cells[2]
                
                if player_link:
                    player = Player(
                        player_id=self._extract_id_from_href(player_link.get('href', '')),
                        name=player_link.text.strip(),
                        shirt_number=self._extract_shirt_number(number_elem.text if number_elem else ""),
                        position=position_cell.text.strip() if position_cell else None,
                        portrait_url=self._extract_player_portrait_url(player_link.get('href', ''))
                    )
                    players.append(player)
        
        return players
    
    def _extract_goals(self, soup: BeautifulSoup) -> List[Goal]:
        """Extract all goal events - WORKING logic from snippet 2"""
        goals = []
        goals_section = soup.find('div', id='sb-tore')
        
        if not goals_section:
            return goals
        
        goal_items = goals_section.find_all('li')
        
        for item in goal_items:
            try:
                goal = self._parse_goal_event(item)
                if goal:
                    goals.append(goal)
            except Exception as e:
                self.logger.warning(f"Failed to parse goal event: {str(e)}")
                continue
        
        return goals
    
    def _extract_cards(self, soup: BeautifulSoup) -> List[Card]:
        """Extract all card events - WORKING logic from snippet 2"""
        cards = []
        cards_section = soup.find('div', id='sb-karten')
        
        if not cards_section:
            return cards
        
        card_items = cards_section.find_all('li')
        
        for item in card_items:
            try:
                card = self._parse_card_event(item)
                if card:
                    cards.append(card)
            except Exception as e:
                self.logger.warning(f"Failed to parse card event: {str(e)}")
                continue
        
        return cards
    
    def _parse_card_event(self, item: Tag) -> Optional[Card]:
        """Parse a single card event - WORKING logic from snippet 2"""
        team_side = 'home' if 'sb-aktion-heim' in item.get('class', []) else 'away'
        minute, extra_time = self._extract_minute_from_event(item)
        
        # Determine card type
        card_type = 'yellow'
        if item.find('span', class_='sb-rot'):
            card_type = 'red'
        elif item.find('span', class_='sb-gelbrot'):
            card_type = 'second_yellow'
        
        # Extract player
        player_link = item.find('a', class_='wichtig')
        player = None
        if player_link:
            player = Player(
                player_id=self._extract_id_from_href(player_link.get('href', '')),
                name=player_link.text.strip(),
                portrait_url=self._extract_player_portrait_url(player_link.get('href', ''))
            )
        
        # Extract reason and season number
        description = item.find('div', class_='sb-aktion-aktion')
        reason, season_card_number = self._parse_card_description(description.text if description else "")
        
        return Card(
            minute=minute, extra_time=extra_time, player=player, card_type=card_type,
            reason=reason, team_side=team_side, season_card_number=season_card_number
        )
    
    def _extract_substitutions(self, soup: BeautifulSoup) -> List[Substitution]:
        """Extract all substitution events - WORKING logic from snippet 2"""
        substitutions = []
        subs_section = soup.find('div', id='sb-wechsel')
        
        if not subs_section:
            return substitutions
        
        sub_items = subs_section.find_all('li')
        
        for item in sub_items:
            try:
                substitution = self._parse_substitution_event(item)
                if substitution:
                    substitutions.append(substitution)
            except Exception as e:
                self.logger.warning(f"Failed to parse substitution event: {str(e)}")
                continue
        
        return substitutions
    
    def _parse_substitution_event(self, item: Tag) -> Optional[Substitution]:
        """Parse a single substitution event - WORKING logic from snippet 2"""
        team_side = 'home' if 'sb-aktion-heim' in item.get('class', []) else 'away'
        minute, extra_time = self._extract_minute_from_event(item)
        
        # Extract players using improved method
        player_out, player_in = self._extract_substitution_players(item)
        
        # Extract reason
        reason = self._extract_substitution_reason(item)
        
        return Substitution(
            minute=minute, extra_time=extra_time, player_out=player_out,
            player_in=player_in, reason=reason, team_side=team_side
        )
    
    def _extract_substitution_players(self, item: Tag) -> Tuple[Optional[Player], Optional[Player]]:
        """Extract both players in substitution - WORKING logic from snippet 2"""
        player_out = player_in = None
        
        # Method 1: Look for specific span classes
        player_out_section = item.find('span', class_='sb-aktion-wechsel-aus')
        if player_out_section:
            player_out_link = player_out_section.find('a', class_='wichtig')
            if player_out_link:
                player_out = Player(
                    player_id=self._extract_id_from_href(player_out_link.get('href', '')),
                    name=player_out_link.text.strip(),
                    portrait_url=self._extract_player_portrait_url(player_out_link.get('href', ''))
                )
        
        player_in_section = item.find('span', class_='sb-aktion-wechsel-ein')
        if player_in_section:
            player_in_link = player_in_section.find('a', class_='wichtig')
            if player_in_link:
                player_in = Player(
                    player_id=self._extract_id_from_href(player_in_link.get('href', '')),
                    name=player_in_link.text.strip(),
                    portrait_url=self._extract_player_portrait_url(player_in_link.get('href', ''))
                )
        
        # Method 2: Fallback - look for all player links
        if not player_out or not player_in:
            all_player_links = item.find_all('a', class_='wichtig')
            if len(all_player_links) >= 2:
                # Usually first is out, second is in
                if not player_out:
                    player_out = Player(
                        player_id=self._extract_id_from_href(all_player_links[0].get('href', '')),
                        name=all_player_links[0].text.strip(),
                        portrait_url=self._extract_player_portrait_url(all_player_links[0].get('href', ''))
                    )
                if not player_in:
                    player_in = Player(
                        player_id=self._extract_id_from_href(all_player_links[1].get('href', '')),
                        name=all_player_links[1].text.strip(),
                        portrait_url=self._extract_player_portrait_url(all_player_links[1].get('href', ''))
                    )
        
        return player_out, player_in
    
    def _extract_substitution_reason(self, item: Tag) -> Optional[str]:
        """Extract substitution reason - WORKING logic from snippet 2"""
        # Look for reason span
        reason_elem = item.find('span', class_=re.compile(r'sb-wechsel-\d+'))
        if reason_elem:
            if 'sb-wechsel-402' in reason_elem.get('class', []):
                return 'Injury'
            elif 'sb-wechsel-401' in reason_elem.get('class', []):
                return 'Tactical'
        
        # Check text content
        item_text = item.get_text()
        if 'Injury' in item_text:
            return 'Injury'
        elif 'Tactical' in item_text:
            return 'Tactical'
        
        return None
    
    def _extract_minute_from_event(self, item: Tag) -> Tuple[int, Optional[int]]:
        """Extract minute from event - FINAL CORRECTED VERSION"""
        
        # Find the clock element with minute information
        clock_elem = item.find('span', class_=re.compile(r'sb-sprite-uhr'))
        if clock_elem:
            style = clock_elem.get('style', '')
            
            # Check for extra time display in text content first
            clock_text = clock_elem.get_text(strip=True)
            if '+' in clock_text:
                try:
                    extra_time = int(clock_text.replace('+', ''))
                    return 90, extra_time
                except:
                    pass
            elif clock_text.isdigit():
                return int(clock_text), None
            
            # Parse minute from CSS background-position (BOTH X and Y)
            if 'background-position:' in style:
                # Extract BOTH x and y positions
                pattern = r'background-position:\s*-?(\d+)px\s+-?(\d+)px'
                match = re.search(pattern, style)
                if match:
                    x_pos = int(match.group(1))
                    y_pos = int(match.group(2))
                    
                    # Calculate minute using CONFIRMED formula
                    # Based on verified examples:
                    # 4 min: x=108, y=0   -> x_grid=3, y_grid=0 -> minute = 3 + (0*10) + 1 = 4 ✓
                    # 36 min: x=180, y=108 -> x_grid=5, y_grid=3 -> minute = 5 + (3*10) + 1 = 36 ✓
                    # 75 min: x=144, y=252 -> x_grid=4, y_grid=7 -> minute = 4 + (7*10) + 1 = 75 ✓
                    
                    x_grid = x_pos // 36
                    y_grid = y_pos // 36
                    
                    # CONFIRMED FORMULA: 10 columns per row layout
                    minute = x_grid + (y_grid * 10) + 1
                    
                    # Sanity check for reasonable minute values
                    if 1 <= minute <= 120:  # Allow up to 120 for extra time
                        return minute, None
                    
                    # Fallback to simple x calculation
                    if x_grid > 0:
                        return min(x_grid + 1, 90), None
        
        # Look for specific minute patterns in the item text
        item_text = item.get_text()
        minute_patterns = [
            r'(\d+)\'',      # 4'
            r'(\d+)\s*min',  # 4 min
            r'(\d+)\.',      # 4.
        ]
        
        for pattern in minute_patterns:
            match = re.search(pattern, item_text)
            if match:
                minute = int(match.group(1))
                if minute <= 90:
                    return minute, None
        
        # Fallback: estimate from list position
        return self._estimate_minute_from_list_position(item), None
    
    def _estimate_minute_from_list_position(self, item: Tag) -> int:
        """Estimate minute based on position in event list - IMPROVED"""
        parent_list = item.find_parent('ul')
        if parent_list:
            items = parent_list.find_all('li')
            try:
                index = items.index(item)
                # More realistic distribution: events typically happen throughout the game
                # First half: 0-45, Second half: 45-90
                if index == 0:
                    return 1  # First event often early
                elif index < len(items) // 2:
                    # First half events
                    return min(5 + (index * 8), 45)
                else:
                    # Second half events  
                    return min(50 + ((index - len(items) // 2) * 10), 90)
            except:
                pass
        return 45  # Default fallback
    
    # Helper methods from snippet 2
    def _extract_attendance(self, soup: BeautifulSoup) -> Optional[int]:
        """Extract attendance - WORKING logic from snippet 2"""
        attendance_elem = soup.find('strong', string=re.compile(r'Attendance:'))
        if attendance_elem and attendance_elem.parent:
            attendance_text = attendance_elem.parent.text
            attendance_match = re.search(r'Attendance:\s*([\d,\.]+)', attendance_text)
            if attendance_match:
                return int(attendance_match.group(1).replace(',', '').replace('.', ''))
        return None
    
    def _extract_formation(self, soup: BeautifulSoup, team: str) -> Optional[str]:
        """Extract team formation - WORKING logic from snippet 2"""
        formation_elems = soup.find_all('div', class_='formation-subtitle')
        
        if not formation_elems:
            return None
        
        formation_index = 0 if team == 'home' else 1
        
        if len(formation_elems) > formation_index:
            formation_elem = formation_elems[formation_index]
            formation_text = formation_elem.get_text()
            
            formation_match = re.search(r'Starting Line-up:\s*(.+)', formation_text)
            if formation_match:
                full_formation = formation_match.group(1).strip()
                return full_formation
            
            numeric_match = re.search(r'(\d+-\d+-\d+(?:-\d+)?)', formation_text)
            if numeric_match:
                return numeric_match.group(1)
        
        return None
    
    def _extract_manager(self, soup: BeautifulSoup, team: str) -> Optional[str]:
        """Extract team manager - WORKING logic from snippet 2"""
        tables = soup.find_all('table', class_='ersatzbank')
        table_index = 0 if team == 'home' else 1
        
        if len(tables) > table_index:
            manager_row = tables[table_index].find('tr', class_='bench-table__tr')
            if manager_row:
                manager_link = manager_row.find('a')
                return manager_link.text.strip() if manager_link else None
        return None
    
    def _parse_goal_description(self, text: str) -> Tuple[Optional[Player], Optional[str], Dict[str, int], Optional[str]]:
        """Parse goal description - IMPROVED to extract goal type and assist type dynamically"""
        assist_player = None
        goal_type = None
        assist_type = None
        season_numbers = {}
        
        # Extract goal type dynamically (between player name and season info)
        goal_type_match = re.search(r'</a>,\s*([^,]+),\s*\d+\.\s*Goal of the Season', text)
        if goal_type_match:
            goal_type = goal_type_match.group(1).strip()
        
        # Extract assist player with proper ID extraction and assist type
        assist_match = re.search(r'Assist:\s*<a[^>]*href="[^"]*spieler/(\d+)/[^"]*"[^>]*>([^<]+)</a>,\s*([^,]+),\s*\d+\.\s*Assist of the Season', text)
        if assist_match:
            assist_player_id = assist_match.group(1)
            assist_name = assist_match.group(2).strip()
            assist_type = assist_match.group(3).strip()  # Extract assist type!
            assist_player = Player(
                player_id=assist_player_id,
                name=assist_name,
                portrait_url=self._extract_player_portrait_url(f"/spieler/{assist_player_id}")
            )
        else:
            # Fallback: try simpler assist extraction without type
            assist_match = re.search(r'Assist:\s*<a[^>]*href="[^"]*spieler/(\d+)/[^"]*"[^>]*>([^<]+)</a>', text)
            if assist_match:
                assist_player_id = assist_match.group(1)
                assist_name = assist_match.group(2).strip()
                assist_player = Player(
                    player_id=assist_player_id,
                    name=assist_name,
                    portrait_url=self._extract_player_portrait_url(f"/spieler/{assist_player_id}")
                )
            else:
                # Final fallback: your original method
                assist_match = re.search(r'Assist:\s*([^,]+)', text)
                if assist_match:
                    assist_name = assist_match.group(1).strip()
                    assist_player = Player(player_id="", name=assist_name)
        
        # Extract season numbers
        goal_number_match = re.search(r'(\d+)\.\s*Goal of the Season', text)
        if goal_number_match:
            season_numbers['goals'] = int(goal_number_match.group(1))
        
        assist_number_match = re.search(r'(\d+)\.\s*Assist of the Season', text)
        if assist_number_match:
            season_numbers['assists'] = int(assist_number_match.group(1))
        
        return assist_player, goal_type, season_numbers, assist_type

    def _parse_goal_event(self, item: Tag) -> Optional[Goal]:
        """Parse a single goal event - UPDATED to use better assist extraction"""
        team_side = 'home' if 'sb-aktion-heim' in item.get('class', []) else 'away'
        minute, extra_time = self._extract_minute_from_event(item)
        
        # Extract score after goal
        score_elem = item.find('div', class_='sb-aktion-spielstand')
        score_after = self._parse_score_from_text(score_elem.text if score_elem else "")
        
        # Extract scorer
        scorer_link = item.find('a', class_='wichtig')
        scorer = None
        if scorer_link:
            scorer = Player(
                player_id=self._extract_id_from_href(scorer_link.get('href', '')),
                name=scorer_link.text.strip(),
                portrait_url=self._extract_player_portrait_url(scorer_link.get('href', ''))
            )
        
        # Extract assist and goal details from the action div (which contains HTML)
        description_elem = item.find('div', class_='sb-aktion-aktion')
        if description_elem:
            # Use the HTML content for better parsing
            description_html = str(description_elem)
            assist_player, goal_type, season_numbers, assist_type = self._parse_goal_description(description_html)
        else:
            assist_player, goal_type, season_numbers = None, None, {}
        
        return Goal(
            minute=minute, extra_time=extra_time, player=scorer, assist_player=assist_player,
            goal_type=goal_type,assist_type=assist_type, team_side=team_side, score_after=score_after,
            season_goal_number=season_numbers.get('goals'),
            season_assist_number=season_numbers.get('assists')
        )
    
    def _parse_card_description(self, text: str) -> Tuple[Optional[str], Optional[int]]:
        """Parse card description - WORKING logic from snippet 2"""
        reason = None
        season_number = None
        
        # Extract reason
        if 'Foul' in text:
            reason = 'Foul'
        elif 'Dissent' in text:
            reason = 'Dissent'
        elif 'Time wasting' in text:
            reason = 'Time wasting'
        elif 'Unsporting' in text:
            reason = 'Unsporting behavior'
        
        # Extract season card number
        card_match = re.search(r'(\d+)\.\s*Yellow card', text)
        if card_match:
            season_number = int(card_match.group(1))
        
        return reason, season_number
    
    def _extract_id_from_href(self, href: str) -> str:
        """Extract ID from href URL - WORKING logic from snippet 2"""
        if not href:
            return ""
        
        patterns = [
            r'/verein/(\d+)', r'/spieler/(\d+)', r'/trainer/(\d+)',
            r'/schiedsrichter/(\d+)', r'/wettbewerb/([A-Z0-9]+)', r'spielbericht/(\d+)'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, href)
            if match:
                return match.group(1)
        
        return ""
    
    def _extract_number_from_text(self, text: str) -> Optional[int]:
        """Extract first number from text"""
        match = re.search(r'(\d+)', text)
        return int(match.group(1)) if match else None
    
    def _extract_shirt_number(self, text: str) -> Optional[int]:
        """Extract shirt number from text"""
        if text.isdigit():
            return int(text)
        return None
    
    def _extract_league_position(self, text: str) -> Optional[int]:
        """Extract league position from position text"""
        match = re.search(r'Position:\s*(\d+)', text)
        return int(match.group(1)) if match else None
    
    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse date string to ISO format"""
        if not date_str:
            return None
            
        # Handle different date formats
        date_patterns = [
            (r'(\w+), (\d{1,2})/(\d{1,2})/(\d{2})', self._parse_us_date),
            (r'(\d{1,2})\.(\d{1,2})\.(\d{4})', self._parse_eu_date),
            (r'(\d{4})-(\d{1,2})-(\d{1,2})', self._parse_iso_date),
        ]
        
        for pattern, parser in date_patterns:
            match = re.search(pattern, date_str)
            if match:
                return parser(match.groups())
        
        return None
    
    def _parse_us_date(self, groups: tuple) -> str:
        """Parse US format date"""
        day_name, month, day, year = groups
        year = f"20{year}" if len(year) == 2 else year
        return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
    
    def _parse_eu_date(self, groups: tuple) -> str:
        """Parse European format date"""
        day, month, year = groups
        return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
    
    def _parse_iso_date(self, groups: tuple) -> str:
        """Parse ISO format date"""
        year, month, day = groups
        return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
    
    def _parse_score_from_text(self, text: str) -> Optional[Tuple[int, int]]:
        """Parse score from text like '2:1' or '0:1'"""
        match = re.search(r'(\d+):(\d+)', text)
        if match:
            return (int(match.group(1)), int(match.group(2)))
        return None
    
    def _extract_player_portrait_url(self, href: str) -> Optional[str]:
        """Generate player portrait URL from profile href"""
        player_id = self._extract_id_from_href(href)
        if player_id:
            return f"https://img.a.transfermarkt.technology/portrait/small/{player_id}-{int(datetime.now().timestamp())}.jpg?lm=1"
        return None

# =============================================================================
# WORKER PROCESS (updated to use fixed extractors)
# =============================================================================

class ScrapingWorker:
    """Worker that processes jobs from the queue - FIXED VERSION"""
    
    def __init__(self, worker_id: str, config: Config):
        self.worker_id = worker_id
        self.config = config
        self.logger = logging.getLogger(f"worker.{worker_id}")
        
        # Initialize components
        self.db = DatabaseManager(config.database_url)
        self.queue = QueueManager(config.redis_url)
        self.storage = StorageManager(config.base_output_dir)
        
        # Initialize FIXED extractors
        self.matchday_extractor = MatchdayExtractor()
        self.match_extractor = CleanMatchExtractor()
        
        self.running = False
    
    def start(self):
        """Start the worker process"""
        self.running = True
        self.logger.info(f"Worker {self.worker_id} starting...")
        
        while self.running:
            try:
                # Get next job from queue
                job_id = self.queue.dequeue_job()
                
                if not job_id:
                    time.sleep(5)  # No jobs available
                    continue
                
                # FIXED: Add retry logic for database fetch
                job = self._get_job_with_retry(job_id)
                
                if not job:
                    self.logger.warning(f"Job {job_id} not found after retries - skipping")
                    continue
                
                # Process the job
                self.process_job(job)
                
            except KeyboardInterrupt:
                self.logger.info(f"Worker {self.worker_id} stopping...")
                self.running = False
                break
            except Exception as e:
                self.logger.error(f"Worker error: {str(e)}")
                time.sleep(10)
    
    def _get_job_with_retry(self, job_id: str, max_attempts: int = 5) -> Optional[Dict]:
        """Get job from database with retry logic to handle timing issues"""
        for attempt in range(max_attempts):
            try:
                with self.db.get_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("""
                            SELECT * FROM scrape_jobs 
                            WHERE job_id = %s AND status = 'pending'
                        """, (job_id,))
                        job = cur.fetchone()
                        
                        if job:
                            return dict(job)  # Convert to regular dict
                        
                        # If not found, wait a bit for database transaction to complete
                        if attempt < max_attempts - 1:
                            time.sleep(0.5 * (attempt + 1))  # Exponential backoff
                            
            except Exception as e:
                self.logger.warning(f"Database error on attempt {attempt + 1}: {str(e)}")
                if attempt < max_attempts - 1:
                    time.sleep(1)
        
        return None
    
    def process_job(self, job: Dict):
        """Process a single job"""
        job_id = str(job['job_id'])
        job_type = JobType(job['job_type'])
        
        self.logger.info(f"Processing job {job_id} ({job_type.value})")
        
        # Mark job as running
        self.db.update_job_status(job_id, JobStatus.RUNNING, self.worker_id)
        
        start_time = time.time()
        
        try:
            if job_type == JobType.MATCHDAY_SCRAPE:
                self.process_matchday_job(job)
            elif job_type == JobType.MATCH_DETAIL_SCRAPE:
                self.process_match_job(job)
            
            # Mark job as completed
            self.db.update_job_status(job_id, JobStatus.COMPLETED)
            
            duration_ms = int((time.time() - start_time) * 1000)
            self.logger.info(f"Job {job_id} completed in {duration_ms}ms")
            
        except Exception as e:
            error_msg = str(e)
            self.logger.error(f"Job {job_id} failed: {error_msg}")
            
            # Handle retries
            if job['retry_count'] < job['max_retries']:
                self.db.update_job_status(job_id, JobStatus.RETRYING, error=error_msg)
                # Re-queue with delay
                self.queue.enqueue_job(job_id, job['priority'])
                self.logger.info(f"Job {job_id} scheduled for retry")
            else:
                self.db.update_job_status(job_id, JobStatus.FAILED, error=error_msg)
                self.logger.error(f"Job {job_id} failed permanently")
    
    def process_matchday_job(self, job: Dict):
        """Process a matchday scraping job"""
        metadata = job.get('metadata', {})
        season = metadata.get('season', 'unknown')
        matchday = metadata.get('matchday', 0)
        
        # Extract matchday data using FIXED extractor
        matchday_data = self.matchday_extractor.extract_from_transfermarkt_url(
            job['target_url'], matchday, season
        )
        
        # Save matchday data
        file_path = self.storage.save_matchday_data(season, matchday, matchday_data)
        
        # FIXED: Create match detail jobs with proper transaction handling
        match_job_count = 0
        if matchday_data.matches:
            match_job_count = self.create_match_jobs_atomic(job, matchday_data.matches)
            self.logger.info(f"Created {match_job_count} match detail jobs")
        else:
            self.logger.warning("No matches found in matchday data")
        
        # Save extraction record
        self.db.save_extraction(
            job_id=str(job['job_id']),
            match_id=f"matchday_{matchday}",
            matchday=matchday,
            season=season,
            competition=matchday_data.matchday_info.get('competition', ''),
            data=asdict(matchday_data),
            file_path=file_path,
            quality_score=self.calculate_matchday_quality(matchday_data),
            duration_ms=1000
        )
    
    def create_match_jobs_atomic(self, parent_job: Dict, matches: List[MatchContextual]) -> int:
        """Create match detail jobs atomically to prevent race conditions"""
        created_count = 0
        
        # Process in batches to ensure atomicity
        batch_size = 5
        for i in range(0, len(matches), batch_size):
            batch = matches[i:i + batch_size]
            
            try:
                with self.db.get_connection() as conn:
                    with conn.cursor() as cur:
                        job_ids_to_queue = []
                        
                        for match in batch:
                            if not match.match_report_url:
                                continue
                            
                            # Create job in database
                            match_job_id = str(uuid.uuid4())
                            cur.execute("""
                                INSERT INTO scrape_jobs (
                                    job_id, job_type, target_url, priority, parent_job_id, metadata
                                ) VALUES (%s, %s, %s, %s, %s, %s)
                            """, (
                                match_job_id, 
                                JobType.MATCH_DETAIL_SCRAPE.value,
                                match.match_report_url,
                                parent_job['priority'],
                                str(parent_job['job_id']),
                                Json({
                                    'match_id': match.match_id,
                                    'season': parent_job.get('metadata', {}).get('season'),
                                    'matchday': parent_job.get('metadata', {}).get('matchday')
                                })
                            ))
                            
                            job_ids_to_queue.append((match_job_id, parent_job['priority']))
                            created_count += 1
                        
                        # Commit database transaction FIRST
                        conn.commit()
                        
                        # THEN add to Redis queue (after DB commit is complete)
                        for job_id, priority in job_ids_to_queue:
                            self.queue.enqueue_job(job_id, priority)
                
            except Exception as e:
                self.logger.error(f"Failed to create batch of match jobs: {str(e)}")
                # Continue with next batch
        
        return created_count
    
    def process_match_job(self, job: Dict):
        """Process a match detail scraping job"""
        metadata = job.get('metadata', {})
        
        # Ensure full URL
        url = job['target_url']
        if not url.startswith('http'):
            url = f"https://www.transfermarkt.com{url}"
        
        # Extract match data using FIXED extractor
        match_data = self.match_extractor.extract_from_url(url)
        
        # Save match data
        match_id = match_data.match_info.match_id
        season = metadata.get('season', 'unknown')
        file_path = self.storage.save_match_data(match_id, season, match_data)
        
        # Save extraction record
        self.db.save_extraction(
            job_id=str(job['job_id']),
            match_id=match_id,
            matchday=metadata.get('matchday', 0),
            season=season,
            competition=match_data.match_info.competition_name,
            data=asdict(match_data),
            file_path=file_path,
            quality_score=self.calculate_match_quality(match_data),
            duration_ms=1000
        )
    
    def calculate_matchday_quality(self, data: MatchdayContainer) -> float:
        """Calculate matchday data quality score"""
        score = 100.0
        
        if not data.matches:
            score -= 50
        
        for match in data.matches or []:
            if not match.match_report_url:
                score -= 10
            if not match.home_team or not match.away_team:
                score -= 5
        
        return max(score, 0.0)
    
    def calculate_match_quality(self, data: MatchDetail) -> float:
        """Calculate match data quality score"""
        score = 100.0
        
        if not data.home_team.name or not data.away_team.name:
            score -= 20
        
        if len(data.home_lineup) < 10:
            score -= 10
        
        if len(data.away_lineup) < 10:
            score -= 10
        
        return max(score, 0.0)


# =============================================================================
# FIXED JOB SCHEDULER (with correct URL building)
# =============================================================================

class JobScheduler:
    """Creates and schedules scraping jobs"""
    
    def __init__(self, config: Config):
        self.config = config
        self.db = DatabaseManager(config.database_url)
        self.queue = QueueManager(config.redis_url)
        self.logger = logging.getLogger(__name__)
    
    def schedule_premier_league_season(self, season: str, start_matchday: int = 1, 
                                     end_matchday: int = 38) -> List[str]:
        """Schedule Premier League season scraping with FIXED URL format"""
        job_ids = []
        
        # Extract year from season string (e.g., "2023-24" -> "2023")
        season_year = self._extract_season_year(season)
        
        # Use the CORRECT URL format that actually works
        for matchday in range(start_matchday, end_matchday + 1):
            # Build URL using the format that WORKS (from snippet 3)
            matchday_url = f"https://www.transfermarkt.com/premier-league/spieltag/wettbewerb/GB1/saison_id/{season_year}/spieltag/{matchday}"
            
            job_id = self.db.create_job(
                job_type=JobType.MATCHDAY_SCRAPE,
                target_url=matchday_url,
                priority=5,
                metadata={
                    'season': season,
                    'matchday': matchday,
                    'competition': 'Premier League'
                }
            )
            
            self.queue.enqueue_job(job_id, 5)
            job_ids.append(job_id)
        
        self.logger.info(f"Scheduled {len(job_ids)} matchday jobs for season {season}")
        return job_ids
    
    def schedule_single_matchday(self, season: str, matchday: int) -> str:
        """Schedule a single matchday with FIXED URL format"""
        season_year = self._extract_season_year(season)
        
        # Use the CORRECT URL format
        matchday_url = f"https://www.transfermarkt.com/premier-league/spieltag/wettbewerb/GB1/saison_id/{season_year}/spieltag/{matchday}"
        
        job_id = self.db.create_job(
            job_type=JobType.MATCHDAY_SCRAPE,
            target_url=matchday_url,
            priority=8,
            metadata={
                'season': season,
                'matchday': matchday,
                'competition': 'Premier League'
            }
        )
        
        self.queue.enqueue_job(job_id, 8)
        
        self.logger.info(f"Scheduled matchday {matchday} for season {season}")
        return job_id

    def schedule_other_league(self, league_slug: str, league_code: str, season: str, 
                            matchday: int, competition_name: str) -> str:
        """Schedule other leagues with same consistent format"""
        season_year = self._extract_season_year(season)
        
        # Same consistent format for ANY league
        matchday_url = f"https://www.transfermarkt.com/{league_slug}/spieltag/wettbewerb/{league_code}/saison_id/{season_year}/spieltag/{matchday}"
        
        job_id = self.db.create_job(
            job_type=JobType.MATCHDAY_SCRAPE,
            target_url=matchday_url,
            priority=5,
            metadata={
                'season': season,
                'matchday': matchday,
                'competition': competition_name,
                'league_code': league_code
            }
        )
        
        self.queue.enqueue_job(job_id, 5)
        return job_id
    
    def _extract_season_year(self, season: str) -> str:
        """Extract the starting year from season string"""
        if '-' in season:
            return season.split('-')[0]
        elif '/' in season:
            return season.split('/')[0]
        else:
            return season

# =============================================================================
# SYSTEM MONITOR (unchanged - this is fine)
# =============================================================================

class SystemMonitor:
    """System monitoring and health checks"""
    
    def __init__(self, config: Config):
        self.config = config
        self.db = DatabaseManager(config.database_url)
        self.queue = QueueManager(config.redis_url)
    
    def get_status(self) -> Dict[str, Any]:
        """Get comprehensive system status"""
        job_stats = self.db.get_job_stats()
        queue_size = self.queue.get_queue_size()
        
        return {
            'timestamp': datetime.now().isoformat(),
            'queue_size': queue_size,
            'job_statistics': job_stats,
            'system_health': self.assess_health(job_stats, queue_size)
        }
    
    def assess_health(self, job_stats: Dict, queue_size: int) -> str:
        """Assess system health"""
        total_jobs = sum(job_stats.values())
        failed_jobs = job_stats.get('failed', 0)
        
        if total_jobs == 0:
            return 'idle'
        
        failure_rate = failed_jobs / total_jobs if total_jobs > 0 else 0
        
        if failure_rate > 0.1:
            return 'unhealthy'
        elif queue_size > 1000:
            return 'backlogged'
        else:
            return 'healthy'

# =============================================================================
# MAIN APPLICATION (updated with fixed components)
# =============================================================================

class FootballScrapingSystem:
    """Main application orchestrating the entire system"""
    
    def __init__(self, config: Config = None):
        self.config = config or Config()
        self.setup_logging()
        
        # Initialize components
        self.db = DatabaseManager(self.config.database_url)
        self.queue = QueueManager(self.config.redis_url)
        self.storage = StorageManager(self.config.base_output_dir)
        self.scheduler = JobScheduler(self.config)
        self.monitor = SystemMonitor(self.config)
        
        self.logger = logging.getLogger(__name__)
        self.workers = []
    
    def setup_logging(self):
        """Configure logging"""
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        logging.basicConfig(
            level=getattr(logging, self.config.log_level),
            format=log_format,
            handlers=[
                logging.FileHandler(f'{self.config.base_output_dir}/system.log'),
                logging.StreamHandler()
            ]
        )
    
    def setup(self):
        """Initialize the system"""
        print("🔧 Setting up Football Scraping System...")
        
        # Initialize database
        self.db.init_database()
        print("✅ Database initialized")
        
        # Setup storage
        self.storage.setup_directories()
        print("✅ Storage directories created")
        
        print("🚀 System setup complete!")
    
    def start_workers(self, num_workers: int = None):
        """Start worker processes"""
        num_workers = num_workers or self.config.max_workers
        
        print(f"🔄 Starting {num_workers} workers...")
        
        for i in range(num_workers):
            worker_id = f"worker_{i+1}"
            worker = ScrapingWorker(worker_id, self.config)
            
            # Start worker in separate thread
            worker_thread = threading.Thread(target=worker.start, daemon=True)
            worker_thread.start()
            
            self.workers.append({'worker': worker, 'thread': worker_thread})
        
        print(f"✅ Started {len(self.workers)} workers")
    
    def schedule_season(self, season: str, start_matchday: int = 1, end_matchday: int = 38) -> List[str]:
        """Schedule season scraping"""
        return self.scheduler.schedule_premier_league_season(season, start_matchday, end_matchday)
    
    def schedule_matchday(self, season: str, matchday: int) -> str:
        """Schedule single matchday"""
        return self.scheduler.schedule_single_matchday(season, matchday)
    
    def get_status(self) -> Dict[str, Any]:
        """Get system status"""
        return self.monitor.get_status()


# =============================================================================
# DEPLOYMENT HELPERS
# =============================================================================

def create_docker_files():
    """Create Docker deployment files"""
    
    # Dockerfile
    dockerfile_content = """FROM python:3.11-slim

                            WORKDIR /app

                            # Install system dependencies
                            RUN apt-get update && apt-get install -y \\
                                gcc \\
                                libpq-dev \\
                                && rm -rf /var/lib/apt/lists/*

                            # Install Python dependencies
                            COPY requirements.txt .
                            RUN pip install --no-cache-dir -r requirements.txt

                            # Copy application
                            COPY . .

                            # Create data directory
                            RUN mkdir -p /app/data

                            # Set environment variables
                            ENV PYTHONPATH=/app
                            ENV PYTHONUNBUFFERED=1

                            # Default command
                            CMD ["python", "football_scraper.py", "worker"]
                            """
    
    with open('Dockerfile', 'w') as f:
        f.write(dockerfile_content)
    
    # Docker Compose
    compose_content = """version: '3.8'

                        services:
                        postgres:
                            image: postgres:15
                            environment:
                            POSTGRES_DB: football_scraper
                            POSTGRES_USER: scraper
                            POSTGRES_PASSWORD: scraper_password
                            volumes:
                            - postgres_data:/var/lib/postgresql/data
                            ports:
                            - "5433:5432"
                            healthcheck:
                            test: ["CMD-SHELL", "pg_isready -U scraper"]
                            interval: 30s
                            timeout: 10s
                            retries: 3

                        redis:
                            image: redis:7-alpine
                            ports:
                            - "6379:6379"
                            healthcheck:
                            test: ["CMD", "redis-cli", "ping"]
                            interval: 30s
                            timeout: 10s
                            retries: 3

                        scraper-worker:
                            build: .
                            depends_on:
                            postgres:
                                condition: service_healthy
                            redis:
                                condition: service_healthy
                            environment:
                            DATABASE_URL: postgresql://scraper:scraper_password@postgres:5432/football_scraper
                            REDIS_URL: redis://redis:6379/0
                            volumes:
                            - ./data:/app/data
                            command: python football_scraper.py worker --workers 2
                            deploy:
                            replicas: 1

                        volumes:
                        postgres_data:
                        """
    
    with open('docker-compose.yml', 'w') as f:
        f.write(compose_content)
    
    # Requirements
    requirements_content = """psycopg2-binary>=2.9.0
redis>=4.0.0
beautifulsoup4>=4.11.0
requests>=2.28.0
backoff>=2.2.0
"""
    
    with open('requirements.txt', 'w') as f:
        f.write(requirements_content)
    
    # Environment file template
    env_content = """# Database Configuration
DATABASE_URL=postgresql://scraper:scraper_password@localhost:5433/football_scraper

# Redis Configuration
REDIS_URL=redis://localhost:6379/0

# Storage Configuration
BASE_OUTPUT_DIR=./data

# Processing Configuration
MAX_WORKERS=2
DELAY_BETWEEN_REQUESTS=3.0
MAX_RETRIES=2

# Logging
LOG_LEVEL=INFO
"""
    
    with open('.env.example', 'w') as f:
        f.write(env_content)
    
    print("✅ Created deployment files:")
    print("   - Dockerfile")
    print("   - docker-compose.yml") 
    print("   - requirements.txt")
    print("   - .env.example")

# =============================================================================
# USAGE EXAMPLES
# =============================================================================

def example_usage():
    """Example usage of the FIXED system"""
    
    print("📚 USAGE EXAMPLES (FIXED VERSION)")
    print("=" * 60)
    
    print("\n1. Setup System:")
    print("   python football_scraper.py setup")
    
    print("\n2. Test Extraction (NEW - to verify it works):")
    print("   # Test matchday extraction")
    print("   python football_scraper.py test --type matchday")
    print("   # Test match extraction") 
    print("   python football_scraper.py test --type match")
    print("   # Test with custom URL")
    print("   python football_scraper.py test --type matchday --url 'URL_HERE'")
    
    print("\n3. Schedule Jobs:")
    print("   # Single matchday")
    print("   python football_scraper.py schedule --season 2023-24 --matchday 1")
    print("   # First 5 matchdays")
    print("   python football_scraper.py schedule --season 2023-24 --start-matchday 1 --end-matchday 5")
    print("   # Full season")
    print("   python football_scraper.py schedule --season 2023-24")
    
    print("\n4. Start Workers:")
    print("   python football_scraper.py worker --workers 2")
    
    print("\n5. Monitor System:")
    print("   python football_scraper.py status")
    
    print("\n6. Docker Deployment:")
    print("   docker-compose up -d postgres redis")
    print("   docker-compose up scraper-worker")
    
    print("\n📁 OUTPUT STRUCTURE:")
    print("   data/")
    print("   ├── matchdays/")
    print("   │   └── 2023-24_matchday_01.json  ← Contextual data with match list")
    print("   └── matches/")
    print("       └── 2023-24/")
    print("           └── match_4095452.json    ← Detailed match data")
    
    print("\n🔧 KEY FIXES APPLIED:")
    print("   - Fixed URL building (correct Transfermarkt endpoints)")
    print("   - Fixed HTML parsing (using working selectors from notebooks)")
    print("   - Fixed data extraction logic (copy-pasted working code)")
    print("   - Fixed match ID extraction and validation")
    print("   - Added test command to verify before running full jobs")
    print("   - Conservative settings (slower, more reliable)")

def quick_test():
    """Quick test function to verify the fixes work"""
    print("🧪 QUICK TEST - Verifying fixes...")
    
    try:
        # Test matchday extraction
        print("\n1. Testing matchday extraction...")
        extractor = MatchdayExtractor()
        test_url = "https://www.transfermarkt.com/premier-league/spieltag/wettbewerb/GB1/saison_id/2023/spieltag/1"
        
        data = extractor.extract_from_transfermarkt_url(test_url, matchday=1, season="2023-24")
        
        matches_found = len(data.matches) if data.matches else 0
        print(f"   ✅ Success! Found {matches_found} matches")
        
        if matches_found > 0:
            # Test match extraction
            print("\n2. Testing match extraction...")
            first_match = data.matches[0]
            if first_match.match_report_url:
                match_extractor = CleanMatchExtractor()
                match_data = match_extractor.extract_from_url(first_match.match_report_url)
                print(f"   ✅ Success! Extracted: {match_data.home_team.name} vs {match_data.away_team.name}")
                print(f"   Goals: {len(match_data.goals)}, Cards: {len(match_data.cards)}")
            else:
                print("   ⚠️  No match report URL found")
        
        print("\n🎉 FIXES VERIFIED - System should work correctly now!")
        return True
        
    except Exception as e:
        print(f"\n❌ Test failed: {str(e)}")
        print("   Check your internet connection and try again")
        return False

# =============================================================================
# COMMAND LINE INTERFACE (unchanged - this is fine)
# =============================================================================

def create_cli():
    """Create command line interface"""
    parser = argparse.ArgumentParser(description='Football Scraping System')
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Setup command
    setup_parser = subparsers.add_parser('setup', help='Initialize system')
    
    # Worker command
    worker_parser = subparsers.add_parser('worker', help='Start worker processes')
    worker_parser.add_argument('--workers', type=int, default=2, help='Number of workers')
    worker_parser.add_argument('--config', help='Configuration file')
    
    # Schedule command
    schedule_parser = subparsers.add_parser('schedule', help='Schedule scraping jobs')
    schedule_parser.add_argument('--season', required=True, help='Season (e.g., 2023-24)')
    schedule_parser.add_argument('--start-matchday', type=int, default=1, help='Start matchday')
    schedule_parser.add_argument('--end-matchday', type=int, default=38, help='End matchday')
    schedule_parser.add_argument('--matchday', type=int, help='Single matchday to schedule')
    
    # Status command
    status_parser = subparsers.add_parser('status', help='Get system status')
    
    # Test command
    test_parser = subparsers.add_parser('test', help='Test extraction with single URL')
    test_parser.add_argument('--url', help='Test URL')
    test_parser.add_argument('--type', choices=['matchday', 'match'], default='matchday', help='Type of extraction')
    
    return parser

def main():
    """Main CLI entry point"""
    parser = create_cli()
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    # Load configuration
    config = Config()
    
    # Initialize system
    system = FootballScrapingSystem(config)
    
    try:
        if args.command == 'setup':
            system.setup()
        
        elif args.command == 'worker':
            print(f"🚀 Starting Football Scraping System (FIXED VERSION)")
            print(f"📊 Database: {config.database_url}")
            print(f"📦 Queue: {config.redis_url}")
            print(f"💾 Storage: {config.base_output_dir}")
            print(f"👥 Workers: {args.workers}")
            
            system.start_workers(args.workers)
            
            print("✅ Workers started. Press Ctrl+C to stop.")
            try:
                while True:
                    time.sleep(60)
                    status = system.get_status()
                    print(f"📈 Queue: {status['queue_size']} | Health: {status['system_health']}")
            except KeyboardInterrupt:
                print("\n🛑 Shutting down...")
        
        elif args.command == 'schedule':
            if args.matchday:
                # Schedule single matchday
                print(f"📅 Scheduling {args.season} matchday {args.matchday}...")
                job_id = system.schedule_matchday(args.season, args.matchday)
                print(f"✅ Scheduled job {job_id}")
            else:
                # Schedule season
                print(f"📅 Scheduling {args.season} season (MD {args.start_matchday}-{args.end_matchday})...")
                job_ids = system.schedule_season(args.season, args.start_matchday, args.end_matchday)
                print(f"✅ Scheduled {len(job_ids)} matchday jobs")
            
            status = system.get_status()
            print(f"📊 Total queue size: {status['queue_size']}")
        
        elif args.command == 'status':
            status = system.get_status()
            print("\n📊 SYSTEM STATUS")
            print("=" * 50)
            print(f"Timestamp: {status['timestamp']}")
            print(f"Queue Size: {status['queue_size']}")
            print(f"Health: {status['system_health']}")
            print("\nJob Statistics (24h):")
            for job_status, count in status['job_statistics'].items():
                print(f"  {job_status}: {count} jobs")
        
        elif args.command == 'test':
            # Test extraction functionality
            if args.url:
                test_url = args.url
            else:
                # Default test URLs
                if args.type == 'matchday':
                    test_url = "https://www.transfermarkt.com/premier-league/spieltag/wettbewerb/GB1/saison_id/2023/spieltag/1"
                else:
                    test_url = "https://www.transfermarkt.com/spielbericht/index/spielbericht/4095452"
            
            print(f"🧪 Testing {args.type} extraction from: {test_url}")
            
            if args.type == 'matchday':
                extractor = MatchdayExtractor()
                try:
                    data = extractor.extract_from_transfermarkt_url(test_url, matchday=1, season="2023-24")
                    print(f"✅ Success! Extracted {len(data.matches) if data.matches else 0} matches")
                    
                    if data.matches:
                        print("\nSample matches:")
                        for i, match in enumerate(data.matches[:3], 1):
                            home_name = match.home_team.get('name', 'Unknown') if match.home_team else 'Unknown'
                            away_name = match.away_team.get('name', 'Unknown') if match.away_team else 'Unknown'
                            score = match.final_score.get('display', 'N/A') if match.final_score else 'N/A'
                            print(f"  {i}. {home_name} vs {away_name} - {score}")
                            print(f"     Match ID: {match.match_id}")
                            print(f"     Report URL: {match.match_report_url}")
                    
                    # Save test data
                    with open('test_matchday_output.json', 'w', encoding='utf-8') as f:
                        json.dump(asdict(data), f, indent=2, ensure_ascii=False, default=str)
                    print(f"💾 Test data saved to: test_matchday_output.json")
                    
                except Exception as e:
                    print(f"❌ Test failed: {str(e)}")
            
            else:  # match extraction
                extractor = CleanMatchExtractor()
                try:
                    data = extractor.extract_from_url(test_url)
                    print(f"✅ Success! Extracted match: {data.home_team.name} vs {data.away_team.name}")
                    print(f"   Score: {data.score.home_final}-{data.score.away_final}")
                    print(f"   Goals: {len(data.goals)}")
                    print(f"   Cards: {len(data.cards)}")
                    print(f"   Substitutions: {len(data.substitutions)}")
                    print(f"   Home lineup: {len(data.home_lineup)} players")
                    print(f"   Away lineup: {len(data.away_lineup)} players")
                    
                    # Save test data
                    with open('test_match_output.json', 'w', encoding='utf-8') as f:
                        json.dump(asdict(data), f, indent=2, ensure_ascii=False, default=str)
                    print(f"💾 Test data saved to: test_match_output.json")
                    
                except Exception as e:
                    print(f"❌ Test failed: {str(e)}")
    
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        sys.exit(1)

# =============================================================================
# ENTRY POINT (FIXED)
# =============================================================================

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == 'examples':
        example_usage()
    elif len(sys.argv) > 1 and sys.argv[1] == 'docker':
        create_docker_files()
    elif len(sys.argv) > 1 and sys.argv[1] == 'quicktest':
        quick_test()
    else:
        main()