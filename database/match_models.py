"""
Database models for match data - separate file for better organization.
File: database/match_models.py

Import your existing Base from database_models.py to maintain consistency.
"""
from typing import Dict, Any

from sqlalchemy import (
    Column, String, Integer, DateTime, Boolean, Text, ForeignKey,
    PrimaryKeyConstraint, Index, JSON
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

# Import your existing Base to keep everything using the same declarative base
from .database_models import Base

class Player(Base):
    """
    Player model - independent of teams for maximum flexibility.
    Links to teams happen in post-processing via junction tables.
    """
    __tablename__ = 'players'

    # Primary identifiers
    player_id = Column(
        String(50), primary_key=True, nullable=False,
        doc="Unique player identifier from source"
    )
    name = Column(
        String(255), nullable=False,
        doc="Full player name"
    )
    
    # Optional player attributes
    portrait_url = Column(
        Text, nullable=True,
        doc="URL to player portrait image"
    )
    
    # Metadata
    created_at = Column(
        DateTime, default=func.now(), nullable=False
    )
    updated_at = Column(
        DateTime, default=func.now(), onupdate=func.now(), nullable=False
    )
    is_active = Column(
        Boolean, default=True, nullable=False
    )

    def __repr__(self) -> str:
        return f"<Player(id='{self.player_id}', name='{self.name}')>"

    def to_dict(self) -> Dict[str, Any]:
        return {
            'player_id': self.player_id,
            'name': self.name,
            'portrait_url': self.portrait_url,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
            'is_active': self.is_active
        }

class Match(Base):
    """
    Core match model containing match info, teams, and score.
    Competition linking happens in post-processing.
    """
    __tablename__ = 'matches'

    # Primary identifiers
    match_id = Column(
        String(50), primary_key=True, nullable=False,
        doc="Unique match identifier from source"
    )
    
    # Competition info (raw from source)
    competition_name = Column(
        String(255), nullable=True,
        doc="Competition name as extracted"
    )
    competition_id = Column(String(50), ForeignKey('competitions.competition_id'), nullable=True)
    competition_logo = Column(
        Text, nullable=True,
        doc="Competition logo URL"
    )
    
    # Match scheduling
    matchday = Column(
        Integer, nullable=True,
        doc="Matchday/gameweek number"
    )
    season = Column(
        String(50), nullable=True,
        doc="Season identifier (e.g., '2023-24')"
    )
    date = Column(
        String(50), nullable=True,
        doc="Match date as string"
    )
    time = Column(
        String(20), nullable=True,
        doc="Match time"
    )
    day_of_week = Column(
        String(20), nullable=True,
        doc="Day of the week"
    )
    
    # Venue and officials
    venue = Column(
        String(255), nullable=True,
        doc="Stadium/venue name"
    )
    attendance = Column(
        Integer, nullable=True,
        doc="Match attendance"
    )
    referee = Column(
        String(255), nullable=True,
        doc="Referee name"
    )
    referee_id = Column(
        String(50), nullable=True,
        doc="Referee identifier"
    )
    
    # Team information (raw from source)
    home_team_id = Column(
        String(50), nullable=True,
        doc="Home team ID from source"
    )
    home_team_name = Column(
        String(255), nullable=True,
        doc="Home team name"
    )
    home_team_short_name = Column(
        String(100), nullable=True,
        doc="Home team short name"
    )
    home_team_logo_url = Column(
        Text, nullable=True,
        doc="Home team logo URL"
    )
    home_team_league_position = Column(
        Integer, nullable=True,
        doc="Home team league position at time of match"
    )
    home_team_formation = Column(
        String(20), nullable=True,
        doc="Home team formation"
    )
    home_team_manager = Column(
        String(255), nullable=True,
        doc="Home team manager"
    )
    
    away_team_id = Column(
        String(50), nullable=True,
        doc="Away team ID from source"
    )
    away_team_name = Column(
        String(255), nullable=True,
        doc="Away team name"
    )
    away_team_short_name = Column(
        String(100), nullable=True,
        doc="Away team short name"
    )
    away_team_logo_url = Column(
        Text, nullable=True,
        doc="Away team logo URL"
    )
    away_team_league_position = Column(
        Integer, nullable=True,
        doc="Away team league position at time of match"
    )
    away_team_formation = Column(
        String(20), nullable=True,
        doc="Away team formation"
    )
    away_team_manager = Column(
        String(255), nullable=True,
        doc="Away team manager"
    )
    
    # Score information
    home_final_score = Column(
        Integer, nullable=True,
        doc="Home team final score"
    )
    away_final_score = Column(
        Integer, nullable=True,
        doc="Away team final score"
    )
    home_ht_score = Column(
        Integer, nullable=True,
        doc="Home team half-time score"
    )
    away_ht_score = Column(
        Integer, nullable=True,
        doc="Away team half-time score"
    )
    
    # Match report and source
    match_report_url = Column(
        Text, nullable=True,
        doc="URL to match report"
    )
    source_url = Column(
        Text, nullable=True,
        doc="Source URL where match data was extracted"
    )
    
    # Community predictions (JSON field)
    community_predictions = Column(
        JSON, nullable=True,
        doc="Community predictions data as JSON"
    )
    
    # Extraction metadata
    extraction_metadata = Column(
        JSON, nullable=True,
        doc="Extraction metadata as JSON"
    )
    
    # Standard metadata
    created_at = Column(
        DateTime, default=func.now(), nullable=False
    )
    updated_at = Column(
        DateTime, default=func.now(), onupdate=func.now(), nullable=False
    )
    is_active = Column(
        Boolean, default=True, nullable=False
    )

    # Relationships
    goals = relationship("Goal", back_populates="match", lazy="selectin")
    cards = relationship("Card", back_populates="match", lazy="selectin")
    substitutions = relationship("Substitution", back_populates="match", lazy="selectin")
    lineups = relationship("MatchLineup", back_populates="match", lazy="selectin")

    def __repr__(self) -> str:
        return (
            f"<Match(id='{self.match_id}', "
            f"home='{self.home_team_name}', away='{self.away_team_name}', "
            f"score='{self.home_final_score}:{self.away_final_score}')>"
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            'match_id': self.match_id,
            'competition_name': self.competition_name,
            'competition_id': self.competition_id,
            'competition_logo': self.competition_logo,
            'matchday': self.matchday,
            'season': self.season,
            'date': self.date,
            'time': self.time,
            'day_of_week': self.day_of_week,
            'venue': self.venue,
            'attendance': self.attendance,
            'referee': self.referee,
            'referee_id': self.referee_id,
            'home_team_id': self.home_team_id,
            'home_team_name': self.home_team_name,
            'home_team_short_name': self.home_team_short_name,
            'home_team_logo_url': self.home_team_logo_url,
            'home_team_league_position': self.home_team_league_position,
            'home_team_formation': self.home_team_formation,
            'home_team_manager': self.home_team_manager,
            'away_team_id': self.away_team_id,
            'away_team_name': self.away_team_name,
            'away_team_short_name': self.away_team_short_name,
            'away_team_logo_url': self.away_team_logo_url,
            'away_team_league_position': self.away_team_league_position,
            'away_team_formation': self.away_team_formation,
            'away_team_manager': self.away_team_manager,
            'home_final_score': self.home_final_score,
            'away_final_score': self.away_final_score,
            'home_ht_score': self.home_ht_score,
            'away_ht_score': self.away_ht_score,
            'match_report_url': self.match_report_url,
            'source_url': self.source_url,
            'community_predictions': self.community_predictions,
            'extraction_metadata': self.extraction_metadata,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
            'is_active': self.is_active
        }

class MatchLineup(Base):
    """
    Junction table for match lineups - links matches to players with lineup details.
    """
    __tablename__ = 'match_lineups'

    # Composite primary key
    match_id = Column(
        String(50), ForeignKey('matches.match_id'), nullable=False
    )
    player_id = Column(
        String(50), ForeignKey('players.player_id'), nullable=False
    )
    team_side = Column(
        String(10), nullable=False,
        doc="'home' or 'away'"
    )
    
    # Lineup details
    lineup_type = Column(
        String(20), nullable=False,
        doc="'starting', 'substitute'"
    )
    shirt_number = Column(
        Integer, nullable=True,
        doc="Player's shirt number"
    )
    position = Column(
        String(50), nullable=True,
        doc="Player's position"
    )
    is_captain = Column(
        Boolean, default=False, nullable=False,
        doc="Whether player is captain"
    )
    
    # Metadata
    created_at = Column(
        DateTime, default=func.now(), nullable=False
    )
    
    __table_args__ = (
        PrimaryKeyConstraint('match_id', 'player_id', 'team_side'),
    )

    # Relationships
    match = relationship("Match", back_populates="lineups")
    player = relationship("Player")

    def __repr__(self) -> str:
        return (
            f"<MatchLineup(match='{self.match_id}', "
            f"player='{self.player_id}', side='{self.team_side}')>"
        )

class Goal(Base):
    """
    Goals scored in matches with detailed information.
    """
    __tablename__ = 'goals'

    # Primary key
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Foreign keys
    match_id = Column(
        String(50), ForeignKey('matches.match_id'), nullable=False
    )
    player_id = Column(
        String(50), ForeignKey('players.player_id'), nullable=True,
        doc="Player who scored (can be null for own goals)"
    )
    assist_player_id = Column(
        String(50), ForeignKey('players.player_id'), nullable=True,
        doc="Player who provided assist"
    )
    
    # Goal details
    minute = Column(
        Integer, nullable=False,
        doc="Minute when goal was scored"
    )
    extra_time = Column(
        Integer, nullable=True,
        doc="Extra time minutes"
    )
    goal_type = Column(
        String(100), nullable=True,
        doc="Type of goal (e.g., 'Left-footed shot')"
    )
    assist_type = Column(
        String(100), nullable=True,
        doc="Type of assist (e.g., 'Header', 'Pass')"
    )
    team_side = Column(
        String(10), nullable=False,
        doc="'home' or 'away'"
    )
    
    # Score tracking
    home_score_after = Column(
        Integer, nullable=True,
        doc="Home score after this goal"
    )
    away_score_after = Column(
        Integer, nullable=True,
        doc="Away score after this goal"
    )
    
    # Season statistics
    season_goal_number = Column(
        Integer, nullable=True,
        doc="Player's goal number for the season"
    )
    season_assist_number = Column(
        Integer, nullable=True,
        doc="Assist provider's assist number for the season"
    )
    
    # Metadata
    created_at = Column(
        DateTime, default=func.now(), nullable=False
    )
    
    # Relationships
    match = relationship("Match", back_populates="goals")
    scorer = relationship("Player", foreign_keys=[player_id])
    assist_provider = relationship("Player", foreign_keys=[assist_player_id])

    def __repr__(self) -> str:
        return (
            f"<Goal(match='{self.match_id}', minute={self.minute}, "
            f"scorer='{self.player_id}', side='{self.team_side}')>"
        )

class Card(Base):
    """
    Cards (yellow/red) issued during matches.
    """
    __tablename__ = 'cards'

    # Primary key
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Foreign keys
    match_id = Column(
        String(50), ForeignKey('matches.match_id'), nullable=False
    )
    player_id = Column(
        String(50), ForeignKey('players.player_id'), nullable=True
    )
    
    # Card details
    minute = Column(
        Integer, nullable=False,
        doc="Minute when card was issued"
    )
    extra_time = Column(
        Integer, nullable=True,
        doc="Extra time minutes"
    )
    card_type = Column(
        String(20), nullable=False,
        doc="'yellow', 'red', 'second_yellow'"
    )
    reason = Column(
        String(255), nullable=True,
        doc="Reason for the card"
    )
    team_side = Column(
        String(10), nullable=False,
        doc="'home' or 'away'"
    )
    season_card_number = Column(
        Integer, nullable=True,
        doc="Player's card number for the season"
    )
    
    # Metadata
    created_at = Column(
        DateTime, default=func.now(), nullable=False
    )
    
    # Relationships
    match = relationship("Match", back_populates="cards")
    player = relationship("Player")

    def __repr__(self) -> str:
        return (
            f"<Card(match='{self.match_id}', minute={self.minute}, "
            f"type='{self.card_type}', player='{self.player_id}')>"
        )

class Substitution(Base):
    """
    Player substitutions during matches.
    """
    __tablename__ = 'substitutions'

    # Primary key
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Foreign keys
    match_id = Column(
        String(50), ForeignKey('matches.match_id'), nullable=False
    )
    player_out_id = Column(
        String(50), ForeignKey('players.player_id'), nullable=True
    )
    player_in_id = Column(
        String(50), ForeignKey('players.player_id'), nullable=True
    )
    
    # Substitution details
    minute = Column(
        Integer, nullable=False,
        doc="Minute when substitution occurred"
    )
    extra_time = Column(
        Integer, nullable=True,
        doc="Extra time minutes"
    )
    reason = Column(
        String(100), nullable=True,
        doc="Reason for substitution (e.g., 'Tactical', 'Injury')"
    )
    team_side = Column(
        String(10), nullable=False,
        doc="'home' or 'away'"
    )
    
    # Metadata
    created_at = Column(
        DateTime, default=func.now(), nullable=False
    )
    
    # Relationships
    match = relationship("Match", back_populates="substitutions")
    player_out = relationship("Player", foreign_keys=[player_out_id])
    player_in = relationship("Player", foreign_keys=[player_in_id])

    def __repr__(self) -> str:
        return (
            f"<Substitution(match='{self.match_id}', minute={self.minute}, "
            f"out='{self.player_out_id}', in='{self.player_in_id}')>"
        )

class MatchdayInfo(Base):
    """
    Matchday-level information including league tables and top scorers.
    Stores the broader context around a matchday.
    """
    __tablename__ = 'matchday_info'

    # Primary key
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Matchday identifiers
    competition_id = Column(
        String(100), nullable=True,
        doc="Competition identifier"
    )
    competition_name = Column(
        String(255), nullable=True,
        doc="Competition name"
    )
    season = Column(
        String(50), nullable=True,
        doc="Season (e.g., '2023-24')"
    )
    matchday_number = Column(
        Integer, nullable=True,
        doc="Matchday/gameweek number"
    )
    
    # Source information
    source_url = Column(
        Text, nullable=True,
        doc="Source URL for matchday data"
    )
    
    # Structured data as JSON
    league_table = Column(
        JSON, nullable=True,
        doc="League table at this matchday"
    )
    top_scorers = Column(
        JSON, nullable=True,
        doc="Top scorers data"
    )
    matchday_summary = Column(
        JSON, nullable=True,
        doc="Matchday statistics summary"
    )
    
    # Extraction metadata
    extraction_metadata = Column(
        JSON, nullable=True,
        doc="Extraction metadata"
    )
    
    # Standard metadata
    created_at = Column(
        DateTime, default=func.now(), nullable=False
    )
    updated_at = Column(
        DateTime, default=func.now(), onupdate=func.now(), nullable=False
    )
    is_active = Column(
        Boolean, default=True, nullable=False
    )

    __table_args__ = (
        # Unique constraint on competition + season + matchday
        Index('unique_matchday', 'competition_id', 'season', 'matchday_number', unique=True),
    )

    def __repr__(self) -> str:
        return (
            f"<MatchdayInfo(competition='{self.competition_id}', "
            f"season='{self.season}', matchday={self.matchday_number})>"
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'competition_id': self.competition_id,
            'competition_name': self.competition_name,
            'season': self.season,
            'matchday_number': self.matchday_number,
            'source_url': self.source_url,
            'league_table': self.league_table,
            'top_scorers': self.top_scorers,
            'matchday_summary': self.matchday_summary,
            'extraction_metadata': self.extraction_metadata,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
            'is_active': self.is_active
        }