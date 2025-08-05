# database/schemas/match_schema.py
from enum import Enum as PyEnum

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from database.base import Base


class TeamSide(PyEnum):
    home = "home"
    away = "away"


class MatchCompetition(Base):
    __tablename__ = "m_competitions"

    competition_id = Column(String(50), primary_key=True)
    name = Column(String(200), nullable=False)
    logo_url = Column(Text)

    matches = relationship("Match", back_populates="competition")


class LeagueTableEntry(Base):
    __tablename__ = "league_table"

    entry_id = Column(String(50), primary_key=True)
    matchday_id = Column(String(50), ForeignKey("matchdays.matchday_id"))
    team_id = Column(String(50), ForeignKey("m_teams.team_id"))
    season = Column(String(20), nullable=False, index=True)
    competition = Column(String(100), nullable=False, index=True)
    position = Column(Integer)
    movement = Column(String(10), nullable=True)
    matches_played = Column(Integer)
    goal_difference = Column(Integer)
    points = Column(Integer)

    matchday = relationship("Matchday", back_populates="league_entries")
    team = relationship("TeamInMatch", back_populates="league_entries")


class TeamInMatch(Base):
    __tablename__ = "m_teams"

    team_id = Column(String(50), primary_key=True)
    name = Column(String(200), nullable=False)
    short_name = Column(String(50))
    profile_url = Column(Text)
    logo_url = Column(Text)

    matches_home = relationship(
        "Match", back_populates="home_team", foreign_keys="Match.home_team_id"
    )
    matches_away = relationship(
        "Match", back_populates="away_team", foreign_keys="Match.away_team_id"
    )
    league_entries = relationship("LeagueTableEntry", back_populates="team")


class Lineup(Base):
    __tablename__ = "lineups"

    lineup_id = Column(String(50), primary_key=True)
    match_id = Column(String(50), ForeignKey("matches.match_id"))
    team_side = Column(String(4))  # VARCHAR(4) in your DB, not enum
    player_id = Column(String(50), ForeignKey("players.player_id"))
    shirt_number = Column(Integer)
    position = Column(String(20), nullable=True)
    is_captain = Column(Boolean, default=False)
    is_starter = Column(Boolean, default=True)
    position_x = Column(Numeric(5, 2))
    position_y = Column(Numeric(5, 2))
    minutes_played = Column(Integer, default=0)
    player_stats = Column(JSONB)

    match = relationship("Match", back_populates="lineups")
    player = relationship("Player", back_populates="lineup_entries")


class CommunityPrediction(Base):
    __tablename__ = "community_predictions"

    prediction_id = Column(String(50), primary_key=True)
    match_id = Column(String(50), ForeignKey("matches.match_id"), unique=True)
    home_win_pct = Column(Numeric(5, 2))
    draw_pct = Column(Numeric(5, 2))
    away_win_pct = Column(Numeric(5, 2))

    match = relationship("Match", back_populates="community_prediction")


class TopScorer(Base):
    __tablename__ = "top_scorers"

    scorer_id = Column(String(50), primary_key=True)
    matchday_id = Column(String(50), ForeignKey("matchdays.matchday_id"))
    player_id = Column(String(50), ForeignKey("players.player_id"))
    season = Column(String(20), nullable=False, index=True)
    league = Column(String(100), nullable=False, index=True)
    goals_this_matchday = Column(Integer)
    total_goals = Column(Integer)

    matchday = relationship("Matchday", back_populates="top_scorers")
    player = relationship("Player", back_populates="top_scorers")


class MatchTeam(Base):
    __tablename__ = "match_teams"

    match_id = Column(String(50), ForeignKey("matches.match_id"), primary_key=True)
    team_side = Column(String(4), primary_key=True)  # VARCHAR(4) in your DB, not enum
    team_id = Column(String(50), ForeignKey("m_teams.team_id"), nullable=False)
    formation = Column(String(20))
    possession_pct = Column(Numeric(5, 2))
    shots_on_target = Column(Integer)
    shots_off_target = Column(Integer)
    corners = Column(Integer)
    fouls = Column(Integer)
    offsides = Column(Integer)
    match_stats = Column(JSONB)

    match = relationship("Match", back_populates="match_teams")
    team = relationship("TeamInMatch")


class Substitution(Base):
    __tablename__ = "substitutions"

    id = Column(
        Integer, primary_key=True
    )  # Your DB uses INTEGER id, not substitution_id
    match_id = Column(String(50), ForeignKey("matches.match_id"))
    player_out_id = Column(String(50), ForeignKey("players.player_id"))
    player_in_id = Column(String(50), ForeignKey("players.player_id"))
    minute = Column(Integer)
    extra_time = Column(Integer, nullable=True)
    reason = Column(String(100))  # VARCHAR(100) in your DB
    team_side = Column(String(10))  # VARCHAR(10) in your DB, not enum
    created_at = Column(DateTime(timezone=True), default=func.now())

    match = relationship("Match", back_populates="substitutions")
    player_out = relationship(
        "Player", back_populates="subs_out", foreign_keys=[player_out_id]
    )
    player_in = relationship(
        "Player", back_populates="subs_in", foreign_keys=[player_in_id]
    )


class Goal(Base):
    __tablename__ = "goals"

    id = Column(Integer, primary_key=True)  # Your DB uses INTEGER id, not goal_id
    match_id = Column(String(50), ForeignKey("matches.match_id"))
    player_id = Column(String(50), ForeignKey("players.player_id"))
    assist_player_id = Column(String(50), ForeignKey("players.player_id"))
    minute = Column(Integer)
    extra_time = Column(Integer, nullable=True)
    goal_type = Column(String(100))  # VARCHAR(100) in your DB
    assist_type = Column(String(100))  # VARCHAR(100) in your DB
    team_side = Column(String(10))  # VARCHAR(10) in your DB, not enum
    home_score_after = Column(Integer)  # Different field name
    away_score_after = Column(Integer)  # Different field name
    season_goal_number = Column(Integer)
    season_assist_number = Column(Integer)
    created_at = Column(DateTime(timezone=True), default=func.now())

    match = relationship("Match", back_populates="goals")
    player = relationship("Player", back_populates="goals", foreign_keys=[player_id])
    assist_player = relationship(
        "Player", back_populates="assisted_goals", foreign_keys=[assist_player_id]
    )


class Card(Base):
    __tablename__ = "cards"

    id = Column(Integer, primary_key=True)  # Your DB uses INTEGER id, not card_id
    match_id = Column(String(50), ForeignKey("matches.match_id"))
    player_id = Column(String(50), ForeignKey("players.player_id"))
    minute = Column(Integer)
    extra_time = Column(Integer, nullable=True)
    card_type = Column(String(20))
    reason = Column(String(255))  # VARCHAR(255) in your DB
    team_side = Column(String(10))  # VARCHAR(10) in your DB, not enum
    season_card_number = Column(Integer, nullable=True)
    created_at = Column(DateTime(timezone=True), default=func.now())

    match = relationship("Match", back_populates="cards")
    player = relationship("Player", back_populates="cards")


class Player(Base):
    __tablename__ = "players"

    player_id = Column(String(50), primary_key=True)
    name = Column(String(255), nullable=False)  # VARCHAR(255) in your DB
    portrait_url = Column(Text)
    created_at = Column(DateTime(timezone=True), default=func.now())
    updated_at = Column(
        DateTime(timezone=True), default=func.now(), onupdate=func.now()
    )
    is_active = Column(Boolean, default=True)  # Required field with default
    short_name = Column(Text)  # TEXT in your DB
    profile_url = Column(Text)

    lineup_entries = relationship("Lineup", back_populates="player")
    goals = relationship("Goal", back_populates="player", foreign_keys="Goal.player_id")
    assisted_goals = relationship(
        "Goal", back_populates="assist_player", foreign_keys="Goal.assist_player_id"
    )
    cards = relationship("Card", back_populates="player")
    subs_out = relationship(
        "Substitution",
        back_populates="player_out",
        foreign_keys="Substitution.player_out_id",
    )
    subs_in = relationship(
        "Substitution",
        back_populates="player_in",
        foreign_keys="Substitution.player_in_id",
    )
    top_scorers = relationship("TopScorer", back_populates="player")


class Referee(Base):
    __tablename__ = "referees"

    referee_id = Column(String(50), primary_key=True)
    name = Column(String(200), nullable=False)
    profile_url = Column(Text)

    matches = relationship("Match", back_populates="referee_obj")


class Matchday(Base):
    __tablename__ = "matchdays"

    matchday_id = Column(String(50), primary_key=True)
    season = Column(String(20), nullable=False, index=True)
    number = Column(Integer, nullable=False)
    competition = Column(String(100), nullable=False, index=True)
    source_url = Column(Text)
    extraction_time = Column(DateTime(timezone=True))
    total_matches = Column(Integer)

    summary = relationship("MatchdaySummary", uselist=False, back_populates="matchday")
    matches = relationship("Match", back_populates="matchday")
    league_entries = relationship("LeagueTableEntry", back_populates="matchday")
    top_scorers = relationship("TopScorer", back_populates="matchday")


class MatchdaySummary(Base):
    __tablename__ = "matchday_summary"

    matchday_id = Column(
        String(50), ForeignKey("matchdays.matchday_id"), primary_key=True
    )
    season = Column(String(20), nullable=False)
    competition = Column(String(100), nullable=False)
    matches_count = Column(Integer)
    goals = Column(Integer)
    own_goals = Column(Integer)
    yellow_cards = Column(Integer)
    second_yellow_cards = Column(Integer, nullable=True)
    red_cards = Column(Integer)
    total_attendance = Column(Integer)
    average_attendance = Column(Integer)
    sold_out_matches = Column(Integer)

    matchday = relationship("Matchday", back_populates="summary")


class Match(Base):
    __tablename__ = "matches"

    # Primary identifiers
    match_id = Column(String(50), primary_key=True)
    matchday_id = Column(String(50), ForeignKey("matchdays.matchday_id"))
    competition_id = Column(String(50), ForeignKey("m_competitions.competition_id"))
    season = Column(String(50))

    # Denormalized fields
    competition_name = Column(String(255), nullable=True)
    matchday_number = Column("matchday", Integer, nullable=True)
    referee_id = Column(String(50), ForeignKey("referees.referee_id"))
    referee_name = Column("referee", String(200), nullable=True)

    # Team denormalization
    home_team_id = Column(String(50), ForeignKey("m_teams.team_id"))
    home_team_name = Column(String(255), nullable=True)
    home_team_short_name = Column(String(100), nullable=True)
    home_team_logo_url = Column(Text, nullable=True)
    home_team_league_position = Column(Integer, nullable=True)
    home_team_formation = Column(String(20), nullable=True)
    home_team_manager = Column(String(255), nullable=True)

    away_team_id = Column(String(50), ForeignKey("m_teams.team_id"))
    away_team_name = Column(String(255), nullable=True)
    away_team_short_name = Column(String(100), nullable=True)
    away_team_logo_url = Column(Text, nullable=True)
    away_team_league_position = Column(Integer, nullable=True)
    away_team_formation = Column(String(20), nullable=True)
    away_team_manager = Column(String(255), nullable=True)

    # Match details
    date = Column(String(50))
    day_of_week = Column(String(20), nullable=True)
    time = Column(String(20))
    venue = Column(String(255))
    attendance = Column(Integer)
    match_report_url = Column(Text, nullable=True)

    community_prediction = relationship(
        "CommunityPrediction", back_populates="match", uselist=False
    )

    # Scores
    home_final_score = Column(Integer)
    away_final_score = Column(Integer)
    home_ht_score = Column(Integer)
    away_ht_score = Column(Integer)

    # Metadata
    created_at = Column(DateTime(timezone=True), default=func.now())
    updated_at = Column(
        DateTime(timezone=True), default=func.now(), onupdate=func.now()
    )
    is_active = Column(Boolean, default=True)

    # Relationships
    competition = relationship("MatchCompetition", back_populates="matches")
    matchday = relationship("Matchday", back_populates="matches")
    referee_obj = relationship(
        "Referee",
        back_populates="matches",
        foreign_keys=[referee_id],
    )
    home_team = relationship("TeamInMatch", foreign_keys=[home_team_id])
    away_team = relationship("TeamInMatch", foreign_keys=[away_team_id])
    lineups = relationship("Lineup", back_populates="match")
    substitutions = relationship("Substitution", back_populates="match")
    goals = relationship("Goal", back_populates="match")
    cards = relationship("Card", back_populates="match")
    match_teams = relationship("MatchTeam", back_populates="match")


Index("ix_matches_competition_name", Match.competition_name)
Index("ix_matches_matchday_number", Match.matchday)
