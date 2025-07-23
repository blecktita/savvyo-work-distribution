# database/converters/match_converter.py
"""
Match data converter that integrates with existing database architecture.
Converts dataclass models to SQLAlchemy models following existing patterns.
"""

from typing import List, Optional
from sqlalchemy.orm import Session

# Import dataclass models from the scraper
from extractors.extractor_matchday import (
    MatchDetail, MatchdayContainer,
    Player as DataclassPlayer, Team as DataclassTeam,
    Goal as DataclassGoal, Card as DataclassCard,
    Substitution as DataclassSubstitution
)

from .match_models import (
    Match, Player, MatchLineup, Goal, Card, Substitution, MatchdayInfo
)

from exceptions import DatabaseOperationError


class MatchDataConverter:
    """
    Converts dataclass models to SQLAlchemy models for database storage.
    Integrates with existing database architecture patterns.
    """
    
    def __init__(self):
        self.session: Optional[Session] = None
        self._player_cache = {}  # Cache to avoid duplicate players

    def set_session(self, session: Session):
        """
        Set the database session for operations.
        
        Args:
            session: SQLAlchemy session
        """
        self.session = session
        self._player_cache.clear()  # Clear cache for new session

    def convert_and_save_match(self, match_detail: MatchDetail) -> bool:
        """
        Complete conversion and save to database following existing patterns.
        
        Args:
            match_detail: MatchDetail dataclass from extractor
            
        Returns:
            True if successful, False otherwise
        """
        if not self.session:
            raise DatabaseOperationError("No database session set")

        try:
            # 1. Convert main match record
            match = self._convert_match_detail(match_detail)
            
            # Check if match already exists
            existing_match = self.session.query(Match).filter(
                Match.match_id == match.match_id
            ).first()
            
            if existing_match:
                # Update existing match
                self._update_match_fields(existing_match, match)
                match = existing_match
            else:
                # Add new match
                self.session.add(match)
            
            # Flush to get any generated IDs
            self.session.flush()
            
            # 2. Convert and save players and lineups
            players, lineups = self._create_players_and_lineups(match_detail)
            
            # Save players (use merge to handle duplicates)
            for player in players:
                self.session.merge(player)
            
            # Delete existing lineups for this match (if updating)
            if existing_match:
                self.session.query(MatchLineup).filter(
                    MatchLineup.match_id == match_detail.match_info.match_id
                ).delete()
            
            # Add new lineups
            for lineup in lineups:
                self.session.add(lineup)
            
            # 3. Convert and save events
            self._save_match_events(match_detail, existing_match is not None)
            
            # 4. Flush all changes
            self.session.flush()
            
            return True
            
        except Exception as e:
            print(f"❌ Error in convert_and_save_match: {e}")
            import traceback
            print(traceback.format_exc())
            return False

    def _convert_match_detail(self, match_detail: MatchDetail) -> Match:
        """
        Convert a MatchDetail dataclass to SQLAlchemy Match model.
        """
        # Get the absolute URL that was actually scraped
        source_url = None
        if match_detail.extraction_metadata:
            source_url = match_detail.extraction_metadata.get('source_url')
        
        # Extract the relative match_report_url from the absolute source_url
        match_report_url = None
        if source_url:
            # Convert: "https://www.transfermarkt.com/spielbericht/index/spielbericht/4087924"
            # To: "/spielbericht/index/spielbericht/4087924"
            if '/spielbericht/' in source_url:
                match_report_url = source_url.split('transfermarkt.com')[-1]
        
        return Match(
            match_id=match_detail.match_info.match_id,
            competition_name=match_detail.match_info.competition_name,
            competition_id=match_detail.match_info.competition_id,
            competition_logo=match_detail.match_info.competition_logo,
            matchday=match_detail.match_info.matchday,
            season=match_detail.match_info.season,
            date=match_detail.match_info.date,
            time=match_detail.match_info.time,
            venue=match_detail.match_info.venue,
            attendance=match_detail.match_info.attendance,
            referee=match_detail.match_info.referee,
            referee_id=match_detail.match_info.referee_id,
            
            # Home team info
            home_team_id=match_detail.home_team.team_id,
            home_team_name=match_detail.home_team.name,
            home_team_short_name=match_detail.home_team.short_name,
            home_team_logo_url=match_detail.home_team.logo_url,
            home_team_league_position=match_detail.home_team.league_position,
            home_team_formation=match_detail.home_team.formation,
            home_team_manager=match_detail.home_team.manager,
            
            # Away team info
            away_team_id=match_detail.away_team.team_id,
            away_team_name=match_detail.away_team.name,
            away_team_short_name=match_detail.away_team.short_name,
            away_team_logo_url=match_detail.away_team.logo_url,
            away_team_league_position=match_detail.away_team.league_position,
            away_team_formation=match_detail.away_team.formation,
            away_team_manager=match_detail.away_team.manager,
            
            # Score info
            home_final_score=match_detail.score.home_final,
            away_final_score=match_detail.score.away_final,
            home_ht_score=match_detail.score.home_ht,
            away_ht_score=match_detail.score.away_ht,
            
            # CLEAR DISTINCTION:
            match_report_url=match_report_url,  # "/spielbericht/index/spielbericht/4087924"
            source_url=source_url,              # "https://www.transfermarkt.com/spielbericht/index/spielbericht/4087924"
            
            # Metadata
            extraction_metadata=match_detail.extraction_metadata
        )

    def _update_match_fields(self, existing_match: Match, new_match: Match):
        """
        Update existing match with new data.
        """
        # List of fields to update (exclude primary key and timestamps)
        update_fields = [
            'competition_name', 'competition_id', 'competition_logo',
            'matchday', 'season', 'date', 'time', 'venue', 'attendance',
            'referee', 'referee_id', 'home_team_id', 'home_team_name',
            'home_team_short_name', 'home_team_logo_url', 'home_team_league_position',
            'home_team_formation', 'home_team_manager', 'away_team_id',
            'away_team_name', 'away_team_short_name', 'away_team_logo_url',
            'away_team_league_position', 'away_team_formation', 'away_team_manager',
            'home_final_score', 'away_final_score', 'home_ht_score', 'away_ht_score',
            'match_report_url', 'source_url',  # FIXED: Add missing URL fields
            'extraction_metadata'
        ]
        
        for field in update_fields:
            if hasattr(new_match, field):
                setattr(existing_match, field, getattr(new_match, field))

    def _create_players_and_lineups(
        self, 
        match_detail: MatchDetail
        ) -> tuple[List[Player], List[MatchLineup]]:
        """
        Create Player and MatchLineup records from match detail.
        """
        players = []
        lineups = []

        home_team_id = match_detail.home_team.team_id
        away_team_id = match_detail.away_team.team_id
        
        # Process home team starting lineup
        for player_data in match_detail.home_lineup:
            player = self._get_or_create_player(player_data)
            players.append(player)
            
            lineup = MatchLineup(
                match_id=match_detail.match_info.match_id,
                player_id=player.player_id,
                team_side='home',
                team_id = home_team_id,
                lineup_type='starting',
                shirt_number=player_data.shirt_number,
                position=player_data.position,
                is_captain=player_data.is_captain
            )
            lineups.append(lineup)
        
        # Process home substitutes
        for player_data in match_detail.home_substitutes:
            player = self._get_or_create_player(player_data)
            players.append(player)
            
            lineup = MatchLineup(
                match_id=match_detail.match_info.match_id,
                player_id=player.player_id,
                team_side='home',
                team_id = home_team_id,
                lineup_type='substitute',
                shirt_number=player_data.shirt_number,
                position=player_data.position,
                is_captain=player_data.is_captain
            )
            lineups.append(lineup)
        
        # Process away team starting lineup
        for player_data in match_detail.away_lineup:
            player = self._get_or_create_player(player_data)
            players.append(player)
            
            lineup = MatchLineup(
                match_id=match_detail.match_info.match_id,
                player_id=player.player_id,
                team_side='away',
                team_id = away_team_id,
                lineup_type='starting',
                shirt_number=player_data.shirt_number,
                position=player_data.position,
                is_captain=player_data.is_captain
            )
            lineups.append(lineup)
        
        # Process away substitutes
        for player_data in match_detail.away_substitutes:
            player = self._get_or_create_player(player_data)
            players.append(player)
            
            lineup = MatchLineup(
                match_id=match_detail.match_info.match_id,
                player_id=player.player_id,
                team_side='away',
                team_id = away_team_id,
                lineup_type='substitute',
                shirt_number=player_data.shirt_number,
                position=player_data.position,
                is_captain=player_data.is_captain
            )
            lineups.append(lineup)
        
        return players, lineups

    def _save_match_events(self, match_detail: MatchDetail, is_update: bool = False):
        """
        Save match events (goals, cards, substitutions).
        """
        if is_update:
            # Delete existing events for this match
            self.session.query(Goal).filter(
                Goal.match_id == match_detail.match_info.match_id
            ).delete()
            self.session.query(Card).filter(
                Card.match_id == match_detail.match_info.match_id
            ).delete()
            self.session.query(Substitution).filter(
                Substitution.match_id == match_detail.match_info.match_id
            ).delete()
        
        # Add goals
        for goal_data in match_detail.goals:
            goal = self._create_goal(match_detail.match_info.match_id, goal_data)
            if goal:
                self.session.add(goal)
        
        # Add cards
        for card_data in match_detail.cards:
            card = self._create_card(match_detail.match_info.match_id, card_data)
            if card:
                self.session.add(card)
        
        # Add substitutions
        for sub_data in match_detail.substitutions:
            substitution = self._create_substitution(match_detail.match_info.match_id, sub_data)
            if substitution:
                self.session.add(substitution)

    def _create_goal(self, match_id: str, goal_data: DataclassGoal) -> Optional[Goal]:
        """
        Create Goal record from dataclass.
        """
        # Ensure scorer exists in database
        scorer_id = None
        if goal_data.player:
            scorer = self._get_or_create_player(goal_data.player)
            scorer_id = scorer.player_id
        
        # Ensure assist provider exists
        assist_id = None
        if goal_data.assist_player:
            assist_provider = self._get_or_create_player(goal_data.assist_player)
            assist_id = assist_provider.player_id
        
        # Calculate score after goal
        home_score = None
        away_score = None
        if goal_data.score_after:
            home_score, away_score = goal_data.score_after
        
        return Goal(
            match_id=match_id,
            player_id=scorer_id,
            assist_player_id=assist_id,
            minute=goal_data.minute,
            extra_time=goal_data.extra_time,
            goal_type=goal_data.goal_type,
            assist_type=goal_data.assist_type,
            team_side=goal_data.team_side,
            home_score_after=home_score,
            away_score_after=away_score,
            season_goal_number=goal_data.season_goal_number,
            season_assist_number=goal_data.season_assist_number
        )

    def _create_card(self, match_id: str, card_data: DataclassCard) -> Optional[Card]:
        """
        Create Card record from dataclass.
        """
        # Ensure player exists
        player_id = None
        if card_data.player:
            player = self._get_or_create_player(card_data.player)
            player_id = player.player_id
        
        return Card(
            match_id=match_id,
            player_id=player_id,
            minute=card_data.minute,
            extra_time=card_data.extra_time,
            card_type=card_data.card_type,
            reason=card_data.reason,
            team_side=card_data.team_side,
            season_card_number=card_data.season_card_number
        )

    def _create_substitution(self, match_id: str, sub_data: DataclassSubstitution) -> Optional[Substitution]:
        """
        Create Substitution record from dataclass.
        """
        # Ensure players exist
        player_out_id = None
        if sub_data.player_out:
            player_out = self._get_or_create_player(sub_data.player_out)
            player_out_id = player_out.player_id
        
        player_in_id = None
        if sub_data.player_in:
            player_in = self._get_or_create_player(sub_data.player_in)
            player_in_id = player_in.player_id
        
        return Substitution(
            match_id=match_id,
            player_out_id=player_out_id,
            player_in_id=player_in_id,
            minute=sub_data.minute,
            extra_time=sub_data.extra_time,
            reason=sub_data.reason,
            team_side=sub_data.team_side
        )

    def create_matchday_info(self, matchday_container: MatchdayContainer) -> Optional[MatchdayInfo]:
        """
        Create MatchdayInfo from MatchdayContainer.
        """
        return MatchdayInfo(
            competition_id=matchday_container.matchday_info.get('competition_id'),
            competition_name=matchday_container.matchday_info.get('competition_name'),
            season=matchday_container.matchday_info.get('season'),
            matchday_number=matchday_container.matchday_info.get('matchday_number'),
            source_url=matchday_container.matchday_info.get('source_url'),
            league_table=matchday_container.league_table,
            top_scorers=matchday_container.top_scorers,
            matchday_summary=matchday_container.matchday_summary,
            extraction_metadata=matchday_container.metadata
        )

    def _get_or_create_player(self, player_data: DataclassPlayer) -> Player:
        """
        Get existing player or create new one, with caching following existing patterns.
        """
        if not player_data or not player_data.player_id:
            raise ValueError("Player data and player_id are required")
        
        # Check cache first
        if player_data.player_id in self._player_cache:
            return self._player_cache[player_data.player_id]
        
        # Check database
        existing_player = self.session.query(Player).filter(
            Player.player_id == player_data.player_id
        ).first()
        
        if existing_player:
            # Update existing player with new information
            if player_data.name:
                existing_player.name = player_data.name
            if player_data.portrait_url:
                existing_player.portrait_url = player_data.portrait_url
            
            self._player_cache[player_data.player_id] = existing_player
            return existing_player
        
        # Create new player
        new_player = Player(
            player_id=player_data.player_id,
            name=player_data.name or "",
            portrait_url=player_data.portrait_url
        )
        
        self._player_cache[player_data.player_id] = new_player
        return new_player
        """
        Get existing player or create new one, with caching following existing patterns.
        """
        if not player_data or not player_data.player_id:
            raise ValueError("Player data and player_id are required")
        
        # Check cache first
        if player_data.player_id in self._player_cache:
            return self._player_cache[player_data.player_id]
        
        # Check database
        existing_player = self.session.query(Player).filter(
            Player.player_id == player_data.player_id
        ).first()
        
        if existing_player:
            # Update existing player with new information
            if player_data.name:
                existing_player.name = player_data.name
            if player_data.portrait_url:
                existing_player.portrait_url = player_data.portrait_url
            
            self._player_cache[player_data.player_id] = existing_player
            return existing_player
        
        # Create new player
        new_player = Player(
            player_id=player_data.player_id,
            name=player_data.name or "",
            portrait_url=player_data.portrait_url
        )
        
        self._player_cache[player_data.player_id] = new_player
        return new_player