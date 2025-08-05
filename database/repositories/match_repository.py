# database/repositories/match_repository.py
import uuid
from typing import Any, Dict, Optional

from sqlalchemy.exc import SQLAlchemyError

from database.schemas import (
    Card,
    CommunityPrediction,
    Goal,
    Lineup,
    Match,
    MatchTeam,
    Player,
    Referee,
    Substitution,
    m_competition,
    m_team,
)
from exceptions import DatabaseOperationError

from ..core.database_manager import DatabaseManager
from .base_repository import BaseRepository
from .player_repository import PlayerRepository


class MatchRepository(BaseRepository[Match]):
    """
    CRUD for Match and related entities - Fixed to work with actual extractor data
    """

    def __init__(self, db_manager: DatabaseManager):
        super().__init__(db_manager, Match)

    def get_by_id(self, entity_id: str) -> Optional[Match]:
        """Get match by ID"""
        try:
            with self.db_manager.get_session() as session:
                return session.query(Match).get(entity_id)
        except SQLAlchemyError as e:
            raise DatabaseOperationError(f"Failed to get match {entity_id}: {e}")

    def update(self, entity_id: str, update_data: Dict[str, Any]) -> Optional[Match]:
        """Update match record"""
        try:
            with self.db_manager.get_session() as session:
                match = session.query(Match).get(entity_id)
                if match:
                    for key, value in update_data.items():
                        setattr(match, key, value)
                    session.flush()
                    session.refresh(match)
                return match
        except SQLAlchemyError as e:
            raise DatabaseOperationError(f"Failed to update match {entity_id}: {e}")

    def delete(self, entity_id: str, soft_delete: bool = True) -> bool:
        """Delete match record"""
        try:
            with self.db_manager.get_session() as session:
                match = session.query(Match).get(entity_id)
                if match:
                    if soft_delete and hasattr(match, "is_active"):
                        match.is_active = False
                    else:
                        session.delete(match)
                    return True
                return False
        except SQLAlchemyError as e:
            raise DatabaseOperationError(f"Failed to delete match {entity_id}: {e}")

    def upsert(self, ctx: Any) -> None:
        """
        Upsert match and all related data — now guarantees players are merged
        before skipping existing matches.
        """
        print(f"🔍 MATCH DEBUG - {ctx.match_info.match_id}:")
        print(
            f"   Home team_id: '{ctx.home_team.team_id}' (type: {type(ctx.home_team.team_id)})"
        )
        print(
            f"   Away team_id: '{ctx.away_team.team_id}' (type: {type(ctx.away_team.team_id)})"
        )
        try:
            with self.db_manager.get_session() as session:
                # ————————————————————————————————
                # 1. Upsert Competition & Referee & Teams (unchanged)
                # ————————————————————————————————
                competition_id = str(ctx.match_info.competition_id)
                session.merge(
                    m_competition(
                        competition_id=competition_id,
                        name=ctx.match_info.competition_name,
                        logo_url=getattr(ctx.match_info, "competition_logo", None),
                    )
                )
                if ctx.match_info.referee_id:
                    session.merge(
                        Referee(
                            referee_id=str(ctx.match_info.referee_id),
                            name=ctx.match_info.referee,
                            profile_url=getattr(
                                ctx.match_info, "referee_profile_url", ""
                            ),
                        )
                    )
                home_team_id = str(ctx.home_team.team_id)
                away_team_id = str(ctx.away_team.team_id)
                session.merge(
                    m_team(
                        team_id=home_team_id,
                        name=ctx.home_team.name,
                        short_name=getattr(ctx.home_team, "short_name", None),
                        profile_url=getattr(ctx.home_team, "profile_url", None),
                        logo_url=ctx.home_team.logo_url,
                    )
                )
                session.merge(
                    m_team(
                        team_id=away_team_id,
                        name=ctx.away_team.name,
                        short_name=getattr(ctx.away_team, "short_name", None),
                        profile_url=getattr(ctx.away_team, "profile_url", None),
                        logo_url=ctx.away_team.logo_url,
                    )
                )

                # ————————————————————————————————
                # 2. Collect & Upsert ALL Players (moved above the existing-match check)
                # ————————————————————————————————
                all_players = []
                all_players.extend(ctx.home_lineup or [])
                all_players.extend(ctx.away_lineup or [])
                all_players.extend(ctx.home_substitutes or [])
                all_players.extend(ctx.away_substitutes or [])
                for goal in ctx.goals or []:
                    if goal.player:
                        all_players.append(goal.player)
                    if goal.assist_player:
                        all_players.append(goal.assist_player)
                for card in ctx.cards or []:
                    if card.player:
                        all_players.append(card.player)
                for sub in ctx.substitutions or []:
                    if sub.player_out:
                        all_players.append(sub.player_out)
                    if sub.player_in:
                        all_players.append(sub.player_in)

                player_repo = PlayerRepository(self.db_manager)
                seen = set()
                for p in all_players:
                    if not p.player_id or p.player_id in seen:
                        continue
                    seen.add(p.player_id)
                    player_repo.upsert(
                        player_id=p.player_id,
                        name=p.name,
                        short_name=getattr(p, "short_name", None),
                        profile_url=getattr(p, "profile_url", None),
                        portrait_url=getattr(p, "portrait_url", None),
                        is_active=True,
                    )

                # ————————————————————————————————
                # 3. Now skip existing match if present
                # ————————————————————————————————
                existing_match = (
                    session.query(Match)
                    .filter_by(match_id=ctx.match_info.match_id)
                    .first()
                )
                if existing_match:
                    print(
                        f"⏭️ Match {ctx.match_info.match_id} already exists, skipping upsert"
                    )
                    return

                # ————————————————————————————————
                # 4. Main Match Record and all child upserts
                # ————————————————————————————————
                match_data = self._build_match_data(ctx, home_team_id, away_team_id)
                session.merge(Match(**match_data))

                if hasattr(ctx, "community_predictions") and ctx.community_predictions:
                    pred_data = self._build_prediction_data(ctx)
                    if pred_data:
                        session.merge(CommunityPrediction(**pred_data))

                if ctx.home_team.formation:
                    session.merge(
                        MatchTeam(
                            match_id=ctx.match_info.match_id,
                            team_side="home",
                            team_id=home_team_id,
                            formation=ctx.home_team.formation,
                        )
                    )
                if ctx.away_team.formation:
                    session.merge(
                        MatchTeam(
                            match_id=ctx.match_info.match_id,
                            team_side="away",
                            team_id=away_team_id,
                            formation=ctx.away_team.formation,
                        )
                    )
                session.flush()
                self._upsert_lineups(session, ctx, home_team_id, away_team_id)
                if ctx.goals:
                    self._upsert_goals(session, ctx)
                if ctx.cards:
                    self._upsert_cards(session, ctx)
                if ctx.substitutions:
                    self._upsert_substitutions(session, ctx)

        except SQLAlchemyError as e:
            raise DatabaseOperationError(f"Match upsert failed: {e}")

    def _build_match_data(self, ctx: Any, home_team_id: str, away_team_id: str) -> Dict:
        """Build match data dictionary from extractor output"""
        return {
            "match_id": ctx.match_info.match_id,
            "matchday_id": getattr(ctx.match_info, "matchday_id", None),
            "competition_id": str(ctx.match_info.competition_id),
            "season": getattr(ctx.match_info, "season", None),
            "competition_name": ctx.match_info.competition_name,
            "matchday_number": ctx.match_info.matchday,
            "referee_id": (
                str(ctx.match_info.referee_id) if ctx.match_info.referee_id else None
            ),
            "referee_name": ctx.match_info.referee,
            # Home team data
            "home_team_id": home_team_id,
            "home_team_name": ctx.home_team.name,
            "home_team_short_name": getattr(ctx.home_team, "short_name", None),
            "home_team_logo_url": ctx.home_team.logo_url,
            "home_team_league_position": ctx.home_team.league_position,
            "home_team_formation": ctx.home_team.formation,
            "home_team_manager": ctx.home_team.manager,
            # Away team data
            "away_team_id": away_team_id,
            "away_team_name": ctx.away_team.name,
            "away_team_short_name": getattr(ctx.away_team, "short_name", None),
            "away_team_logo_url": ctx.away_team.logo_url,
            "away_team_league_position": ctx.away_team.league_position,
            "away_team_formation": ctx.away_team.formation,
            "away_team_manager": ctx.away_team.manager,
            # Match details
            "date": ctx.match_info.date,
            "day_of_week": getattr(ctx.match_info, "day_of_week", None),
            "time": ctx.match_info.time,
            "venue": ctx.match_info.venue,
            "attendance": ctx.match_info.attendance,
            "match_report_url": getattr(ctx.match_info, "match_report_url", None),
            # Scores
            "home_final_score": ctx.score.home_final,
            "away_final_score": ctx.score.away_final,
            "home_ht_score": ctx.score.home_ht,
            "away_ht_score": ctx.score.away_ht,
            # Metadata
            "is_active": True,
        }

    def _build_prediction_data(self, ctx: Any) -> Optional[Dict]:
        """Build community prediction data if available"""
        if not hasattr(ctx, "community_predictions") or not ctx.community_predictions:
            return None

        preds = ctx.community_predictions
        return {
            "prediction_id": str(uuid.uuid4()),
            "match_id": ctx.match_info.match_id,
            "home_win_pct": preds.get("home_win_percentage", 0.0),
            "draw_pct": preds.get("draw_percentage", 0.0),
            "away_win_pct": preds.get("away_win_percentage", 0.0),
        }

    def _upsert_lineups(self, session, ctx: Any, home_team_id: str, away_team_id: str):
        """Upsert lineup data"""
        # Home starting XI
        for player in ctx.home_lineup or []:
            if player.player_id:
                session.merge(
                    Lineup(
                        lineup_id=str(uuid.uuid4()),
                        match_id=ctx.match_info.match_id,
                        team_side="home",
                        player_id=player.player_id,
                        shirt_number=player.shirt_number,
                        position=player.position,
                        is_captain=player.is_captain,
                        is_starter=True,
                    )
                )

        # Away starting XI
        for player in ctx.away_lineup or []:
            if player.player_id:
                session.merge(
                    Lineup(
                        lineup_id=str(uuid.uuid4()),
                        match_id=ctx.match_info.match_id,
                        team_side="away",
                        player_id=player.player_id,
                        shirt_number=player.shirt_number,
                        position=player.position,
                        is_captain=player.is_captain,
                        is_starter=True,
                    )
                )

        # Home substitutes
        for player in ctx.home_substitutes or []:
            if player.player_id:
                session.merge(
                    Lineup(
                        lineup_id=str(uuid.uuid4()),
                        match_id=ctx.match_info.match_id,
                        team_side="home",
                        player_id=player.player_id,
                        shirt_number=player.shirt_number,
                        position=player.position,
                        is_captain=getattr(player, "is_captain", False),
                        is_starter=False,
                    )
                )

        # Away substitutes
        for player in ctx.away_substitutes or []:
            if player.player_id:
                session.merge(
                    Lineup(
                        lineup_id=str(uuid.uuid4()),
                        match_id=ctx.match_info.match_id,
                        team_side="away",
                        player_id=player.player_id,
                        shirt_number=player.shirt_number,
                        position=player.position,
                        is_captain=getattr(player, "is_captain", False),
                        is_starter=False,
                    )
                )

    def _upsert_goals(self, session, ctx: Any):
        """Upsert goal data"""
        for goal in ctx.goals:
            if goal.player and goal.player.player_id:
                # Parse score_after tuple to separate home/away scores
                home_score_after = away_score_after = None
                if goal.score_after and len(goal.score_after) == 2:
                    home_score_after, away_score_after = goal.score_after

                # Handle assist_player_id - only set if assist player exists and has valid ID
                assist_player_id = None
                if (
                    goal.assist_player
                    and goal.assist_player.player_id
                    and goal.assist_player.player_id.strip()
                ):
                    assist_player_id = goal.assist_player.player_id

                session.merge(
                    Goal(
                        match_id=ctx.match_info.match_id,
                        player_id=goal.player.player_id,
                        assist_player_id=assist_player_id,  # Can be None
                        minute=goal.minute,
                        extra_time=goal.extra_time,
                        goal_type=goal.goal_type,
                        assist_type=goal.assist_type,
                        team_side=goal.team_side,
                        home_score_after=home_score_after,
                        away_score_after=away_score_after,
                        season_goal_number=goal.season_goal_number,
                        season_assist_number=goal.season_assist_number,
                    )
                )

    def _upsert_cards(self, session, ctx: Any):
        """Upsert card data"""
        for card in ctx.cards:
            if card.player and card.player.player_id:
                session.merge(
                    Card(
                        match_id=ctx.match_info.match_id,
                        player_id=card.player.player_id,
                        minute=card.minute,
                        extra_time=card.extra_time,
                        card_type=card.card_type,
                        reason=card.reason,
                        team_side=card.team_side,
                        season_card_number=card.season_card_number,
                    )
                )

    def _upsert_substitutions(self, session, ctx: Any):
        """Upsert substitution data"""
        for sub in ctx.substitutions:
            if (
                sub.player_out
                and sub.player_in
                and sub.player_out.player_id
                and sub.player_in.player_id
            ):
                session.merge(
                    Substitution(
                        match_id=ctx.match_info.match_id,
                        player_out_id=sub.player_out.player_id,
                        player_in_id=sub.player_in.player_id,
                        minute=sub.minute,
                        extra_time=sub.extra_time,
                        reason=sub.reason,
                        team_side=sub.team_side,
                    )
                )
