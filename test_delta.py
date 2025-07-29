#!/usr/bin/env python3
import os
import re
import time
import uuid
from pathlib import Path
from datetime import datetime

from dotenv import load_dotenv
from requests.exceptions import HTTPError
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

from database.base import Base
from extractors.extractor_matchday import MatchdayExtractor, CleanMatchExtractor

from database.schemas import (
    Competition as StdCompetition,
    m_competition as ScrapedCompetition,
    m_team as  Team,
    Player,
    Referee,
    Matchday,
    MatchdaySummary,
    LeagueTableEntry,
    TopScorer,
    MatchTeam,
    Lineup,
    Substitution,
    Goal,
    Card,
    CommunityPrediction,
    Match
)


# ————————————————————————————————————————————————————————————
# 0) Helpers & config
# ————————————————————————————————————————————————————————————
load_dotenv()

def create_engine_from_env():
    user = os.getenv('POSTGRES_USER')
    pw   = os.getenv('POSTGRES_PASS')
    host = os.getenv('POSTGRES_HOST', 'localhost')
    port = os.getenv('POSTGRES_PORT', '5432')
    db   = os.getenv('POSTGRES_DB')
    if not all([user, pw, db]):
        raise RuntimeError("Missing POSTGRES_… env vars")
    return create_engine(f"postgresql://{user}:{pw}@{host}:{port}/{db}", echo=False)

def mk_uuid():
    return str(uuid.uuid4())

def mk_player(session, p):
    """Upsert a player and return the ORM instance (or None)."""
    if not p or not getattr(p, 'player_id', None) or not p.player_id.isdigit():
        return None
    pid = str(int(p.player_id))
    short = None
    parts = p.name.split()
    if len(parts) >= 2:
        short = f"{parts[0][0]}. {parts[-1]}"
    player = session.merge(Player(
        player_id=pid,
        name=p.name,
        short_name=short,
        profile_url=None,
        portrait_url=getattr(p, 'portrait_url', None)
    ))
    return player

# ————————————————————————————————————————————————————————————
# 1) Boot & session
# ————————————————————————————————————————————————————————————
engine  = create_engine_from_env()
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()

# ————————————————————————————————————————————————————————————
# 2) Scrape the matchday container
# ————————————————————————————————————————————————————————————
season = "2023-24"
md     = 3
url    = (
    "https://www.transfermarkt.com/premier-league/spieltag/"
    "wettbewerb/GB1/"
    f"saison_id/{season.split('-')[0]}/spieltag/{md}"
)

md_ex    = MatchdayExtractor()
container = md_ex.extract_from_transfermarkt_url(url, md, season)

""" # For testing: only process first match
if __name__ == "__main__":
    container.matches = container.matches[:1]
    print(f"→ Testing with only {len(container.matches)} match(es)") """

# ————————————————————————————————————————————————————————————
# 3) Persist matchday + summary
# ————————————————————————————————————————————————————————————
# 3a) matchday
existing_md = (
    session.query(Matchday)
           .filter_by(season=season, number=container.matchday_info["number"],
                      competition=container.matchday_info["competition"])
           .first()
)
if existing_md:
    matchday_id = existing_md.matchday_id
else:
    matchday_id = mk_uuid()
    session.add(Matchday(
        matchday_id=matchday_id,
        season=season,
        number=container.matchday_info["number"],
        competition=container.matchday_info["competition"],
        source_url=container.matchday_info["source_url"],
        extraction_time=datetime.fromisoformat(container.metadata["extraction_time"]),
        total_matches=container.metadata["total_matches"]
    ))

# 3b) summary (use Session.get instead of deprecated .get())
summary = session.get(MatchdaySummary, matchday_id)
if summary is None:
    session.add(MatchdaySummary(
        matchday_id=matchday_id,
        season=season,
        competition=container.matchday_info["competition"],
        matches_count=container.matchday_summary["current_matchday"]["matches"],
        goals=container.matchday_summary["current_matchday"]["goals"],
        own_goals=container.matchday_summary["current_matchday"]["own_goals"],
        yellow_cards=container.matchday_summary["current_matchday"]["yellow_cards"],
        second_yellow_cards=None,
        red_cards=container.matchday_summary["current_matchday"]["red_cards"],
        total_attendance=int(container.matchday_summary["current_matchday"]["total_attendance"]),
        average_attendance=int(container.matchday_summary["current_matchday"]["average_attendance"]),
        sold_out_matches=container.matchday_summary["current_matchday"]["sold_out_matches"]
    ))
else:
    summary.matches_count = container.matchday_summary["current_matchday"]["matches"]
    summary.goals         = container.matchday_summary["current_matchday"]["goals"]
    summary.own_goals     = container.matchday_summary["current_matchday"]["own_goals"]
    summary.yellow_cards  = container.matchday_summary["current_matchday"]["yellow_cards"]
    summary.red_cards     = container.matchday_summary["current_matchday"]["red_cards"]
    summary.total_attendance   = int(container.matchday_summary["current_matchday"]["total_attendance"])
    summary.average_attendance = int(container.matchday_summary["current_matchday"]["average_attendance"])
    summary.sold_out_matches   = container.matchday_summary["current_matchday"]["sold_out_matches"]

# 3c) league table & top scorers
for t in container.league_table["teams"]:
    # first make sure the Team record is up‑to‑date
    tid = str(int(Path(t["profile_url"]).parts[-3]))
    session.merge(Team(
        team_id    = tid,
        name       = t["name"],
        short_name = t["short_name"],
        profile_url= t["profile_url"],
        logo_url   = t["logo_url"],
    ))

    # then either update or insert that day’s table entry
    existing = (
        session.query(LeagueTableEntry)
               .filter_by(matchday_id=matchday_id, team_id=tid)
               .one_or_none()
    )
    if existing:
        existing.position        = t["position"]
        existing.matches_played  = t["matches"]
        existing.goal_difference = int(t["goal_difference"])
        existing.points          = t["points"]
    else:
        session.add(LeagueTableEntry(
            entry_id       = mk_uuid(),
            matchday_id    = matchday_id,
            team_id        = tid,
            season         = season,
            competition    = container.matchday_info["competition"],
            position       = t["position"],
            movement       = None,
            matches_played = t["matches"],
            goal_difference= int(t["goal_difference"]),
            points         = t["points"],
        ))
session.commit()

for s in container.top_scorers:
    m = re.search(r'/spieler/(\d+)', s["profile_url"])
    if not m:
        continue
    pid = str(int(m.group(1)))
    session.merge(Player(
        player_id=pid,
        name=s["name"],
        short_name=s["short_name"],
        profile_url=s["profile_url"],
        portrait_url=None
    ))
    session.add(TopScorer(
        scorer_id=mk_uuid(),
        matchday_id=matchday_id,
        player_id=pid,
        season=season,
        league=container.matchday_info["competition"],
        goals_this_matchday=int(s["goals_this_matchday"] or 0),
        total_goals=s["total_goals"]
    ))

session.commit()
print("→ Persisted matchday, summary, league table & top scorers")

# ————————————————————————————————————————————————————————————
# 4) Scrape & persist each match detail
# ————————————————————————————————————————————————————————————
match_ex = CleanMatchExtractor()

existing_ids = {
    row[0] for row in session.execute(text("SELECT match_id FROM matches")).all()
}

for ctx in container.matches:
    if not ctx.match_report_url:
        continue

    # derive match_id
    mid_m = re.search(r"spielbericht/(\d+)", ctx.match_report_url)
    if not mid_m:
        continue
    match_id = mid_m.group(1)

    # skip existing
    if session.get(Match, match_id):
        print(f"→ Match {match_id} already exists, skipping")
        continue

    existing_ids.add(match_id)

    # fetch detail (with retries)
    for attempt in range(1,4):
        try:
            detail = match_ex.extract_from_url(ctx.match_report_url)
            break
        except HTTPError:
            time.sleep(attempt * 2)
    else:
        print(f"⚠ Skipping match {match_id} after 3 failed HTTP attempts")
        continue

    try:
        # a) upsert scraped Competition & standard Competition lookup
        code = str(detail.match_info.competition_id)
        std = session.get(StdCompetition, code)
        official_name = std.competition_name if std else None

        session.merge(
            ScrapedCompetition(
                competition_id = code,
                name           = official_name,
                logo_url       = detail.match_info.competition_logo
            )
        )

        # b) upsert referee
        ref = session.merge(Referee(
            referee_id=str(detail.match_info.referee_id),
            name=detail.match_info.referee,
            profile_url=""
        ))

        # c) upsert teams
        teams = {}
        for side in ("home","away"):
            td = getattr(detail, f"{side}_team")
            tid = str(int(td.team_id))
            teams[side] = session.merge(Team(
                team_id=tid,
                name=td.name,
                short_name=None,
                profile_url="",
                logo_url=td.logo_url
            ))
        
        # derive weekday
        dow = None
        if detail.match_info.date:
            try:
                dow = datetime.fromisoformat(detail.match_info.date).strftime('%A')
            except ValueError:
                pass

        # d) create Match row with ALL denormalized fields
        match = Match(
            match_id             = match_id,
            matchday_id          = matchday_id,
            competition_id       = code,
            competition_name     = official_name,
            season               = season,
            matchday_number  = container.matchday_info["number"],
            referee_id           = ref.referee_id,
            referee_name        = ref.name,

            home_team_id         = teams["home"].team_id,
            home_team_name       = teams["home"].name,
            home_team_short_name = teams["home"].short_name,
            home_team_logo_url   = teams["home"].logo_url,
            home_team_league_position = detail.home_team.league_position,
            home_team_formation       = detail.home_team.formation,
            home_team_manager         = detail.home_team.manager,

            away_team_id         = teams["away"].team_id,
            away_team_name       = teams["away"].name,
            away_team_short_name = teams["away"].short_name,
            away_team_logo_url   = teams["away"].logo_url,
            away_team_league_position = detail.away_team.league_position,
            away_team_formation       = detail.away_team.formation,
            away_team_manager         = detail.away_team.manager,

            date                 = detail.match_info.date,
            day_of_week          = dow,
            time                 = detail.match_info.time,
            venue                = detail.match_info.venue,
            attendance           = detail.match_info.attendance,
            match_report_url     = ctx.match_report_url,

            home_final_score     = detail.score.home_final,
            away_final_score     = detail.score.away_final,
            home_ht_score        = detail.score.home_ht,
            away_ht_score        = detail.score.away_ht,
        )
        session.add(match)

        # e) community prediction
        pred = CommunityPrediction(
            prediction_id = mk_uuid(),
            match_id      = match.match_id,
            home_win_pct  = ctx.community_predictions.get("home_win_percentage", 0.0),
            draw_pct      = ctx.community_predictions.get("draw_percentage", 0.0),
            away_win_pct  = ctx.community_predictions.get("away_win_percentage", 0.0)
        )
        session.add(pred)

        # f) MatchTeam (formation + stats)
        for side in ("home","away"):
            td = getattr(detail, f"{side}_team")
            session.add(MatchTeam(
                match_id        = match.match_id,
                team_side       = side,
                team_id         = getattr(match, f"{side}_team_id"),
                formation       = td.formation,
                possession_pct  = getattr(td, "possession_pct", None),
                shots_on_target = getattr(td, "shots_on_target", None),
                shots_off_target= getattr(td, "shots_off_target", None),
                corners         = getattr(td, "corners", None),
                fouls           = getattr(td, "fouls", None),
                offsides        = getattr(td, "offsides", None),
                match_stats     = {}
            ))

        # g) lineups
        for p in detail.home_lineup + detail.away_lineup:
            orm = mk_player(session, p)
            if orm:
                side = "home" if p in detail.home_lineup else "away"
                session.add(Lineup(
                    lineup_id    = mk_uuid(),
                    match_id     = match.match_id,
                    team_side    = side,
                    player_id    = orm.player_id,
                    shirt_number = p.shirt_number,
                    position     = p.position,
                    is_captain   = p.is_captain
                ))

        # h) substitutions
        for sub in detail.substitutions:
            o = mk_player(session, sub.player_out)
            i = mk_player(session, sub.player_in)
            session.add(Substitution(
                match_id      = match.match_id,
                team_side     = sub.team_side,
                minute        = sub.minute,
                extra_time    = sub.extra_time,
                player_out_id = o.player_id if o else None,
                player_in_id  = i.player_id if i else None,
                reason        = sub.reason
            ))

        # i) goals
        for g in detail.goals:
            pl = mk_player(session, g.player)
            ap = mk_player(session, g.assist_player)
            session.add(Goal(
                match_id           = match.match_id,
                team_side          = g.team_side,
                minute             = g.minute,
                extra_time         = g.extra_time,
                player_id          = pl.player_id if pl else None,
                assist_player_id   = ap.player_id if ap else None,
                goal_type          = g.goal_type,
                assist_type        = g.assist_type,
                home_score_after   = g.score_after[0],
                away_score_after   = g.score_after[1],
                season_goal_number = g.season_goal_number,
                season_assist_number = g.season_assist_number
            ))

        # j) cards
        for c in detail.cards:
            pl = mk_player(session, c.player)
            session.add(Card(
                match_id            = match.match_id,
                team_side           = c.team_side,
                minute              = c.minute,
                extra_time          = c.extra_time,
                player_id           = pl.player_id if pl else None,
                card_type           = c.card_type,
                reason              = c.reason,
                season_card_number  = getattr(c, "season_card_number", None)
            ))

        session.commit()
        print(f"→ Committed match {match.match_id}")

    except SQLAlchemyError as e:
        session.rollback()
        print(f"⚠ Skipped match {match_id}: {e}")

# ————————————————————————————————————————————————————————————
# 5) Final row counts (for sanity)
# ————————————————————————————————————————————————————————————
print("\n🏁 Final row counts:")
for tbl in Base.metadata.sorted_tables:
    cnt = session.execute(text(f"SELECT COUNT(*) FROM {tbl.name}")).scalar()
    print(f"  • {tbl.name}: {cnt}")

session.close()
