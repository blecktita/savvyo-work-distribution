#!/usr/bin/env python3
"""
Direct Progress Sync Script - Uses your exact database schema
If data exists in teams table, mark it as completed in progress tables.
"""

import argparse

from sqlalchemy import text

from coordination.coordinator import create_work_tracker


class DirectProgressSync:
    """
    Sync progress tables with actual teams data
    """

    def __init__(self, environment: str = "production"):
        self.environment = environment
        self.progress_monitor = create_work_tracker(environment)
        print(f"🔄 Direct sync initialized for {environment}")

    def analyze_teams_vs_progress(self):
        """Show what's in teams vs what's in progress tables."""
        print("\n📊 TEAMS DATA vs PROGRESS TABLES")
        print("=" * 50)

        with self.progress_monitor.db_service.transaction() as session:
            # What's actually in teams table
            teams_result = session.execute(
                text(
                    """
                SELECT 
                    competition_id,
                    COUNT(DISTINCT season_year) as seasons,
                    COUNT(*) as total_teams,
                    MIN(created_at) as first_created,
                    MAX(created_at) as last_created
                FROM teams 
                GROUP BY competition_id
                ORDER BY total_teams DESC
                LIMIT 20
            """
                )
            )

            print("📊 TOP 20 COMPETITIONS IN TEAMS TABLE:")
            for row in teams_result.fetchall():
                comp_id, seasons, teams, first, last = row
                print(
                    f"   {comp_id}: {teams} teams, {seasons} seasons, latest: {last.date()}"
                )

            # What's in competition_progress table
            comp_progress_result = session.execute(
                text(
                    """
                SELECT competition_id, status, completed_at
                FROM competition_progress
                ORDER BY completed_at DESC
                LIMIT 10
            """
                )
            )

            print("\n📈 COMPETITION_PROGRESS TABLE (latest 10):")
            comp_progress_rows = comp_progress_result.fetchall()
            if comp_progress_rows:
                for row in comp_progress_rows:
                    comp_id, status, completed = row
                    completed_str = completed.date() if completed else "None"
                    print(f"   {comp_id}: {status}, completed: {completed_str}")
            else:
                print("   (empty)")

            # What's in season_progress table
            season_progress_result = session.execute(
                text(
                    """
                SELECT competition_id, COUNT(*) as season_count,
                       COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed_count
                FROM season_progress
                GROUP BY competition_id
                ORDER BY season_count DESC
                LIMIT 10
            """
                )
            )

            print("\n📅 SEASON_PROGRESS TABLE (top 10):")
            season_progress_rows = season_progress_result.fetchall()
            if season_progress_rows:
                for row in season_progress_rows:
                    comp_id, total, completed = row
                    print(f"   {comp_id}: {completed}/{total} seasons completed")
            else:
                print("   (empty)")

    def sync_progress_with_teams_data(self, dry_run: bool = True):
        """
        Sync progress tables with teams table data.
        Simple rule: if teams exist, mark as completed.
        """
        print(f"\n🔄 SYNCING PROGRESS TABLES (dry_run={dry_run})")
        print("=" * 50)

        if dry_run:
            print("🔍 DRY RUN - showing what would be done")

        stats = {
            "competitions_to_add": 0,
            "competitions_to_update": 0,
            "seasons_to_add": 0,
            "seasons_to_update": 0,
        }

        with self.progress_monitor.db_service.transaction() as session:
            # Get all competitions that have teams data
            teams_data = session.execute(
                text(
                    """
                SELECT 
                    t.competition_id,
                    COUNT(DISTINCT t.season_year) as seasons_count,
                    COUNT(*) as total_teams,
                    MAX(t.created_at) as latest_created,
                    c.competition_url
                FROM teams t
                LEFT JOIN competitions c ON t.competition_id = c.competition_id
                GROUP BY t.competition_id, c.competition_url
                ORDER BY total_teams DESC
            """
                )
            )

            for row in teams_data.fetchall():
                comp_id, seasons_count, total_teams, latest_created, comp_url = row

                print(
                    f"\n📊 Processing {comp_id}: {total_teams} teams, {seasons_count} seasons"
                )

                # Handle competition_progress
                comp_progress = session.execute(
                    text(
                        """
                    SELECT id, status FROM competition_progress
                    WHERE competition_id = :comp_id
                """
                    ),
                    {"comp_id": comp_id},
                ).fetchone()

                if comp_progress is None:
                    # Need to add competition_progress record
                    stats["competitions_to_add"] += 1
                    print("   ➕ Would ADD competition_progress record")

                    if not dry_run:
                        # Use actual competition_url from competitions table, or fallback
                        url = comp_url if comp_url else f"MISSING_URL_FOR_{comp_id}"

                        session.execute(
                            text(
                                """
                            INSERT INTO competition_progress 
                            (competition_id, competition_url, status, seasons_discovered, completed_at)
                            VALUES (:comp_id, :url, 'completed', :seasons, :completed_at)
                        """
                            ),
                            {
                                "comp_id": comp_id,
                                "url": url,
                                "seasons": seasons_count,
                                "completed_at": latest_created,
                            },
                        )
                        print(f"   ✅ ADDED competition_progress for {comp_id}")

                elif comp_progress[1] != "completed":
                    # Need to update competition_progress record
                    stats["competitions_to_update"] += 1
                    print(
                        f"   🔄 Would UPDATE competition_progress: {comp_progress[1]} -> completed"
                    )

                    if not dry_run:
                        session.execute(
                            text(
                                """
                            UPDATE competition_progress 
                            SET status = 'completed', completed_at = :completed_at, seasons_discovered = :seasons
                            WHERE competition_id = :comp_id
                        """
                            ),
                            {
                                "comp_id": comp_id,
                                "completed_at": latest_created,
                                "seasons": seasons_count,
                            },
                        )
                        print(f"   ✅ UPDATED competition_progress for {comp_id}")

                # Handle season_progress for this competition
                season_data = session.execute(
                    text(
                        """
                    SELECT season_year, COUNT(*) as team_count, MAX(created_at) as latest
                    FROM teams 
                    WHERE competition_id = :comp_id
                    GROUP BY season_year
                """
                    ),
                    {"comp_id": comp_id},
                )

                for season_row in season_data.fetchall():
                    season_year, team_count, season_latest = season_row

                    # Check if season_progress record exists
                    season_progress = session.execute(
                        text(
                            """
                        SELECT id, status, clubs_saved FROM season_progress
                        WHERE competition_id = :comp_id AND season_year = :season_year
                    """
                        ),
                        {"comp_id": comp_id, "season_year": str(season_year)},
                    ).fetchone()

                    if season_progress is None:
                        # Need to add season_progress record
                        stats["seasons_to_add"] += 1
                        print(
                            f"   ➕ Would ADD season_progress: {season_year} ({team_count} teams)"
                        )

                        if not dry_run:
                            session.execute(
                                text(
                                    """
                                INSERT INTO season_progress 
                                (competition_id, season_id, season_year, status, completed_at, clubs_saved)
                                VALUES (:comp_id, :season_id, :season_year, 'completed', :completed_at, :clubs_saved)
                            """
                                ),
                                {
                                    "comp_id": comp_id,
                                    "season_id": f"{comp_id}_{season_year}",
                                    "season_year": str(season_year),
                                    "completed_at": season_latest,
                                    "clubs_saved": team_count,
                                },
                            )

                    elif (
                        season_progress[1] != "completed"
                        or season_progress[2] != team_count
                    ):
                        # Need to update season_progress record
                        stats["seasons_to_update"] += 1
                        print(
                            f"   🔄 Would UPDATE season_progress: {season_year} ({team_count} teams)"
                        )

                        if not dry_run:
                            session.execute(
                                text(
                                    """
                                UPDATE season_progress 
                                SET status = 'completed', completed_at = :completed_at, clubs_saved = :clubs_saved
                                WHERE competition_id = :comp_id AND season_year = :season_year
                            """
                                ),
                                {
                                    "comp_id": comp_id,
                                    "season_year": str(season_year),
                                    "completed_at": season_latest,
                                    "clubs_saved": team_count,
                                },
                            )

            if not dry_run:
                session.commit()
                print("\n✅ SYNC COMPLETED")

        print("\n📊 SUMMARY:")
        print(f"   Competitions to add: {stats['competitions_to_add']}")
        print(f"   Competitions to update: {stats['competitions_to_update']}")
        print(f"   Seasons to add: {stats['seasons_to_add']}")
        print(f"   Seasons to update: {stats['seasons_to_update']}")

        if dry_run:
            print("\n💡 Run with --execute to actually make these changes")

        return stats

    def sync_specific_competition(self, competition_id: str, dry_run: bool = True):
        """Sync progress for one specific competition."""
        print(f"\n🎯 SYNCING {competition_id}")
        print("=" * 30)

        with self.progress_monitor.db_service.transaction() as session:
            # Check teams data for this competition
            teams_check = session.execute(
                text(
                    """
                SELECT 
                    COUNT(DISTINCT season_year) as seasons,
                    COUNT(*) as teams,
                    MAX(created_at) as latest
                FROM teams 
                WHERE competition_id = :comp_id
            """
                ),
                {"comp_id": competition_id},
            ).fetchone()

            if not teams_check or teams_check[1] == 0:
                print(f"❌ No teams data found for {competition_id}")
                return

            seasons, teams, latest = teams_check
            print(f"📊 Found: {teams} teams across {seasons} seasons")

            if dry_run:
                print(f"🔍 Would mark {competition_id} as completed")
                return

            # Get competition URL
            comp_url_result = session.execute(
                text(
                    """
                SELECT competition_url FROM competitions WHERE competition_id = :comp_id
            """
                ),
                {"comp_id": competition_id},
            ).fetchone()

            comp_url = (
                comp_url_result[0]
                if comp_url_result
                else f"MISSING_URL_FOR_{competition_id}"
            )

            # Sync competition_progress
            session.execute(
                text(
                    """
                INSERT INTO competition_progress 
                (competition_id, competition_url, status, seasons_discovered, completed_at)
                VALUES (:comp_id, :url, 'completed', :seasons, :completed_at)
                ON CONFLICT (competition_id) DO UPDATE SET
                    status = 'completed',
                    completed_at = :completed_at,
                    seasons_discovered = :seasons
            """
                ),
                {
                    "comp_id": competition_id,
                    "url": comp_url,
                    "seasons": seasons,
                    "completed_at": latest,
                },
            )

            # Sync season_progress for all seasons
            seasons_data = session.execute(
                text(
                    """
                SELECT season_year, COUNT(*) as team_count, MAX(created_at) as latest
                FROM teams 
                WHERE competition_id = :comp_id
                GROUP BY season_year
            """
                ),
                {"comp_id": competition_id},
            )

            for season_year, team_count, season_latest in seasons_data.fetchall():
                session.execute(
                    text(
                        """
                    INSERT INTO season_progress 
                    (competition_id, season_id, season_year, status, completed_at, clubs_saved)
                    VALUES (:comp_id, :season_id, :season_year, 'completed', :completed_at, :clubs_saved)
                    ON CONFLICT (competition_id, season_id) DO UPDATE SET
                        status = 'completed',
                        completed_at = :completed_at,
                        clubs_saved = :clubs_saved
                """
                    ),
                    {
                        "comp_id": competition_id,
                        "season_id": f"{competition_id}_{season_year}",
                        "season_year": str(season_year),
                        "completed_at": season_latest,
                        "clubs_saved": team_count,
                    },
                )

            session.commit()
            print(f"✅ Synced {competition_id}: {seasons} seasons marked completed")

    def cleanup(self):
        if hasattr(self, "progress_monitor"):
            self.progress_monitor.db_service.cleanup()


def main():
    parser = argparse.ArgumentParser(description="Sync progress tables with teams data")
    parser.add_argument(
        "--environment", default="production", help="Database environment"
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually make changes (default is dry run)",
    )
    parser.add_argument("--competition", help="Sync specific competition only")
    parser.add_argument(
        "--analyze", action="store_true", help="Just analyze current state"
    )

    args = parser.parse_args()

    try:
        syncer = DirectProgressSync(args.environment)

        if args.analyze:
            syncer.analyze_teams_vs_progress()
        elif args.competition:
            syncer.sync_specific_competition(args.competition, dry_run=not args.execute)
        else:
            syncer.sync_progress_with_teams_data(dry_run=not args.execute)

    except KeyboardInterrupt:
        print("\n⏹️ Interrupted")
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()
    finally:
        if "syncer" in locals():
            syncer.cleanup()


if __name__ == "__main__":
    main()
