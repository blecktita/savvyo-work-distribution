#!/usr/bin/env python3
"""
Database Inspector - Check if data was actually saved to the database.
Run this on your host machine to verify the club data.
"""

import argparse
from datetime import datetime, timedelta
from typing import Dict, List

import pandas as pd
from sqlalchemy import text

# Import your existing database components
from database.factory.database_factory import create_database_service


class DatabaseInspector:
    """Inspect database to verify saved data using your existing database service."""

    def __init__(self, environment: str = "production"):
        """Initialize database inspector."""
        self.environment = environment
        self.db_service = None
        self._setup_database_connection()

    def _setup_database_connection(self):
        """Setup database connection using your existing service."""
        try:
            self.db_service = create_database_service(self.environment)
            self.db_service.initialize()
            print(f"✅ Connected to {self.environment} database using your service")

        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            raise

    def check_recent_club_saves(self, hours_back: int = 24) -> pd.DataFrame:
        """Check clubs saved in the last N hours."""
        cutoff_time = datetime.now() - timedelta(hours=hours_back)

        query = text(
            """
            SELECT 
                competition_id,
                season_year,
                COUNT(*) as clubs_count,
                MIN(created_at) as first_saved,
                MAX(created_at) as last_saved
            FROM teams 
            WHERE created_at >= :cutoff_time
            GROUP BY competition_id, season_year
            ORDER BY last_saved DESC
        """
        )

        try:
            with self.db_service.transaction() as session:
                result = session.execute(query, {"cutoff_time": cutoff_time})
                df = pd.DataFrame(result.fetchall(), columns=result.keys())
                return df
        except Exception as e:
            print(f"❌ Error querying recent clubs: {e}")
            return pd.DataFrame()

    def check_specific_competition(self, competition_id: str) -> Dict:
        """Check data for a specific competition."""
        print(f"🔍 Inspecting competition: {competition_id}")

        # Check clubs/teams
        clubs_query = text(
            """
            SELECT 
                season_year,
                COUNT(*) as clubs_count,
                MIN(created_at) as first_saved,
                MAX(created_at) as last_saved,
                string_agg(club_name, ', ') as sample_clubs
            FROM teams 
            WHERE competition_id = :comp_id
            GROUP BY season_year
            ORDER BY season_year DESC
        """
        )

        # Check individual clubs for this competition
        individual_clubs_query = text(
            """
            SELECT 
                club_name,
                season_year,
                squad_size,
                total_market_value,
                created_at
            FROM teams 
            WHERE competition_id = :comp_id
            ORDER BY created_at DESC
            LIMIT 20
        """
        )

        try:
            with self.db_service.transaction() as session:
                # Get summary by season
                result = session.execute(clubs_query, {"comp_id": competition_id})
                clubs_df = pd.DataFrame(result.fetchall(), columns=result.keys())

                # Get individual clubs
                result2 = session.execute(
                    individual_clubs_query, {"comp_id": competition_id}
                )
                individual_df = pd.DataFrame(result2.fetchall(), columns=result2.keys())

                return {
                    "clubs_data": clubs_df,
                    "individual_clubs": individual_df,
                    "total_clubs": (
                        clubs_df["clubs_count"].sum() if not clubs_df.empty else 0
                    ),
                }
        except Exception as e:
            print(f"❌ Error querying competition {competition_id}: {e}")
            return {
                "clubs_data": pd.DataFrame(),
                "individual_clubs": pd.DataFrame(),
                "total_clubs": 0,
            }

    def get_database_summary(self) -> Dict:
        """Get overall database summary."""
        summary_query = text(
            """
            SELECT 
                COUNT(*) as total_teams,
                COUNT(DISTINCT competition_id) as unique_competitions,
                COUNT(DISTINCT season_year) as unique_seasons,
                MIN(created_at) as oldest_record,
                MAX(created_at) as newest_record
            FROM teams
        """
        )

        competitions_query = text(
            """
            SELECT 
                COUNT(*) as total_competitions
            FROM competitions
        """
        )

        try:
            with self.db_service.transaction() as session:
                # Get teams summary
                result = session.execute(summary_query)
                teams_summary = result.fetchone()

                # Get competitions count
                result2 = session.execute(competitions_query)
                comp_summary = result2.fetchone()

                return {
                    "teams": dict(teams_summary._mapping) if teams_summary else {},
                    "competitions": dict(comp_summary._mapping) if comp_summary else {},
                }
        except Exception as e:
            print(f"❌ Error getting database summary: {e}")
            return {"teams": {}, "competitions": {}}

    def verify_work_completion(self, work_id: str) -> Dict:
        """Verify if a specific work order was properly saved."""
        # Extract competition ID from work_id (format: comp_ISRF_97b0546c)
        parts = work_id.split("_")
        if len(parts) >= 3:
            competition_id = parts[1]
            return self.check_specific_competition(competition_id)
        else:
            print(f"❌ Invalid work_id format: {work_id}")
            return {}

    def check_recent_activity_detailed(self, hours_back: int = 1) -> None:
        """Check detailed recent activity."""
        cutoff_time = datetime.now() - timedelta(hours=hours_back)

        detailed_query = text(
            """
            SELECT 
                club_name,
                competition_id,
                season_year,
                squad_size,
                total_market_value,
                created_at
            FROM teams 
            WHERE created_at >= :cutoff_time
            ORDER BY created_at DESC
            LIMIT 50
        """
        )

        try:
            with self.db_service.transaction() as session:
                result = session.execute(detailed_query, {"cutoff_time": cutoff_time})
                df = pd.DataFrame(result.fetchall(), columns=result.keys())

                if not df.empty:
                    print(f"\n📋 DETAILED ACTIVITY (Last {hours_back} hour(s)):")
                    print(f"Total clubs saved: {len(df)}")
                    print("\nRecent clubs:")
                    for _, row in df.head(10).iterrows():
                        print(
                            f"  {row['club_name']} ({row['competition_id']}, {row['season_year']}) - {row['created_at']}"
                        )
                else:
                    print(f"\n📋 No activity in the last {hours_back} hour(s)")

        except Exception as e:
            print(f"❌ Error checking recent activity: {e}")

    def run_full_inspection(self):
        """Run comprehensive database inspection."""
        print("🔍 COMPREHENSIVE DATABASE INSPECTION")
        print("=" * 50)

        # 1. Database summary
        print("\n📊 DATABASE SUMMARY:")
        summary = self.get_database_summary()

        if summary["teams"]:
            teams_data = summary["teams"]
            print(f"  TEAMS TABLE:")
            print(f"    Total teams: {teams_data.get('total_teams', 0):,}")
            print(
                f"    Unique competitions: {teams_data.get('unique_competitions', 0)}"
            )
            print(f"    Unique seasons: {teams_data.get('unique_seasons', 0)}")
            print(
                f"    Date range: {teams_data.get('oldest_record')} to {teams_data.get('newest_record')}"
            )

        if summary["competitions"]:
            comp_data = summary["competitions"]
            print(f"  COMPETITIONS TABLE:")
            print(f"    Total competitions: {comp_data.get('total_competitions', 0):,}")

        # 2. Recent activity (last 24 hours)
        print("\n📅 RECENT ACTIVITY (Last 24 hours):")
        recent_df = self.check_recent_club_saves(24)
        if not recent_df.empty:
            print(f"  Total competitions with new data: {len(recent_df)}")
            print(f"  Total clubs saved: {recent_df['clubs_count'].sum():,}")
            print("\n  Recent competitions:")
            for _, row in recent_df.head(10).iterrows():
                print(
                    f"    {row['competition_id']} ({row['season_year']}): {row['clubs_count']} clubs"
                )
        else:
            print("  No recent activity found")

        # 3. Last hour activity (most recent)
        self.check_recent_activity_detailed(1)

    def cleanup(self):
        """Close database connection."""
        if self.db_service:
            self.db_service.cleanup()


def main():
    parser = argparse.ArgumentParser(description="Database Inspector")
    parser.add_argument(
        "--environment", default="production", help="Database environment"
    )
    parser.add_argument("--competition", help="Check specific competition ID")
    parser.add_argument("--work-id", help="Verify specific work order completion")
    parser.add_argument(
        "--hours", type=int, default=24, help="Hours back to check for recent activity"
    )

    args = parser.parse_args()

    try:
        inspector = DatabaseInspector(args.environment)

        if args.work_id:
            print(f"🔍 Verifying work order: {args.work_id}")
            result = inspector.verify_work_completion(args.work_id)
            if result and result.get("total_clubs", 0) > 0:
                print(f"✅ Total clubs in database: {result['total_clubs']}")
                if not result["clubs_data"].empty:
                    print("\nClubs by season:")
                    for _, row in result["clubs_data"].iterrows():
                        print(
                            f"  Season {row['season_year']}: {row['clubs_count']} clubs"
                        )
                        if row.get("sample_clubs"):
                            print(f"    Sample clubs: {row['sample_clubs'][:100]}...")

                if not result["individual_clubs"].empty:
                    print(f"\nRecent individual clubs (last 20):")
                    for _, row in result["individual_clubs"].head(5).iterrows():
                        print(
                            f"  {row['club_name']} - Squad: {row['squad_size']}, Value: {row['total_market_value']}"
                        )
            else:
                print("❌ No data found for this work order")

        elif args.competition:
            result = inspector.check_specific_competition(args.competition)
            if result and result.get("total_clubs", 0) > 0:
                print(f"✅ Total clubs for {args.competition}: {result['total_clubs']}")
                if not result["clubs_data"].empty:
                    print("\nClubs by season:")
                    print(result["clubs_data"].to_string(index=False))
            else:
                print(f"❌ No data found for competition {args.competition}")

        else:
            inspector.run_full_inspection()

    except Exception as e:
        print(f"❌ Inspection failed: {e}")
        import traceback

        traceback.print_exc()
    finally:
        if "inspector" in locals():
            inspector.cleanup()


if __name__ == "__main__":
    main()
