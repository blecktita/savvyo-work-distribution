#!/usr/bin/env python3
"""
Simple PostgreSQL Table Truncate Script
======================================

Usage:
  python clear_tables.py

This script truncates the specified PostgreSQL tables (CASCADE) and exits.
Configure your DB connection via environment variables:
  POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASS, POSTGRES_DB

Set the `TABLES_TO_CLEAR` list below to the tables you want emptied.
"""

import os

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# List the Postgres tables you want to clear
TABLES_TO_CLEAR = [
    "goals",
    "cards",
    "matchdays",
    "matchday_summary",
    "matches",
    "community_predictions",
    "referees",
    "m_competitions",
    "matchday_info",
    "m_teams",
    "league_table",
    "match_teams",
    "lineups",
    "players",
    "match_lineups",
    "substitutions",
    "top_scorers",
]


def main():
    load_dotenv()
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASS")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB")

    if not all([user, password, db]):
        print(
            "Error: Please set POSTGRES_USER, POSTGRES_PASS, and POSTGRES_DB in your environment."
        )
        return

    url = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    engine = create_engine(url)

    with engine.begin() as conn:
        for table in TABLES_TO_CLEAR:
            print(f"Truncating table '{table}'...")
            conn.execute(text(f"TRUNCATE TABLE {table} CASCADE"))
    print("\n✓ All specified tables have been cleared.")


if __name__ == "__main__":
    main()
