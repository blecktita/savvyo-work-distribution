#!/usr/bin/env python3
"""
Migration script to fix matchday matches with missing season
"""

import logging
import os

from sqlalchemy import text

from database.factory.database_factory import create_database_service

# ----------------------------------------------
# Configuration
# ----------------------------------------------
ENV = os.getenv("ENV", "production")
MATCHES_TABLE = os.getenv("MATCHES_TABLE", "matches")
FIX_MATCHDAY = int(os.getenv("FIX_MATCHDAY", "12"))
FIX_COMPETITION_ID = os.getenv("FIX_COMPETITION_ID", "L3")
FIX_SEASON = os.getenv("FIX_SEASON", "2024")
FIX_MATCHDAY_ID = os.getenv("FIX_MATCHDAY_ID", "e53d256d-56b5-4e0b-9026-5e9b92c9e7a2")

# ----------------------------------------------
# Logging setup
# ----------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


# ----------------------------------------------
# Update logic
# ----------------------------------------------
def update_old_matches():
    """
    Update matches for specific matchday where season is null.
    """
    # ***> initialize DB service <***
    service = create_database_service(ENV)
    with service.db_manager.engine.begin() as conn:
        try:
            logger.info(
                "🔄 Updating matches in %s for matchday %s...",
                MATCHES_TABLE,
                FIX_MATCHDAY,
            )
            # ***> execute update statement <***
            update_sql = (
                f"UPDATE {MATCHES_TABLE} "
                "SET season = :season, matchday_id = :matchday_id "
                "WHERE matchday = :matchday "
                "AND competition_id = :comp_id "
                "AND season IS NULL"
            )
            conn.execute(
                text(update_sql),
                {
                    "season": FIX_SEASON,
                    "matchday_id": FIX_MATCHDAY_ID,
                    "matchday": FIX_MATCHDAY,
                    "comp_id": FIX_COMPETITION_ID,
                },
            )
            logger.info("✅ Matches updated successfully.")
        except Exception as e:
            logger.error("❌ Update failed: %s", e)
            raise


if __name__ == "__main__":
    update_old_matches()
