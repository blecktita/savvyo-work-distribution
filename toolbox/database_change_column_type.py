#!/usr/bin/env python3
"""
Script to migrate the movement column from INTEGER to VARCHAR(10)
"""

import os

from sqlalchemy import text

from database.factory.database_factory import create_database_service


def migrate_movement_column():
    """Apply the migration to change movement column type"""
    service = create_database_service(os.getenv("ENV", "production"))

    # Get a raw connection to execute DDL
    with service.db_manager.engine.begin() as conn:
        try:
            print("🔄 Migrating movement column from INTEGER to VARCHAR(10)...")

            # Check current column type
            result = conn.execute(
                text(
                    """
                SELECT column_name, data_type, character_maximum_length 
                FROM information_schema.columns 
                WHERE table_name = 'league_table' AND column_name = 'movement'
            """
                )
            )

            current_info = result.fetchone()
            if current_info:
                print(f"📋 Current column info: {current_info}")

            # Alter the column type
            conn.execute(
                text("ALTER TABLE league_table ALTER COLUMN movement TYPE VARCHAR(10)")
            )

            # Verify the change
            result = conn.execute(
                text(
                    """
                SELECT column_name, data_type, character_maximum_length 
                FROM information_schema.columns 
                WHERE table_name = 'league_table' AND column_name = 'movement'
            """
                )
            )

            new_info = result.fetchone()
            print(f"✅ New column info: {new_info}")
            print("🎉 Migration completed successfully!")

        except Exception as e:
            print(f"❌ Migration failed: {e}")
            raise


if __name__ == "__main__":
    migrate_movement_column()
