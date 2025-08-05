#!/usr/bin/env python3
"""
SQLite Database Analyzer
========================
Analyzes your football.db to see exactly what data you have
"""

import sqlite3
from pathlib import Path

import pandas as pd


def analyze_sqlite_database(db_path: str):
    """Comprehensive analysis of SQLite database"""

    if not Path(db_path).exists():
        print(f"❌ Database file not found: {db_path}")
        return

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    print("=" * 80)
    print("SQLITE DATABASE ANALYSIS")
    print("=" * 80)
    print(f"Database: {db_path}")
    print(f"Size: {Path(db_path).stat().st_size / 1024 / 1024:.2f} MB")

    # Get all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = [row[0] for row in cursor.fetchall()]

    print(f"\nFound {len(tables)} tables:")

    total_rows = 0
    table_info = {}

    for table in tables:
        try:
            # Get table structure
            cursor.execute(f"PRAGMA table_info({table})")
            columns = cursor.fetchall()

            # Get row count
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            row_count = cursor.fetchone()[0]
            total_rows += row_count

            # Get sample data
            cursor.execute(f"SELECT * FROM {table} LIMIT 3")
            sample_rows = cursor.fetchall()

            table_info[table] = {
                "columns": columns,
                "row_count": row_count,
                "sample_rows": sample_rows,
            }

            print(f"\n📊 {table.upper()} ({row_count:,} rows)")
            print("   Columns:")
            for col in columns:
                pk_marker = " 🔑" if col[5] else ""
                nn_marker = " ⚠️" if col[3] else ""
                print(f"     - {col[1]} ({col[2]}){pk_marker}{nn_marker}")

            if sample_rows:
                print("   Sample data:")
                for i, row in enumerate(sample_rows[:2]):
                    print(f"     Row {i + 1}: {dict(row)}")

        except Exception as e:
            print(f"   ❌ Error analyzing {table}: {e}")

    print(f"\n📈 SUMMARY:")
    print(f"   Total tables: {len(tables)}")
    print(f"   Total rows: {total_rows:,}")

    # Check what will be migrated
    print(f"\n🔄 MIGRATION COMPATIBILITY:")

    migration_tables = {
        "teams": "m_teams",
        "competitions": "m_competitions",
        "players": "players",
        "matches": "matches",
        "goals": "goals",
        "cards": "cards",
        "substitutions": "substitutions",
    }

    will_migrate = []
    wont_migrate = []

    for sqlite_table, postgres_table in migration_tables.items():
        if sqlite_table in tables:
            row_count = table_info[sqlite_table]["row_count"]
            will_migrate.append(
                f"✅ {sqlite_table} → {postgres_table} ({row_count:,} rows)"
            )
        else:
            wont_migrate.append(f"❌ {sqlite_table} (not found)")

    print("   Will migrate:")
    for item in will_migrate:
        print(f"     {item}")

    if wont_migrate:
        print("   Won't migrate:")
        for item in wont_migrate:
            print(f"     {item}")

    # Check for additional tables not in migration script
    extra_tables = [t for t in tables if t not in migration_tables.keys()]
    if extra_tables:
        print(f"\n⚠️  ADDITIONAL TABLES NOT IN MIGRATION SCRIPT:")
        for table in extra_tables:
            row_count = table_info[table]["row_count"]
            print(f"     📋 {table} ({row_count:,} rows)")
        print("     💡 These would need custom migration logic")

    # Suggest migration strategy
    print(f"\n🎯 MIGRATION RECOMMENDATION:")
    if len(will_migrate) >= 4:  # At least teams, matches, players, goals
        print("     ✅ Good coverage - basic migration will work well")
        print("     📝 Consider adding custom logic for extra tables")
    elif len(will_migrate) >= 2:
        print("     ⚠️  Partial coverage - some key data will migrate")
        print("     🔧 May need to adapt table names in migration script")
    else:
        print("     ❌ Poor coverage - migration script needs major adaptation")
        print("     🛠️  Consider custom migration for your table structure")

    conn.close()

    return table_info


def suggest_migration_modifications(table_info):
    """Suggest modifications to migration script based on actual data"""
    print(f"\n" + "=" * 80)
    print("MIGRATION SCRIPT MODIFICATIONS NEEDED")
    print("=" * 80)

    modifications = []

    # Check for different table names
    if "team" in table_info and "teams" not in table_info:
        modifications.append("📝 Change 'teams' to 'team' in migration script")

    if "match" in table_info and "matches" not in table_info:
        modifications.append("📝 Change 'matches' to 'match' in migration script")

    # Check for unique tables that need custom migration
    extra_tables = []
    standard_tables = {
        "teams",
        "competitions",
        "players",
        "matches",
        "goals",
        "cards",
        "substitutions",
    }

    for table_name in table_info.keys():
        if (
            table_name not in standard_tables
            and table_info[table_name]["row_count"] > 0
        ):
            extra_tables.append(table_name)

    if extra_tables:
        modifications.append(f"🔧 Add custom migration for: {', '.join(extra_tables)}")

    if modifications:
        print("Needed modifications:")
        for mod in modifications:
            print(f"   {mod}")
    else:
        print("✅ No modifications needed - standard migration should work!")


def main():
    print("SQLite Database Analyzer")
    print("=" * 40)

    db_path = input(
        "Enter path to your football.db file (or press Enter for 'football.db'): "
    ).strip()
    if not db_path:
        db_path = "football.db"

    table_info = analyze_sqlite_database(db_path)

    if table_info:
        suggest_migration_modifications(table_info)

        print(f"\n🚀 NEXT STEPS:")
        print("   1. Review the analysis above")
        print("   2. Modify migration script if needed")
        print("   3. Run the migration")
        print("   4. Add custom logic for extra tables if desired")


if __name__ == "__main__":
    main()
