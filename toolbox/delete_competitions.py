#!/usr/bin/env python3
"""
Delete specific competitions from progress tables.
"""

from sqlalchemy import text

from coordination.coordinator import create_work_tracker


def delete_competitions_from_progress():
    """Delete specific competitions from both progress tables."""

    # List of competitions to delete
    competitions_to_delete = [
        "212S",
        "BRCB",
        "BRRN",
        "G17L",
        "STEP_COMP",
        "MAZR",
        "RO1R",
        "BEYP",
        "PONL",
    ]

    print(
        f"🗑️ Deleting {len(competitions_to_delete)} competitions from progress tables..."
    )

    try:
        # Create progress tracker
        progress_monitor = create_work_tracker(
            "production"
        )  # Change to your environment

        with progress_monitor.db_service.transaction() as session:
            # Delete from season_progress table
            print("\n📅 Deleting from season_progress table...")
            for comp_id in competitions_to_delete:
                result = session.execute(
                    text(
                        """
                    DELETE FROM season_progress 
                    WHERE competition_id = :comp_id
                """
                    ),
                    {"comp_id": comp_id},
                )

                rows_affected = result.rowcount
                if rows_affected > 0:
                    print(f"   ✅ Deleted {rows_affected} season records for {comp_id}")
                else:
                    print(f"   ➖ No season records found for {comp_id}")

            # Delete from competition_progress table
            print("\n🏆 Deleting from competition_progress table...")
            for comp_id in competitions_to_delete:
                result = session.execute(
                    text(
                        """
                    DELETE FROM competition_progress 
                    WHERE competition_id = :comp_id
                """
                    ),
                    {"comp_id": comp_id},
                )

                rows_affected = result.rowcount
                if rows_affected > 0:
                    print(f"   ✅ Deleted competition record for {comp_id}")
                else:
                    print(f"   ➖ No competition record found for {comp_id}")

            # Commit the transaction
            session.commit()
            print(
                "\n✅ Successfully deleted all specified competitions from progress tables"
            )

    except Exception as e:
        print(f"❌ Error during deletion: {e}")
        raise
    finally:
        if "progress_monitor" in locals():
            progress_monitor.db_service.cleanup()


if __name__ == "__main__":
    # Confirmation prompt
    print("⚠️ WARNING: This will permanently delete competition progress data!")
    print("Competitions to delete:")
    for comp in [
        "212S",
        "BRCB",
        "BRRN",
        "G17L",
        "HO1A",
        "STEP_COMP",
        "MAZR",
        "RO1R",
        "BEYP",
        "PONL",
    ]:
        print(f"   • {comp}")

    confirm = input("\nType 'DELETE' to proceed: ")

    if confirm == "DELETE":
        delete_competitions_from_progress()
        print("\n🎉 Deletion completed!")
    else:
        print("❌ Operation cancelled")
