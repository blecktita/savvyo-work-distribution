# cleanup_duplicates.py
"""
Clean up duplicate work orders in GitHub repository
"""

import json
import os
import subprocess
from collections import defaultdict
from pathlib import Path


def cleanup_duplicate_work_orders(repo_path="./scraping-work"):
    """
    Remove duplicate work orders, keeping only the newest one for each competition.
    """
    repo_path = Path(repo_path)

    print("🧹 Starting duplicate cleanup...")

    # Track competitions and their files
    competitions = defaultdict(list)

    # Scan available_competitions folder
    available_folder = repo_path / "available_work"

    if not available_folder.exists():
        print("❌ Available competitions folder not found!")
        return

    # Find all work order files
    work_files = list(available_folder.glob("comp_*.json"))
    print(f"📁 Found {len(work_files)} work order files")

    # Group files by competition_id
    for work_file in work_files:
        try:
            with open(work_file, "r") as f:
                work_data = json.load(f)
                competition_id = work_data["competition_id"]
                created_at = work_data["created_at"]

                competitions[competition_id].append(
                    {
                        "file": work_file,
                        "created_at": created_at,
                        "work_data": work_data,
                    }
                )
        except Exception as e:
            print(f"⚠️ Error reading {work_file}: {e}")
            continue

    # Find and remove duplicates
    files_to_delete = []
    duplicates_found = 0

    for competition_id, file_list in competitions.items():
        if len(file_list) > 1:
            duplicates_found += 1
            print(
                f"🔍 Found {len(file_list)} duplicates for competition {competition_id}"
            )

            # Sort by created_at to keep the newest
            file_list.sort(key=lambda x: x["created_at"], reverse=True)

            # Keep the first (newest), delete the rest
            keep_file = file_list[0]
            delete_files = file_list[1:]

            print(f"  ✅ Keeping: {keep_file['file'].name}")

            for delete_file in delete_files:
                print(f"  🗑️ Deleting: {delete_file['file'].name}")
                files_to_delete.append(delete_file["file"])

    if not files_to_delete:
        print("🎉 No duplicates found! Repository is clean.")
        return

    # Confirm deletion
    print(f"\n📊 Summary:")
    print(f"   Competitions with duplicates: {duplicates_found}")
    print(f"   Files to delete: {len(files_to_delete)}")

    confirm = input(f"\n❓ Delete {len(files_to_delete)} duplicate files? (y/N): ")

    if confirm.lower() != "y":
        print("❌ Cleanup cancelled")
        return

    # Delete duplicate files
    deleted_count = 0
    for file_path in files_to_delete:
        try:
            os.remove(file_path)
            deleted_count += 1
            print(f"🗑️ Deleted: {file_path.name}")
        except Exception as e:
            print(f"❌ Error deleting {file_path.name}: {e}")

    print(f"\n✅ Successfully deleted {deleted_count} duplicate files")

    # Commit changes to git
    try:
        os.chdir(repo_path)
        subprocess.run(["git", "add", "."], check=True)
        subprocess.run(
            ["git", "commit", "-m", f"Clean up {deleted_count} duplicate work orders"],
            check=True,
        )
        subprocess.run(["git", "push"], check=True)
        print("📤 Changes committed and pushed to GitHub")
    except subprocess.CalledProcessError as e:
        print(f"⚠️ Git operations failed: {e}")
        print("   You may need to manually commit the changes")

    # Final summary
    remaining_files = len(list(available_folder.glob("comp_*.json")))
    print(f"\n🎯 Cleanup complete!")
    print(f"   Remaining work orders: {remaining_files}")
    print(f"   Unique competitions: {len(competitions)}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Clean up duplicate work orders")
    parser.add_argument(
        "--repo-path",
        default="./scraping-work",
        help="Path to scraping work repository",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be deleted without actually deleting",
    )

    args = parser.parse_args()

    if args.dry_run:
        print("🔍 DRY RUN MODE - No files will be deleted")

    cleanup_duplicate_work_orders(args.repo_path)
