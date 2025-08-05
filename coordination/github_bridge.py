# coordination/github_bridge.py
"""
GitHub Bridge for distributed scraping coordination.
Replaces direct database access for worker machines.
UPDATED with atomic claiming to prevent race conditions.
"""

import gzip
import json
import os
import subprocess
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional


class GitHubWorkBridge:
    """
    Handles work distribution through GitHub repository.
    """

    def __init__(
        self,
        repo_path: str = "./scraping-work",
        repo_url: str = None,
        archive_path: str = "./work_archive",
    ):
        """
        Initialize GitHub bridge.

        Args:
            repo_path: Local path to git repository
            repo_url: GitHub repository URL (for cloning)
            archive_path: Path for compressed backup storage
        """
        self.repo_path = Path(repo_path)
        self.repo_url = repo_url
        self.archive_path = Path(archive_path)

        # Create folder structure
        self.folders = {
            "available": self.repo_path / "available_work",
            "claims": self.repo_path / "work_queue",
            "claimed": self.repo_path / "active_work",
            "completed": self.repo_path / "completed_work",
            "failed": self.repo_path / "retry_queue",
        }

        # Archive structure
        self.archive_folders = {
            "completed": self.archive_path / "completed",
            "failed": self.archive_path / "failed",
            "metadata": self.archive_path / "metadata",
        }

        self._setup_repository()
        self._create_folder_structure()
        self._setup_archive_structure()

        print(f"🌐 GitHub bridge initialized: {self.repo_path}")
        print(f"📦 Archive location: {self.archive_path}")

    def _setup_archive_structure(self):
        """
        Setup archive folder structure
        """
        for folder in self.archive_folders.values():
            folder.mkdir(parents=True, exist_ok=True)

        # Create archive index if it doesn't exist
        self.archive_index_file = (
            self.archive_folders["metadata"] / "archive_index.json"
        )
        if not self.archive_index_file.exists():
            initial_index = {
                "created_at": datetime.now().isoformat(),
                "total_archived": 0,
                "daily_summaries": {},
                "competition_summaries": {},
                "last_updated": datetime.now().isoformat(),
            }
            with open(self.archive_index_file, "w") as f:
                json.dump(initial_index, f, indent=2)

    def _setup_repository(self):
        """
        Setup git repository
        """
        if not self.repo_path.exists():
            if self.repo_url:
                subprocess.run(
                    ["git", "clone", self.repo_url, str(self.repo_path)], check=True
                )
            else:
                self.repo_path.mkdir(parents=True)
                subprocess.run(["git", "init"], cwd=self.repo_path, check=True)

        # Always pull latest changes
        try:
            subprocess.run(["git", "pull"], cwd=self.repo_path, check=False)
        except:
            pass  # Ignore if no remote or other git issues

    def _create_folder_structure(self):
        """Create necessary folders."""
        for folder in self.folders.values():
            folder.mkdir(parents=True, exist_ok=True)

    def _compress_json_data(self, data: Dict, compression_level: int = 6) -> bytes:
        """
        Compress JSON data using gzip.

        Args:
            data: Dictionary to compress
            compression_level: Compression level (1-9, higher = better compression)

        Returns:
            Compressed bytes
        """
        json_str = json.dumps(data, separators=(",", ":"))
        return gzip.compress(json_str.encode("utf-8"), compresslevel=compression_level)

    def _decompress_json_data(self, compressed_data: bytes) -> Dict:
        """
        Decompress JSON data from gzip.

        Args:
            compressed_data: Compressed bytes

        Returns:
            Decompressed dictionary
        """
        json_str = gzip.decompress(compressed_data).decode("utf-8")
        return json.loads(json_str)

    def _update_archive_index(self, work_result: Dict, archive_type: str):
        """
        Update the archive index with new work entry.
        ROBUST: Handles any data type issues.
        """
        try:
            # Load current index
            with open(self.archive_index_file, "r") as f:
                index = json.load(f)

            # Update counters
            index["total_archived"] += 1
            index["last_updated"] = datetime.now().isoformat()

            # Update daily summary
            today = datetime.now().strftime("%Y-%m-%d")
            if today not in index["daily_summaries"]:
                index["daily_summaries"][today] = {
                    "completed": 0,
                    "failed": 0,
                    "competitions": [],
                }

            index["daily_summaries"][today][archive_type] += 1

            # ROBUST: Handle competitions field regardless of type
            competitions_today = index["daily_summaries"][today]["competitions"]
            comp_id = work_result.get("competition_id", "unknown")

            # Convert to list if it's not already
            if not isinstance(competitions_today, list):
                if hasattr(competitions_today, "__iter__") and not isinstance(
                    competitions_today, str
                ):
                    competitions_today = list(competitions_today)
                else:
                    competitions_today = []
                index["daily_summaries"][today]["competitions"] = competitions_today

            # Add competition if not already present
            if comp_id not in competitions_today:
                competitions_today.append(comp_id)

            # Update competition summary
            if comp_id not in index["competition_summaries"]:
                index["competition_summaries"][comp_id] = {
                    "completed": 0,
                    "failed": 0,
                    "last_processed": None,
                }

            index["competition_summaries"][comp_id][archive_type] += 1
            index["competition_summaries"][comp_id]["last_processed"] = (
                datetime.now().isoformat()
            )

            # Save updated index
            with open(self.archive_index_file, "w") as f:
                json.dump(index, f, indent=2)

        except Exception as e:
            print(f"⚠️ Error updating archive index: {e}")
            print(f"   Error type: {type(e).__name__}")
            print(
                f"   Work result keys: {list(work_result.keys()) if work_result else 'None'}"
            )

            # Try to continue without crashing
            try:
                # Minimal update - just increment counter
                with open(self.archive_index_file, "r") as f:
                    index = json.load(f)
                index["total_archived"] = index.get("total_archived", 0) + 1
                index["last_updated"] = datetime.now().isoformat()
                with open(self.archive_index_file, "w") as f:
                    json.dump(index, f, indent=2)
                print("   ✅ Minimal archive index update successful")
            except Exception as fallback_error:
                print(f"   ❌ Fallback update also failed: {fallback_error}")

    def _calculate_file_size_mb(self, file_path: Path) -> float:
        """Calculate file size in MB."""
        try:
            return file_path.stat().st_size / (1024 * 1024)
        except:
            return 0.0

    def archive_processed_work(self, work_result: Dict):
        """
        Archive processed work with compression instead of deleting.
        Enhanced with proper JSON serialization.
        """
        work_id = work_result.get("work_id", "unknown")
        competition_id = work_result.get("competition_id", "unknown")

        try:
            # Clean work_result for JSON serialization
            clean_work_result = self._clean_for_json(work_result.copy())

            # Determine archive type and prepare data
            if clean_work_result.get("status") == "failed":
                archive_type = "failed"
                archive_folder = self.archive_folders["failed"]
            else:
                archive_type = "completed"
                archive_folder = self.archive_folders["completed"]

            # Create date-based subfolder
            today = datetime.now().strftime("%Y-%m-%d")
            daily_folder = archive_folder / today
            daily_folder.mkdir(exist_ok=True)

            # Prepare archive entry with metadata
            archive_entry = {
                "archived_at": datetime.now().isoformat(),
                "archive_type": archive_type,
                "original_size_estimate": len(
                    json.dumps(clean_work_result, default=str)
                ),
                "work_data": clean_work_result,
            }

            # Remove file path info from archive (this was causing the PosixPath issue)
            if "_file_path" in archive_entry["work_data"]:
                del archive_entry["work_data"]["_file_path"]

            # Compress and save
            compressed_data = self._compress_json_data(archive_entry)
            archive_file = daily_folder / f"{work_id}.json.gz"

            with open(archive_file, "wb") as f:
                f.write(compressed_data)

            # Calculate compression stats
            original_size = archive_entry["original_size_estimate"]
            compressed_size = len(compressed_data)
            compression_ratio = (
                (1 - compressed_size / original_size) * 100 if original_size > 0 else 0
            )

            # Update archive index
            self._update_archive_index(clean_work_result, archive_type)

            # Remove original file
            if "_file_path" in work_result and Path(work_result["_file_path"]).exists():
                os.remove(work_result["_file_path"])

            # Commit removal to git
            self._git_add_commit_push(f"Archive processed work: {work_id} (attempt 1)")

            print(f"📦 Archived: {work_id} ({competition_id})")
            print(
                f"   💾 Size: {original_size:,} → {compressed_size:,} bytes ({compression_ratio:.1f}% savings)"
            )
            print(f"   📁 Location: {archive_file}")

        except Exception as e:
            print(f"❌ Error archiving work {work_id}: {e}")
            print(f"   🔍 Error details: {type(e).__name__}: {str(e)}")

            # Fallback to original deletion behavior
            if "_file_path" in work_result:
                try:
                    os.remove(work_result["_file_path"])
                    self._git_add_commit_push(
                        f"Archive processed work: {work_id} (fallback deletion)"
                    )
                    print(f"⚠️ Fallback: Deleted {work_id} after archive failure")
                except Exception as delete_error:
                    print(
                        f"🚨 Critical: Could not delete or archive {work_id}: {delete_error}"
                    )

    def _clean_for_json(self, data):
        """
        Clean data structure for JSON serialization.
        Converts non-JSON-serializable objects to strings.

        Args:
            data: Data structure to clean (dict, list, or any object)

        Returns:
            JSON-serializable version of the data
        """
        if isinstance(data, dict):
            return {key: self._clean_for_json(value) for key, value in data.items()}
        elif isinstance(data, list):
            return [self._clean_for_json(item) for item in data]
        elif isinstance(data, Path):
            return str(data)
        elif isinstance(data, datetime):
            return data.isoformat()
        elif hasattr(data, "isoformat"):  # Handle other datetime-like objects
            return data.isoformat()
        elif hasattr(data, "__dict__"):  # Handle custom objects
            return str(data)
        elif callable(data):  # Handle functions
            return str(data)
        else:
            return data

    def get_archive_statistics(self) -> Dict:
        """
        Get statistics about archived work.

        Returns:
            Dictionary with archive statistics
        """
        try:
            with open(self.archive_index_file, "r") as f:
                index = json.load(f)

            # Calculate total archive size
            total_size_mb = 0
            file_count = 0

            for archive_folder in [
                self.archive_folders["completed"],
                self.archive_folders["failed"],
            ]:
                for file_path in archive_folder.rglob("*.json.gz"):
                    total_size_mb += self._calculate_file_size_mb(file_path)
                    file_count += 1

            stats = {
                "total_archived": index.get("total_archived", 0),
                "total_size_mb": round(total_size_mb, 2),
                "file_count": file_count,
                "average_file_size_kb": (
                    round((total_size_mb * 1024) / file_count, 2)
                    if file_count > 0
                    else 0
                ),
                "recent_activity": dict(
                    list(index.get("daily_summaries", {}).items())[-7:]
                ),  # Last 7 days
                "top_competitions": dict(
                    sorted(
                        index.get("competition_summaries", {}).items(),
                        key=lambda x: x[1].get("completed", 0) + x[1].get("failed", 0),
                        reverse=True,
                    )[:10]
                ),  # Top 10 by volume
            }

            return stats

        except Exception as e:
            print(f"⚠️ Error getting archive statistics: {e}")
            return {"error": str(e)}

    def retrieve_archived_work(
        self,
        work_id: str = None,
        competition_id: str = None,
        date: str = None,
        limit: int = 100,
    ) -> List[Dict]:
        """
        Retrieve archived work based on filters.

        Args:
            work_id: Specific work ID to retrieve
            competition_id: Filter by competition ID
            date: Filter by date (YYYY-MM-DD format)
            limit: Maximum number of results

        Returns:
            List of matching archived work entries
        """
        results = []
        processed = 0

        try:
            # Search through archive folders
            search_folders = []
            if date:
                # Search specific date
                for archive_type in ["completed", "failed"]:
                    date_folder = self.archive_folders[archive_type] / date
                    if date_folder.exists():
                        search_folders.append(date_folder)
            else:
                # Search all dates
                for archive_type in ["completed", "failed"]:
                    search_folders.extend(self.archive_folders[archive_type].iterdir())

            for folder in search_folders:
                if not folder.is_dir():
                    continue

                for archive_file in folder.glob("*.json.gz"):
                    if processed >= limit:
                        break

                    try:
                        # Check work_id filter before decompressing
                        if work_id and not archive_file.stem.replace(
                            ".json", ""
                        ).startswith(work_id):
                            continue

                        # Decompress and load
                        with open(archive_file, "rb") as f:
                            compressed_data = f.read()

                        archive_entry = self._decompress_json_data(compressed_data)
                        work_data = archive_entry.get("work_data", {})

                        # Apply filters
                        if (
                            competition_id
                            and work_data.get("competition_id") != competition_id
                        ):
                            continue

                        # Add archive metadata
                        work_data["_archive_info"] = {
                            "archived_at": archive_entry.get("archived_at"),
                            "archive_type": archive_entry.get("archive_type"),
                            "archive_file": str(archive_file),
                            "file_size_mb": self._calculate_file_size_mb(archive_file),
                        }

                        results.append(work_data)
                        processed += 1

                    except Exception as e:
                        print(f"⚠️ Error reading archive file {archive_file}: {e}")
                        continue

                if processed >= limit:
                    break

            return results

        except Exception as e:
            print(f"❌ Error retrieving archived work: {e}")
            return []

    def cleanup_old_archives(self, days_to_keep: int = 90) -> Dict:
        """
        Clean up archive files older than specified days.

        Args:
            days_to_keep: Number of days of archives to keep

        Returns:
            Cleanup statistics
        """
        cutoff_date = datetime.now().timestamp() - (days_to_keep * 24 * 60 * 60)

        stats = {
            "files_removed": 0,
            "size_freed_mb": 0,
            "folders_cleaned": 0,
            "errors": [],
        }

        try:
            for archive_type in ["completed", "failed"]:
                archive_folder = self.archive_folders[archive_type]

                for date_folder in archive_folder.iterdir():
                    if not date_folder.is_dir():
                        continue

                    # Check if folder is old enough to clean
                    try:
                        folder_date = datetime.strptime(
                            date_folder.name, "%Y-%m-%d"
                        ).timestamp()
                        if folder_date >= cutoff_date:
                            continue  # Keep this folder
                    except ValueError:
                        continue  # Skip non-date folders

                    # Clean up old files in this folder
                    files_in_folder = 0
                    for archive_file in date_folder.glob("*.json.gz"):
                        try:
                            file_size = self._calculate_file_size_mb(archive_file)
                            archive_file.unlink()
                            stats["files_removed"] += 1
                            stats["size_freed_mb"] += file_size
                            files_in_folder += 1
                        except Exception as e:
                            stats["errors"].append(
                                f"Could not remove {archive_file}: {e}"
                            )

                    # Remove empty folder
                    try:
                        if files_in_folder > 0:
                            date_folder.rmdir()
                            stats["folders_cleaned"] += 1
                    except OSError:
                        pass  # Folder not empty or other issue

            stats["size_freed_mb"] = round(stats["size_freed_mb"], 2)

            print(f"🧹 Archive cleanup completed:")
            print(
                f"   📁 Removed {stats['files_removed']} files in {stats['folders_cleaned']} folders"
            )
            print(f"   💾 Freed {stats['size_freed_mb']} MB")
            if stats["errors"]:
                print(f"   ⚠️ {len(stats['errors'])} errors occurred")

            return stats

        except Exception as e:
            stats["errors"].append(f"Cleanup failed: {e}")
            return stats

    def _git_add_commit_push(self, message: str, max_retries: int = 5) -> bool:
        """Add, commit, and push with improved conflict resolution."""
        for attempt in range(max_retries):
            try:
                # Check if there are any changes to commit
                result = subprocess.run(
                    ["git", "status", "--porcelain"],
                    cwd=self.repo_path,
                    capture_output=True,
                    text=True,
                )

                if not result.stdout.strip():
                    # No changes to commit, but this might be okay for atomic operations
                    print(f"⚠️ No changes to commit for: {message}")
                    return True

                # Add all changes
                subprocess.run(
                    ["git", "add", "."],
                    cwd=self.repo_path,
                    check=True,
                    capture_output=True,
                )

                # Commit
                subprocess.run(
                    ["git", "commit", "-m", message],
                    cwd=self.repo_path,
                    check=True,
                    capture_output=True,
                )

                # Pull before push to handle any conflicts
                try:
                    subprocess.run(
                        ["git", "pull", "--no-edit", "origin", "main"],
                        cwd=self.repo_path,
                        check=True,
                        capture_output=True,
                    )
                except subprocess.CalledProcessError:
                    # Pull failed, might be due to conflicts - continue to push anyway
                    pass

                # Push
                subprocess.run(
                    ["git", "push", "origin", "main"],
                    cwd=self.repo_path,
                    check=True,
                    capture_output=True,
                )

                print(f"✅ Successfully pushed: {message} (attempt {attempt + 1})")
                return True

            except subprocess.CalledProcessError as e:
                if attempt < max_retries - 1:
                    # Exponential backoff with jitter
                    base_wait = 2**attempt
                    jitter = time.time() % 1  # Random jitter based on current time
                    wait_time = base_wait + jitter

                    print(f"❌ Git operation failed on attempt {attempt + 1}")
                    print(f"⏳ Retrying in {wait_time:.1f} seconds...")
                    time.sleep(wait_time)

                    # Force pull latest state before retry
                    try:
                        subprocess.run(
                            ["git", "fetch", "origin"],
                            cwd=self.repo_path,
                            capture_output=True,
                        )
                        subprocess.run(
                            ["git", "reset", "--hard", "origin/main"],
                            cwd=self.repo_path,
                            capture_output=True,
                        )
                    except:
                        pass
                else:
                    print(f"🚨 All {max_retries} attempts failed for: {message}")
                    return False

        return False

    def _git_pull(self):
        """Pull latest changes with retry logic."""
        max_attempts = 3

        for attempt in range(1, max_attempts + 1):
            try:
                subprocess.run(
                    ["git", "pull", "--no-edit", "origin", "main"],
                    cwd=self.repo_path,
                    check=True,
                    capture_output=True,
                )
                return
            except subprocess.CalledProcessError:
                if attempt < max_attempts:
                    time.sleep(attempt * 2)
                else:
                    print(f"⚠️ Failed to pull after {max_attempts} attempts")

    # HOST MACHINE METHODS

    def create_competition_work_order(
        self, competition: Dict, completed_seasons: List[str] = None
    ) -> str:
        """
        Create work order for a competition (HOST MACHINE).

        Args:
            competition: Dict with competition_id and competition_url
            completed_seasons: List of already completed seasons to skip

        Returns:
            work_id of created work order
        """
        work_id = f"comp_{competition['competition_id']}_{uuid.uuid4().hex[:8]}"

        work_order = {
            "work_id": work_id,
            "competition_id": competition["competition_id"],
            "competition_url": competition["competition_url"],
            "completed_seasons": completed_seasons or [],
            "created_at": datetime.now().isoformat(),
            "status": "available",
        }

        # Write to available folder
        work_file = self.folders["available"] / f"{work_id}.json"
        with open(work_file, "w") as f:
            json.dump(work_order, f, indent=2)

        # Commit and push
        self._git_add_commit_push(f"Create work order: {work_id}")

        print(f"📋 Created work order: {work_id} for {competition['competition_id']}")
        return work_id

    def get_completed_work(self) -> List[Dict]:
        """
        Get all completed work for processing (HOST MACHINE).

        Returns:
            List of completed work results
        """
        self._git_pull()

        completed_work = []
        for work_file in self.folders["completed"].glob("*.json"):
            try:
                with open(work_file, "r") as f:
                    work_result = json.load(f)
                    work_result["_file_path"] = work_file
                    completed_work.append(work_result)
            except Exception as e:
                print(f"⚠️ Error reading {work_file}: {e}")

        return completed_work

    # WORKER MACHINE METHODS - ATOMIC VERSION

    def claim_available_work(self, worker_id: str) -> Optional[Dict]:
        """
        Atomically claim work using file creation to prevent race conditions.
        FIXED: Uses timestamp-based sorting and cleans up stale claims.

        Args:
            worker_id: Unique identifier for this worker

        Returns:
            Work order dict if successfully claimed, None otherwise
        """
        # Pull latest state
        self._git_pull()

        # Find available work
        available_files = list(self.folders["available"].glob("*.json"))
        if not available_files:
            return None

        # Try to claim the first available work item
        work_file = available_files[0]
        work_id = work_file.stem

        # FIRST: Clean up any stale claims for this work item
        self._cleanup_stale_claims(work_id)

        # Create unique claim file name with worker and high-precision timestamp
        claim_id = f"{worker_id}_{int(time.time() * 1000)}_{uuid.uuid4().hex[:8]}"
        claim_file = self.folders["claims"] / f"{work_id}_{claim_id}.json"

        try:
            # Load work order
            with open(work_file, "r") as f:
                work_order = json.load(f)

            # Create claim file with work data + claim info
            claim_data = {
                **work_order,
                "claimed_by": worker_id,
                "claimed_at": datetime.now().isoformat(),
                "claim_id": claim_id,
                "status": "claiming",
            }

            # Write claim file
            with open(claim_file, "w") as f:
                json.dump(claim_data, f, indent=2)

            # Try to push the claim atomically
            commit_msg = f"Attempt claim: {work_id} by {worker_id} (attempt 1)"

            if self._git_add_commit_push(commit_msg):
                # SUCCESS! Now verify we won the race
                self._git_pull()  # Get latest state

                # Check if our claim file still exists
                if claim_file.exists():
                    # Check if we were the first to claim this work
                    claim_files = list(self.folders["claims"].glob(f"{work_id}_*.json"))

                    if claim_files:
                        # Sort by TIMESTAMP extracted from filename
                        def extract_timestamp(file_path):
                            try:
                                parts = file_path.stem.split("_")
                                for part in parts:
                                    if part.isdigit() and len(part) >= 10:
                                        return int(part)
                                return 0
                            except:
                                return 0

                        claim_files.sort(key=extract_timestamp)

                        # Debug logging to help verify the fix
                        print(
                            f"🔍 DEBUG: Found {len(claim_files)} claim files for {work_id}"
                        )
                        print(
                            f"🔍 DEBUG: Timestamps: {[extract_timestamp(f) for f in claim_files]}"
                        )
                        print(
                            f"🔍 DEBUG: Our timestamp: {extract_timestamp(claim_file)}"
                        )
                        print(f"🔍 DEBUG: Winner: {claim_files[0].name}")
                        print(f"🔍 DEBUG: Our file: {claim_file.name}")

                        if claim_files[0] == claim_file:
                            # WE WON! Move to claimed folder and remove from available
                            print(f"🏆 {worker_id} won claim race for {work_id}")

                            # Move our claim to claimed folder
                            claimed_file = (
                                self.folders["claimed"]
                                / f"{worker_id}_{int(time.time())}.json"
                            )
                            claim_data["status"] = "claimed"

                            with open(claimed_file, "w") as f:
                                json.dump(claim_data, f, indent=2)

                            # Remove from available and claims
                            work_file.unlink(missing_ok=True)
                            claim_file.unlink(missing_ok=True)

                            # Clean up other claim files for this work
                            for other_claim in claim_files[1:]:
                                other_claim.unlink(missing_ok=True)

                            # Commit the final state
                            final_msg = (
                                f"Claim work: {work_id} by {worker_id} (attempt 1)"
                            )
                            if self._git_add_commit_push(final_msg):
                                print(
                                    f"✅ Claimed work: {work_id} for {work_order.get('competition_id', 'UNKNOWN')}"
                                )
                                return claim_data
                            else:
                                print(
                                    f"⚠️ Failed to finalize claim for {work_id}, but work was claimed"
                                )
                                return claim_data
                        else:
                            # We lost the race, clean up our claim
                            winner_file = claim_files[0]
                            winner_timestamp = extract_timestamp(winner_file)
                            our_timestamp = extract_timestamp(claim_file)

                            print(f"❌ {worker_id} lost claim race for {work_id}")
                            print(f"   🏆 Winner timestamp: {winner_timestamp}")
                            print(f"   ⏰ Our timestamp: {our_timestamp}")
                            print(f"   ⏱️ Lost by: {our_timestamp - winner_timestamp}ms")

                            claim_file.unlink(missing_ok=True)
                            self._cleanup_abandoned_work(work_id, work_file)
                            self._git_add_commit_push(
                                f"Clean up failed claim: {work_id} by {worker_id} (attempt 1)"
                            )
                    else:
                        print(f"⚠️ No claim files found for {work_id} after claiming")
                        if work_file.exists():
                            self._cleanup_abandoned_work(work_id, work_file)
                            self._git_add_commit_push(
                                f"Clean up stale work: {work_id} (attempt 1)"
                            )
                else:
                    print(f"⚠️ Claim file disappeared for {work_id}")
            else:
                # Push failed, clean up
                if claim_file.exists():
                    claim_file.unlink(missing_ok=True)

        except Exception as e:
            print(f"❌ Claim attempt failed for {work_id}: {e}")
            # Clean up on error
            if claim_file.exists():
                claim_file.unlink(missing_ok=True)

        return None

    def _cleanup_stale_claims(self, work_id: str, max_age_minutes: int = 30):
        """
        Clean up stale claims that are older than max_age_minutes.

        Args:
            work_id: Work ID to clean claims for
            max_age_minutes: Claims older than this are considered stale
        """
        try:
            current_time = int(time.time() * 1000)
            cutoff_time = current_time - (max_age_minutes * 60 * 1000)

            claim_files = list(self.folders["claims"].glob(f"{work_id}_*.json"))
            stale_claims = []

            for claim_file in claim_files:
                try:
                    # Extract timestamp from filename
                    parts = claim_file.stem.split("_")
                    claim_timestamp = None

                    for part in parts:
                        if part.isdigit() and len(part) >= 10:
                            claim_timestamp = int(part)
                            break

                    if claim_timestamp and claim_timestamp < cutoff_time:
                        age_minutes = (current_time - claim_timestamp) / (60 * 1000)
                        stale_claims.append((claim_file, age_minutes))

                except Exception as e:
                    print(f"⚠️ Error checking claim file {claim_file}: {e}")
                    # If we can't parse it, consider it stale
                    stale_claims.append((claim_file, 999))

            if stale_claims:
                print(f"🧹 Found {len(stale_claims)} stale claims for {work_id}")

                for claim_file, age_minutes in stale_claims:
                    try:
                        claim_file.unlink()
                        print(
                            f"   🗑️ Removed stale claim: {claim_file.name} (age: {age_minutes:.1f} minutes)"
                        )
                    except Exception as e:
                        print(f"   ⚠️ Failed to remove {claim_file.name}: {e}")

                # Commit cleanup
                if stale_claims:
                    self._git_add_commit_push(
                        f"Clean up {len(stale_claims)} stale claims for {work_id}"
                    )
                    print(
                        f"✅ Cleaned up {len(stale_claims)} stale claims for {work_id}"
                    )

        except Exception as e:
            print(f"⚠️ Error during stale claim cleanup for {work_id}: {e}")

    def _cleanup_abandoned_work(self, work_id: str, work_file: Path):
        """
        Check if work was abandoned and handle cleanup safely.
        Moves suspicious work to retry queue instead of deleting.

        Args:
            work_id: ID of the work item
            work_file: Path to work file in available folder

        Returns:
            True if cleanup was performed, False otherwise
        """
        try:
            # Wait for winner to complete their operations
            time.sleep(2)

            # Re-check current state
            self._git_pull()

            # Check if work is still in available but there are no active claims
            if work_file.exists():
                claim_files = list(self.folders["claims"].glob(f"{work_id}_*.json"))

                # Check if work exists in other folders (claimed, completed, failed)
                work_found_elsewhere = False

                for folder_name in ["claimed", "completed", "failed"]:
                    folder = self.folders[folder_name]
                    for check_file in folder.glob("*.json"):
                        try:
                            with open(check_file, "r") as f:
                                data = json.load(f)
                                if data.get("work_id") == work_id:
                                    work_found_elsewhere = True
                                    break
                        except:
                            continue
                    if work_found_elsewhere:
                        break

                if claim_files:
                    # Active claims exist, don't cleanup
                    print(f"✅ Work {work_id} has active claims - NOT cleaning up")
                    return False
                elif work_found_elsewhere:
                    # Work successfully processed, safe to remove from available
                    print(
                        f"✅ Work {work_id} found elsewhere - removing from available"
                    )
                    work_file.unlink(missing_ok=True)
                    return True
                else:
                    # Work appears abandoned - move to retry queue for safety
                    print(f"⚠️ Work {work_id} appears abandoned - moving to retry queue")

                    try:
                        # Load work data
                        with open(work_file, "r") as f:
                            work_data = json.load(f)

                        # Add retry info
                        work_data["status"] = "failed"
                        work_data["error_message"] = (
                            "Abandoned during claim race - auto-retry"
                        )
                        work_data["failed_at"] = datetime.now().isoformat()
                        work_data["retry_count"] = work_data.get("retry_count", 0) + 1

                        # Move to retry queue
                        retry_file = (
                            self.folders["failed"]
                            / f"abandoned_{work_id}_{int(time.time())}.json"
                        )
                        with open(retry_file, "w") as f:
                            json.dump(work_data, f, indent=2)

                        # Remove from available
                        work_file.unlink(missing_ok=True)

                        print(f"📝 Moved {work_id} to retry queue for later processing")
                        return True

                    except Exception as move_error:
                        print(
                            f"⚠️ Failed to move {work_id} to retry queue: {move_error}"
                        )
                        # Fallback: just remove from available to break the loop
                        work_file.unlink(missing_ok=True)
                        return True

            return False

        except Exception as e:
            print(f"⚠️ Error during cleanup check for {work_id}: {e}")
            return False

    def submit_completed_work(self, work_order: Dict, results: Dict):
        """
        Submit completed work results with atomic support (WORKER MACHINE).

        Args:
            work_order: Original work order
            results: Results from scraping
        """
        max_attempts = 3
        base_delay = 2

        work_result = {
            **work_order,
            "completed_at": datetime.now().isoformat(),
            "status": "completed",
            **results,
        }

        # Use claim_id for file naming if available, otherwise worker_id + timestamp
        if "claimed_by" in work_order:
            file_id = f"{work_order['claimed_by']}_{int(time.time())}"
        else:
            file_id = work_order.get("claim_id", work_order["work_id"])

        for attempt in range(1, max_attempts + 1):
            try:
                # Write to completed folder
                completed_file = self.folders["completed"] / f"{file_id}.json"
                with open(completed_file, "w") as f:
                    json.dump(work_result, f, indent=2)

                # Remove from claimed folder (find the right file)
                claimed_files = list(
                    self.folders["claimed"].glob(f"{work_order['claimed_by']}_*.json")
                )
                for claimed_file in claimed_files:
                    try:
                        with open(claimed_file, "r") as f:
                            claimed_data = json.load(f)
                            if claimed_data.get("work_id") == work_order["work_id"]:
                                claimed_file.unlink()
                                break
                    except:
                        continue

                # Try to commit and push
                try:
                    self._git_add_commit_push(f"Complete work: {work_order['work_id']}")
                    print(f"✅ Submitted completed work: {work_order['work_id']}")
                    return

                except Exception as git_error:
                    print(
                        f"Git operation failed during submission (attempt {attempt}): {git_error}"
                    )
                    if attempt < max_attempts:
                        time.sleep(base_delay * attempt)
                        continue
                    else:
                        # Final attempt failed, but work is still saved locally
                        print(
                            f"⚠️ Work completed but git submission failed: {work_order['work_id']}"
                        )
                        print(f"Work saved locally at: {completed_file}")
                        return

            except Exception as e:
                print(f"❌ Error submitting work (attempt {attempt}): {e}")
                if attempt < max_attempts:
                    time.sleep(base_delay * attempt)
                else:
                    raise

    def submit_failed_work(self, work_order: Dict, error_message: str):
        """
        Submit failed work with atomic support (WORKER MACHINE).

        Args:
            work_order: Original work order
            error_message: Error description
        """
        max_attempts = 3
        base_delay = 2

        work_result = {
            **work_order,
            "failed_at": datetime.now().isoformat(),
            "status": "failed",
            "error_message": error_message,
        }

        # Use claim_id for file naming if available
        if "claimed_by" in work_order:
            file_id = f"{work_order['claimed_by']}_{int(time.time())}"
        else:
            file_id = work_order.get("claim_id", work_order["work_id"])

        for attempt in range(1, max_attempts + 1):
            try:
                # Write to failed folder
                failed_file = self.folders["failed"] / f"{file_id}.json"
                with open(failed_file, "w") as f:
                    json.dump(work_result, f, indent=2)

                # Remove from claimed folder (find the right file)
                claimed_files = list(
                    self.folders["claimed"].glob(f"{work_order['claimed_by']}_*.json")
                )
                for claimed_file in claimed_files:
                    try:
                        with open(claimed_file, "r") as f:
                            claimed_data = json.load(f)
                            if claimed_data.get("work_id") == work_order["work_id"]:
                                claimed_file.unlink()
                                break
                    except:
                        continue

                # Try to commit and push
                try:
                    self._git_add_commit_push(f"Fail work: {work_order['work_id']}")
                    print(
                        f"❌ Submitted failed work: {work_order['work_id']} - {error_message}"
                    )
                    return

                except Exception as git_error:
                    print(
                        f"Git operation failed during failure submission (attempt {attempt}): {git_error}"
                    )
                    if attempt < max_attempts:
                        time.sleep(base_delay * attempt)
                        continue
                    else:
                        print(
                            f"⚠️ Work failure recorded locally: {work_order['work_id']}"
                        )
                        return

            except Exception as e:
                print(f"❌ Error submitting failed work (attempt {attempt}): {e}")
                if attempt < max_attempts:
                    time.sleep(base_delay * attempt)
                else:
                    print(f"⚠️ Could not submit failure for: {work_order['work_id']}")
                    return

    def get_work_status(self) -> Dict:
        """Get overview of work status."""
        self._git_pull()

        status = {}
        for name, folder in self.folders.items():
            if name == "claims":
                continue  # Skip claims folder in status
            status[name] = len(list(folder.glob("*.json")))

        return status

    def get_failed_work(self) -> List[Dict]:
        """Get all failed work for processing (HOST MACHINE)."""
        self._git_pull()

        failed_work = []
        for work_file in self.folders["failed"].glob("*.json"):
            try:
                with open(work_file, "r") as f:
                    work_result = json.load(f)
                    work_result["_file_path"] = work_file
                    failed_work.append(work_result)
            except Exception as e:
                print(f"⚠️ Error reading {work_file}: {e}")

        return failed_work

    def retry_failed_work(self, failed_work: Dict):
        """Move failed work back to available for retry."""
        # Remove retry_count for the new work order
        work_order = {
            k: v
            for k, v in failed_work.items()
            if k
            not in [
                "failed_at",
                "error_message",
                "_file_path",
                "claimed_by",
                "claimed_at",
                "claim_id",
            ]
        }
        work_order["status"] = "available"
        work_order["retry_count"] = failed_work.get("retry_count", 0) + 1

        # Create new available work
        work_file = self.folders["available"] / f"{work_order['work_id']}.json"
        with open(work_file, "w") as f:
            json.dump(work_order, f, indent=2)

        # Remove from failed
        if "_file_path" in failed_work:
            os.remove(failed_work["_file_path"])

        self._git_add_commit_push(f"Retry failed work: {work_order['work_id']}")

    def create_match_work_order(
        self,
        competition: Dict,
        incomplete_matchdays: List[Dict] = None,
        next_matchday: int = 1,
    ) -> str:
        """
        Create work order for match data collection.

        Args:
            competition: Dict with competition info
            incomplete_matchdays: List of matchdays needing completion
            next_matchday: Next matchday number to discover

        Returns:
            work_id of created work order
        """
        work_id = f"match_{competition['competition_id']}_{competition['season_year']}_{uuid.uuid4().hex[:8]}"

        work_order = {
            "work_id": work_id,
            "work_type": "match_data",
            "competition_id": competition["competition_id"],
            "competition_code": competition["competition_code"],
            "competition_name": competition["competition_name"],
            "season_year": competition["season_year"],
            "incomplete_matchdays": incomplete_matchdays or [],
            "next_matchday_to_discover": next_matchday,
            "created_at": datetime.now().isoformat(),
            "status": "available",
        }

        # Write to available folder
        work_file = self.folders["available"] / f"{work_id}.json"
        with open(work_file, "w") as f:
            json.dump(work_order, f, indent=2)

        # Commit and push
        self._git_add_commit_push(f"Create match work order: {work_id}")

        return work_id

    def get_completed_match_work(self) -> List[Dict]:
        """
        Get completed match work for processing (HOST MACHINE).

        Returns:
            List of completed match work results
        """
        self._git_pull()

        completed_work = []
        for work_file in self.folders["completed"].glob("match_*.json"):
            try:
                with open(work_file, "r") as f:
                    work_result = json.load(f)
                    work_result["_file_path"] = work_file
                    completed_work.append(work_result)
            except Exception as e:
                print(f"⚠️ Error reading {work_file}: {e}")

        return completed_work

    def get_failed_match_work(self) -> List[Dict]:
        """
        Get failed match work for retry processing (HOST MACHINE).

        Returns:
            List of failed match work results
        """
        self._git_pull()

        failed_work = []
        for work_file in self.folders["failed"].glob("match_*.json"):
            try:
                with open(work_file, "r") as f:
                    work_result = json.load(f)
                    work_result["_file_path"] = work_file
                    failed_work.append(work_result)
            except Exception as e:
                print(f"⚠️ Error reading {work_file}: {e}")

        return failed_work

    def get_match_work_status(self) -> Dict:
        """
        Get status of match work specifically.

        Returns:
            Dict with counts of match work in each status
        """
        self._git_pull()

        status = {}
        for name, folder in self.folders.items():
            if name == "claims":
                continue
            match_files = list(folder.glob("match_*.json"))
            status[name] = len(match_files)

        return status
