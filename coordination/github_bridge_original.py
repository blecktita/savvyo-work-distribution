# coordination/github_bridge.py
"""
GitHub Bridge for distributed scraping coordination.
Replaces direct database access for worker machines.
"""

import os
import json
import uuid
import subprocess
from typing import List, Dict, Optional
from datetime import datetime
from pathlib import Path
import time

class GitHubWorkBridge:
    """
    Handles work distribution through GitHub repository.
    """
    
    def __init__(self, repo_path: str = "./scraping-work", repo_url: str = None):
        """
        Initialize GitHub bridge.
        
        Args:
            repo_path: Local path to git repository
            repo_url: GitHub repository URL (for cloning)
        """
        self.repo_path = Path(repo_path)
        self.repo_url = repo_url
        
        # Create folder structure
        self.folders = {
            'available': self.repo_path / 'available_competitions',
            'claimed': self.repo_path / 'claimed_competitions', 
            'completed': self.repo_path / 'completed_competitions',
            'failed': self.repo_path / 'failed_competitions'
        }
        
        self._setup_repository()
        self._create_folder_structure()
        
        print(f"🌐 GitHub bridge initialized: {self.repo_path}")
    
    def _setup_repository(self):
        """Setup git repository."""
        if not self.repo_path.exists():
            if self.repo_url:
                subprocess.run(['git', 'clone', self.repo_url, str(self.repo_path)], check=True)
            else:
                self.repo_path.mkdir(parents=True)
                subprocess.run(['git', 'init'], cwd=self.repo_path, check=True)
        
        # Always pull latest changes
        try:
            subprocess.run(['git', 'pull'], cwd=self.repo_path, check=False)
        except:
            pass  # Ignore if no remote or other git issues
    
    def _create_folder_structure(self):
        """Create necessary folders."""
        for folder in self.folders.values():
            folder.mkdir(parents=True, exist_ok=True)
    
    def _git_add_commit_push(self, message: str):
        """Add, commit and push changes with atomic operations and retries."""
        max_attempts = 5
        base_delay = 2
        
        for attempt in range(1, max_attempts + 1):
            try:
                # Step 1: Add changes
                subprocess.run(['git', 'add', '.'], cwd=self.repo_path, check=True)
                
                # Step 2: Commit (might fail if nothing to commit)
                try:
                    subprocess.run(['git', 'commit', '-m', message], cwd=self.repo_path, check=True)
                except subprocess.CalledProcessError as e:
                    if "nothing to commit" in str(e):
                        print(f"Nothing to commit for: {message}")
                        return  # Success - nothing needed committing
                    else:
                        raise  # Real error, re-raise
                
                # Step 3: Fetch latest changes
                subprocess.run(['git', 'fetch', 'origin', 'main'], cwd=self.repo_path, check=True)
                
                # Step 4: Check if we're behind
                result = subprocess.run(['git', 'merge-base', '--is-ancestor', 'origin/main', 'HEAD'], 
                                    cwd=self.repo_path, capture_output=True)
                
                if result.returncode != 0:  # We're behind, need to pull
                    print(f"Behind remote, pulling changes... (attempt {attempt})")
                    subprocess.run(['git', 'pull', '--no-edit', 'origin', 'main'], cwd=self.repo_path, check=True)
                
                # Step 5: Try to push
                subprocess.run(['git', 'push', 'origin', 'main'], cwd=self.repo_path, check=True)
                
                print(f"✅ Successfully pushed: {message} (attempt {attempt})")
                return  # Success!
                
            except subprocess.CalledProcessError as e:
                print(f"❌ Git operation failed on attempt {attempt}: {e}")
                
                if attempt < max_attempts:
                    delay = base_delay * attempt
                    print(f"⏳ Retrying in {delay} seconds...")
                    time.sleep(delay)
                else:
                    print(f"🚨 All {max_attempts} attempts failed for: {message}")
                    # Don't raise - log and continue to prevent worker crashes
                    return  # Ignore commit failures (nothing to commit, etc.)
    
    def _git_pull(self):
        """Pull latest changes with retry logic."""
        max_attempts = 3
        
        for attempt in range(1, max_attempts + 1):
            try:
                subprocess.run(['git', 'pull', '--no-edit', 'origin', 'main'], cwd=self.repo_path, check=True)
                return
            except subprocess.CalledProcessError:
                if attempt < max_attempts:
                    time.sleep(attempt * 2)
                else:
                    print(f"⚠️ Failed to pull after {max_attempts} attempts")
    
    # HOST MACHINE METHODS
    
    def create_competition_work_order(self, competition: Dict, completed_seasons: List[str] = None) -> str:
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
            "competition_id": competition['competition_id'],
            "competition_url": competition['competition_url'],
            "completed_seasons": completed_seasons or [],
            "created_at": datetime.now().isoformat(),
            "status": "available"
        }
        
        # Write to available folder
        work_file = self.folders['available'] / f"{work_id}.json"
        with open(work_file, 'w') as f:
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
        for work_file in self.folders['completed'].glob('*.json'):
            try:
                with open(work_file, 'r') as f:
                    work_result = json.load(f)
                    work_result['_file_path'] = work_file
                    completed_work.append(work_result)
            except Exception as e:
                print(f"⚠️ Error reading {work_file}: {e}")
        
        return completed_work
    
    def archive_processed_work(self, work_result: Dict):
        """
        Remove processed work file (HOST MACHINE).
        """
        if '_file_path' in work_result:
            try:
                os.remove(work_result['_file_path'])
                self._git_add_commit_push(f"Archive processed work: {work_result.get('work_id', 'unknown')}")
                print(f"🗄️ Archived: {work_result.get('work_id', 'unknown')}")
            except Exception as e:
                print(f"⚠️ Error archiving work: {e}")
    
    # WORKER MACHINE METHODS
    
    def claim_available_work(self, worker_id: str) -> Optional[Dict]:
        """
        Claim an available work order with optimistic locking (WORKER MACHINE).
        
        Args:
            worker_id: Unique identifier for this worker
            
        Returns:
            Work order dict if successfully claimed, None otherwise
        """
        max_attempts = 5
        base_delay = 1
        
        for attempt in range(1, max_attempts + 1):
            try:
                # Always start with fresh state
                self._git_pull()
                
                # Look for available work
                available_files = list(self.folders['available'].glob('*.json'))
                
                if not available_files:
                    print(f"No work available (attempt {attempt})")
                    return None
                
                # Sort by creation time for fairness
                available_files.sort(key=lambda f: f.stat().st_mtime)
                
                # Try to claim the oldest available work
                work_file = available_files[0]
                
                try:
                    # Read work order
                    with open(work_file, 'r') as f:
                        work_order = json.load(f)
                    
                    # Create unique claim identifier to avoid conflicts
                    claim_id = f"{worker_id}_{uuid.uuid4().hex[:8]}"
                    
                    # Prepare claimed work with claim ID
                    work_order['claimed_at'] = datetime.now().isoformat()
                    work_order['worker_id'] = worker_id
                    work_order['claim_id'] = claim_id
                    work_order['status'] = 'in_progress'
                    
                    claimed_file = self.folders['claimed'] / f"{claim_id}.json"
                    with open(claimed_file, 'w') as f:
                        json.dump(work_order, f, indent=2)
                    
                    # Try to remove from available BEFORE committing
                    try:
                        os.remove(work_file)
                    except FileNotFoundError:
                        # Another worker already claimed this work
                        if claimed_file.exists():
                            os.remove(claimed_file)
                        print(f"Work {work_file.name} already claimed by another worker")
                        
                        # Wait and retry
                        time.sleep(base_delay * attempt)
                        continue
                    
                    # Now commit the claim
                    try:
                        self._git_add_commit_push(f"Claim work: {work_order['work_id']} by {claim_id}")
                        print(f"✅ Claimed work: {work_order['work_id']} for {work_order['competition_id']}")
                        return work_order
                        
                    except Exception as git_error:
                        # Git operation failed, cleanup and retry
                        print(f"Git operation failed during claim: {git_error}")
                        
                        # Cleanup: restore the work file and remove claim
                        try:
                            with open(work_file, 'w') as f:
                                # Restore original work order (without claim info)
                                original_work = {k: v for k, v in work_order.items() 
                                            if k not in ['claimed_at', 'worker_id', 'claim_id', 'status']}
                                original_work['status'] = 'available'
                                json.dump(original_work, f, indent=2)
                            
                            if claimed_file.exists():
                                os.remove(claimed_file)
                        except:
                            pass  # Best effort cleanup
                        
                        # Wait and retry
                        time.sleep(base_delay * attempt)
                        continue
                    
                except Exception as e:
                    print(f"❌ Error processing work file {work_file}: {e}")
                    # Wait and retry with next file
                    time.sleep(base_delay * attempt)
                    continue
                    
            except Exception as e:
                print(f"❌ Error in work claiming attempt {attempt}: {e}")
                time.sleep(base_delay * attempt)
                continue
        
        print(f"❌ Failed to claim work after {max_attempts} attempts")
        return None
    
    def submit_completed_work(self, work_order: Dict, results: Dict):
        """
        Submit completed work results with improved reliability (WORKER MACHINE).
        
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
            **results
        }
        
        # Use claim_id for file naming if available, otherwise work_id
        file_id = work_order.get('claim_id', work_order['work_id'])
        
        for attempt in range(1, max_attempts + 1):
            try:
                # Write to completed folder
                completed_file = self.folders['completed'] / f"{file_id}.json"
                with open(completed_file, 'w') as f:
                    json.dump(work_result, f, indent=2)
                
                # Remove from claimed folder
                claimed_file = self.folders['claimed'] / f"{file_id}.json"
                if claimed_file.exists():
                    os.remove(claimed_file)
                
                # Try to commit and push
                try:
                    self._git_add_commit_push(f"Complete work: {work_order['work_id']}")
                    print(f"✅ Submitted completed work: {work_order['work_id']}")
                    return
                    
                except Exception as git_error:
                    print(f"Git operation failed during submission (attempt {attempt}): {git_error}")
                    if attempt < max_attempts:
                        time.sleep(base_delay * attempt)
                        continue
                    else:
                        # Final attempt failed, but work is still saved locally
                        print(f"⚠️ Work completed but git submission failed: {work_order['work_id']}")
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
        Submit failed work with improved reliability (WORKER MACHINE).
        
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
            "error_message": error_message
        }
        
        file_id = work_order.get('claim_id', work_order['work_id'])
        
        for attempt in range(1, max_attempts + 1):
            try:
                # Write to failed folder
                failed_file = self.folders['failed'] / f"{file_id}.json"
                with open(failed_file, 'w') as f:
                    json.dump(work_result, f, indent=2)
                
                # Remove from claimed folder
                claimed_file = self.folders['claimed'] / f"{file_id}.json"
                if claimed_file.exists():
                    os.remove(claimed_file)
                
                # Try to commit and push
                try:
                    self._git_add_commit_push(f"Fail work: {work_order['work_id']}")
                    print(f"❌ Submitted failed work: {work_order['work_id']} - {error_message}")
                    return
                    
                except Exception as git_error:
                    print(f"Git operation failed during failure submission (attempt {attempt}): {git_error}")
                    if attempt < max_attempts:
                        time.sleep(base_delay * attempt)
                        continue
                    else:
                        print(f"⚠️ Work failure recorded locally: {work_order['work_id']}")
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
            status[name] = len(list(folder.glob('*.json')))
        
        return status
    
    def get_failed_work(self) -> List[Dict]:
        """Get all failed work for processing (HOST MACHINE)."""
        self._git_pull()
        
        failed_work = []
        for work_file in self.folders['failed'].glob('*.json'):
            try:
                with open(work_file, 'r') as f:
                    work_result = json.load(f)
                    work_result['_file_path'] = work_file
                    failed_work.append(work_result)
            except Exception as e:
                print(f"⚠️ Error reading {work_file}: {e}")
        
        return failed_work

    def retry_failed_work(self, failed_work: Dict):
        """Move failed work back to available for retry."""
        # Remove retry_count for the new work order
        work_order = {k: v for k, v in failed_work.items() 
                    if k not in ['failed_at', 'error_message', '_file_path']}
        work_order['status'] = 'available'
        work_order['retry_count'] = failed_work.get('retry_count', 0) + 1
        
        # Create new available work
        work_file = self.folders['available'] / f"{work_order['work_id']}.json"
        with open(work_file, 'w') as f:
            json.dump(work_order, f, indent=2)
        
        # Remove from failed
        if '_file_path' in failed_work:
            os.remove(failed_work['_file_path'])
        
        self._git_add_commit_push(f"Retry failed work: {work_order['work_id']}")