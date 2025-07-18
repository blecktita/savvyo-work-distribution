# coordination/github_bridge.py
"""
GitHub Bridge for distributed scraping coordination.
Replaces direct database access for worker machines.
UPDATED with atomic claiming to prevent race conditions.
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
            'claims': self.repo_path / 'claim_attempts',  # NEW: Atomic claims folder
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
    
    def _git_add_commit_push(self, message: str, max_retries: int = 5) -> bool:
        """Add, commit, and push with improved conflict resolution."""
        for attempt in range(max_retries):
            try:
                # Check if there are any changes to commit
                result = subprocess.run(['git', 'status', '--porcelain'], 
                                      cwd=self.repo_path, capture_output=True, text=True)
                
                if not result.stdout.strip():
                    # No changes to commit, but this might be okay for atomic operations
                    print(f"⚠️ No changes to commit for: {message}")
                    return True
                
                # Add all changes
                subprocess.run(['git', 'add', '.'], cwd=self.repo_path, check=True, 
                             capture_output=True)
                
                # Commit
                subprocess.run(['git', 'commit', '-m', message], 
                             cwd=self.repo_path, check=True, capture_output=True)
                
                # Pull before push to handle any conflicts
                try:
                    subprocess.run(['git', 'pull', '--no-edit', 'origin', 'main'], 
                                 cwd=self.repo_path, check=True, capture_output=True)
                except subprocess.CalledProcessError:
                    # Pull failed, might be due to conflicts - continue to push anyway
                    pass
                
                # Push
                subprocess.run(['git', 'push', 'origin', 'main'], 
                             cwd=self.repo_path, check=True, capture_output=True)
                
                print(f"✅ Successfully pushed: {message} (attempt {attempt + 1})")
                return True
                
            except subprocess.CalledProcessError as e:
                if attempt < max_retries - 1:
                    # Exponential backoff with jitter
                    base_wait = (2 ** attempt)
                    jitter = time.time() % 1  # Random jitter based on current time
                    wait_time = base_wait + jitter
                    
                    print(f"❌ Git operation failed on attempt {attempt + 1}")
                    print(f"⏳ Retrying in {wait_time:.1f} seconds...")
                    time.sleep(wait_time)
                    
                    # Force pull latest state before retry
                    try:
                        subprocess.run(['git', 'fetch', 'origin'], cwd=self.repo_path, 
                                     capture_output=True)
                        subprocess.run(['git', 'reset', '--hard', 'origin/main'], 
                                     cwd=self.repo_path, capture_output=True)
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
                subprocess.run(['git', 'pull', '--no-edit', 'origin', 'main'], 
                             cwd=self.repo_path, check=True, capture_output=True)
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
    
   
   # WORKER MACHINE METHODS - ATOMIC VERSION
    def claim_available_work(self, worker_id: str) -> Optional[Dict]:
        """
        Atomically claim work using file creation to prevent race conditions.
        
        Args:
            worker_id: Unique identifier for this worker
            
        Returns:
            Work order dict if successfully claimed, None otherwise
        """
        # Pull latest state
        self._git_pull()
        
        # Find available work
        available_files = list(self.folders['available'].glob('*.json'))
        if not available_files:
            return None
        
        # Try to claim the first available work item
        work_file = available_files[0]
        work_id = work_file.stem
        
        # Create unique claim file name with worker and high-precision timestamp
        claim_id = f"{worker_id}_{int(time.time() * 1000)}_{uuid.uuid4().hex[:8]}"
        claim_file = self.folders['claims'] / f"{work_id}_{claim_id}.json"
        
        try:
            # Load work order
            with open(work_file, 'r') as f:
                work_order = json.load(f)
            
            # Create claim file with work data + claim info
            claim_data = {
                **work_order,
                'claimed_by': worker_id,
                'claimed_at': datetime.now().isoformat(),
                'claim_id': claim_id,
                'status': 'claiming'
            }
            
            # Write claim file
            with open(claim_file, 'w') as f:
                json.dump(claim_data, f, indent=2)
            
            # Try to push the claim atomically
            commit_msg = f"Attempt claim: {work_id} by {worker_id}"
            
            if self._git_add_commit_push(commit_msg):
                # SUCCESS! Now verify we won the race
                self._git_pull()  # Get latest state
                
                # Check if our claim file still exists
                if claim_file.exists():
                    # Check if we were the first to claim this work
                    claim_files = list(self.folders['claims'].glob(f"{work_id}_*.json"))
                    
                    if claim_files:
                        # Sort by filename (which includes timestamp)
                        claim_files.sort(key=lambda x: x.name)
                        
                        if claim_files[0] == claim_file:
                            # WE WON! Move to claimed folder and remove from available
                            print(f"🏆 {worker_id} won claim race for {work_id}")
                            
                            # Move our claim to claimed folder
                            claimed_file = self.folders['claimed'] / f"{worker_id}_{int(time.time())}.json"
                            claim_data['status'] = 'claimed'
                            
                            with open(claimed_file, 'w') as f:
                                json.dump(claim_data, f, indent=2)
                            
                            # Remove from available and claims
                            work_file.unlink(missing_ok=True)
                            claim_file.unlink(missing_ok=True)
                            
                            # Clean up other claim files for this work
                            for other_claim in claim_files[1:]:
                                other_claim.unlink(missing_ok=True)
                            
                            # Commit the final state
                            final_msg = f"Claim work: {work_id} by {worker_id}"
                            if self._git_add_commit_push(final_msg):
                                print(f"✅ Claimed work: {work_id} for {work_order.get('competition_id', 'UNKNOWN')}")
                                return claim_data
                            else:
                                print(f"⚠️ Failed to finalize claim for {work_id}, but work was claimed")
                                return claim_data
                        else:
                            # We lost the race, clean up our claim
                            print(f"❌ {worker_id} lost claim race for {work_id}")
                            claim_file.unlink(missing_ok=True)
                            self._git_add_commit_push(f"Clean up failed claim: {work_id} by {worker_id}")
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
            **results
        }
        
        # Use claim_id for file naming if available, otherwise worker_id + timestamp
        if 'claimed_by' in work_order:
            file_id = f"{work_order['claimed_by']}_{int(time.time())}"
        else:
            file_id = work_order.get('claim_id', work_order['work_id'])
        
        for attempt in range(1, max_attempts + 1):
            try:
                # Write to completed folder
                completed_file = self.folders['completed'] / f"{file_id}.json"
                with open(completed_file, 'w') as f:
                    json.dump(work_result, f, indent=2)
                
                # Remove from claimed folder (find the right file)
                claimed_files = list(self.folders['claimed'].glob(f"{work_order['claimed_by']}_*.json"))
                for claimed_file in claimed_files:
                    try:
                        with open(claimed_file, 'r') as f:
                            claimed_data = json.load(f)
                            if claimed_data.get('work_id') == work_order['work_id']:
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
            "error_message": error_message
        }
        
        # Use claim_id for file naming if available
        if 'claimed_by' in work_order:
            file_id = f"{work_order['claimed_by']}_{int(time.time())}"
        else:
            file_id = work_order.get('claim_id', work_order['work_id'])
        
        for attempt in range(1, max_attempts + 1):
            try:
                # Write to failed folder
                failed_file = self.folders['failed'] / f"{file_id}.json"
                with open(failed_file, 'w') as f:
                    json.dump(work_result, f, indent=2)
                
                # Remove from claimed folder (find the right file)
                claimed_files = list(self.folders['claimed'].glob(f"{work_order['claimed_by']}_*.json"))
                for claimed_file in claimed_files:
                    try:
                        with open(claimed_file, 'r') as f:
                            claimed_data = json.load(f)
                            if claimed_data.get('work_id') == work_order['work_id']:
                                claimed_file.unlink()
                                break
                    except:
                        continue
                
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
            if name == 'claims':
                continue  # Skip claims folder in status
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
                    if k not in ['failed_at', 'error_message', '_file_path', 'claimed_by', 'claimed_at', 'claim_id']}
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