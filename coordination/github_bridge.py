# coordination/github_bridge.py
"""
GitHub Bridge for distributed scraping coordination.
Replaces direct database access for worker machines.
"""

import os
import json
import uuid
import subprocess
import time
from typing import List, Dict, Optional
from datetime import datetime
from pathlib import Path


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
        """Add, commit and push changes."""
        try:
            subprocess.run(['git', 'add', '.'], cwd=self.repo_path, check=True)
            subprocess.run(['git', 'commit', '-m', message], cwd=self.repo_path, check=True)
            subprocess.run(['git', 'push'], cwd=self.repo_path, check=False)
        except subprocess.CalledProcessError:
            pass  # Ignore commit failures (nothing to commit, etc.)
    
    def _git_pull(self):
        """Pull latest changes."""
        try:
            subprocess.run(['git', 'pull'], cwd=self.repo_path, check=False)
        except:
            pass
    
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
        Claim an available work order (WORKER MACHINE).
        
        Args:
            worker_id: Unique identifier for this worker
            
        Returns:
            Work order dict if successfully claimed, None otherwise
        """
        self._git_pull()
        
        # Look for available work
        available_files = list(self.folders['available'].glob('*.json'))
        
        if not available_files:
            return None
        
        # Try to claim the first available work
        work_file = available_files[0]
        
        try:
            # Read work order
            with open(work_file, 'r') as f:
                work_order = json.load(f)
            
            # Move to claimed folder with worker info
            work_order['claimed_at'] = datetime.now().isoformat()
            work_order['worker_id'] = worker_id
            work_order['status'] = 'in_progress'
            
            claimed_file = self.folders['claimed'] / work_file.name
            with open(claimed_file, 'w') as f:
                json.dump(work_order, f, indent=2)
            
            # Remove from available
            os.remove(work_file)
            
            # Commit changes
            self._git_add_commit_push(f"Claim work: {work_order['work_id']} by {worker_id}")
            
            print(f"✅ Claimed work: {work_order['work_id']} for {work_order['competition_id']}")
            return work_order
            
        except Exception as e:
            print(f"❌ Error claiming work from {work_file}: {e}")
            return None
    
    def submit_completed_work(self, work_order: Dict, results: Dict):
        """
        Submit completed work results (WORKER MACHINE).
        
        Args:
            work_order: Original work order
            results: Results from scraping
        """
        work_result = {
            **work_order,
            "completed_at": datetime.now().isoformat(),
            "status": "completed",
            **results
        }
        
        # Write to completed folder
        completed_file = self.folders['completed'] / f"{work_order['work_id']}.json"
        with open(completed_file, 'w') as f:
            json.dump(work_result, f, indent=2)
        
        # Remove from claimed folder
        claimed_file = self.folders['claimed'] / f"{work_order['work_id']}.json"
        if claimed_file.exists():
            os.remove(claimed_file)
        
        # Commit and push
        self._git_add_commit_push(f"Complete work: {work_order['work_id']}")
        
        print(f"✅ Submitted completed work: {work_order['work_id']}")
    
    def submit_failed_work(self, work_order: Dict, error_message: str):
        """
        Submit failed work (WORKER MACHINE).
        
        Args:
            work_order: Original work order  
            error_message: Error description
        """
        work_result = {
            **work_order,
            "failed_at": datetime.now().isoformat(),
            "status": "failed",
            "error_message": error_message
        }
        
        # Write to failed folder
        failed_file = self.folders['failed'] / f"{work_order['work_id']}.json"
        with open(failed_file, 'w') as f:
            json.dump(work_result, f, indent=2)
        
        # Remove from claimed folder
        claimed_file = self.folders['claimed'] / f"{work_order['work_id']}.json"
        if claimed_file.exists():
            os.remove(claimed_file)
        
        # Commit and push
        self._git_add_commit_push(f"Fail work: {work_order['work_id']}")
        
        print(f"❌ Submitted failed work: {work_order['work_id']} - {error_message}")
    
    def get_work_status(self) -> Dict:
        """Get overview of work status."""
        self._git_pull()
        
        status = {}
        for name, folder in self.folders.items():
            status[name] = len(list(folder.glob('*.json')))
        
        return status