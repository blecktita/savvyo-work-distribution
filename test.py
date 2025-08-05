#!/usr/bin/env python3
"""
Debug script to run on worker machine to diagnose the issue
"""

import json
import subprocess
import sys
from pathlib import Path

def debug_worker_environment():
    print("🔍 WORKER MACHINE DEBUG")
    print("=" * 50)
   
    # 1. Check current directory and repository
    current_dir = Path.cwd()
    print(f"1. Current working directory: {current_dir}")
   
    # Check for scraping-work directory
    repo_paths_to_check = [
        Path("./scraping-work"),
        Path("../scraping-work"),
        current_dir / "scraping-work",
        Path.home() / "scraping-work"
    ]
   
    repo_path = None
    for path in repo_paths_to_check:
        if path.exists():
            repo_path = path
            print(f"   Found repository at: {path}")
            break
   
    if not repo_path:
        print("❌ No scraping-work repository found!")
        print("   Checked paths:")
        for path in repo_paths_to_check:
            print(f"     - {path}")
        return
   
    # 2. Check git status
    print(f"\n2. Git Repository Status:")
    try:
        # Check remote
        result = subprocess.run(
            ["git", "remote", "-v"],
            cwd=repo_path,
            capture_output=True,
            text=True
        )
        print(f"   Remote: {result.stdout.strip()}")
       
        # Check current branch
        result = subprocess.run(
            ["git", "branch", "--show-current"],
            cwd=repo_path,
            capture_output=True,
            text=True
        )
        print(f"   Current branch: {result.stdout.strip()}")
       
        # Test pull
        result = subprocess.run(
            ["git", "pull", "--dry-run"],
            cwd=repo_path,
            capture_output=True,
            text=True
        )
        print(f"   Pull access: {'OK' if result.returncode == 0 else 'FAILED'}")
        if result.stderr:
            print(f"   Pull error: {result.stderr.strip()}")
           
    except Exception as e:
        print(f"   Git check failed: {e}")
   
    # 3. Check folder structure
    print(f"\n3. Repository Structure:")
    folders = {
        "available": repo_path / "available_work",
        "claims": repo_path / "work_queue",
        "claimed": repo_path / "active_work",
        "completed": repo_path / "completed_work",
        "failed": repo_path / "retry_queue"
    }
   
    for name, folder in folders.items():
        exists = folder.exists()
        if exists:
            json_files = list(folder.glob("*.json"))
            print(f"   {name}: {len(json_files)} files")
            # Show first few files
            for file in json_files[:3]:
                print(f"     - {file.name}")
        else:
            print(f"   {name}: MISSING")
   
    # 4. Test available work files
    print(f"\n4. Available Work Analysis:")
    available_folder = folders["available"]
    if available_folder.exists():
        json_files = list(available_folder.glob("*.json"))
        print(f"   Found {len(json_files)} JSON files")
       
        for i, file in enumerate(json_files[:3]):
            print(f"\n   File {i+1}: {file.name}")
            try:
                with open(file, 'r') as f:
                    data = json.load(f)
               
                print(f"     work_type: {data.get('work_type', 'MISSING')}")
                print(f"     work_id: {data.get('work_id', 'MISSING')}")
                print(f"     competition_id: {data.get('competition_id', 'MISSING')}")
                print(f"     status: {data.get('status', 'MISSING')}")
                print(f"     All keys: {list(data.keys())}")
               
                # Check if this matches worker filtering
                work_type = data.get('work_type')
                if work_type == 'match_data':
                    print(f"     ✅ MATCHES worker filter")
                else:
                    print(f"     ❌ FAILS worker filter (expected 'match_data', got '{work_type}')")
                   
            except Exception as e:
                print(f"     ❌ Error reading file: {e}")
   
    # 5. Test Python import
    print(f"\n5. Python Module Test:")
    try:
        # Test if we can import the bridge
        sys.path.append(str(current_dir))
        from coordination.github_bridge import GitHubWorkBridge
       
        # Test initialization
        bridge = GitHubWorkBridge(
            repo_path=str(repo_path),
            repo_url="https://github.com/blecktita/savvyo-work-distribution.git"  # Adjust as needed
        )
       
        print("   ✅ GitHubWorkBridge import successful")
       
        # Test work claiming
        available_files = list(bridge.folders["available"].glob("*.json"))
        print(f"   Bridge sees {len(available_files)} available files")
       
    except Exception as e:
        print(f"   ❌ Python module test failed: {e}")
        import traceback
        traceback.print_exc()
   
    print("=" * 50)

if __name__ == "__main__":
    debug_worker_environment()