#========================================
# File: enqueue.py
#========================================
#!/usr/bin/env python3
"""
enqueue.py: Add a new job to the GitHub-managed queue
"""
import os
import sys
import json
import uuid
import time
import subprocess

# Path to your cloned jobs repo (env var or current dir)
REPO_PATH = os.environ.get("JOBS_REPO", os.getcwd())
PENDING_DIR = os.path.join(REPO_PATH, "jobs", "pending")


def git_cmd(*args):
    """Run a git command in the repo."""
    return subprocess.check_call(["git", "-C", REPO_PATH] + list(args))


def enqueue(target_url, job_type="SCRAPE", priority=5, metadata=None):
    """
    Create a new job JSON under jobs/pending and push to GitHub.
    """
    job_id = str(uuid.uuid4())
    payload = {
        "job_id": job_id,
        "job_type": job_type,
        "target_url": target_url,
        "priority": priority,
        "metadata": metadata or {},
        "created_at": time.time(),
        "scheduled_at": time.time(),
    }
    os.makedirs(PENDING_DIR, exist_ok=True)
    filepath = os.path.join(PENDING_DIR, f"{job_id}.json")
    with open(filepath, "w") as f:
        json.dump(payload, f)

    git_cmd("add", filepath)
    git_cmd("commit", "-m", f"enqueue job {job_id}")
    git_cmd("push")
    print(f"Enqueued job {job_id}")


def main():
    if len(sys.argv) < 2:
        print("Usage: enqueue.py <target_url> [job_type] [priority]")
        sys.exit(1)
    target_url = sys.argv[1]
    job_type = sys.argv[2] if len(sys.argv) >= 3 else "SCRAPE"
    priority = int(sys.argv[3]) if len(sys.argv) >= 4 else 5
    enqueue(target_url, job_type, priority)


if __name__ == "__main__":
    main()