#========================================
# File: worker.py
#========================================
#!/usr/bin/env python3
"""
worker.py: Continuously polls for new jobs, processes them, and reports results
"""
import os
import json
import time
import uuid
import subprocess
import logging
from glob import glob

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configurable dirs and poll interval
REPO_PATH = os.environ.get("JOBS_REPO", os.getcwd())
PENDING_DIR = os.path.join(REPO_PATH, "jobs", "pending")
RUNNING_DIR = os.path.join(REPO_PATH, "jobs", "running")
COMPLETED_DIR = os.path.join(REPO_PATH, "jobs", "completed")
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "10"))


def git_cmd(*args):
    """Run a git command in the repo."""
    return subprocess.check_call(["git", "-C", REPO_PATH] + list(args))


def claim_job():
    """Pull latest, pick one pending job, move to running, commit+push."""
    git_cmd("pull")
    pendings = glob(os.path.join(PENDING_DIR, "*.json"))
    if not pendings:
        return None
    src = pendings[0]
    job = json.load(open(src))
    os.makedirs(RUNNING_DIR, exist_ok=True)
    dst = os.path.join(RUNNING_DIR, os.path.basename(src))
    os.rename(src, dst)
    job["worker_id"] = os.uname().nodename
    job["started_at"] = time.time()
    with open(dst, "w") as f:
        json.dump(job, f)
    git_cmd("add", src, dst)
    git_cmd("commit", "-m", f"job {job['job_id']} claimed")
    git_cmd("push")
    return job


def process_job(job):
    """User-defined scraping logic; returns result dict"""
    logger.info(f"Processing job {job['job_id']}...")
    # TODO: replace with real work
    time.sleep(2)
    return {
        "job_id": job["job_id"],
        "extraction_id": str(uuid.uuid4()),
        "extracted_data": {"foo": "bar"},
        "metadata": {"duration": 2},
    }


def report_job(result):
    """Write result JSON, remove running, commit+push."""
    os.makedirs(COMPLETED_DIR, exist_ok=True)
    result_path = os.path.join(COMPLETED_DIR, f"{result['job_id']}.result.json")
    with open(result_path, "w") as f:
        json.dump(result, f)
    running_path = os.path.join(RUNNING_DIR, f"{result['job_id']}.json")
    if os.path.exists(running_path):
        os.remove(running_path)
    git_cmd("add", running_path, result_path)
    git_cmd("commit", "-m", f"job {result['job_id']} completed")
    git_cmd("push")
    logger.info(f"Reported completion of job {result['job_id']}")


def main():
    while True:
        try:
            job = claim_job()
            if not job:
                time.sleep(POLL_INTERVAL)
                continue
            result = process_job(job)
            report_job(result)
        except subprocess.CalledProcessError as e:
            logger.error(f"Git error: {e}")
            time.sleep(POLL_INTERVAL)
        except Exception as e:
            logger.exception(f"Unexpected error: {e}")
            time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()