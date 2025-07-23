#========================================
# File: finalize.py
#========================================
#!/usr/bin/env python3
"""
finalize.py: Pull completed jobs from repo and update the database
"""
import os
import json
import subprocess
import logging
from glob import glob

import psycopg2

# Path to your cloned jobs repo (env var or current dir)
REPO_PATH = os.environ.get("JOBS_REPO", os.getcwd())
PENDING_DIR = os.path.join(REPO_PATH, "jobs", "pending")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configurable paths and DB URL\REPO_PATH = os.environ.get("JOBS_REPO", os.getcwd())
COMPLETED_DIR = os.path.join(REPO_PATH, "jobs", "completed")
DB_URL = os.environ.get("DATABASE_URL")


def git_cmd(*args):
    """Run a git command in the repo."""
    return subprocess.check_call(["git", "-C", REPO_PATH] + list(args))


def apply_results():
    """
    Pull from GitHub, process each .result.json, update DB, then remove & push
    """
    git_cmd("pull")
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    for filepath in glob(os.path.join(COMPLETED_DIR, "*.result.json")):
        with open(filepath) as f:
            res = json.load(f)
        job_id = res.get("job_id")
        metadata = res.get("metadata", {})
        extracted_data = res.get("extracted_data", {})
        extraction_id = res.get("extraction_id")

        # 1) mark job completed
        cur.execute(
            """
            UPDATE scrape_jobs
               SET status='completed',
                   completed_at=NOW(),
                   metadata = metadata || %s::jsonb
             WHERE job_id = %s;
            """,
            (json.dumps(metadata), job_id),
        )

        # 2) insert extraction record
        cur.execute(
            """
            INSERT INTO extractions (
                extraction_id, job_id, extracted_data, created_at
            ) VALUES (%s, %s, %s::jsonb, NOW());
            """,
            (extraction_id, job_id, json.dumps(extracted_data)),
        )
        conn.commit()

        # 3) cleanup and push
        os.remove(filepath)
        git_cmd("rm", filepath)
        git_cmd("commit", "-m", f"finalize job {job_id}")

    cur.close()
    conn.close()
    git_cmd("push")
    logger.info("Finalization complete.")


def main():
    if not DB_URL:
        logger.error("DATABASE_URL not set in env")
        sys.exit(1)
    apply_results()

if __name__ == "__main__":
    main()