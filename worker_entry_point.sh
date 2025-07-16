#!/bin/bash
# worker_runner.sh - for WORKER machines (no database)

REPO_URL="https://github.com/yourusername/scraping-work.git"  # Replace with your repo
ENVIRONMENT="production"
MAX_WORK=50

echo "🤖 Starting WORKER machine..."
echo "🔗 GitHub repository: $REPO_URL"

# Run worker
python worker_main.py \
    --repo-url "$REPO_URL" \
    --environment "$ENVIRONMENT" \
    --max-work "$MAX_WORK"