#!/bin/bash
# host_runner.sh - for HOST machine with database

REPO_URL="https://github.com/yourusername/scraping-work.git"  # Replace with your repo
ENVIRONMENT="production"
MAX_CYCLES=100

echo "🏠 Starting HOST machine work manager..."
echo "📊 Using database environment: $ENVIRONMENT"
echo "🔗 GitHub repository: $REPO_URL"

# Run host work manager
python host_work_manager.py \
    --repo-url "$REPO_URL" \
    --environment "$ENVIRONMENT" \
    --max-cycles "$MAX_CYCLES"