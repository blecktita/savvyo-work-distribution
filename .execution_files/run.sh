#!/bin/bash

# Sequential multi-session scraper runner
# Runs ONE session at a time to prevent VPN conflicts
# CRITICAL: Never run multiple instances of this script simultaneously!

TOTAL_SESSIONS=50  # How many sessions to run
DELAY_BETWEEN_SESSIONS=100  # 5 minutes between sessions (recommended for VPN safety)

# Create lock file to prevent multiple instances
LOCK_FILE="/tmp/scraper_session.lock"
if [ -f "$LOCK_FILE" ]; then
    echo "❌ Another scraper session is already running!"
    echo "   Lock file exists: $LOCK_FILE"
    echo "   If this is incorrect, delete the lock file and try again."
    exit 1
fi

# Create lock file
echo $ > "$LOCK_FILE"
echo "🔒 Created session lock file"

# Cleanup function
cleanup() {
    echo "🧹 Cleaning up session lock..."
    rm -f "$LOCK_FILE"
}

# Set trap to cleanup on exit
trap cleanup EXIT

echo "🚀 Starting multi-session scraper (${TOTAL_SESSIONS} sessions)"
echo "⏱️ Delay between sessions: ${DELAY_BETWEEN_SESSIONS} seconds"

for i in $(seq 1 $TOTAL_SESSIONS); do
    echo ""
    echo "🎯 ===== SESSION $i of $TOTAL_SESSIONS ====="
    echo "⏰ Started at: $(date)"
    
    # Run the scraper
    uv run --active main.py
    
    # Check if this was the last session
    if [ $i -lt $TOTAL_SESSIONS ]; then
        echo "⏱️ Session $i completed. Waiting ${DELAY_BETWEEN_SESSIONS} seconds before next session..."
        echo "💤 Next session starts at: $(date -d "+${DELAY_BETWEEN_SESSIONS} seconds")"
        sleep $DELAY_BETWEEN_SESSIONS
    else
        echo "🎉 All sessions completed!"
    fi
done

echo ""
echo "✅ Multi-session run finished at: $(date)"