# macOS Automation Recipe Book: Complete launchd Guide

## 📖 Table of Contents
1. [Overview & Why launchd](#overview--why-launchd)
2. [Prerequisites](#prerequisites)
3. [Understanding launchd](#understanding-launchd)
4. [Recipe 1: Basic Scheduled Script](#recipe-1-basic-scheduled-script)
5. [Recipe 2: Database Backup Automation](#recipe-2-database-backup-automation)
6. [Recipe 3: API Health Monitoring](#recipe-3-api-health-monitoring)
7. [Recipe 4: Log Rotation & Cleanup](#recipe-4-log-rotation--cleanup)
8. [Troubleshooting Guide](#troubleshooting-guide)
9. [Best Practices](#best-practices)
10. [Quick Reference](#quick-reference)

---

## Overview & Why launchd

### What is launchd?
**launchd** is Apple's preferred system for running scheduled tasks on macOS. It's more powerful and reliable than traditional cron jobs.

### Why Use launchd Instead of Cron?
- ✅ **Better macOS Integration:** Respects system sleep/wake cycles
- ✅ **User Permissions:** Runs with proper user context and permissions
- ✅ **Automatic Restart:** Can restart failed processes automatically
- ✅ **Resource Management:** Better memory and CPU management
- ✅ **Logging:** Integrated with macOS logging system
- ✅ **Security:** Works with macOS security policies (no "Operation not permitted" errors)

### When to Use launchd vs Cron
- **Use launchd for:** User-specific tasks, GUI applications, tasks needing full user permissions
- **Use cron for:** System-wide tasks (if you have admin access)

---

## Prerequisites

### Required Knowledge
- Basic terminal/command line usage
- Understanding of file permissions
- Basic knowledge of the processes you want to automate

### Required Tools
```bash
# Check if you have these commands
which launchctl  # Should return: /bin/launchctl
which plist      # For editing plist files (optional)
```

### Required Directories
```bash
# User launch agents (recommended for most tasks)
~/Library/LaunchAgents/

# System launch daemons (requires admin - rarely needed)
/Library/LaunchDaemons/
```

---

## Understanding launchd

### Key Concepts

**Launch Agent vs Launch Daemon:**
- **Launch Agent:** Runs when user is logged in (use for most development tasks)
- **Launch Daemon:** Runs system-wide, even when no user is logged in

**Property List (plist) Files:**
- XML files that define what to run and when
- Stored in `~/Library/LaunchAgents/` for user agents
- Follow reverse domain naming: `com.company.project.task.plist`

### Basic plist Structure
```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.savvyo.example.task</string>
    <key>ProgramArguments</key>
    <array>
        <string>/path/to/your/script.sh</string>
    </array>
    <key>StartCalendarInterval</key>
    <dict>
        <key>Hour</key>
        <integer>10</integer>
        <key>Minute</key>
        <integer>0</integer>
    </dict>
</dict>
</plist>
```

---

## Recipe 1: Basic Scheduled Script

### Problem
Run a simple script every day at a specific time.

### Solution Steps

**Step 1: Create Your Script**
```bash
# Create a scripts directory
mkdir -p ~/Scripts

# Create a simple test script
cat > ~/Scripts/daily_task.sh << 'EOF'
#!/bin/bash

# Add essential PATH for tools
export PATH="/usr/local/bin:/opt/homebrew/bin:$PATH"

# Your script logic here
echo "$(date): Daily task executed!" >> ~/Scripts/logs/daily_task.log
EOF

# Make it executable
chmod +x ~/Scripts/daily_task.sh

# Create log directory
mkdir -p ~/Scripts/logs
```

**Step 2: Test Your Script Manually**
```bash
# Always test manually first!
~/Scripts/daily_task.sh

# Check the output
cat ~/Scripts/logs/daily_task.log
```

**Step 3: Create the Launch Agent**
```bash
cat > ~/Library/LaunchAgents/com.savvyo.daily.task.plist << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.savvyo.daily.task</string>
    <key>ProgramArguments</key>
    <array>
        <string>/bin/bash</string>
        <string>/Users/YOUR_USERNAME/Scripts/daily_task.sh</string>
    </array>
    <key>StartCalendarInterval</key>
    <dict>
        <key>Hour</key>
        <integer>9</integer>
        <key>Minute</key>
        <integer>0</integer>
    </dict>
    <key>StandardOutPath</key>
    <string>/Users/YOUR_USERNAME/Scripts/logs/daily_task.log</string>
    <key>StandardErrorPath</key>
    <string>/Users/YOUR_USERNAME/Scripts/logs/daily_task_error.log</string>
</dict>
</plist>
EOF

# Replace YOUR_USERNAME with your actual username
sed -i '' "s/YOUR_USERNAME/$(whoami)/g" ~/Library/LaunchAgents/com.savvyo.daily.task.plist
```

**Step 4: Load and Test**
```bash
# Load the launch agent
launchctl load ~/Library/LaunchAgents/com.savvyo.daily.task.plist

# Verify it's loaded
launchctl list | grep com.savvyo.daily.task

# Test it manually (don't wait for scheduled time)
launchctl start com.savvyo.daily.task

# Check the results
cat ~/Scripts/logs/daily_task.log
cat ~/Scripts/logs/daily_task_error.log
```

---

## Recipe 2: Database Backup Automation

### Problem
Automatically backup a Docker database every weekday at 10 AM.

### Solution Steps

**Step 1: Prepare the Backup Script**
```bash
# Copy your existing backup script to Scripts directory
cp /path/to/your/backup/script.sh ~/Scripts/database_backup.sh

# Make it executable
chmod +x ~/Scripts/database_backup.sh
```

**Step 2: Modify Script for launchd Compatibility**

Edit `~/Scripts/database_backup.sh` and ensure it has:

```bash
#!/bin/bash

# CRITICAL: Add Docker to PATH (launchd has minimal PATH)
export PATH="/usr/local/bin:/opt/homebrew/bin:$PATH"

# Use absolute paths for all directories
BACKUP_DIR="$HOME/Scripts/backups"
LOG_FILE="$HOME/Scripts/logs/backup.log"

# Create directories if they don't exist
mkdir -p "${BACKUP_DIR}"
mkdir -p "$(dirname "${LOG_FILE}")"

# Rest of your backup logic...
```

**Key Modifications Needed:**
- Add `export PATH="/usr/local/bin:/opt/homebrew/bin:$PATH"`
- Change all relative paths (`./backups`) to absolute paths (`$HOME/Scripts/backups`)
- Ensure Docker commands will work in minimal environment

**Step 3: Create Weekday Schedule Launch Agent**
```bash
cat > ~/Library/LaunchAgents/com.savvyo.database.backup.plist << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.savvyo.database.backup</string>
    <key>ProgramArguments</key>
    <array>
        <string>/bin/bash</string>
        <string>/Users/YOUR_USERNAME/Scripts/database_backup.sh</string>
    </array>
    <key>StartCalendarInterval</key>
    <array>
        <dict>
            <key>Hour</key>
            <integer>10</integer>
            <key>Minute</key>
            <integer>0</integer>
            <key>Weekday</key>
            <integer>1</integer>
        </dict>
        <dict>
            <key>Hour</key>
            <integer>10</integer>
            <key>Minute</key>
            <integer>0</integer>
            <key>Weekday</key>
            <integer>2</integer>
        </dict>
        <dict>
            <key>Hour</key>
            <integer>10</integer>
            <key>Minute</key>
            <integer>0</integer>
            <key>Weekday</key>
            <integer>3</integer>
        </dict>
        <dict>
            <key>Hour</key>
            <integer>10</integer>
            <key>Minute</key>
            <integer>0</integer>
            <key>Weekday</key>
            <integer>4</integer>
        </dict>
        <dict>
            <key>Hour</key>
            <integer>10</integer>
            <key>Minute</key>
            <integer>0</integer>
            <key>Weekday</key>
            <integer>5</integer>
        </dict>
    </array>
    <key>StandardOutPath</key>
    <string>/var/log/savvyo/backup.log</string>
    <key>StandardErrorPath</key>
    <string>/var/log/savvyo/backup_error.log</string>
</dict>
</plist>
EOF

# Replace username
sed -i '' "s/YOUR_USERNAME/$(whoami)/g" ~/Library/LaunchAgents/com.savvyo.database.backup.plist
```

**Step 4: Create Log Directory**
```bash
# Create log directory (may need sudo)
sudo mkdir -p /var/log/savvyo
sudo chown $(whoami):staff /var/log/savvyo
```

**Step 5: Load and Test**
```bash
# Load the agent
launchctl load ~/Library/LaunchAgents/com.savvyo.database.backup.plist

# Test immediately
launchctl start com.savvyo.database.backup

# Check results
cat /var/log/savvyo/backup.log
cat /var/log/savvyo/backup_error.log

# Check backup files
ls -la ~/Scripts/backups/
```

---

## Recipe 3: API Health Monitoring

### Problem
Monitor API health every 5 minutes and alert if down.

### Solution Steps

**Step 1: Create Health Check Script**
```bash
cat > ~/Scripts/api_health_check.sh << 'EOF'
#!/bin/bash

# Add PATH for curl and other tools
export PATH="/usr/local/bin:/opt/homebrew/bin:$PATH"

API_URL="http://localhost:5001/health"
LOG_FILE="$HOME/Scripts/logs/api_health.log"
ERROR_LOG="$HOME/Scripts/logs/api_health_error.log"

# Create log directory
mkdir -p "$(dirname "$LOG_FILE")"

# Check API health
response=$(curl -s -o /dev/null -w "%{http_code}" "$API_URL" 2>/dev/null)

timestamp=$(date '+%Y-%m-%d %H:%M:%S')

if [ "$response" = "200" ]; then
    echo "[$timestamp] API Health: OK (200)" >> "$LOG_FILE"
else
    echo "[$timestamp] API Health: FAILED (HTTP: $response)" >> "$ERROR_LOG"
    # Optional: Send alert (email, Slack, etc.)
    # curl -X POST -H 'Content-type: application/json' --data '{"text":"API Down!"}' YOUR_SLACK_WEBHOOK
fi
EOF

chmod +x ~/Scripts/api_health_check.sh
```

**Step 2: Create Frequent Monitoring Launch Agent**
```bash
cat > ~/Library/LaunchAgents/com.savvyo.api.health.plist << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.savvyo.api.health</string>
    <key>ProgramArguments</key>
    <array>
        <string>/bin/bash</string>
        <string>/Users/YOUR_USERNAME/Scripts/api_health_check.sh</string>
    </array>
    <key>StartInterval</key>
    <integer>300</integer>  <!-- Run every 300 seconds (5 minutes) -->
    <key>StandardOutPath</key>
    <string>/Users/YOUR_USERNAME/Scripts/logs/api_health.log</string>
    <key>StandardErrorPath</key>
    <string>/Users/YOUR_USERNAME/Scripts/logs/api_health_system_error.log</string>
</dict>
</plist>
EOF

sed -i '' "s/YOUR_USERNAME/$(whoami)/g" ~/Library/LaunchAgents/com.savvyo.api.health.plist
```

**Step 3: Load and Monitor**
```bash
launchctl load ~/Library/LaunchAgents/com.savvyo.api.health.plist
launchctl start com.savvyo.api.health

# Monitor the logs
tail -f ~/Scripts/logs/api_health.log
```

---

## Recipe 4: Log Rotation & Cleanup

### Problem
Prevent log files from growing too large and clean up old backups.

### Solution Steps

**Step 1: Create Cleanup Script**
```bash
cat > ~/Scripts/cleanup_logs.sh << 'EOF'
#!/bin/bash

export PATH="/usr/local/bin:/opt/homebrew/bin:$PATH"

LOG_DIR="$HOME/Scripts/logs"
BACKUP_DIR="$HOME/Scripts/backups"
RETENTION_DAYS=7

echo "$(date): Starting log cleanup..."

# Rotate large log files
for log_file in "$LOG_DIR"/*.log; do
    if [ -f "$log_file" ] && [ $(stat -f%z "$log_file") -gt 10485760 ]; then  # 10MB
        mv "$log_file" "${log_file}.$(date +%Y%m%d)"
        touch "$log_file"
        echo "Rotated large log file: $(basename "$log_file")"
    fi
done

# Clean up old rotated logs
find "$LOG_DIR" -name "*.log.*" -mtime +$RETENTION_DAYS -delete

# Clean up old backups (if they exist)
if [ -d "$BACKUP_DIR" ]; then
    deleted_count=$(find "$BACKUP_DIR" -name "*.sql.gz" -mtime +$RETENTION_DAYS -delete -print | wc -l)
    echo "Deleted $deleted_count old backup files"
fi

echo "$(date): Log cleanup completed"
EOF

chmod +x ~/Scripts/cleanup_logs.sh
```

**Step 2: Schedule Weekly Cleanup**
```bash
cat > ~/Library/LaunchAgents/com.savvyo.log.cleanup.plist << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.savvyo.log.cleanup</string>
    <key>ProgramArguments</key>
    <array>
        <string>/bin/bash</string>
        <string>/Users/YOUR_USERNAME/Scripts/cleanup_logs.sh</string>
    </array>
    <key>StartCalendarInterval</key>
    <dict>
        <key>Hour</key>
        <integer>2</integer>
        <key>Minute</key>
        <integer>0</integer>
        <key>Weekday</key>
        <integer>0</integer>  <!-- Sunday -->
    </dict>
    <key>StandardOutPath</key>
    <string>/Users/YOUR_USERNAME/Scripts/logs/cleanup.log</string>
    <key>StandardErrorPath</key>
    <string>/Users/YOUR_USERNAME/Scripts/logs/cleanup_error.log</string>
</dict>
</plist>
EOF

sed -i '' "s/YOUR_USERNAME/$(whoami)/g" ~/Library/LaunchAgents/com.savvyo.log.cleanup.plist
launchctl load ~/Library/LaunchAgents/com.savvyo.log.cleanup.plist
```

---

## Troubleshooting Guide

### Common Issues & Solutions

#### 1. "Operation not permitted" Error
**Problem:** Script can't access files or directories
```bash
# Check file permissions
ls -la ~/Scripts/your_script.sh

# Fix permissions
chmod +x ~/Scripts/your_script.sh

# Move script out of restricted directories
mv ~/Documents/script.sh ~/Scripts/
```

#### 2. "Command not found" Error
**Problem:** launchd can't find required tools (docker, node, etc.)
```bash
# Add to your script
export PATH="/usr/local/bin:/opt/homebrew/bin:$PATH"

# Or find the exact path
which docker  # Add this path to your script
```

#### 3. Launch Agent Won't Load
```bash
# Check syntax
plutil ~/Library/LaunchAgents/com.your.agent.plist

# Check for errors
launchctl load -w ~/Library/LaunchAgents/com.your.agent.plist

# View system logs
log show --predicate 'process == "launchd"' --last 1h | grep your.agent
```

#### 4. Script Runs Manually But Not via launchd
**Common causes:**
- Missing PATH exports
- Relative paths instead of absolute paths
- Different working directory
- Missing environment variables

**Debug steps:**
```bash
# Add debug info to your script
echo "PATH: $PATH" >> /tmp/debug.log
echo "PWD: $(pwd)" >> /tmp/debug.log
echo "USER: $(whoami)" >> /tmp/debug.log
```

#### 5. Check Launch Agent Status
```bash
# List all user agents
launchctl list | grep com.savvyo

# Check specific agent status
launchctl list com.savvyo.your.agent

# View logs
tail -f ~/Scripts/logs/your_script.log
```

### Debugging Commands

```bash
# Test your script manually first
~/Scripts/your_script.sh

# Test with launchd environment
sudo -u $(whoami) ~/Scripts/your_script.sh

# Check plist syntax
plutil -lint ~/Library/LaunchAgents/com.your.agent.plist

# Reload after changes
launchctl unload ~/Library/LaunchAgents/com.your.agent.plist
launchctl load ~/Library/LaunchAgents/com.your.agent.plist

# Force run immediately
launchctl start com.your.agent

# Check system logs
log show --predicate 'subsystem == "com.apple.launchd"' --last 1h
```

---

## Best Practices

### 1. Script Organization
```
~/Scripts/
├── database_backup.sh
├── api_health_check.sh
├── cleanup_logs.sh
├── logs/
│   ├── backup.log
│   ├── health.log
│   └── cleanup.log
└── backups/
    ├── backup_20250722.sql.gz
    └── backup_20250721.sql.gz
```

### 2. Naming Conventions
- **Scripts:** `snake_case.sh`
- **Launch agents:** `com.company.project.task.plist`
- **Log files:** `script_name.log` and `script_name_error.log`

### 3. Essential Script Headers
```bash
#!/bin/bash
set -euo pipefail  # Exit on error, undefined vars, pipe failures

# Add required PATHs
export PATH="/usr/local/bin:/opt/homebrew/bin:$PATH"

# Use absolute paths
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
LOG_DIR="$HOME/Scripts/logs"
```

### 4. Logging Best Practices
```bash
# Function for consistent logging
log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S'): $1" >> "$LOG_FILE"
}

# Use throughout script
log "Starting backup process..."
log "Backup completed successfully"
```

### 5. Error Handling
```bash
# Check prerequisites
if ! command -v docker &> /dev/null; then
    echo "ERROR: Docker not found in PATH" >&2
    exit 1
fi

# Check if services are running
if ! docker ps | grep -q "your_container"; then
    echo "ERROR: Container not running" >&2
    exit 1
fi
```

### 6. Testing Checklist
- [ ] Script runs manually
- [ ] All paths are absolute
- [ ] Required tools are in PATH
- [ ] Log directories exist
- [ ] Permissions are correct
- [ ] plist syntax is valid
- [ ] Launch agent loads without errors
- [ ] Manual start works
- [ ] Scheduled execution works

---

## Quick Reference

### Essential Commands

```bash
# Load/unload launch agents
launchctl load ~/Library/LaunchAgents/com.your.agent.plist
launchctl unload ~/Library/LaunchAgents/com.your.agent.plist

# List agents
launchctl list | grep com.your

# Manual start
launchctl start com.your.agent

# Check status
launchctl list com.your.agent

# Remove agent completely
launchctl unload ~/Library/LaunchAgents/com.your.agent.plist
rm ~/Library/LaunchAgents/com.your.agent.plist
```

### Time Formats

**Daily at specific time:**
```xml
<key>StartCalendarInterval</key>
<dict>
    <key>Hour</key>
    <integer>14</integer>
    <key>Minute</key>
    <integer>30</integer>
</dict>
```

**Weekdays only:**
```xml
<key>StartCalendarInterval</key>
<array>
    <dict>
        <key>Hour</key><integer>9</integer>
        <key>Minute</key><integer>0</integer>
        <key>Weekday</key><integer>1</integer> <!-- Monday -->
    </dict>
    <!-- Repeat for each weekday 2-5 -->
</array>
```

**Every N minutes:**
```xml
<key>StartInterval</key>
<integer>300</integer> <!-- 300 seconds = 5 minutes -->
```

**Monthly (first of month):**
```xml
<key>StartCalendarInterval</key>
<dict>
    <key>Day</key><integer>1</integer>
    <key>Hour</key><integer>2</integer>
    <key>Minute</key><integer>0</integer>
</dict>
```

### Directory Locations
- **User Launch Agents:** `~/Library/LaunchAgents/`
- **Scripts:** `~/Scripts/`
- **Logs:** `~/Scripts/logs/` or `/var/log/your_app/`
- **Data:** `~/Scripts/data/` or `~/Scripts/backups/`

### Exit Codes
- **0:** Success
- **1:** General error
- **126:** Permission denied or not executable
- **127:** Command not found

---

## Summary

This guide provides everything needed to implement robust automation on macOS using launchd. The key advantages over cron include better macOS integration, proper user permissions, and more reliable execution.

**Remember:**
1. Always test scripts manually first
2. Use absolute paths everywhere
3. Add required tools to PATH
4. Check logs for debugging
5. Follow naming conventions
6. Document your automation for team members

For additional help, refer to:
- `man launchd.plist` - Complete plist reference
- `man launchctl` - Launch control commands
- `/System/Library/LaunchAgents/` - System examples to learn from