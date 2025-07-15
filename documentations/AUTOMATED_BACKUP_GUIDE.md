# Automated Database Backup Scheduling Documentation

## Overview

This documentation provides comprehensive instructions for setting up automated database backups using cron jobs on Linux/macOS systems. The automated backup system will run the enhanced backup script (`database_backup.sh`) at scheduled intervals.

## Prerequisites

- Linux or macOS system
- Cron service installed and running
- Enhanced backup script (`database_backup.sh`) properly configured
- Database container running and accessible
- Sufficient disk space for backups

---

## 🕐 Understanding Cron Schedule Format

### Cron Syntax
```
* * * * *
│ │ │ │ │
│ │ │ │ └── Day of week (0-7, where 0 and 7 = Sunday)
│ │ │ └──── Month (1-12)
│ │ └───── Day of month (1-31)
│ └────── Hour (0-23)
└─────── Minute (0-59)
```

### Special Characters
- `*` = Every unit (every minute, every hour, etc.)
- `,` = List separator (1,3,5 = 1st, 3rd, and 5th)
- `-` = Range (1-5 = 1 through 5)
- `/` = Step values (*/2 = every 2 units)

### Common Schedule Examples
```bash
# Every minute
* * * * *

# Every hour at minute 0
0 * * * *

# Every day at midnight
0 0 * * *

# Every weekday at 9 AM
0 9 * * 1-5

# Every Sunday at 2 AM
0 2 * * 0

# Twice daily (6 AM and 6 PM)
0 6,18 * * *

# Every 6 hours
0 */6 * * *

# First day of every month at midnight
0 0 1 * *
```

---

## 🚀 Setting Up Automated Backups

### Step 1: Prepare Your Environment

```bash
# Navigate to your project directory
cd /path/to/your/project

# Verify backup script exists and is executable
ls -la database_backup.sh
chmod +x database_backup.sh

# Test the backup script manually first
./database_backup.sh
```

### Step 2: Get Your Project Path

```bash
# Get the full path to your project
pwd
# Example output: /home/user/Documents/PROJECTS/PRODUCT_DEV/product-dev

# Save this path - you'll need it for the cron job
PROJECT_PATH=$(pwd)
echo "Project path: $PROJECT_PATH"
```

### Step 3: Create Log Directory

```bash
# Create directory for backup logs
sudo mkdir -p /var/log/savvyo
sudo touch /var/log/savvyo/backup.log
sudo touch /var/log/savvyo/backup_error.log

# Set permissions (replace 'username' with your actual username)
sudo chown username:username /var/log/savvyo/backup.log
sudo chown username:username /var/log/savvyo/backup_error.log
sudo chmod 644 /var/log/savvyo/*.log
```

### Step 4: Open Crontab Editor

```bash
# Open crontab for editing
crontab -e

# If prompted, choose your preferred editor (nano is recommended for beginners)
```

### Step 5: Add Cron Job

Add one of these lines to your crontab (replace `/path/to/your/project` with your actual path):

#### Recommended Schedule (Weekdays at 10 AM):
```bash
# Daily backup at 10 AM on weekdays
0 10 * * 1-5 cd /path/to/your/project && ./database_backup.sh >> /var/log/savvyo/backup.log 2>> /var/log/savvyo/backup_error.log
```

#### Alternative Schedules:
```bash
# Every day at 2 AM (including weekends)
0 2 * * * cd /path/to/your/project && ./database_backup.sh >> /var/log/savvyo/backup.log 2>> /var/log/savvyo/backup_error.log

# Twice daily (6 AM and 6 PM) on weekdays
0 6,18 * * 1-5 cd /path/to/your/project && ./database_backup.sh >> /var/log/savvyo/backup.log 2>> /var/log/savvyo/backup_error.log

# Every 8 hours
0 */8 * * * cd /path/to/your/project && ./database_backup.sh >> /var/log/savvyo/backup.log 2>> /var/log/savvyo/backup_error.log

# Weekly on Sunday at 3 AM
0 3 * * 0 cd /path/to/your/project && ./database_backup.sh >> /var/log/savvyo/backup.log 2>> /var/log/savvyo/backup_error.log
```

### Step 6: Save and Exit

- **nano**: Press `Ctrl+X`, then `Y`, then `Enter`
- **vim**: Press `Esc`, type `:wq`, then `Enter`
- **emacs**: Press `Ctrl+X`, then `Ctrl+S`, then `Ctrl+X`, then `Ctrl+C`

### Step 7: Verify Installation

```bash
# List your cron jobs to verify
crontab -l

# Check cron service status on macOS
sudo launchctl list | grep cron

# On Linux systems, use:
# sudo systemctl status cron
```

---

## 🎛️ Different Backup Schedules

### High-Frequency Backups (Data-Critical Environments)
```bash
# Every 2 hours during business hours (8 AM - 6 PM)
0 8-18/2 * * 1-5 cd /path/to/project && ./database_backup.sh >> /var/log/savvyo/backup.log 2>&1

# Every hour during peak times
0 9-17 * * 1-5 cd /path/to/project && ./database_backup.sh >> /var/log/savvyo/backup.log 2>&1
```

### Standard Frequency (Recommended)
```bash
# Daily on weekdays
0 10 * * 1-5 cd /path/to/project && ./database_backup.sh >> /var/log/savvyo/backup.log 2>&1

# Daily including weekends
0 2 * * * cd /path/to/project && ./database_backup.sh >> /var/log/savvyo/backup.log 2>&1
```

### Low-Frequency Backups (Stable Data)
```bash
# Weekly on Sundays
0 3 * * 0 cd /path/to/project && ./database_backup.sh >> /var/log/savvyo/backup.log 2>&1

# Bi-weekly (every other Sunday)
0 3 */14 * 0 cd /path/to/project && ./database_backup.sh >> /var/log/savvyo/backup.log 2>&1
```

### Custom Schedules
```bash
# Business days only, avoiding peak hours
0 22 * * 1-5 cd /path/to/project && ./database_backup.sh >> /var/log/savvyo/backup.log 2>&1

# End of month backups
0 23 28-31 * * cd /path/to/project && ./database_backup.sh >> /var/log/savvyo/backup.log 2>&1

# Quarterly backups (1st day of Jan, Apr, Jul, Oct)
0 1 1 1,4,7,10 * cd /path/to/project && ./database_backup.sh >> /var/log/savvyo/backup.log 2>&1
```

---

## 🛑 Managing Automated Backups

### Viewing Current Cron Jobs
```bash
# List all cron jobs for current user
crontab -l

# List cron jobs with line numbers
crontab -l | cat -n
```

### Temporarily Disabling Backups
```bash
# Method 1: Comment out the line in crontab
crontab -e
# Add # at the beginning of the backup line:
# 0 10 * * 1-5 cd /path/to/project && ./database_backup.sh >> /var/log/savvyo/backup.log 2>&1

# Method 2: Remove the cron job temporarily
crontab -l > backup_crontab.txt  # Save current crontab
crontab -r  # Remove all cron jobs
# To restore later: crontab backup_crontab.txt
```

### Stopping Automated Backups Permanently
```bash
# Edit crontab and remove the backup line
crontab -e

# Or remove all cron jobs
crontab -r
```

### Modifying Schedule
```bash
# Edit existing cron jobs
crontab -e
# Modify the time/frequency as needed
# Save and exit
```

---

## 📋 Log Management

### Log File Locations
```bash
# Standard output (backup progress and success messages)
~/logs/savvyo/backup.log          # macOS user home directory
/var/log/savvyo/backup.log        # Linux systems

# Error output (error messages and failures)
~/logs/savvyo/backup_error.log    # macOS user home directory
/var/log/savvyo/backup_error.log  # Linux systems

# System cron logs
/var/log/system.log               # macOS
/var/log/cron                     # Most Linux systems
/var/log/cron.log                 # Some Ubuntu systems
```

### Viewing Logs

#### Recent Backup Activity
```bash
# Last 50 lines of backup log (macOS)
tail -50 ~/logs/savvyo/backup.log

# Last 50 lines of backup log (Linux)
tail -50 /var/log/savvyo/backup.log

# Last 20 lines with timestamps (macOS)
tail -20 ~/logs/savvyo/backup.log | while read line; do echo "[$(date)] $line"; done

# Follow log in real-time (macOS)
tail -f ~/logs/savvyo/backup.log
```

#### Error Checking
```bash
# Check for errors (macOS)
tail -50 ~/logs/savvyo/backup_error.log

# Check for errors (Linux)
tail -50 /var/log/savvyo/backup_error.log

# Search for specific errors (macOS)
grep -i "error\|fail\|exception" ~/logs/savvyo/backup_error.log

# Check if cron job is running (macOS)
grep "backup_database_with_stats" /var/log/system.log

# Check if cron job is running (Linux)
grep "backup_database_with_stats" /var/log/cron
```

#### Log Analysis
```bash
# Count successful backups today (macOS)
grep "$(date +%Y-%m-%d)" ~/logs/savvyo/backup.log | grep -c "Backup completed successfully"

# Count successful backups today (Linux)
grep "$(date +%Y-%m-%d)" /var/log/savvyo/backup.log | grep -c "Backup completed successfully"

# Find backup completion times (macOS)
grep "Backup completed successfully" ~/logs/savvyo/backup.log | tail -10

# Check backup sizes over time (macOS)
grep "Backup size:" ~/logs/savvyo/backup.log | tail -10
```

### Log Rotation and Cleanup

#### Automatic Log Rotation
Create `/etc/logrotate.d/savvyo-backup`:
```bash
sudo nano /etc/logrotate.d/savvyo-backup
```

Add this content:
```
/var/log/savvyo/*.log {
    daily
    missingok
    rotate 30
    compress
    delaycompress
    notifempty
    create 644 username username
}
```

#### Manual Log Cleanup
```bash
# Archive old logs
gzip /var/log/savvyo/backup.log.old

# Remove logs older than 30 days
find /var/log/savvyo/ -name "*.log.*" -mtime +30 -delete

# Truncate large log files
> /var/log/savvyo/backup.log  # Clear the file
```

---

## 🔧 Troubleshooting

### Common Issues and Solutions

#### 1. Cron Job Not Running
```bash
# Check if cron service is running (macOS)
sudo launchctl list | grep cron

# Check if cron service is running (Linux)
sudo systemctl status cron
sudo systemctl start cron  # Start if stopped

# Check system cron logs (macOS)
sudo tail -f /var/log/system.log

# Check system cron logs (Linux)
sudo tail -f /var/log/cron
```

#### 2. Permission Denied Errors
```bash
# Make backup script executable
chmod +x /path/to/project/database_backup.sh

# Check log file permissions (macOS)
ls -la ~/logs/savvyo/

# Check log file permissions (Linux)
ls -la /var/log/savvyo/
sudo chown username:username /var/log/savvyo/*.log
```

#### 3. Path Issues
```bash
# Use absolute paths in cron jobs
# Wrong: cd project && ./backup.sh
# Right: cd /full/path/to/project && ./database_backup.sh

# Check if path exists
ls -la /path/to/your/project/database_backup.sh
```

#### 4. Environment Variables
```bash
# Add environment variables to crontab if needed
crontab -e

# Add at the top (macOS):
PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin
HOME=/Users/username

# Add at the top (Linux):
PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin
HOME=/home/username
```

#### 5. Docker Issues
```bash
# Check if Docker daemon is accessible
docker ps

# On Linux systems, ensure user is in docker group:
sudo usermod -aG docker username

# On macOS, Docker Desktop should handle permissions automatically
```

### Testing Cron Jobs

#### Test Without Waiting
```bash
# Run the exact command from your cron job (macOS)
cd /path/to/your/project && ./database_backup.sh >> ~/logs/savvyo/backup.log 2>> ~/logs/savvyo/backup_error.log

# Run the exact command from your cron job (Linux)
cd /path/to/your/project && ./database_backup.sh >> /var/log/savvyo/backup.log 2>> /var/log/savvyo/backup_error.log

# Check results
echo "Exit code: $?"
# macOS:
tail -20 ~/logs/savvyo/backup.log
# Linux:
tail -20 /var/log/savvyo/backup.log
```

#### Create Test Schedule
```bash
# Temporarily set backup to run in 2 minutes
# If current time is 14:30, set: 32 14 * * *
crontab -e

# Add test job (macOS):
32 14 * * * cd /path/to/project && echo "Test backup at $(date)" >> ~/logs/savvyo/backup.log

# Add test job (Linux):
32 14 * * * cd /path/to/project && echo "Test backup at $(date)" >> /var/log/savvyo/backup.log

# Remove test job after verification
```

---

## 📊 Monitoring and Alerts

### Simple Backup Monitoring Script

Create `check_backup_status.sh`:
```bash
#!/bin/bash

BACKUP_DIR="./backups"
LOG_FILE="/var/log/savvyo/backup.log"
ALERT_EMAIL="admin@yourcompany.com"

# Check if backup ran today
if grep -q "$(date +%Y-%m-%d)" "$LOG_FILE" && grep -q "Backup completed successfully" "$LOG_FILE"; then
    echo "✅ Backup completed successfully today"
else
    echo "❌ No successful backup found for today"
    # Uncomment to send email alert:
    # echo "Backup failed on $(hostname) at $(date)" | mail -s "Backup Alert" "$ALERT_EMAIL"
fi

# Check backup file age
LATEST_BACKUP=$(ls -t "${BACKUP_DIR}"/savvyo_backup_*.sql.gz 2>/dev/null | head -1)
if [ ! -z "$LATEST_BACKUP" ]; then
    BACKUP_AGE_HOURS=$(( ($(date +%s) - $(stat -c %Y "$LATEST_BACKUP")) / 3600 ))
    if [ "$BACKUP_AGE_HOURS" -gt 48 ]; then
        echo "⚠️  Latest backup is ${BACKUP_AGE_HOURS} hours old"
    else
        echo "✅ Latest backup is ${BACKUP_AGE_HOURS} hours old"
    fi
fi
```

### Schedule Monitoring
```bash
# Run backup check daily at 11 AM
crontab -e

# Add:
0 11 * * * cd /path/to/project && ./check_backup_status.sh >> /var/log/savvyo/backup_monitor.log 2>&1
```

---

## 📝 Best Practices

### 1. Backup Verification
- Always test restore process monthly
- Monitor backup file sizes for anomalies
- Verify backup integrity regularly

### 2. Schedule Optimization
- Avoid peak database usage times
- Consider database maintenance windows
- Balance frequency with storage capacity

### 3. Resource Management
- Monitor disk space regularly
- Implement log rotation
- Clean up old backups automatically

### 4. Security
- Protect backup files with appropriate permissions
- Consider encrypting sensitive backups
- Secure log files from unauthorized access

### 5. Documentation
- Document any custom modifications
- Keep backup schedule information updated
- Maintain emergency contact procedures

---

## 📞 Emergency Procedures

### If Automated Backups Fail

1. **Check cron service**:
   ```bash
   # macOS:
   sudo launchctl list | grep cron
   
   # Linux:
   sudo systemctl status cron
   sudo systemctl restart cron
   ```

2. **Run manual backup**:
   ```bash
   cd /path/to/project
   ./database_backup.sh
   ```

3. **Check logs**:
   ```bash
   # macOS:
   tail -50 ~/logs/savvyo/backup_error.log
   
   # Linux:
   tail -50 /var/log/savvyo/backup_error.log
   ```

4. **Verify database connectivity**:
   ```bash
   docker ps | grep savvyo_database
   ```

### Recovery from Log Issues

```bash
# Reset log files (macOS)
rm ~/logs/savvyo/*.log
touch ~/logs/savvyo/backup.log
touch ~/logs/savvyo/backup_error.log

# Reset log files (Linux)
sudo rm /var/log/savvyo/*.log
sudo touch /var/log/savvyo/backup.log
sudo touch /var/log/savvyo/backup_error.log
sudo chown username:username /var/log/savvyo/*.log
```

---

## 📅 Maintenance Schedule

### Weekly Tasks
- Review backup logs for errors
- Check available disk space
- Verify recent backup files exist

### Monthly Tasks
- Test backup restoration process
- Review and clean old log files
- Update backup retention policies

### Quarterly Tasks
- Review backup schedule effectiveness
- Update documentation
- Test disaster recovery procedures

---

**Last Updated**: July 15, 2025  
**Version**: 1.0  
**Next Review Date**: August 15, 2025

---

## Quick Reference Commands

```bash
# View current cron jobs
crontab -l

# Edit cron jobs
crontab -e

# Check recent backups
ls -la ./backups/ | tail -5

# Monitor backup logs (macOS)
tail -f ~/logs/savvyo/backup.log

# Monitor backup logs (Linux)
tail -f /var/log/savvyo/backup.log

# Test backup manually
./database_backup.sh

# Check cron service (macOS)
sudo launchctl list | grep cron

# Check cron service (Linux)
sudo systemctl status cron
```