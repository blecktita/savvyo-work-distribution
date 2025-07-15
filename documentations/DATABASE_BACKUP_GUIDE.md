# Database Backup & Restore Documentation

## Overview

This documentation provides step-by-step instructions for backing up and restoring the Savvyo PostgreSQL database running in Docker containers. The system includes automated daily backups, manual backup options, and a safe restore process.

## Prerequisites

- Docker and Docker Compose installed
- Savvyo database container running (`savvyo_database`)
- Bash shell access
- Sufficient disk space for backups (recommended: 10GB+ free space)

## Files Included

- `backup_database.sh` - Main backup script
- `restore_database.sh` - Database restore script
- `./backups/` - Directory where all backup files are stored

---

## 🔧 Initial Setup

### Step 1: Make Scripts Executable

```bash
chmod +x backup_database.sh
chmod +x restore_database.sh
```

### Step 2: Verify Database Container

Ensure your database container is running:

```bash
docker ps | grep savvyo_database
```

If not running, start it:

```bash
docker-compose up -d savvyo_db_manager
```

### Step 3: Test Your First Backup

Run a manual backup to verify everything works:

```bash
./backup_database.sh
```

Expected output:
```
=== Starting Database Backup ===
Date: Mon Jul 15 14:30:00 UTC 2025
Container: savvyo_database
Database: production_savvyo_db
Backup file: ./backups/savvyo_backup_20250715_143000.sql
Creating database backup...
Compressing backup...
✅ Backup completed successfully!
Backup size: 15M
```

---

## 📅 Setting Up Automated Daily Backups

### Option 1: Using Crontab (Linux/macOS)

1. Open crontab editor:
```bash
crontab -e
```

2. Add this line for daily backups at 2:00 AM:
```bash
0 2 * * * cd /full/path/to/your/project && ./backup_database.sh >> /var/log/db_backup.log 2>&1
```

3. Replace `/full/path/to/your/project` with your actual project path:
```bash
# Find your current path
pwd
# Example result: /home/user/Documents/PROJECTS/PRODUCT_DEV/product-dev
```

4. Save and exit the crontab editor

### Option 2: Using Docker Compose (Alternative)

Add this service to your `docker-compose.yml`:

```yaml
  backup_service:
    image: postgres:15-alpine
    container_name: savvyo_backup
    restart: unless-stopped
    profiles: ["backup"]
    
    volumes:
      - ./backups:/backups
      - ./backup_database.sh:/backup_database.sh
    
    environment:
      - POSTGRES_USER=aseathletics_datascience
      - POSTGRES_DB=production_savvyo_db
    
    command: >
      sh -c "
        apk add --no-cache dcron &&
        echo '0 2 * * * /backup_database.sh' | crontab - &&
        crond -f
      "
    
    networks:
      - savvyo_network
    
    depends_on:
      - savvyo_db_manager
```

Start with:
```bash
docker-compose --profile backup up -d backup_service
```

---

## 💾 Manual Backup Process

### Creating a Manual Backup

```bash
./backup_database.sh
```

### What the Backup Script Does

1. **Checks container status** - Ensures database container is running
2. **Creates backup directory** - Makes `./backups/` folder if needed
3. **Generates SQL dump** - Uses `pg_dump` to export all data
4. **Compresses backup** - Uses gzip to reduce file size
5. **Verifies integrity** - Tests the compressed file
6. **Cleans old backups** - Removes backups older than 30 days
7. **Reports status** - Shows backup size and location

### Backup File Naming Convention

Backups are named with timestamp:
```
savvyo_backup_YYYYMMDD_HHMMSS.sql.gz
```

Examples:
- `savvyo_backup_20250715_143000.sql.gz` (July 15, 2025 at 14:30:00)
- `savvyo_backup_20250716_020000.sql.gz` (July 16, 2025 at 02:00:00)

---

## 🔄 Database Restore Process

### Listing Available Backups

```bash
ls -lh ./backups/savvyo_backup_*.sql.gz
```

### Restoring from Backup

⚠️ **WARNING**: Restoring will replace ALL current data in the database!

```bash
./restore_database.sh ./backups/savvyo_backup_20250715_143000.sql.gz
```

### Restore Process Steps

1. **Safety confirmation** - Script asks for confirmation
2. **Creates safety backup** - Backs up current data before restore
3. **Decompresses backup file** - Extracts the SQL commands
4. **Restores database** - Applies the backup to PostgreSQL
5. **Verifies restore** - Counts tables and records
6. **Reports completion** - Shows restore statistics

### Example Restore Session

```bash
$ ./restore_database.sh ./backups/savvyo_backup_20250715_143000.sql.gz

=== Database Restore ===
Date: Mon Jul 15 16:45:00 UTC 2025
Container: savvyo_database
Database: production_savvyo_db
Backup file: ./backups/savvyo_backup_20250715_143000.sql.gz

⚠️  WARNING: This will REPLACE all data in the database!
⚠️  Make sure you have a current backup before proceeding.

Are you sure you want to restore from ./backups/savvyo_backup_20250715_143000.sql.gz? (type 'yes' to continue): yes

Creating safety backup first...
✅ Safety backup created: ./backups/pre_restore_backup_20250715_164500.sql.gz
Restoring database from backup...
Verifying restore...

=== Restore Complete ===
✅ Database restored successfully!
📊 Tables found: 5
🏆 Competitions: 1,234
⚽ Teams: 5,678
🛡️  Safety backup: ./backups/pre_restore_backup_20250715_164500.sql.gz

🎉 Restore completed at: Mon Jul 15 16:46:30 UTC 2025
```

---

## 📁 Backup Management

### Viewing Backup Information

```bash
# List all backups with sizes
ls -lh ./backups/

# Check total backup storage usage
du -sh ./backups/

# View the 10 most recent backups
ls -lt ./backups/savvyo_backup_*.sql.gz | head -10
```

### Manual Cleanup of Old Backups

```bash
# Remove backups older than 30 days
find ./backups/ -name "savvyo_backup_*.sql.gz" -type f -mtime +30 -delete

# Remove backups older than 7 days
find ./backups/ -name "savvyo_backup_*.sql.gz" -type f -mtime +7 -delete
```

### Changing Retention Policy

Edit `backup_database.sh` and modify this line:
```bash
# Change from 30 days to your preferred retention period
RETENTION_DAYS=30
```

---

## 🚨 Troubleshooting

### Common Issues and Solutions

#### 1. "Container not running" Error

```bash
ERROR: Container savvyo_database is not running!
```

**Solution:**
```bash
docker-compose up -d savvyo_db_manager
```

#### 2. "Permission denied" Error

```bash
bash: ./backup_database.sh: Permission denied
```

**Solution:**
```bash
chmod +x backup_database.sh
chmod +x restore_database.sh
```

#### 3. "Backup file empty" Error

```bash
ERROR: Backup file was not created or is empty!
```

**Solutions:**
- Check database container logs: `docker logs savvyo_database`
- Verify database credentials in the script
- Ensure sufficient disk space

#### 4. "No space left on device" Error

**Solutions:**
- Clean up old backups: `rm ./backups/savvyo_backup_2025070*.sql.gz`
- Check available space: `df -h`
- Move backups to external storage

#### 5. Database Connection Issues

```bash
psql: could not connect to server
```

**Solutions:**
- Check container status: `docker ps | grep savvyo_database`
- Verify database credentials in `.env` file
- Check network connectivity: `docker network ls`

### Checking Backup Integrity

```bash
# Test if backup file can be decompressed
gunzip -t ./backups/savvyo_backup_20250715_143000.sql.gz

# View backup file contents (first 20 lines)
gunzip -c ./backups/savvyo_backup_20250715_143000.sql.gz | head -20
```

### Emergency Recovery

If all else fails and you need to recover data:

1. **Stop all containers:**
```bash
docker-compose down
```

2. **Check all available volumes:**
```bash
docker volume ls | grep savvyo
```

3. **Test each volume for data:**
```bash
# Replace volume_name with each volume found
docker run --rm -v volume_name:/data alpine ls -la /data
```

4. **Manually restore from volume with data:**
```bash
# Use the volume that contains your data
docker-compose up -d savvyo_db_manager
```

---

## 📋 Best Practices

### 1. Regular Testing
- Test restore process monthly on a separate environment
- Verify backup integrity weekly

### 2. Multiple Backup Locations
- Store critical backups on external drives
- Consider cloud storage for important backups

### 3. Monitoring
- Check backup logs regularly
- Set up alerts for backup failures

### 4. Documentation
- Keep this documentation updated
- Document any custom changes to scripts

### 5. Access Control
- Protect backup files with appropriate permissions
- Store database credentials securely

---

## 📞 Emergency Contacts

- **Database Administrator**: [Add contact information]
- **System Administrator**: [Add contact information]
- **Project Manager**: [Add contact information]

---

## 📝 Maintenance Log

| Date | Action | Performed By | Notes |
|------|--------|--------------|-------|
| 2025-07-15 | Initial setup | [Your Name] | Created backup system |
| | | | |
| | | | |

---

**Last Updated**: July 15, 2025  
**Version**: 1.0  
**Next Review Date**: August 15, 2025