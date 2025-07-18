#!/bin/bash

# Quick Backup Summary Script
# Shows a summary of all backups and database status

# Configuration
CONTAINER_NAME="savvyo_database"
DB_NAME="production_savvyo_db"
DB_USER="aseathletics_datascience"
BACKUP_DIR="./backups"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}=================================================================${NC}"
echo -e "${BLUE}                    BACKUP SUMMARY${NC}"
echo -e "${BLUE}=================================================================${NC}"

# Check if container is running
if docker ps | grep -q "${CONTAINER_NAME}"; then
    echo -e "${GREEN}🐳 Database container: RUNNING${NC}"
    
    # Get current data counts
    COMPETITIONS=$(docker exec "${CONTAINER_NAME}" psql -U "${DB_USER}" -d "${DB_NAME}" -t -c "SELECT count(*) FROM competitions;" 2>/dev/null | xargs || echo "0")
    TEAMS=$(docker exec "${CONTAINER_NAME}" psql -U "${DB_USER}" -d "${DB_NAME}" -t -c "SELECT count(*) FROM teams;" 2>/dev/null | xargs || echo "0")
    
    echo "🏆 Current competitions: $(printf "%'d" $COMPETITIONS)"
    echo "⚽ Current teams: $(printf "%'d" $TEAMS)"
else
    echo -e "${YELLOW}🐳 Database container: NOT RUNNING${NC}"
fi

echo ""
echo -e "${BLUE}📁 BACKUP FILES:${NC}"

if ls "${BACKUP_DIR}"/savvyo_backup_*.sql.gz 1> /dev/null 2>&1; then
    BACKUP_COUNT=$(ls -1 "${BACKUP_DIR}"/savvyo_backup_*.sql.gz | wc -l)
    TOTAL_SIZE=$(du -sh "${BACKUP_DIR}" 2>/dev/null | cut -f1 || echo "Unknown")
    
    echo "📊 Total backups: $BACKUP_COUNT"
    echo "💾 Total size: $TOTAL_SIZE"
    
    echo ""
    echo "📅 RECENT BACKUPS:"
    ls -lh "${BACKUP_DIR}"/savvyo_backup_*.sql.gz | tail -5 | while read line; do
        echo "   $line"
    done
    
    # Latest backup info
    LATEST_BACKUP=$(ls -t "${BACKUP_DIR}"/savvyo_backup_*.sql.gz 2>/dev/null | head -1)
    if [ ! -z "$LATEST_BACKUP" ]; then
        BACKUP_DATE=$(stat -c %Y "$LATEST_BACKUP" 2>/dev/null || stat -f %m "$LATEST_BACKUP" 2>/dev/null)
        BACKUP_AGE_HOURS=$(( ($(date +%s) - BACKUP_DATE) / 3600 ))
        
        echo ""
        echo "🕐 Latest backup: $(basename "$LATEST_BACKUP")"
        echo "⏰ Age: ${BACKUP_AGE_HOURS} hours ago"
        
        if [ "$BACKUP_AGE_HOURS" -gt 24 ]; then
            echo -e "${YELLOW}⚠️  WARNING: Latest backup is over 24 hours old${NC}"
        fi
    fi
else
    echo "❌ No backup files found"
fi

echo ""
echo -e "${BLUE}🛠️  AVAILABLE COMMANDS:${NC}"
echo "   ./backup_database_with_stats.sh     - Create detailed backup"
echo "   ./verify_backup_freshness.sh        - Check backup freshness"
echo "   ./restore_database.sh <backup_file> - Restore from backup"
echo "   ./backup_summary.sh                 - Show this summary"

echo -e "${BLUE}=================================================================${NC}"