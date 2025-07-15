#!/bin/bash

# Backup Freshness Verification Script
# This script compares the latest backup with current database to ensure data freshness

set -e

# Configuration
CONTAINER_NAME="savvyo_database"
DB_NAME="production_savvyo_db"
DB_USER="aseathletics_datascience"
BACKUP_DIR="./backups"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}=================================================================${NC}"
echo -e "${BLUE}              BACKUP FRESHNESS VERIFICATION${NC}"
echo -e "${BLUE}=================================================================${NC}"

# Find the latest backup
LATEST_BACKUP=$(ls -t "${BACKUP_DIR}"/savvyo_backup_*.sql.gz 2>/dev/null | head -1)
LATEST_STATS=$(ls -t "${BACKUP_DIR}"/backup_stats_*.txt 2>/dev/null | head -1)

if [ -z "$LATEST_BACKUP" ]; then
    echo -e "${RED}❌ No backup files found in ${BACKUP_DIR}${NC}"
    exit 1
fi

echo "🔍 Latest backup: $(basename "$LATEST_BACKUP")"
echo "📊 Latest stats: $(basename "$LATEST_STATS")"

# Get backup timestamp
BACKUP_DATE=$(stat -c %Y "$LATEST_BACKUP" 2>/dev/null || stat -f %m "$LATEST_BACKUP" 2>/dev/null)
BACKUP_AGE_HOURS=$(( ($(date +%s) - BACKUP_DATE) / 3600 ))

echo "⏰ Backup age: ${BACKUP_AGE_HOURS} hours old"

# Check if container is running
if ! docker ps | grep -q "${CONTAINER_NAME}"; then
    echo -e "${RED}❌ ERROR: Container ${CONTAINER_NAME} is not running!${NC}"
    exit 1
fi

echo ""
echo -e "${YELLOW}📊 COMPARING CURRENT DATA WITH BACKUP...${NC}"

# Get current database statistics
echo "Getting current database state..."

CURRENT_COMPETITIONS=$(docker exec "${CONTAINER_NAME}" psql -U "${DB_USER}" -d "${DB_NAME}" -t -c "SELECT count(*) FROM competitions;" 2>/dev/null | xargs || echo "0")
CURRENT_TEAMS=$(docker exec "${CONTAINER_NAME}" psql -U "${DB_USER}" -d "${DB_NAME}" -t -c "SELECT count(*) FROM teams;" 2>/dev/null | xargs || echo "0")
CURRENT_COMP_PROGRESS=$(docker exec "${CONTAINER_NAME}" psql -U "${DB_USER}" -d "${DB_NAME}" -t -c "SELECT count(*) FROM competition_progress;" 2>/dev/null | xargs || echo "0")
CURRENT_SEASON_PROGRESS=$(docker exec "${CONTAINER_NAME}" psql -U "${DB_USER}" -d "${DB_NAME}" -t -c "SELECT count(*) FROM season_progress;" 2>/dev/null | xargs || echo "0")

# Get latest records to check for recent activity
LATEST_COMPETITION=$(docker exec "${CONTAINER_NAME}" psql -U "${DB_USER}" -d "${DB_NAME}" -t -c "SELECT created_at FROM competitions ORDER BY created_at DESC LIMIT 1;" 2>/dev/null | xargs || echo "No data")
LATEST_TEAM=$(docker exec "${CONTAINER_NAME}" psql -U "${DB_USER}" -d "${DB_NAME}" -t -c "SELECT created_at FROM teams ORDER BY created_at DESC LIMIT 1;" 2>/dev/null | xargs || echo "No data")

echo ""
echo -e "${BLUE}=================================================================${NC}"
echo -e "${BLUE}                    CURRENT DATABASE STATE${NC}"
echo -e "${BLUE}=================================================================${NC}"
echo "🏆 Competitions: $(printf "%'d" $CURRENT_COMPETITIONS)"
echo "⚽ Teams: $(printf "%'d" $CURRENT_TEAMS)"
echo "📈 Competition Progress: $(printf "%'d" $CURRENT_COMP_PROGRESS)"
echo "📊 Season Progress: $(printf "%'d" $CURRENT_SEASON_PROGRESS)"
echo ""
echo "🕐 Latest competition added: $LATEST_COMPETITION"
echo "🕐 Latest team added: $LATEST_TEAM"

# Extract counts from backup stats file if available
if [ ! -z "$LATEST_STATS" ] && [ -f "$LATEST_STATS" ]; then
    echo ""
    echo -e "${BLUE}=================================================================${NC}"
    echo -e "${BLUE}                   BACKUP STATISTICS${NC}"
    echo -e "${BLUE}=================================================================${NC}"
    
    # Extract key information from stats file
    BACKUP_COMPETITIONS=$(grep -A 10 "EXACT ROW COUNTS" "$LATEST_STATS" | grep "competitions" | awk '{print $2}' | tr -d ',' || echo "Unknown")
    BACKUP_TEAMS=$(grep -A 10 "EXACT ROW COUNTS" "$LATEST_STATS" | grep "teams" | awk '{print $2}' | tr -d ',' || echo "Unknown")
    
    echo "🏆 Competitions in backup: $BACKUP_COMPETITIONS"
    echo "⚽ Teams in backup: $BACKUP_TEAMS"
    
    # Show backup creation time
    BACKUP_TIME=$(grep "Statistics gathered at:" "$LATEST_STATS" | cut -d ':' -f 2- | xargs)
    echo "📅 Backup created: $BACKUP_TIME"
fi

echo ""
echo -e "${BLUE}=================================================================${NC}"
echo -e "${BLUE}                    FRESHNESS ANALYSIS${NC}"
echo -e "${BLUE}=================================================================${NC}"

# Analyze freshness
FRESHNESS_ISSUES=0

# Check backup age
if [ "$BACKUP_AGE_HOURS" -gt 24 ]; then
    echo -e "${YELLOW}⚠️  WARNING: Backup is more than 24 hours old (${BACKUP_AGE_HOURS} hours)${NC}"
    FRESHNESS_ISSUES=$((FRESHNESS_ISSUES + 1))
elif [ "$BACKUP_AGE_HOURS" -gt 8 ]; then
    echo -e "${YELLOW}ℹ️  INFO: Backup is ${BACKUP_AGE_HOURS} hours old${NC}"
else
    echo -e "${GREEN}✅ Backup is recent (${BACKUP_AGE_HOURS} hours old)${NC}"
fi

# Check for recent database activity
RECENT_ACTIVITY=$(docker exec "${CONTAINER_NAME}" psql -U "${DB_USER}" -d "${DB_NAME}" -t -c "
    SELECT COUNT(*) FROM (
        SELECT created_at FROM competitions WHERE created_at > NOW() - INTERVAL '24 hours'
        UNION ALL
        SELECT created_at FROM teams WHERE created_at > NOW() - INTERVAL '24 hours'
    ) recent;" 2>/dev/null | xargs || echo "0")

if [ "$RECENT_ACTIVITY" -gt 0 ]; then
    echo -e "${YELLOW}📊 Recent activity: $RECENT_ACTIVITY new records in last 24 hours${NC}"
    if [ "$BACKUP_AGE_HOURS" -gt 2 ]; then
        echo -e "${YELLOW}⚠️  Consider creating a fresh backup to capture recent changes${NC}"
        FRESHNESS_ISSUES=$((FRESHNESS_ISSUES + 1))
    fi
else
    echo -e "${GREEN}📊 No recent activity detected in last 24 hours${NC}"
fi

# Data integrity check
echo ""
echo -e "${YELLOW}🔍 PERFORMING DATA INTEGRITY CHECKS...${NC}"

# Check for orphaned records or data inconsistencies
ORPHANED_TEAMS=$(docker exec "${CONTAINER_NAME}" psql -U "${DB_USER}" -d "${DB_NAME}" -t -c "
    SELECT COUNT(*) FROM teams t 
    LEFT JOIN competitions c ON t.competition_id = c.competition_id 
    WHERE c.competition_id IS NULL;" 2>/dev/null | xargs || echo "0")

if [ "$ORPHANED_TEAMS" -gt 0 ]; then
    echo -e "${YELLOW}⚠️  Found $ORPHANED_TEAMS teams without corresponding competitions${NC}"
    FRESHNESS_ISSUES=$((FRESHNESS_ISSUES + 1))
else
    echo -e "${GREEN}✅ No orphaned team records found${NC}"
fi

# Check table sizes consistency
DB_SIZE=$(docker exec "${CONTAINER_NAME}" psql -U "${DB_USER}" -d "${DB_NAME}" -t -c "SELECT pg_size_pretty(pg_database_size('${DB_NAME}'));" | xargs)
echo "💾 Current database size: $DB_SIZE"

# Summary table
echo ""
echo -e "${BLUE}=================================================================${NC}"
echo -e "${BLUE}                      SUMMARY TABLE${NC}"
echo -e "${BLUE}=================================================================${NC}"

printf "%-25s | %-15s | %-15s | %-10s\n" "Table" "Current Count" "Backup Count" "Status"
echo "----------------------------------------------------------------"

if [ ! -z "$BACKUP_COMPETITIONS" ] && [ "$BACKUP_COMPETITIONS" != "Unknown" ]; then
    if [ "$CURRENT_COMPETITIONS" -eq "$BACKUP_COMPETITIONS" ]; then
        STATUS="✅ Match"
    elif [ "$CURRENT_COMPETITIONS" -gt "$BACKUP_COMPETITIONS" ]; then
        STATUS="📈 Newer"
        FRESHNESS_ISSUES=$((FRESHNESS_ISSUES + 1))
    else
        STATUS="📉 Older"
        FRESHNESS_ISSUES=$((FRESHNESS_ISSUES + 1))
    fi
    printf "%-25s | %'15d | %'15s | %-10s\n" "Competitions" "$CURRENT_COMPETITIONS" "$BACKUP_COMPETITIONS" "$STATUS"
fi

if [ ! -z "$BACKUP_TEAMS" ] && [ "$BACKUP_TEAMS" != "Unknown" ]; then
    if [ "$CURRENT_TEAMS" -eq "$BACKUP_TEAMS" ]; then
        STATUS="✅ Match"
    elif [ "$CURRENT_TEAMS" -gt "$BACKUP_TEAMS" ]; then
        STATUS="📈 Newer"
        FRESHNESS_ISSUES=$((FRESHNESS_ISSUES + 1))
    else
        STATUS="📉 Older"
        FRESHNESS_ISSUES=$((FRESHNESS_ISSUES + 1))
    fi
    printf "%-25s | %'15d | %'15s | %-10s\n" "Teams" "$CURRENT_TEAMS" "$BACKUP_TEAMS" "$STATUS"
fi

echo ""
echo -e "${BLUE}=================================================================${NC}"
echo -e "${BLUE}                      FINAL VERDICT${NC}"
echo -e "${BLUE}=================================================================${NC}"

if [ "$FRESHNESS_ISSUES" -eq 0 ]; then
    echo -e "${GREEN}🎉 EXCELLENT: Backup appears fresh and up-to-date!${NC}"
    echo -e "${GREEN}✅ No freshness issues detected${NC}"
elif [ "$FRESHNESS_ISSUES" -eq 1 ]; then
    echo -e "${YELLOW}⚠️  GOOD: Minor freshness concern detected${NC}"
    echo -e "${YELLOW}💡 Consider creating a new backup soon${NC}"
else
    echo -e "${RED}🚨 WARNING: Multiple freshness issues detected (${FRESHNESS_ISSUES})${NC}"
    echo -e "${RED}🔄 Recommend creating a fresh backup immediately${NC}"
fi

echo ""
echo -e "${YELLOW}📋 RECOMMENDATIONS:${NC}"

if [ "$BACKUP_AGE_HOURS" -gt 24 ]; then
    echo "• Create a new backup (last backup is over 24 hours old)"
fi

if [ "$RECENT_ACTIVITY" -gt 0 ] && [ "$BACKUP_AGE_HOURS" -gt 2 ]; then
    echo "• New data detected since last backup - consider fresh backup"
fi

if [ "$FRESHNESS_ISSUES" -eq 0 ]; then
    echo "• Current backup is adequate for recovery purposes"
    echo "• Continue with regular backup schedule"
fi

echo ""
echo -e "${BLUE}=================================================================${NC}"

# Exit with appropriate code
if [ "$FRESHNESS_ISSUES" -gt 2 ]; then
    exit 2  # Critical issues
elif [ "$FRESHNESS_ISSUES" -gt 0 ]; then
    exit 1  # Minor issues
else
    exit 0  # All good
fi