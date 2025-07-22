#!/bin/bash

# Enhanced Database Backup Script with Statistics and Verification
# This script creates backups with detailed statistics and integrity checks

set -e  # Exit on any error

# Configuration
CONTAINER_NAME="savvyo_database"
DB_NAME="production_savvyo_db"
DB_USER="aseathletics_datascience"
BACKUP_DIR="./backups"
DATE=$(date +%Y%m%d_%H%M%S)
BACKUP_FILE="${BACKUP_DIR}/savvyo_backup_${DATE}.sql"
COMPRESSED_FILE="${BACKUP_DIR}/savvyo_backup_${DATE}.sql.gz"
STATS_FILE="${BACKUP_DIR}/backup_stats_${DATE}.txt"

# Retention policy (keep backups for this many days)
RETENTION_DAYS=30

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}=================================================================${NC}"
echo -e "${BLUE}           SAVVYO DATABASE BACKUP WITH STATISTICS${NC}"
echo -e "${BLUE}=================================================================${NC}"
echo "📅 Date: $(date)"
echo "🐳 Container: ${CONTAINER_NAME}"
echo "🗄️  Database: ${DB_NAME}"
echo "👤 User: ${DB_USER}"
echo "📁 Backup file: ${BACKUP_FILE}"
echo ""

# Create backup directory if it doesn't exist
mkdir -p "${BACKUP_DIR}"

# Check if container is running
if ! docker ps | grep -q "${CONTAINER_NAME}"; then
    echo -e "${RED}❌ ERROR: Container ${CONTAINER_NAME} is not running!${NC}"
    exit 1
fi

echo -e "${YELLOW}🔍 GATHERING DATABASE STATISTICS...${NC}"

# Create comprehensive statistics file
{
    echo "================================================================="
    echo "           SAVVYO DATABASE BACKUP STATISTICS"
    echo "================================================================="
    echo "Backup Date: $(date)"
    echo "Database: ${DB_NAME}"
    echo "Container: ${CONTAINER_NAME}"
    echo ""
    
    echo "================================================================="
    echo "                    DATABASE INFORMATION"
    echo "================================================================="
    
    # Database size
    echo "📊 DATABASE SIZE:"
    docker exec "${CONTAINER_NAME}" psql -U "${DB_USER}" -d "${DB_NAME}" -c "
        SELECT 
            pg_database.datname as database_name,
            pg_size_pretty(pg_database_size(pg_database.datname)) as size
        FROM pg_database 
        WHERE datname = '${DB_NAME}';"
    
    echo ""
    echo "================================================================="
    echo "                      TABLE STATISTICS"
    echo "================================================================="
    
    # Table information with row counts and sizes
    echo "📋 TABLES OVERVIEW:"
    docker exec "${CONTAINER_NAME}" psql -U "${DB_USER}" -d "${DB_NAME}" -c "
        SELECT 
            schemaname as schema,
            relname as table_name,
            to_char(n_tup_ins, 'FM999,999,999') as total_inserts,
            to_char(n_tup_upd, 'FM999,999,999') as total_updates,
            to_char(n_tup_del, 'FM999,999,999') as total_deletes
        FROM pg_stat_user_tables 
        ORDER BY relname;"
    
    echo ""
    echo "📏 TABLE SIZES AND ROW COUNTS:"
    docker exec "${CONTAINER_NAME}" psql -U "${DB_USER}" -d "${DB_NAME}" -c "
        SELECT 
            schemaname as schema,
            relname as table_name,
            to_char(n_tup_ins - n_tup_del, 'FM999,999,999') as estimated_rows,
            pg_size_pretty(pg_total_relation_size(schemaname||'.'||relname)) as total_size,
            pg_size_pretty(pg_relation_size(schemaname||'.'||relname)) as table_size,
            pg_size_pretty(pg_total_relation_size(schemaname||'.'||relname) - pg_relation_size(schemaname||'.'||relname)) as indexes_size
        FROM pg_stat_user_tables 
        ORDER BY pg_total_relation_size(schemaname||'.'||relname) DESC;"
    
    echo ""
    echo "================================================================="
    echo "                    DETAILED ROW COUNTS"
    echo "================================================================="
    
    # Actual row counts for each table
    echo "🔢 EXACT ROW COUNTS:"
    for table in $(docker exec "${CONTAINER_NAME}" psql -U "${DB_USER}" -d "${DB_NAME}" -t -c "SELECT relname FROM pg_stat_user_tables WHERE schemaname = 'public';"); do
        table=$(echo $table | xargs) # trim whitespace
        if [ ! -z "$table" ]; then
            count=$(docker exec "${CONTAINER_NAME}" psql -U "${DB_USER}" -d "${DB_NAME}" -t -c "SELECT count(*) FROM ${table};" | xargs)
            printf "%-25s: %'15d rows\n" "$table" "$count"
        fi
    done
    
    echo ""
    echo "================================================================="
    echo "                     SAMPLE DATA PREVIEW"
    echo "================================================================="
    
    # Sample data from each table
    for table in $(docker exec "${CONTAINER_NAME}" psql -U "${DB_USER}" -d "${DB_NAME}" -t -c "SELECT relname FROM pg_stat_user_tables WHERE schemaname = 'public' ORDER BY relname;"); do
        table=$(echo $table | xargs) # trim whitespace
        if [ ! -z "$table" ]; then
            echo ""
            echo "📄 SAMPLE DATA FROM: ${table}"
            echo "----------------------------------------"
            docker exec "${CONTAINER_NAME}" psql -U "${DB_USER}" -d "${DB_NAME}" -c "SELECT * FROM ${table} LIMIT 3;" 2>/dev/null || echo "No data available"
        fi
    done
    
    echo ""
    echo "================================================================="
    echo "                      INDEX INFORMATION"
    echo "================================================================="
    
    echo "🗂️ DATABASE INDEXES:"
    docker exec "${CONTAINER_NAME}" psql -U "${DB_USER}" -d "${DB_NAME}" -c "
        SELECT 
            schemaname,
            tablename,
            indexname,
            indexdef
        FROM pg_indexes 
        WHERE schemaname = 'public'
        ORDER BY tablename, indexname;"
    
    echo ""
    echo "================================================================="
    echo "                     RECENT ACTIVITY"
    echo "================================================================="
    
    echo "📈 RECENT DATABASE ACTIVITY:"
    docker exec "${CONTAINER_NAME}" psql -U "${DB_USER}" -d "${DB_NAME}" -c "
        SELECT 
            schemaname,
            relname as tablename,
            n_tup_ins as inserts_since_last_analyze,
            n_tup_upd as updates_since_last_analyze,
            n_tup_del as deletes_since_last_analyze,
            last_vacuum,
            last_autovacuum,
            last_analyze,
            last_autoanalyze
        FROM pg_stat_user_tables 
        ORDER BY relname;"
    
    echo ""
    echo "================================================================="
    echo "                   BACKUP COMPLETION TIME"
    echo "================================================================="
    echo "Statistics gathered at: $(date)"
    
} > "${STATS_FILE}"

echo -e "${GREEN}✅ Statistics saved to: ${STATS_FILE}${NC}"

echo ""
echo -e "${YELLOW}💾 CREATING DATABASE BACKUP...${NC}"

# Create database backup
docker exec "${CONTAINER_NAME}" pg_dump \
    -U "${DB_USER}" \
    -d "${DB_NAME}" \
    --verbose \
    --clean \
    --if-exists \
    --create \
    --format=plain > "${BACKUP_FILE}"

# Check if backup was created successfully
if [ ! -f "${BACKUP_FILE}" ] || [ ! -s "${BACKUP_FILE}" ]; then
    echo -e "${RED}❌ ERROR: Backup file was not created or is empty!${NC}"
    exit 1
fi

echo -e "${YELLOW}🗜️ COMPRESSING BACKUP...${NC}"

# Compress the backup
gzip "${BACKUP_FILE}"

# Verify compressed file
if [ ! -f "${COMPRESSED_FILE}" ]; then
    echo -e "${RED}❌ ERROR: Compressed backup file was not created!${NC}"
    exit 1
fi

BACKUP_SIZE=$(du -h "${COMPRESSED_FILE}" | cut -f1)
STATS_SIZE=$(du -h "${STATS_FILE}" | cut -f1)

echo ""
echo -e "${YELLOW}🧪 PERFORMING BACKUP VERIFICATION...${NC}"

# Test backup integrity
if gunzip -t "${COMPRESSED_FILE}"; then
    echo -e "${GREEN}✅ Backup file integrity check passed${NC}"
else
    echo -e "${RED}❌ WARNING: Backup file integrity check failed!${NC}"
    exit 1
fi

# Extract a few lines to verify content
echo "🔍 Backup content preview:"
gunzip -c "${COMPRESSED_FILE}" | head -20 | grep -E "(CREATE|INSERT|COPY)" | head -3

echo ""
echo -e "${YELLOW}🧹 CLEANING UP OLD BACKUPS...${NC}"

# Clean up old backups (keep only last RETENTION_DAYS days)
old_backups=$(find "${BACKUP_DIR}" -name "savvyo_backup_*.sql.gz" -type f -mtime +${RETENTION_DAYS} | wc -l)
find "${BACKUP_DIR}" -name "savvyo_backup_*.sql.gz" -type f -mtime +${RETENTION_DAYS} -delete
find "${BACKUP_DIR}" -name "backup_stats_*.txt" -type f -mtime +${RETENTION_DAYS} -delete

if [ "$old_backups" -gt 0 ]; then
    echo "🗑️ Removed $old_backups old backup(s)"
fi

# Get current statistics for comparison
current_competitions=$(docker exec "${CONTAINER_NAME}" psql -U "${DB_USER}" -d "${DB_NAME}" -t -c "SELECT count(*) FROM competitions;" 2>/dev/null | xargs || echo "0")
current_teams=$(docker exec "${CONTAINER_NAME}" psql -U "${DB_USER}" -d "${DB_NAME}" -t -c "SELECT count(*) FROM teams;" 2>/dev/null | xargs || echo "0")

echo ""
echo -e "${BLUE}=================================================================${NC}"
echo -e "${BLUE}                    BACKUP SUMMARY${NC}"
echo -e "${BLUE}=================================================================${NC}"
echo -e "${GREEN}✅ Backup completed successfully!${NC}"
echo ""
echo "📊 BACKUP DETAILS:"
echo "   📁 Backup file: ${COMPRESSED_FILE}"
echo "   📈 Statistics file: ${STATS_FILE}"
echo "   💾 Backup size: ${BACKUP_SIZE}"
echo "   📋 Stats size: ${STATS_SIZE}"
echo "   🕐 Completed at: $(date)"
echo ""
echo "📈 CURRENT DATA SUMMARY:"
echo "   🏆 Competitions: $(printf "%'d" $current_competitions)"
echo "   ⚽ Teams: $(printf "%'d" $current_teams)"
echo "   📊 Total backups: $(ls -1 "${BACKUP_DIR}"/savvyo_backup_*.sql.gz 2>/dev/null | wc -l)"
echo ""
echo -e "${YELLOW}📄 To view detailed statistics, run:${NC}"
echo "   cat ${STATS_FILE}"
echo ""
echo -e "${YELLOW}📋 Recent backups:${NC}"
ls -lh "${BACKUP_DIR}"/savvyo_backup_*.sql.gz | tail -5

echo ""
echo -e "${GREEN}🎉 Backup process completed successfully!${NC}"
echo -e "${BLUE}=================================================================${NC}"

# Optional: Show last few lines of stats for immediate verification
echo ""
echo -e "${YELLOW}📊 QUICK STATS PREVIEW:${NC}"
echo "----------------------------------------"
tail -20 "${STATS_FILE}" | head -15