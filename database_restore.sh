#!/bin/bash

# Database Restore Script for Dockerized PostgreSQL
# This script restores a database from backup

set -e  # Exit on any error

# Configuration
CONTAINER_NAME="savvyo_database"
DB_NAME="production_savvyo_db"
DB_USER="aseathletics_datascience"
BACKUP_DIR="./backups"

# Function to show usage
show_usage() {
    echo "Usage: $0 <backup_file>"
    echo ""
    echo "Example:"
    echo "  $0 ./backups/savvyo_backup_20250715_123000.sql.gz"
    echo ""
    echo "Available backups:"
    ls -lh "${BACKUP_DIR}"/savvyo_backup_*.sql.gz 2>/dev/null | tail -10 || echo "No backups found"
    exit 1
}

# Check if backup file is provided
if [ $# -eq 0 ]; then
    echo "ERROR: Please provide a backup file to restore"
    show_usage
fi

BACKUP_FILE="$1"

# Verify backup file exists
if [ ! -f "${BACKUP_FILE}" ]; then
    echo "ERROR: Backup file '${BACKUP_FILE}' does not exist!"
    show_usage
fi

echo "=== Database Restore ==="
echo "Date: $(date)"
echo "Container: ${CONTAINER_NAME}"
echo "Database: ${DB_NAME}"
echo "Backup file: ${BACKUP_FILE}"

# Check if container is running
if ! docker ps | grep -q "${CONTAINER_NAME}"; then
    echo "ERROR: Container ${CONTAINER_NAME} is not running!"
    echo "Start it with: docker-compose up -d savvyo_db_manager"
    exit 1
fi

# Warning prompt
echo ""
echo "⚠️  WARNING: This will REPLACE all data in the database!"
echo "⚠️  Make sure you have a current backup before proceeding."
echo ""
read -p "Are you sure you want to restore from ${BACKUP_FILE}? (type 'yes' to continue): " confirm

if [ "$confirm" != "yes" ]; then
    echo "Restore cancelled."
    exit 0
fi

# Create a safety backup before restore
SAFETY_BACKUP="${BACKUP_DIR}/pre_restore_backup_$(date +%Y%m%d_%H%M%S).sql.gz"
echo "Creating safety backup first..."
docker exec "${CONTAINER_NAME}" pg_dump \
    -U "${DB_USER}" \
    -d "${DB_NAME}" \
    --clean \
    --create \
    --format=plain | gzip > "${SAFETY_BACKUP}"

echo "✅ Safety backup created: ${SAFETY_BACKUP}"

# Decompress and restore
echo "Restoring database from backup..."

if [[ "${BACKUP_FILE}" == *.gz ]]; then
    # Compressed backup
    gunzip -c "${BACKUP_FILE}" | docker exec -i "${CONTAINER_NAME}" psql -U "${DB_USER}" -d postgres
else
    # Uncompressed backup
    cat "${BACKUP_FILE}" | docker exec -i "${CONTAINER_NAME}" psql -U "${DB_USER}" -d postgres
fi

# Verify restore
echo "Verifying restore..."
TABLE_COUNT=$(docker exec "${CONTAINER_NAME}" psql -U "${DB_USER}" -d "${DB_NAME}" -t -c "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public';")
COMP_COUNT=$(docker exec "${CONTAINER_NAME}" psql -U "${DB_USER}" -d "${DB_NAME}" -t -c "SELECT count(*) FROM competitions;" 2>/dev/null || echo "0")
TEAM_COUNT=$(docker exec "${CONTAINER_NAME}" psql -U "${DB_USER}" -d "${DB_NAME}" -t -c "SELECT count(*) FROM teams;" 2>/dev/null || echo "0")

echo ""
echo "=== Restore Complete ==="
echo "✅ Database restored successfully!"
echo "📊 Tables found: ${TABLE_COUNT// /}"
echo "🏆 Competitions: ${COMP_COUNT// /}"
echo "⚽ Teams: ${TEAM_COUNT// /}"
echo "🛡️  Safety backup: ${SAFETY_BACKUP}"
echo ""
echo "🎉 Restore completed at: $(date)"