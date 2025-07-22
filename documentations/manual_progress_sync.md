# Direct Progress Sync Script - Developer Manual

## Overview

The Direct Progress Sync Script is a database synchronization utility that ensures consistency between the `teams` table and progress tracking tables (`competition_progress` and `season_progress`). It follows a simple principle: **if teams data exists, mark the corresponding competition/season as completed in the progress tables**.

## Purpose

This script addresses data inconsistencies that can occur when:
- Teams data is imported directly without updating progress tables
- Progress tracking records become out of sync with actual data
- Manual data fixes are needed to align progress status with reality

## Architecture

### Core Components

1. **DirectProgressSync Class**: Main orchestrator
2. **Database Connection**: Uses SQLAlchemy with transaction management
3. **Progress Monitor**: Leverages existing `create_work_tracker()` infrastructure

### Database Schema Dependencies

The script operates on these tables:
- `teams`: Contains actual team/club data with `competition_id` and `season_year`
- `competitions`: Stores competition metadata including URLs
- `competition_progress`: Tracks completion status per competition
- `season_progress`: Tracks completion status per season within competitions

## Installation & Setup

### Prerequisites

```bash
# Install dependencies using uv
uv add sqlalchemy argparse
```

### Environment Configuration

The script uses the existing `coordination.coordinator` module to establish database connections. Ensure your environment is configured with proper database credentials for the target environment.

## Usage

### Command Line Interface

```bash
# Basic usage - dry run (safe)
uv run --active direct_progress_sync.py

# Execute actual changes
uv run --active direct_progress_sync.py --execute

# Target specific environment
uv run --active direct_progress_sync.py --environment staging --execute

# Analyze current state only
uv run --active direct_progress_sync.py --analyze

# Sync specific competition
uv run --active direct_progress_sync.py --competition "EPL" --execute
```

### Command Line Arguments

| Argument | Description | Default | Required |
|----------|-------------|---------|----------|
| `--environment` | Database environment (production, staging, etc.) | `production` | No |
| `--execute` | Actually perform changes (omit for dry run) | `False` | No |
| `--competition` | Sync only specified competition ID | `None` | No |
| `--analyze` | Only analyze current state, no sync | `False` | No |

## Core Operations

### 1. Analysis Mode (`--analyze`)

**Purpose**: Provides insight into current data state without making changes.

**Output**:
- Top 20 competitions by team count from `teams` table
- Latest 10 records from `competition_progress` table
- Top 10 competitions from `season_progress` table with completion ratios

**Use Case**: Run before sync operations to understand data landscape.

### 2. Full Sync Mode (default)

**Purpose**: Synchronizes all competitions and seasons based on teams data.

**Process**:
1. Identifies competitions with teams data
2. Creates/updates `competition_progress` records
3. Creates/updates `season_progress` records for each season
4. Marks all as 'completed' with appropriate timestamps

### 3. Specific Competition Sync (`--competition`)

**Purpose**: Targets a single competition for focused sync operations.

**Benefits**:
- Faster execution for targeted fixes
- Safer for production environments
- Useful for testing sync logic

## Data Synchronization Logic

### Competition Progress Sync

```sql
-- If no progress record exists
INSERT INTO competition_progress 
(competition_id, competition_url, status, seasons_discovered, completed_at)
VALUES (?, ?, 'completed', ?, ?)

-- If record exists but status != 'completed'
UPDATE competition_progress 
SET status = 'completed', completed_at = ?, seasons_discovered = ?
WHERE competition_id = ?
```

### Season Progress Sync

```sql
-- If no progress record exists
INSERT INTO season_progress 
(competition_id, season_id, season_year, status, completed_at, clubs_saved)
VALUES (?, ?, ?, 'completed', ?, ?)

-- If record exists but needs updating
UPDATE season_progress 
SET status = 'completed', completed_at = ?, clubs_saved = ?
WHERE competition_id = ? AND season_year = ?
```

## Safety Features

### 1. Dry Run Mode (Default)

- All operations run in read-only mode by default
- Shows exactly what would be changed
- Provides statistics summary
- Requires explicit `--execute` flag for actual changes

### 2. Transaction Management

- All database operations wrapped in transactions
- Automatic rollback on errors
- Explicit commit only on successful completion

### 3. Error Handling

- Graceful handling of keyboard interrupts
- Full stack traces for debugging
- Proper resource cleanup

## Output & Logging

### Standard Output Format

```
🔄 Direct sync initialized for production
📊 Processing EPL: 500 teams, 5 seasons
   ➕ Would ADD competition_progress record
   🔄 Would UPDATE season_progress: 2023 (100 teams)
📊 SUMMARY:
   Competitions to add: 1
   Seasons to update: 5
```

### Status Icons

- 🔄 Process status
- 📊 Data analysis
- ➕ Record creation
- 🔄 Record updates
- ✅ Successful operations
- ❌ Errors
- 💡 Helpful tips

## Best Practices

### Development Environment

1. **Always test in staging first**
   ```bash
   uv run --active direct_progress_sync.py --environment staging --execute
   ```

2. **Use dry run for exploration**
   ```bash
   uv run --active direct_progress_sync.py --analyze
   ```

3. **Target specific competitions for focused testing**
   ```bash
   uv run --active direct_progress_sync.py --competition "TEST_COMP" --execute
   ```

### Production Environment

1. **Backup before major syncs**
2. **Use analyze mode first to understand scope**
3. **Consider maintenance windows for large datasets**
4. **Monitor performance on large competition sets**

### Code Maintenance

1. **Update SQL queries if schema changes**
2. **Test with various data scenarios**
3. **Validate URL handling for new competitions**
4. **Monitor memory usage with large datasets**

## Common Use Cases

### 1. After Bulk Data Import
```bash
# Analyze what needs sync
uv run --active direct_progress_sync.py --analyze

# Perform full sync
uv run --active direct_progress_sync.py --execute
```

### 2. Fix Specific Competition
```bash
# Target problematic competition
uv run --active direct_progress_sync.py --competition "SERIE_A" --execute
```

### 3. Environment Migration
```bash
# Sync staging after data copy
uv run --active direct_progress_sync.py --environment staging --execute
```

## Troubleshooting

### Common Issues

1. **Missing Competition URLs**
   - Script handles gracefully with `MISSING_URL_FOR_{comp_id}`
   - Update competitions table separately if needed

2. **Large Dataset Performance**
   - Consider pagination for very large competitions
   - Monitor transaction timeouts

3. **Concurrent Access**
   - Avoid running multiple instances simultaneously
   - Check for locks in production environments

### Error Messages

- `No teams data found for {competition_id}`: Competition has no teams data to sync
- Database connection errors: Check environment configuration
- Transaction errors: May indicate schema changes or constraints

## Integration Points

### Dependencies

- `coordination.coordinator.create_work_tracker()`: Database connection factory
- SQLAlchemy: ORM and transaction management
- Argparse: Command line interface

### Extension Points

- Add new progress table types by extending sync methods
- Implement custom validation rules in sync logic
- Add notification systems for large sync operations

## Security Considerations

- Script requires database write access
- Use appropriate database user permissions
- Consider read-only replicas for analysis mode
- Validate input for competition IDs to prevent injection

## Performance Notes

- Memory usage scales with number of competitions
- Transaction size affects lock duration
- Consider batching for very large datasets
- Index on competition_id and season_year recommended