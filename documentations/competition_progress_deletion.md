# Competition Progress Deletion Script - Developer Manual

## Overview

The Competition Progress Deletion Script is a database cleanup utility designed to permanently remove specific competitions from progress tracking tables. This script provides a safe, transactional way to clean up unwanted or obsolete competition records while maintaining data integrity.

## Purpose

This script addresses scenarios where:
- Test competitions need to be removed from production data
- Obsolete or duplicate competitions clutter progress tables
- Data cleanup is required after migration or import errors
- Specific competitions need to be reset for re-processing

## Architecture

### Core Components

1. **delete_competitions_from_progress()**: Main deletion function
2. **Interactive Confirmation**: Safety mechanism requiring explicit user confirmation
3. **Transaction Management**: Ensures atomic operations across both progress tables
4. **Detailed Logging**: Provides comprehensive feedback on deletion operations

### Database Schema Dependencies

The script operates on these tables:
- `season_progress`: Tracks completion status per season within competitions
- `competition_progress`: Tracks completion status per competition

**Important**: This script does NOT affect the core `teams` or `competitions` tables.

## Installation & Setup

### Prerequisites

```bash
# Install dependencies using uv
uv add sqlalchemy
```

### Environment Configuration

The script uses the existing `coordination.coordinator` module. Ensure your environment has proper database credentials configured.

## Usage

### Command Line Execution

```bash
# Run the deletion script
uv run --active delete_competitions_progress.py
```

### Interactive Process

1. **Warning Display**: Shows list of competitions to be deleted
2. **Confirmation Prompt**: Requires typing 'DELETE' exactly
3. **Execution**: Performs deletion with detailed logging
4. **Completion**: Reports final status

### Example Session

```
⚠️ WARNING: This will permanently delete competition progress data!
Competitions to delete:
   • 212S
   • BRCB
   • BRRN
   • G17L
   • STEP_COMP
   • MAZR
   • RO1R
   • BEYP
   • PONL

Type 'DELETE' to proceed: DELETE

🗑️ Deleting 9 competitions from progress tables...

📅 Deleting from season_progress table...
   ✅ Deleted 3 season records for 212S
   ➖ No season records found for BRCB
   ✅ Deleted 5 season records for BRRN

🏆 Deleting from competition_progress table...
   ✅ Deleted competition record for 212S
   ✅ Deleted competition record for BRCB

✅ Successfully deleted all specified competitions from progress tables

🎉 Deletion completed!
```

## Script Configuration

### Competitions List

The script contains a hardcoded list of competitions to delete:

```python
competitions_to_delete = [
    "212S",
    "BRCB", 
    "BRRN",
    "G17L",
    "STEP_COMP",
    "MAZR",
    "RO1R",
    "BEYP",
    "PONL"
]
```

**Note**: There's a discrepancy in the confirmation display list that includes "HO1A" but it's not in the deletion list. This should be corrected for consistency.

### Environment Setting

```python
progress_monitor = create_work_tracker("production")  # Change to your environment
```

**Important**: Update the environment parameter before running:
- `"production"` for production database
- `"staging"` for staging database
- `"development"` for development database

## Deletion Process

### Step-by-Step Operation

1. **Initialize Connection**: Establishes database connection via work tracker
2. **Begin Transaction**: Starts atomic transaction for both table operations
3. **Season Progress Cleanup**: 
   - Iterates through each competition
   - Deletes all season records for the competition
   - Reports number of records deleted
4. **Competition Progress Cleanup**:
   - Iterates through each competition  
   - Deletes the competition record
   - Reports success or absence of record
5. **Commit Transaction**: Commits all changes atomically
6. **Cleanup**: Properly closes database connections

### SQL Operations

```sql
-- Delete season progress records
DELETE FROM season_progress 
WHERE competition_id = ?

-- Delete competition progress records  
DELETE FROM competition_progress 
WHERE competition_id = ?
```

## Safety Features

### 1. Interactive Confirmation

- Requires explicit typing of 'DELETE'
- Shows complete list of affected competitions
- Provides clear warning about permanent deletion
- Cancels operation for any other input

### 2. Transaction Management

- All operations wrapped in a single transaction
- Automatic rollback on any error
- Atomic success or complete failure

### 3. Detailed Logging

- Progress indication with emoji icons
- Record count reporting for transparency
- Clear success/failure messages
- Error details for troubleshooting

### 4. Resource Cleanup

- Proper database connection cleanup in finally block
- Graceful error handling and reporting

## Output & Logging

### Status Icons

- 🗑️ Deletion process start
- 📅 Season progress operations
- 🏆 Competition progress operations  
- ✅ Successful operations
- ➖ No records found (not an error)
- ❌ Errors or cancellation
- 🎉 Completion celebration
- ⚠️ Warnings

### Logging Levels

1. **Info**: Normal operation progress
2. **Success**: Confirmed deletions with counts
3. **Notice**: No records found (expected scenario)
4. **Error**: Exceptions and failures

## Customization

### Modifying Competition List

To delete different competitions:

```python
competitions_to_delete = [
    "YOUR_COMP_1",
    "YOUR_COMP_2",
    # Add more competition IDs
]
```

**Remember**: Also update the confirmation display list for consistency.

### Environment Targeting

Update the environment parameter:

```python
progress_monitor = create_work_tracker("staging")  # For staging environment
```

### Adding Dry Run Mode

Consider adding a dry-run option for safer testing:

```python
def delete_competitions_from_progress(dry_run=False):
    if dry_run:
        print("🔍 DRY RUN MODE - No actual deletions will occur")
    # ... existing logic with conditional execution
```

## Best Practices

### Pre-Deletion Checks

1. **Verify Environment**: Ensure you're targeting the correct database
2. **Check Data**: Query tables to confirm what will be deleted
3. **Backup Strategy**: Consider backing up affected data first
4. **Test in Staging**: Run in staging environment first

### Execution Guidelines

1. **Run During Maintenance Windows**: Minimize impact on active systems
2. **Monitor Performance**: Watch for locking issues on busy systems
3. **Verify Results**: Check that deletions completed as expected
4. **Update Documentation**: Record what was deleted and why

### Code Maintenance

1. **Keep Lists Synchronized**: Ensure deletion and confirmation lists match
2. **Environment Parameterization**: Consider making environment configurable via command line
3. **Add Logging**: Consider using proper logging framework for production
4. **Error Handling**: Enhance error reporting for specific failure scenarios

## Common Use Cases

### 1. Test Data Cleanup
```bash
# Clean up test competitions after development
uv run --active delete_competitions_progress.py
```

### 2. Migration Cleanup
```bash
# Remove duplicate or corrupted data after migration
# First update the competition list in the script
uv run --active delete_competitions_progress.py
```

### 3. Selective Reset
```bash
# Reset specific competitions for re-processing
# Useful when sync logic changes
uv run --active delete_competitions_progress.py
```

## Troubleshooting

### Common Issues

1. **Environment Mismatch**
   - Symptom: Deleting from wrong database
   - Solution: Verify environment parameter in script

2. **Permission Errors**
   - Symptom: Database access denied
   - Solution: Check database user permissions for DELETE operations

3. **Transaction Timeouts**
   - Symptom: Operation hangs or times out
   - Solution: Check for table locks, run during low-activity periods

4. **Inconsistent Results**
   - Symptom: Some competitions deleted, others not
   - Solution: Check for foreign key constraints or case sensitivity issues

### Error Messages

- `Error during deletion`: Database operation failed, check connection and permissions
- `Operation cancelled`: User didn't type 'DELETE' exactly
- Transaction errors: May indicate locks or constraint violations

## Security Considerations

### Data Safety

- **Permanent Operation**: Deletions cannot be easily reversed
- **No Cascading**: Script only affects progress tables, not core data
- **Atomic Operations**: Either all deletions succeed or none do

### Access Control

- Requires database DELETE privileges
- Should be restricted to authorized personnel
- Consider audit logging for deletion operations

### Production Safety

- Interactive confirmation prevents accidental execution
- Transaction rollback on any failure
- Clear logging for audit trails

## Integration Points

### Dependencies

- `coordination.coordinator.create_work_tracker()`: Database connection factory
- SQLAlchemy: ORM and transaction management

### Related Scripts

- Works alongside `direct_progress_sync.py` for progress table management
- Can be used before sync operations to clean slate specific competitions
- Complements data import and migration scripts

## Future Enhancements

### Suggested Improvements

1. **Command Line Arguments**: Make environment and competition list configurable
2. **Dry Run Mode**: Preview deletions without executing them
3. **Backup Integration**: Automatically backup data before deletion
4. **Batch Processing**: Handle very large competition lists efficiently
5. **Logging Framework**: Replace print statements with proper logging
6. **Configuration File**: External configuration for competition lists

### Example Enhanced Usage

```bash
# Future potential usage
uv run --active delete_competitions_progress.py --environment staging --dry-run
uv run --active delete_competitions_progress.py --config competitions_to_delete.json
uv run --active delete_competitions_progress.py --backup --confirm
```

## Data Recovery

### If Accidental Deletion Occurs

1. **Stop Further Operations**: Prevent additional data loss
2. **Check Backups**: Restore from most recent backup if available
3. **Re-sync from Source**: Use sync scripts to rebuild progress data from teams table
4. **Audit Trail**: Review logs to understand scope of deletion

### Prevention Strategies

1. **Regular Backups**: Implement automated backup schedules
2. **Staging Testing**: Always test deletion scripts in staging first
3. **Version Control**: Track all changes to deletion lists
4. **Documentation**: Record all deletion operations with justification