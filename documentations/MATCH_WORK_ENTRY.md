# Match Worker Processor - Setup & Usage Guide

## Quick Start

### 1. Create Default Profiles
```bash
uv run --active match_worker_entry.py--create-profiles
```

### 2. Start Worker (Most Common)
```bash
uv run --active match_worker_entry.py--repo-url https://github.com/blecktita/savvyo-work-distribution.git
```

### 3. Development Mode
```bash
uv run --active match_worker_entry.py--config-profile development --repo-url https://github.com/blecktita/savvyo-work-distribution.git
```

## Worker Profiles

The GUI includes 5 pre-configured worker profiles:

| Profile | Jobs | Idle Time | Environment | Auto-Start | Best For |
|---------|------|-----------|-------------|------------|----------|
| `development` | 3 | 0.5h | development | No | Testing & debugging |
| `testing` | 5 | 1h | testing | No | QA validation |
| `standard` | 10 | 2h | production | Yes | Normal production (default) |
| `high_capacity` | 25 | 4h | production | Yes | High-volume processing |
| `24x7` | 100 | 8h | production | Yes | Continuous operation |

## Directory Structure

The worker GUI automatically creates and uses shared directories:

```
project_root/
├── configurations/          # Shared config files
│   ├── worker_development.json
│   ├── worker_testing.json
│   ├── worker_standard.json
│   ├── worker_high_capacity.json
│   └── worker_24x7.json
├── logs/match/worker/       # Shared log directory
│   ├── worker_hostname_pid_id_timestamp.log
│   └── worker_session_id_timestamp.json
└── temp_match_data/         # Temporary processing data
    └── [work_id]/
        ├── matchday_1.json
        ├── matchday_2.json
        └── ...
```

## GUI Interface Overview

### Header Bar
```
🤖 Match Worker | 💻 hostname | 🆔 abc123 | ⚡ PROCESSING | ⏱️ 2:45:30 | 🎯 PL-2024
```
Shows worker ID, current status, runtime, and current work.

### Status Panel
- **Work Orders**: Completed/Total with success rate
- **Matches Processed**: Total matches extracted
- **Current Idle**: How long since last work
- **Consecutive Fails**: Recent failure count

### Current Work Panel (When Processing)
```
🎯 Current Work Order
🆔 Work ID: abc123...
🏆 Competition: Premier League  
📅 Season: 2024
⏱️ Elapsed: 00:15:30

📊 Stage: Processing
📆 Current Matchday: 15
🎮 Matches Processed: 142

Progress: ████████████░░░░░░░░ 60.0%
Matchdays: 15/25
🕐 ETA: 14:45:00
```

### Activity Log
Real-time activity with work IDs:
```
14:32:15 ✅ [abc123] Processed matchday 15
14:32:10 ℹ️ [abc123] Extracting matches for matchday 15  
14:32:05 ✅ [abc123] Started processing PL-2024
14:32:00 ℹ️ Looking for available work...
```

## Usage Examples

### Production Worker
```bash
# Standard production worker (auto-starts)
uv run --active match_worker_entry.py\
  --config-profile standard \
  --repo-url https://github.com/blecktita/savvyo-work-distribution.git
```

### High-Capacity Worker
```bash
# High-volume processing
uv run --active match_worker_entry.py\
  --config-profile high_capacity \
  --repo-url https://github.com/blecktita/savvyo-work-distribution.git \
  --max-work 50
```

### Development Worker
```bash
# Development with manual control
uv run --active match_worker_entry.py\
  --config-profile development \
  --repo-url https://github.com/blecktita/savvyo-work-distribution.git \
  --debug
```

### 24/7 Worker
```bash
# Continuous operation
uv run --active match_worker_entry.py\
  --config-profile 24x7 \
  --repo-url https://github.com/blecktita/savvyo-work-distribution.git
```

### Custom Configuration
```bash
# Custom settings
uv run --active match_worker_entry.py\
  --repo-url https://github.com/blecktita/savvyo-work-distribution.git \
  --max-work 15 \
  --max-idle-hours 3.0 \
  --environment production \
  --debug
```

## Configuration Files

Worker profiles are stored in `configurations/worker_[profile].json`:

```json
{
  "https://github.com/blecktita/savvyo-work-distribution.git": "",
  "environment": "production",
  "worker_profile": "standard",
  "max_work_orders": 10,
  "max_consecutive_failures": 15,
  "max_idle_hours": 2.0,
  "work_claim_interval": 30.0,
  "refresh_rate": 2.0,
  "max_activity_history": 150,
  "show_debug": false,
  "auto_start": true,
  "log_dir": "logs/match/worker",
  "config_dir": "configurations",
  "temp_dir": "temp_match_data",
  "enable_sleep_prevention": true,
  "selenium_timeout": 30,
  "vpn_enabled": true,
  "alert_thresholds": {
    "consecutive_failures": 10,
    "processing_time_minutes": 30,
    "idle_time_hours": 1.5,
    "memory_usage_mb": 2000
  }
}
```

## Controls & Shortcuts

| Key | Action | Description |
|-----|--------|-------------|
| `Ctrl+C` | Graceful Stop | Finishes current work, then stops |
| `P` | Pause | Pauses after current work order |
| `R` | Resume | Resumes paused worker |
| `E` | Export | Exports session data and logs |
| `C` | Config | Shows current configuration |
| `S` | Status | Shows detailed progress |

## Monitoring & Logs

### Real-time Monitoring
- **GUI Activity Panel**: Live activity stream
- **Work Progress**: Current processing status
- **Performance Stats**: Throughput and timing metrics

### Log Files
- **Worker Logs**: `logs/match/worker/worker_[id]_[timestamp].log`
- **Session Export**: `logs/match/worker/worker_session_[id]_[timestamp].json`

### Session Export Contains
```json
{
  "session_info": {
    "worker_id": "worker_hostname_pid_id",
    "hostname": "worker-machine-01",
    "start_time": "2024-08-05T14:30:00",
    "config": { ... }
  },
  "statistics": {
    "total_work_orders": 8,
    "successful_completions": 7,
    "failed_attempts": 1,
    "total_matches_processed": 1247,
    "uptime_hours": 2.5
  },
  "work_history": [ ... ],
  "recent_activities": [ ... ]
}
```

## Best Practices

### Development
- Use `development` profile for testing
- Enable debug mode with `--debug`
- Keep `max_work_orders` low (3-5)
- Manual start with `--no-auto-start`

### Production
- Use `standard` profile for normal load
- Use `high_capacity` for high-volume periods
- Use `24x7` for continuous operation
- Monitor logs regularly
- Set up log rotation

### Troubleshooting
- Check worker logs in `logs/match/worker/`
- Use `--debug` flag for detailed output
- Export session data before stopping
- Monitor consecutive failure counts

## Integration with Host GUI

The worker GUI is designed to work alongside the host GUI:

1. **Shared Directories**: Both use same `configurations/` and `logs/` structure
2. **Coordination**: Host creates work, workers claim and process
3. **Monitoring**: Host monitors overall progress, workers monitor individual jobs
4. **Logs**: All logs stored in organized shared structure

## Command Reference

```bash
# Profile management
uv run --active match_worker_entry.py--create-profiles
uv run --active match_worker_entry.py--list-profiles

# Basic usage
uv run --active match_worker_entry.py--repo-url https://github.com/blecktita/savvyo-work-distribution.git
uv run --active match_worker_entry.py--config-profile PROFILE --repo-url https://github.com/blecktita/savvyo-work-distribution.git

# Overrides
uv run --active match_worker_entry.py--repo-url https://github.com/blecktita/savvyo-work-distribution.git \
  --environment production \
  --max-work 20 \
  --max-idle-hours 4.0 \
  --debug \
  --no-auto-start \
  --refresh-rate 1.0
```

## System Requirements

- **Python 3.8+**
- **Chrome/Chromium** (for Selenium)
- **Rich library** (for GUI)
- **Internet connection** (for work claiming and submission)
- **2GB+ RAM** (for processing large competitions)
- **Storage**: Temp space for processing data

The worker GUI provides a professional, user-friendly interface while maintaining all the underlying processing capabilities. It's designed for both development and production use with comprehensive monitoring and logging.