# Match Work GUI - User Manual

## Table of Contents
1. [Quick Start](#quick-start)
2. [Installation & Setup](#installation--setup)
3. [Basic Usage](#basic-usage)
4. [Understanding the Interface](#understanding-the-interface)
5. [Configuration](#configuration)
6. [Common Operations](#common-operations)
7. [Troubleshooting](#troubleshooting)
8. [Advanced Features](#advanced-features)

## Quick Start

### Minimum Required Command
```bash
uv run match_host_entry.py --repo-url https://github.com/blecktita/savvyo-work-distribution.git
```

### Most Common Usage
```bash
uv run match_host_entry.py \
  --repo-url https://github.com/blecktita/savvyo-work-distribution.git \
  --environment production \
  --max-cycles 50
```

That's it! The GUI will start and show you everything happening in real-time.

---

## Installation & Setup

### Prerequisites
- Python 3.8 or higher
- `uv` package manager installed
- Access to your GitHub repository
- Your existing `match_host_work_manager.py` file

### First-Time Setup

1. **Create a configuration file** (recommended):
   ```bash
   uv run --active match_host_entry.py --create-config
   ```
   This creates `match_work_gui_config.json` with sensible defaults.

2. **Test the connection**:
   ```bash
   uv run --active match_host_entry.py --repo-url https://github.com/blecktita/savvyo-work-distribution.git --max-cycles 1
   ```

3. **You're ready to go!**

---

## Basic Usage

### Starting the GUI

#### Simple Start (Production Environment)
```bash
uv run --active match_host_entry.py --repo-url https://github.com/blecktita/savvyo-work-distribution.git
```

#### Development Environment
```bash
uv run --active match_host_entry.py \
  --repo-url https://github.com/blecktita/savvyo-work-distribution.git \
  --environment development \
  --debug
```

#### Limited Run (Good for Testing)
```bash
uv run --active match_host_entry.py \
  --repo-url https://github.com/blecktita/savvyo-work-distribution.git \
  --max-cycles 10
```

### Stopping the GUI

- **Normal stop**: Press `Ctrl+C` once
- **Force quit**: Press `Ctrl+C` twice (if first doesn't work)

The GUI will always try to shut down gracefully and save your metrics.

---

## Understanding the Interface

The GUI is divided into 6 main sections:

### 1. Header Bar (Top)
```
🏠 Match Host Work Manager | 🟢 RUNNING | ⏱️ 00:05:23 | 🔄 Cycle 15 | ⚡ Peak: 12.5/s
```

**What it shows:**
- **Status**: 🟢 RUNNING, ⏸️ PAUSED, 🔴 STOPPED, ❌ ERROR
- **Runtime**: How long the GUI has been running
- **Current cycle**: Which processing cycle you're on
- **Peak performance**: Highest throughput achieved

### 2. Work Status Panel (Left, Top)
```
📊 Work Status
┌─────────┬─────────┬────────┬─────────────────────────────────┐
│ Status  │   Count │  Trend │ Description                     │
├─────────┼─────────┼────────┼──────────────────────────────── ┤
│ Available│      45 │   📈   │ Ready to be claimed            │
│ Claimed  │       8 │   ➡️   │ Currently being processed.     │
│ Completed│      12 │   📈   │ Finished, awaiting processing. │
│ Failed   │       2 │   ⚠️   │ Encountered errors             │
│ Processed│     150 │   📈   │ Successfully archived          │
└─────────┴─────────┴────────┴─────────────────────────────────┘
```

**Key numbers to watch:**
- **Available**: Should stay above 0 (GUI creates more when low)
- **Failed**: Should stay low (investigate if it grows)
- **Processed**: Shows your progress

### 3. Performance Metrics (Left, Middle)
```
📈 Performance Metrics
🔄 Current Cycle: 15
⏱️  Duration: 2.3s
📊 Throughput: 5.2/s

📋 Total Created: 234
✅ Total Processed: 189
❌ Total Errors: 3

⚡ Avg Cycle Time: 2.1s
📊 Avg Throughput: 4.8/s
🏆 Peak Throughput: 12.5/s
```

**What this tells you:**
- **Throughput**: Items processed per second (higher is better)
- **Cycle Time**: How long each cycle takes (lower is better)
- **Error count**: Should remain low

### 4. Activity Log (Left, Bottom)
```
📝 Activity Log (45 total)
14:32:15 ✅ Processed 8 completed items
14:32:10 ✅ Created 10 work orders
14:32:05 ℹ️  Starting cycle 15/100
14:32:00 ✅ Processed 5 completed items
```

**Log symbols:**
- ✅ Success (green)
- ℹ️ Information (white)
- ⚠️ Warning (yellow)
- ❌ Error (red)
- 🚨 Critical (bright red)

### 5. System Stats (Right, Top)
```
📊 System Stats
🌍 Environment: production
📡 State: running

⏱️  Uptime: 0:05:23
🔄 Total Cycles: 15

📊 Error Rate: 1.2%
💾 Cycle History: 15/50
💾 Activity History: 45/100
```

### 6. Alerts Panel (Right, Bottom)
```
🚨 Alerts
✅ All systems normal
```

**Or when there are issues:**
```
🚨 Alerts
⚠️  5 failed jobs need attention
🕐 15 jobs claimed (check for stale)
```

---

## Configuration

### Quick Configuration
The easiest way is to create a config file:

```bash
uv run --active match_host_entry.py --create-config
```

This creates `match_work_gui_config.json`:

```json
{
  "refresh_rate": 2.0,
  "max_activity_history": 200,
  "max_cycle_history": 100,
  "show_debug": false,
  "log_file": "match_work_gui.log",
  "export_metrics": true,
  "alert_thresholds": {
    "error_rate": 5,
    "failed_jobs": 10,
    "stale_claimed": 30
  }
}
```

### Key Settings Explained

| Setting | What it does | Good values |
|---------|--------------|-------------|
| `refresh_rate` | How often GUI updates (per second) | 1.0-4.0 |
| `show_debug` | Show debug messages | `false` for production |
| `log_file` | Where to save detailed logs | `"match_work_gui.log"` |
| `export_metrics` | Save metrics when shutting down | `true` |

### Using Your Config
```bash
uv run --active match_host_entry.py --config my_config.json --repo-url https://github.com/blecktita/savvyo-work-distribution.git
```

---

## Common Operations

### Starting a Production Run
```bash
# Long-running production job
uv run --active match_host_entry.py \
  --repo-url https://github.com/blecktita/savvyo-work-distribution.git \
  --environment production \
  --max-cycles 1000 \
  --log-file production_$(date +%Y%m%d).log
```

### Development/Testing
```bash
# Short test run with debug info
uv run --active match_host_entry.py \
  --repo-url https://github.com/blecktita/savvyo-work-distribution.git \
  --environment development \
  --max-cycles 5 \
  --debug
```

### Monitoring an Existing Process
```bash
# Just watch what's happening (no work creation)
uv run --active match_host_entry.py \
  --repo-url https://github.com/blecktita/savvyo-work-distribution.git \
  --max-cycles 0
```

### Export Metrics Without Running
If you just want to export current state:
```bash
uv run --active match_host_entry.py \
  --repo-url https://github.com/blecktita/savvyo-work-distribution.git \
  --max-cycles 1 \
  --no-export false
```

---

## Troubleshooting

### Common Issues

#### 1. "Repository connection failed"
**Problem**: Can't connect to GitHub repository
**Solutions**:
- Check your internet connection
- Verify the repo URL is correct
- Ensure you have access to the repository
- Try with `--debug` to see detailed error messages

#### 2. GUI shows many failed jobs
**Problem**: High number in "Failed" status
**Solutions**:
- Check the activity log for error details
- Look at the log file for more information
- Consider reducing `--max-cycles` temporarily
- Check your database connection

#### 3. Performance is slow
**Problem**: Low throughput numbers
**Solutions**:
- Check your database/network performance
- Reduce `--refresh-rate` to 1.0 or lower
- Look for error messages in the activity log

#### 4. GUI freezes or crashes
**Problem**: Interface becomes unresponsive
**Solutions**:
- Press `Ctrl+C` to stop gracefully
- Check the log file for error details
- Restart with `--debug` for more information
- Reduce `max_activity_history` in config

### Getting Help

#### View detailed logs:
```bash
tail -f match_work_gui.log
```

#### Run with maximum debugging:
```bash
uv run --active match_host_entry.py \
  --repo-url https://github.com/blecktita/savvyo-work-distribution.git \
  --debug \
  --refresh-rate 1.0 \
  --max-cycles 3
```

#### Check what the GUI exported:
Look for files like `match_work_metrics_20240805_143022.json`

---

## Advanced Features

### Custom Alert Thresholds
Edit your config file to change when alerts trigger:

```json
{
  "alert_thresholds": {
    "error_rate": 2,        // Alert if >2% error rate
    "failed_jobs": 5,       // Alert if >5 failed jobs
    "stale_claimed": 15     // Alert if >15 claimed jobs
  }
}
```

### Performance Tuning
For high-volume environments:

```json
{
  "refresh_rate": 1.0,           // Slower updates
  "max_activity_history": 50,    // Less memory usage
  "max_cycle_history": 25,       // Less memory usage
  "show_debug": false            // Cleaner interface
}
```

### Automated Runs
Create a script for scheduled runs:

```bash
#!/bin/bash
# run_match_work.sh

LOG_FILE="logs/match_work_$(date +%Y%m%d_%H%M).log"
CONFIG_FILE="production_config.json"

uv run --active match_host_entry.py \
  --config "$CONFIG_FILE" \
  --repo-url "https://github.com/blecktita/savvyo-work-distribution.git" \
  --environment production \
  --max-cycles 100 \
  --log-file "$LOG_FILE"
```

### Metrics Analysis
The exported JSON files contain:
- Performance statistics
- Complete cycle history
- Activity logs
- Final status

You can analyze these with any JSON tool or import into Excel/database.

---

## Command Reference

### Required Arguments
- `--repo-url`: GitHub repository URL

### Optional Arguments
- `--environment`: development, staging, or production (default: production)
- `--max-cycles`: Number of cycles to run (default: 100)
- `--config`: Path to JSON config file
- `--debug`: Enable debug logging
- `--refresh-rate`: GUI update frequency (default: 2.0)
- `--log-file`: Log file path
- `--no-export`: Disable metrics export
- `--create-config`: Create default config and exit

### Examples
```bash
# Minimal
uv run --active match_host_entry.py --repo-url REPO_URL

# Production
uv run --active match_host_entry.py --repo-url REPO_URL --environment production

# Development
uv run --active match_host_entry.py --repo-url REPO_URL --environment development --debug

# Custom config
uv run --active match_host_entry.py --config my_config.json --repo-url REPO_URL

# Short test
uv run --active match_host_entry.py --repo-url REPO_URL --max-cycles 5 --debug
```

---

## Tips for Success

1. **Start small**: Use `--max-cycles 5` for your first run
2. **Use configs**: Create a config file for consistent settings
3. **Monitor logs**: Check the log file if something seems wrong
4. **Watch the alerts**: The alerts panel will tell you about problems
5. **Export metrics**: Use the exported JSON for analysis and reporting
6. **Test environments**: Use `--environment development` for testing

Remember: The GUI is designed to be self-explanatory. If numbers are green and trending up, things are working well!