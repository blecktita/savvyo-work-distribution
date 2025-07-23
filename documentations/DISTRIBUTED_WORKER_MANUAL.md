# Distributed Worker System - Complete Manual

## 📋 Table of Contents
1. [Overview](#overview)
2. [Quick Start for Non-Technical Users](#quick-start-for-non-technical-users)
3. [System Requirements](#system-requirements)
4. [Installation Guide](#installation-guide)
5. [Configuration](#configuration)
6. [Running the Worker](#running-the-worker)
7. [Understanding Output & Logs](#understanding-output--logs)
8. [Troubleshooting](#troubleshooting)
9. [Developer Guide](#developer-guide)
10. [Architecture Details](#architecture-details)
11. [Advanced Configuration](#advanced-configuration)
12. [Monitoring & Maintenance](#monitoring--maintenance)

---

## 🎯 Overview

The Distributed Worker System is a tool that automatically processes web scraping tasks across multiple machines. It uses GitHub as a coordination hub, allowing multiple worker machines to safely claim and process work without conflicts.

### Key Features
- **Distributed Processing**: Multiple machines can work together
- **Automatic Coordination**: Workers coordinate through GitHub to avoid conflicts
- **Smart Auto-Stop**: Automatically stops when no work is available
- **Resource Management**: Prevents system sleep and manages resources efficiently
- **Comprehensive Logging**: Detailed logs for monitoring and debugging

### How It Works
1. **Host Machine** creates work orders and uploads them to GitHub
2. **Worker Machines** poll GitHub for available work
3. **Workers** claim work atomically (only one worker gets each task)
4. **Workers** process the work and upload results back to GitHub
5. **Host Machine** retrieves completed work for final processing

---

## 🚀 Quick Start for Non-Technical Users

### Prerequisites
- Python 3.8+ installed on your machine
- Google Chrome browser installed
- Access to the GitHub repository (ask your admin for the URL)

### Step 1: Get the Code
```bash
# Ask your developer to provide these files:
# - wms_worker_processor.py
# - coordination/github_bridge.py
# - (other project files)
```

### Step 2: Simple Run Command
```bash
# Replace YOUR_REPO_URL with the actual GitHub repository URL
python wms_worker_processor.py --repo-url https://github.com/blecktita/savvyo-work-distribution.git
```

### Step 3: Monitor the Output
Look for these key messages:
- ✅ `Worker initialized` - Setup successful
- ✅ `Claimed work` - Found and started processing work
- 🎉 `Completed work` - Successfully finished a task
- 😴 `No work available` - Waiting for more work
- 🛑 `STOPPING` - Automatically stopped (normal behavior)

### When to Stop/Restart
- **Normal stop**: When you see `🛑 STOPPING` - this is expected
- **Manual stop**: Press `Ctrl+C` if you need to stop early
- **Restart**: Just run the same command again to continue

---

## 💻 System Requirements

### Minimum Requirements
- **Operating System**: macOS, Windows 10+, or Linux
- **Python**: Version 3.8 or higher
- **RAM**: 4GB minimum, 8GB recommended
- **Storage**: 2GB free space for temporary files
- **Internet**: Stable broadband connection

### Required Software
- **Python 3.8+**: [Download here](https://python.org/downloads)
- **Google Chrome**: [Download here](https://chrome.google.com)
- **Git**: [Download here](https://git-scm.com/downloads)

### Python Packages
```bash
pip install selenium webdriver-manager requests
```

---

## 📦 Installation Guide

### For Non-Technical Users

#### Step 1: Install Python
1. Go to [python.org/downloads](https://python.org/downloads)
2. Download Python 3.8 or higher
3. Run the installer and check "Add to PATH"
4. Verify installation:
   ```bash
   python --version
   # Should show: Python 3.8.x or higher
   ```

#### Step 2: Install Chrome
1. Download from [chrome.google.com](https://chrome.google.com)
2. Install normally

#### Step 3: Install Required Packages
Open Terminal/Command Prompt and run:
```bash
pip install selenium webdriver-manager requests
```

#### Step 4: Get Project Files
Ask your developer for the project files and place them in a folder like `distributed-worker/`

### For Developers

#### Step 1: Clone Repository
```bash
git clone https://github.com/yourorg/distributed-worker.git
cd distributed-worker
```

#### Step 2: Create Virtual Environment
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

#### Step 3: Install Dependencies
```bash
pip install -r requirements.txt
```

#### Step 4: Configure Environment
```bash
cp .env.example .env
# Edit .env with your configuration
```

---

## ⚙️ Configuration

### Environment Variables
Create a `.env` file:
```bash
GITHUB_REPO_URL=https://github.com/blecktita/savvyo-work-distribution.git
ENVIRONMENT=production
MAX_WORK_ORDERS=50
MAX_CONSECUTIVE_FAILURES=20
MAX_IDLE_HOURS=2.0
```

### Configuration Files
The system uses `ConfigFactory` with three environments:
- **Development**: Testing with minimal data
- **Testing**: Staging environment
- **Production**: Full production runs

---

## 🏃‍♂️ Running the Worker

### Basic Command
```bash
python wms_worker_processor.py --repo-url https://github.com/blecktita/savvyo-work-distribution.git
```

### With Custom Parameters
```bash
python wms_worker_processor.py \
    --repo-url https://github.com/blecktita/savvyo-work-distribution.git \
    --environment production \
    --max-work 100 \
    --max-failures 30 \
    --max-idle-hours 4.0
```

### Parameter Explanation

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--repo-url` | Required | GitHub repository URL for work coordination |
| `--environment` | `production` | Configuration environment (development/testing/production) |
| `--max-work` | `50` | Maximum work orders to process before stopping |
| `--max-failures` | `20` | Stop after this many consecutive "no work" failures |
| `--max-idle-hours` | `2.0` | Stop after this many hours with no successful work |

### Running Multiple Workers
You can run multiple workers on the same machine:
```bash
# Terminal 1
python wms_worker_processor.py --repo-url https://github.com/blecktita/savvyo-work-distribution.git

# Terminal 2 (different window)
python wms_worker_processor.py --repo-url https://github.com/blecktita/savvyo-work-distribution.git
```

Each worker gets a unique ID and they coordinate automatically.

---

## 📊 Understanding Output & Logs

### Startup Messages
```
🚀 Starting worker cycle
   📋 Max work orders: 50
   🛑 Auto-stop conditions:
      • After 20 consecutive failures
      • After 2.0 hours with no successful work
🤖 Worker ASEs-MacBook-Pro.local_13060_b8295861 initialized
🌐 GitHub bridge initialized: ./scraping-work
🌐 WebDriver initialized
```

### Normal Operation
```
🔍 Looking for work... (5/50 completed)
✅ Claimed work: comp_BRQR_1f33961e_abc123
🎯 Processing competition BRQR_1f33961e
✅ Completed BRQR_1f33961e: 156 clubs, 3 seasons
🎉 Completed work: comp_BRQR_1f33961e_abc123
   📈 Progress: 6/50 (2.1 jobs/hour)
```

### No Work Available
```
🔍 Looking for work... (28/50 completed)
😴 No work available, waiting 67.0 seconds...
   📊 Consecutive failures: 10/20 (10 left)
   ⏳ Idle time: 0.5h/2.0h (1.5h left)
```

### Auto-Stop Messages
```
🛑 STOPPING: Reached 20 consecutive failures
   ⏱️ Total runtime: 1.3 hours
   📊 Work completed: 28
   💡 Reason: Consecutive failure limit reached

🏁 Worker cycle completed successfully!
📊 Final Statistics:
   • Work completed: 28
   • Total runtime: 1.3 hours
   • Average completion rate: 21.5 jobs/hour
   • Efficiency: 56.0% of target completed
```

### Status Indicators

| Icon | Meaning |
|------|---------|
| 🚀 | System starting up |
| 🔍 | Looking for work |
| ✅ | Success (claimed work, completed task, etc.) |
| 🎯 | Processing work |
| 🎉 | Work completed successfully |
| 😴 | Waiting (no work available) |
| ❌ | Error or failure |
| 🛑 | System stopping |
| 🏁 | Normal completion |
| ⚠️ | Warning or approaching limit |
| 💡 | Helpful information or recommendation |

---

## 🔧 Troubleshooting

### Common Issues

#### "No module named 'selenium'"
**Solution**: Install required packages
```bash
pip install selenium webdriver-manager requests
```

#### "WebDriver not found"
**Solution**: The system auto-downloads ChromeDriver, but ensure Chrome is installed

#### "Git command not found"
**Solution**: Install Git from [git-scm.com](https://git-scm.com/downloads)

#### "Permission denied" on GitHub
**Solution**: 
1. Check if you have access to the repository
2. Ensure your GitHub credentials are configured:
   ```bash
   git config --global user.name "Your Name"
   git config --global user.email "your.email@example.com"
   ```

#### Worker keeps stopping immediately
**Possible causes**:
- No work available in the queue
- Network connectivity issues
- Repository access problems

**Solutions**:
1. Check queue status with your admin
2. Verify internet connection
3. Try increasing failure/idle limits:
   ```bash
   python wms_worker_processor.py \
       --repo-url https://github.com/blecktita/savvyo-work-distribution.git \
       --max-failures 50 \
       --max-idle-hours 4.0
   ```

#### Multiple workers claiming same work
This is normal! The system is designed to handle race conditions. You'll see:
```
❌ Worker lost claim race for comp_BRQR_1f33961e
```
This is expected behavior in a distributed system.

### Debug Mode
For detailed debugging, modify the script to add:
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

### Getting Help
1. Check the logs for specific error messages
2. Try running with increased timeouts
3. Contact your system administrator with:
   - Complete error message
   - Commands you ran
   - Your operating system
   - Python version (`python --version`)

---

## 👨‍💻 Developer Guide

### Project Structure
```
distributed-worker/
├── wms_worker_processor.py          # Main worker script
├── coordination/
│   ├── github_bridge.py             # GitHub coordination logic
│   └── coordinator.py               # Progress tracking
├── pipelines/
│   └── princpal_orchestrator.py     # Core scraping logic
├── configurations/
│   └── __init__.py                  # Configuration management
├── requirements.txt                 # Python dependencies
└── README.md                       # This manual
```

### Key Classes

#### `DistributedWorker`
Main worker class that:
- Manages worker lifecycle
- Coordinates with GitHub
- Processes work orders
- Handles auto-stop logic

```python
class DistributedWorker:
    def __init__(self, repo_url: str, environment: str = "production")
    def run_worker_cycle(self, max_work_orders: int, max_consecutive_failures: int, max_idle_hours: float)
    def process_work_order(self, work_order: Dict, driver) -> Dict
```

#### `GitHubWorkBridge`
Handles all GitHub coordination:
- Atomic work claiming
- Result submission
- Repository synchronization

```python
class GitHubWorkBridge:
    def claim_available_work(self, worker_id: str) -> Optional[Dict]
    def submit_completed_work(self, work_order: Dict, results: Dict)
    def submit_failed_work(self, work_order: Dict, error_message: str)
```

#### `SleepPreventer`
Prevents system sleep during processing:
```python
class SleepPreventer:
    def start_prevention(self)
    def stop_prevention(self)
```

### Adding New Features

#### Custom Work Processing
Modify `process_work_order()` method:
```python
def process_work_order(self, work_order: Dict, driver) -> Dict:
    # Your custom processing logic here
    competition_id = work_order['competition_id']
    
    # Use existing orchestrator or implement custom logic
    results = your_custom_processor(work_order, driver)
    
    return {
        'seasons_processed': results.seasons,
        'club_data': results.data,
        'total_clubs_scraped': len(results.data),
        'execution_time_seconds': results.duration
    }
```

#### Custom Auto-Stop Conditions
Add new conditions in `run_worker_cycle()`:
```python
# Example: Stop if system memory is low
import psutil
if psutil.virtual_memory().percent > 90:
    print("🛑 STOPPING: Low system memory")
    break
```

### Testing

#### Unit Tests
```bash
python -m pytest tests/
```

#### Integration Tests
```bash
python -m pytest tests/integration/
```

#### Manual Testing
```bash
# Test with development environment
python wms_worker_processor.py \
    --repo-url https://github.com/blecktita/savvyo-work-distribution.git \
    --environment development \
    --max-work 5 \
    --max-failures 5
```

### Code Style
- Follow PEP 8
- Use type hints
- Document complex functions
- Use emoji in user-facing messages for clarity

### Git Workflow
```bash
# Create feature branch
git checkout -b feature/new-feature

# Make changes and test
python wms_worker_processor.py --repo-url ... --environment development

# Commit changes
git add .
git commit -m "Add new feature: description"

# Push and create PR
git push origin feature/new-feature
```

---

## 🏗️ Architecture Details

### Work Distribution Flow
```
┌─────────────┐    ┌──────────────┐    ┌─────────────-┐
│ Host Machine│    │   GitHub     │    │Worker Machine│
│             │    │ Repository   │    │              │
└──────┬──────┘    └──────┬───────┘    └──────┬──────-┘
       │                  │                   │
       │ 1. Create work   │                   │
       ├─────────────────►│                   │
       │                  │ 2. Poll for work  │
       │                  │◄──────────────────┤
       │                  │ 3. Claim work     │
       │                  │◄──────────────────┤
       │                  │ 4. Submit results │
       │                  │◄──────────────────┤
       │ 5. Retrieve      │                   │
       │◄─────────────────┤                   │
```

### Repository Structure
```
scraping-work/                 # GitHub Repository
├── available_work/            # Work orders waiting to be claimed
│   ├── comp_BRQR_abc123.json
│   └── comp_UEFA_def456.json
├── work_queue/                # Temporary claim files
│   └── comp_BRQR_worker1_timestamp_uuid.json
├── active_work/               # Currently being processed
│   └── worker1_timestamp.json
├── completed_work/            # Finished work results
│   └── worker1_timestamp.json
└── retry_queue/               # Failed work for retry
    └── failed_comp_BRQR_timestamp.json
```

### Atomic Claiming Process
1. Worker finds available work file
2. Creates unique claim file with timestamp
3. Pushes claim to GitHub
4. Pulls latest state to check for races
5. Compares timestamps to determine winner
6. Winner moves work to active, losers clean up

### Error Handling Strategy
- **Network errors**: Retry with exponential backoff
- **Git conflicts**: Reset and retry
- **Processing errors**: Submit to retry queue
- **System errors**: Graceful shutdown with cleanup

---

## ⚙️ Advanced Configuration

### Environment-Specific Settings

#### Development Environment
```python
class DevelopmentConfig:
    use_vpn = False
    save_to_database = False
    max_competitions = 5
    debug_logging = True
```

#### Production Environment
```python
class ProductionConfig:
    use_vpn = True
    save_to_database = True
    max_competitions = None
    debug_logging = False
```

### Custom Worker Behavior

#### Resource-Constrained Machines
```bash
python wms_worker_processor.py \
    --repo-url https://github.com/blecktita/savvyo-work-distribution.git \
    --max-work 10 \
    --max-failures 10 \
    --max-idle-hours 1.0
```

#### High-Performance Machines
```bash
python wms_worker_processor.py \
    --repo-url https://github.com/blecktita/savvyo-work-distribution.git \
    --max-work 200 \
    --max-failures 50 \
    --max-idle-hours 8.0
```

### Network Configuration

#### Behind Corporate Firewall
1. Configure Git to use corporate proxy:
   ```bash
   git config --global http.proxy http://proxy.company.com:8080
   ```

2. Set environment variables:
   ```bash
   export HTTPS_PROXY=https://proxy.company.com:8080
   export HTTP_PROXY=http://proxy.company.com:8080
   ```

#### VPN Requirements
Some configurations require VPN. Ensure your VPN is connected before starting the worker.

---

## 📊 Monitoring & Maintenance

### Performance Metrics

#### Key Performance Indicators (KPIs)
- **Completion Rate**: Jobs/hour
- **Success Rate**: Completed / (Completed + Failed)
- **Claim Success Rate**: Successful claims / Total claim attempts
- **System Utilization**: Runtime / Available time

#### Log Analysis
```bash
# Count successful completions
grep "🎉 Completed work" worker.log | wc -l

# Count claim failures
grep "❌.*lost claim race" worker.log | wc -l

# Average completion rate
grep "📈 Progress.*jobs/hour" worker.log | tail -10
```

### System Health Checks

#### Daily Maintenance
1. Check disk space: `df -h`
2. Check memory usage: `free -h`
3. Review error logs
4. Verify network connectivity

#### Weekly Maintenance
1. Clear temporary files
2. Update Chrome browser
3. Review performance metrics
4. Clean up old archive files

### Archive Management

The system automatically archives completed work:
```bash
# View archive statistics
python -c "
from coordination.github_bridge import GitHubWorkBridge
bridge = GitHubWorkBridge(repo_url='your-repo-url')
stats = bridge.get_archive_statistics()
print(stats)
"

# Clean up old archives (90+ days)
python -c "
from coordination.github_bridge import GitHubWorkBridge
bridge = GitHubWorkBridge(repo_url='your-repo-url')
result = bridge.cleanup_old_archives(days_to_keep=90)
print(f'Cleaned up: {result}')
"
```

### Scaling Considerations

#### Adding More Workers
- Each worker needs unique machine/environment
- Optimal worker count: 2-3 per competition source
- Monitor claim success rates (should be >30%)

#### Repository Size Management
- Archive old completed work regularly
- Use compression for large datasets
- Consider repository splitting for very large operations

---

## 🆘 Support & Contact

### Getting Help
1. **First**: Check this manual and troubleshooting section
2. **Second**: Review recent log messages for specific errors
3. **Third**: Contact your system administrator with:
   - Complete error messages
   - Steps to reproduce the issue
   - Your system information
   - Recent log excerpts

### Reporting Issues
When reporting issues, include:
```
System Information:
- OS: macOS 12.3.1
- Python: 3.9.7
- Chrome: 103.0.5060.114

Command Used:
python wms_worker_processor.py --repo-url https://github.com/org/repo

Error Message:
[Complete error message here]

Log Excerpt:
[Last 20 lines of relevant logs]
```

### Contributing
1. Fork the repository
2. Create a feature branch
3. Write tests for new functionality
4. Submit a pull request with detailed description

---

## 📝 Changelog

### Version 2.0.0 (Current)
- ✅ Added dual auto-stop conditions (failure count + idle time)
- ✅ Enhanced atomic work claiming to prevent race conditions
- ✅ Improved logging and progress tracking
- ✅ Added comprehensive error handling
- ✅ Implemented work archiving system

### Version 1.0.0
- ✅ Basic distributed worker functionality
- ✅ GitHub-based coordination
- ✅ Simple auto-stop on consecutive failures

---

*Last updated: [Current Date]*
*Version: 2.0.0*
*Maintainer: [Your Team Name]*