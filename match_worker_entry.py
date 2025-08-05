# match_worker_entry.py
"""
Production-grade GUI for Match Worker machines with user-friendly interface
and comprehensive monitoring capabilities.

Usage:
    python match_worker_entry.py --repo-url https://github.com/blecktita/savvyo-work-distribution.git
    python match_worker_entry.py --config-profile development --max-work 5
"""

import argparse
import json
import logging
import os
import queue
import signal
import socket
import sys
import threading
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

from rich import box
from rich.align import Align
from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.logging import RichHandler
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

# Import the worker processor
from wms_match_worker_processor import MatchDistributedWorker


class WorkerState(Enum):
    """Worker operational states"""

    INITIALIZING = "initializing"
    IDLE = "idle"
    CLAIMING_WORK = "claiming_work"
    PROCESSING = "processing"
    SUBMITTING = "submitting"
    PAUSED = "paused"
    STOPPING = "stopping"
    STOPPED = "stopped"
    ERROR = "error"


class LogLevel(Enum):
    """Log levels for worker activities"""

    DEBUG = "debug"
    INFO = "info"
    SUCCESS = "success"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class WorkerConfig:
    """Worker configuration with sensible defaults"""

    # Core settings
    repo_url: str = ""
    environment: str = "production"
    worker_profile: str = "standard"

    # Work processing
    max_work_orders: int = 10
    max_consecutive_failures: int = 15
    max_idle_hours: float = 2.0
    work_claim_interval: float = 30.0

    # GUI settings
    refresh_rate: float = 2.0
    max_activity_history: int = 150
    show_debug: bool = False
    auto_start: bool = False

    # Paths (shared with host)
    log_dir: str = "logs/match/worker"
    config_dir: str = "configurations"
    temp_dir: str = "temp_match_data"

    # Performance
    enable_sleep_prevention: bool = True
    selenium_timeout: int = 30
    vpn_enabled: bool = True

    # Alerts
    alert_thresholds: Dict[str, Any] = field(
        default_factory=lambda: {
            "consecutive_failures": 10,
            "processing_time_minutes": 30,
            "idle_time_hours": 1.5,
            "memory_usage_mb": 2000,
        }
    )


@dataclass
class WorkOrderProgress:
    """Track progress of current work order"""

    work_id: str
    competition_id: str
    season_year: int
    start_time: datetime
    current_matchday: int = 1
    total_matchdays_found: int = 0
    matches_processed: int = 0
    stage: str = "initializing"  # initializing, discovering, processing, finalizing
    estimated_completion: Optional[datetime] = None


@dataclass
class WorkerStats:
    """Worker performance statistics"""

    session_start: datetime
    total_work_orders: int = 0
    successful_completions: int = 0
    failed_attempts: int = 0
    total_matches_processed: int = 0
    total_matchdays_processed: int = 0
    current_idle_time: float = 0.0
    longest_processing_time: float = 0.0
    average_processing_time: float = 0.0
    uptime_hours: float = 0.0


@dataclass
class ActivityEntry:
    """Activity log entry"""

    timestamp: datetime
    level: LogLevel
    message: str
    details: Optional[str] = None
    work_id: Optional[str] = None


class MatchWorkerGUI:
    """
    Production-grade GUI for Match Worker with comprehensive monitoring
    """

    def __init__(self, config: WorkerConfig):
        self.config = config
        self.console = Console()

        # Worker components
        self.worker: Optional[MatchDistributedWorker] = None
        self.worker_thread: Optional[threading.Thread] = None

        # State management
        self.state = WorkerState.INITIALIZING
        self.shutdown_event = threading.Event()
        self.pause_event = threading.Event()

        # Progress tracking
        self.current_work: Optional[WorkOrderProgress] = None
        self.work_history: List[Dict[str, Any]] = []
        self.stats = WorkerStats(session_start=datetime.now())

        # Activity logging
        self.activity_queue: queue.Queue = queue.Queue()
        self.activity_history: List[ActivityEntry] = []

        # UI components
        self.layout = Layout()
        self.live: Optional[Live] = None

        # Setup
        self._setup_directories()
        self._setup_logging()
        self._setup_signal_handlers()
        self._setup_layout()

        # Generate worker ID
        hostname = socket.gethostname()
        self.worker_id = f"worker_{hostname}_{os.getpid()}_{uuid.uuid4().hex[:8]}"

        self.log_activity("Worker GUI initialized", LogLevel.SUCCESS)

    def _setup_directories(self):
        """Create necessary directories"""
        dirs = [self.config.log_dir, self.config.config_dir, self.config.temp_dir]
        for dir_path in dirs:
            Path(dir_path).mkdir(parents=True, exist_ok=True)

    def _setup_logging(self):
        """Setup logging to shared directory"""
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG if self.config.show_debug else logging.INFO)

        # Create log file in shared directory
        log_file = (
            Path(self.config.log_dir)
            / f"worker_{self.worker_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        )

        # File handler
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(
            logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        )

        # Console handler (warnings only)
        console_handler = RichHandler(
            console=self.console, show_time=False, show_path=False
        )
        console_handler.setLevel(logging.WARNING)

        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

        self.logger.info(f"Worker GUI started - ID: {self.worker_id}")

    def _setup_signal_handlers(self):
        """Setup graceful shutdown handlers"""

        def signal_handler(signum, frame):
            self.logger.info(f"Received signal {signum}, initiating shutdown")
            self.graceful_shutdown()

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    def _setup_layout(self):
        """Setup responsive layout"""
        self.layout.split(
            Layout(name="header", size=4),
            Layout(name="main", ratio=1),
            Layout(name="footer", size=3),
        )

        self.layout["main"].split_row(
            Layout(name="left", ratio=3), Layout(name="right", ratio=2)
        )

        self.layout["left"].split(
            Layout(name="status", size=8),
            Layout(name="current_work", size=10),
            Layout(name="activity", ratio=1),
        )

        self.layout["right"].split(
            Layout(name="stats", ratio=1), Layout(name="controls", size=8)
        )

    def log_activity(
        self,
        message: str,
        level: LogLevel = LogLevel.INFO,
        details: Optional[str] = None,
        work_id: Optional[str] = None,
    ):
        """Thread-safe activity logging"""
        entry = ActivityEntry(
            timestamp=datetime.now(),
            level=level,
            message=message,
            details=details,
            work_id=work_id,
        )

        try:
            self.activity_queue.put(entry, timeout=1.0)
        except queue.Full:
            self.logger.warning("Activity queue full")

        # Log to file
        log_level_map = {
            LogLevel.DEBUG: logging.DEBUG,
            LogLevel.INFO: logging.INFO,
            LogLevel.SUCCESS: logging.INFO,
            LogLevel.WARNING: logging.WARNING,
            LogLevel.ERROR: logging.ERROR,
            LogLevel.CRITICAL: logging.CRITICAL,
        }

        log_msg = f"[{work_id}] {message}" if work_id else message
        if details:
            log_msg += f": {details}"

        self.logger.log(log_level_map[level], log_msg)

    def _process_activity_queue(self):
        """Process queued activities"""
        while not self.activity_queue.empty():
            try:
                entry = self.activity_queue.get_nowait()
                self.activity_history.append(entry)

                if len(self.activity_history) > self.config.max_activity_history:
                    self.activity_history = self.activity_history[
                        -self.config.max_activity_history :
                    ]
            except queue.Empty:
                break

    def create_header(self) -> Panel:
        """Create header with worker info and status"""
        # State indicator
        state_colors = {
            WorkerState.INITIALIZING: "yellow",
            WorkerState.IDLE: "blue",
            WorkerState.CLAIMING_WORK: "cyan",
            WorkerState.PROCESSING: "green",
            WorkerState.SUBMITTING: "yellow",
            WorkerState.PAUSED: "yellow",
            WorkerState.STOPPING: "red",
            WorkerState.STOPPED: "red",
            WorkerState.ERROR: "bright_red",
        }

        state_icons = {
            WorkerState.INITIALIZING: "🔄",
            WorkerState.IDLE: "😴",
            WorkerState.CLAIMING_WORK: "🔍",
            WorkerState.PROCESSING: "⚡",
            WorkerState.SUBMITTING: "📤",
            WorkerState.PAUSED: "⏸️",
            WorkerState.STOPPING: "⏹️",
            WorkerState.STOPPED: "🔴",
            WorkerState.ERROR: "❌",
        }

        # Worker info
        hostname = socket.gethostname()
        worker_short_id = self.worker_id.split("_")[-1]

        # Runtime
        uptime = datetime.now() - self.stats.session_start
        uptime_str = str(uptime).split(".")[0]

        # Current work info
        work_info = ""
        if self.current_work:
            work_info = f" | 🎯 {self.current_work.competition_id}-{self.current_work.season_year}"

        header_text = Text.assemble(
            ("🤖 Match Worker", "bold cyan"),
            f" | 💻 {hostname}",
            f" | 🆔 {worker_short_id}",
            f" | [{state_colors[self.state]}]{state_icons[self.state]} {self.state.value.upper()}[/]",
            f" | ⏱️ {uptime_str}",
            work_info,
            style="white",
        )

        return Panel(Align.center(header_text), box=box.ROUNDED, style="bright_blue")

    def create_status_panel(self) -> Panel:
        """Create worker status overview"""
        table = Table(
            box=box.SIMPLE_HEAD, show_header=True, header_style="bold magenta"
        )
        table.add_column("Metric", style="cyan", no_wrap=True)
        table.add_column("Value", justify="right", style="green", width=12)
        table.add_column("Status", justify="center", width=8)

        # Calculate success rate
        total_attempts = self.stats.successful_completions + self.stats.failed_attempts
        success_rate = (
            (self.stats.successful_completions / total_attempts * 100)
            if total_attempts > 0
            else 0
        )

        # Calculate current idle time
        if self.state == WorkerState.IDLE and hasattr(self, "_idle_start"):
            self.stats.current_idle_time = (
                datetime.now() - self._idle_start
            ).total_seconds() / 3600

        status_items = [
            (
                "Work Orders",
                f"{self.stats.successful_completions}/{self.stats.total_work_orders}",
                "✅" if self.stats.successful_completions > 0 else "⏳",
            ),
            (
                "Success Rate",
                f"{success_rate:.1f}%",
                "✅" if success_rate >= 80 else "⚠️" if success_rate >= 60 else "❌",
            ),
            (
                "Matches Processed",
                f"{self.stats.total_matches_processed:,}",
                "📈" if self.stats.total_matches_processed > 0 else "⏳",
            ),
            (
                "Current Idle",
                f"{self.stats.current_idle_time:.1f}h",
                (
                    "⚠️"
                    if self.stats.current_idle_time
                    > self.config.alert_thresholds["idle_time_hours"]
                    else "✅"
                ),
            ),
            (
                "Consecutive Fails",
                f"{self._get_consecutive_failures()}",
                (
                    "❌"
                    if self._get_consecutive_failures()
                    >= self.config.alert_thresholds["consecutive_failures"]
                    else "✅"
                ),
            ),
        ]

        for metric, value, status in status_items:
            table.add_row(metric, value, status)

        return Panel(
            table, title="📊 Worker Status", box=box.ROUNDED, style="bright_green"
        )

    def create_current_work_panel(self) -> Panel:
        """Create current work progress panel"""
        if not self.current_work:
            return Panel(
                Align.center("No active work order", vertical="middle"),
                title="🎯 Current Work",
                box=box.ROUNDED,
                style="dim",
            )

        work = self.current_work
        elapsed = datetime.now() - work.start_time
        elapsed_str = str(elapsed).split(".")[0]

        # Progress information
        content = [
            f"🆔 Work ID: {work.work_id}",
            f"🏆 Competition: {work.competition_id}",
            f"📅 Season: {work.season_year}",
            f"⏱️ Elapsed: {elapsed_str}",
            "",
            f"📊 Stage: {work.stage.title()}",
            f"📆 Current Matchday: {work.current_matchday}",
            f"🎮 Matches Processed: {work.matches_processed}",
        ]

        if work.total_matchdays_found > 0:
            progress_pct = (work.current_matchday / work.total_matchdays_found) * 100
            progress_bar = "█" * int(progress_pct / 5) + "░" * (
                20 - int(progress_pct / 5)
            )
            content.extend(
                [
                    "",
                    f"Progress: {progress_bar} {progress_pct:.1f}%",
                    f"Matchdays: {work.current_matchday}/{work.total_matchdays_found}",
                ]
            )

        if work.estimated_completion:
            eta_str = work.estimated_completion.strftime("%H:%M:%S")
            content.append(f"🕐 ETA: {eta_str}")

        return Panel(
            "\n".join(content),
            title="🎯 Current Work Order",
            box=box.ROUNDED,
            style="bright_yellow",
        )

    def create_activity_panel(self) -> Panel:
        """Create activity log panel"""
        self._process_activity_queue()

        if not self.activity_history:
            return Panel(
                Align.center("No activity yet...", vertical="middle"),
                title="📝 Activity Log",
                box=box.ROUNDED,
                style="bright_cyan",
            )

        # Filter and format activities
        activities = self.activity_history[-15:]
        if not self.config.show_debug:
            activities = [a for a in activities if a.level != LogLevel.DEBUG]

        activity_lines = []
        for activity in reversed(activities):
            timestamp = activity.timestamp.strftime("%H:%M:%S")

            # Level styling
            level_styles = {
                LogLevel.DEBUG: ("🔍", "dim"),
                LogLevel.INFO: ("ℹ️", "white"),
                LogLevel.SUCCESS: ("✅", "green"),
                LogLevel.WARNING: ("⚠️", "yellow"),
                LogLevel.ERROR: ("❌", "red"),
                LogLevel.CRITICAL: ("🚨", "bright_red"),
            }

            icon, style = level_styles.get(activity.level, ("ℹ️", "white"))

            # Format message with work ID if available
            message = activity.message
            if activity.work_id and len(activity.work_id) > 8:
                work_short = activity.work_id[-8:]
                message = f"[{work_short}] {message}"

            # Truncate long messages
            if len(message) > 65:
                message = message[:62] + "..."

            activity_lines.append(f"[dim]{timestamp}[/] {icon} [{style}]{message}[/]")

        return Panel(
            "\n".join(activity_lines),
            title=f"📝 Activity Log ({len(self.activity_history)} total)",
            box=box.ROUNDED,
            style="bright_cyan",
        )

    def create_stats_panel(self) -> Panel:
        """Create detailed statistics panel"""
        uptime_hours = (
            datetime.now() - self.stats.session_start
        ).total_seconds() / 3600

        content = [
            f"🌍 Environment: {self.config.environment}",
            f"⚙️ Profile: {self.config.worker_profile}",
            f"🖥️ Host: {socket.gethostname()}",
            "",
            f"⏱️ Uptime: {uptime_hours:.1f}h",
            f"📊 Work Orders: {self.stats.total_work_orders}",
            f"✅ Completed: {self.stats.successful_completions}",
            f"❌ Failed: {self.stats.failed_attempts}",
            "",
            f"🎮 Total Matches: {self.stats.total_matches_processed:,}",
            f"📆 Total Matchdays: {self.stats.total_matchdays_processed:,}",
        ]

        # Performance metrics
        if self.stats.successful_completions > 0:
            avg_time = self.stats.average_processing_time / 60  # Convert to minutes
            content.extend(
                [
                    "",
                    f"⚡ Avg Processing: {avg_time:.1f}m",
                    f"🏆 Longest Job: {self.stats.longest_processing_time / 60:.1f}m",
                ]
            )

        # Rate calculations
        if uptime_hours > 0:
            work_rate = self.stats.successful_completions / uptime_hours
            match_rate = self.stats.total_matches_processed / uptime_hours
            content.extend(
                [
                    "",
                    f"📈 Work Rate: {work_rate:.2f}/h",
                    f"📈 Match Rate: {match_rate:.1f}/h",
                ]
            )

        return Panel(
            "\n".join(content),
            title="📊 Session Statistics",
            box=box.ROUNDED,
            style="bright_magenta",
        )

    def create_controls_panel(self) -> Panel:
        """Create control panel with actions"""
        controls = []

        # State-dependent controls
        if self.state == WorkerState.IDLE:
            controls.extend(
                [
                    "🎯 Ready for work orders",
                    "",
                    "Available actions:",
                    "• P - Pause worker",
                    "• R - Force work claim",
                    "• C - Show config",
                    "• E - Export logs",
                ]
            )
        elif self.state == WorkerState.PROCESSING:
            controls.extend(
                [
                    f"⚡ Processing work...",
                    "",
                    "Available actions:",
                    "• S - Show detailed progress",
                    "• P - Pause after current",
                    "• F - Force stop (unsafe)",
                ]
            )
        elif self.state == WorkerState.PAUSED:
            controls.extend(
                [
                    "⏸️ Worker paused",
                    "",
                    "Available actions:",
                    "• R - Resume worker",
                    "• S - Stop worker",
                ]
            )
        else:
            controls.extend(
                [
                    f"State: {self.state.value}",
                    "",
                    "Available actions:",
                    "• Ctrl+C - Graceful stop",
                    "• Wait for completion...",
                ]
            )

        # Configuration info
        controls.extend(
            [
                "",
                "Configuration:",
                f"• Max work: {self.config.max_work_orders}",
                f"• Max idle: {self.config.max_idle_hours}h",
                f"• Environment: {self.config.environment}",
            ]
        )

        return Panel(
            "\n".join(controls),
            title="🎮 Controls & Info",
            box=box.ROUNDED,
            style="bright_yellow",
        )

    def create_footer(self) -> Panel:
        """Create footer with key shortcuts"""
        shortcuts = Text.assemble(
            ("Ctrl+C", "bold red"),
            " Stop | ",
            ("P", "bold yellow"),
            " Pause | ",
            ("R", "bold green"),
            " Resume | ",
            ("E", "bold cyan"),
            " Export | ",
            ("C", "bold blue"),
            " Config",
            style="white",
        )

        return Panel(Align.center(shortcuts), box=box.ROUNDED, style="bright_blue")

    def update_layout(self):
        """Update all layout components"""
        try:
            self.layout["header"].update(self.create_header())
            self.layout["status"].update(self.create_status_panel())
            self.layout["current_work"].update(self.create_current_work_panel())
            self.layout["activity"].update(self.create_activity_panel())
            self.layout["stats"].update(self.create_stats_panel())
            self.layout["controls"].update(self.create_controls_panel())
            self.layout["footer"].update(self.create_footer())
        except Exception as e:
            self.logger.error(f"Layout update failed: {e}")

    def _get_consecutive_failures(self) -> int:
        """Get count of consecutive failures from recent history"""
        if not self.work_history:
            return 0

        consecutive = 0
        for work in reversed(self.work_history[-10:]):  # Check last 10
            if work.get("status") == "failed":
                consecutive += 1
            else:
                break

        return consecutive

    def update_work_progress(self, work_id: str, **kwargs):
        """Update current work progress"""
        if self.current_work and self.current_work.work_id == work_id:
            for key, value in kwargs.items():
                if hasattr(self.current_work, key):
                    setattr(self.current_work, key, value)

    def start_work_order(self, work_order: Dict[str, Any]):
        """Start tracking a new work order"""
        self.current_work = WorkOrderProgress(
            work_id=work_order["work_id"],
            competition_id=work_order["competition_id"],
            season_year=work_order["season_year"],
            start_time=datetime.now(),
            stage="initializing",
        )

        self.stats.total_work_orders += 1
        self.state = WorkerState.PROCESSING

        self.log_activity(
            f"Started processing {work_order['competition_id']}-{work_order['season_year']}",
            LogLevel.SUCCESS,
            work_id=work_order["work_id"],
        )

    def complete_work_order(self, work_id: str, success: bool, **metrics):
        """Complete a work order"""
        if self.current_work and self.current_work.work_id == work_id:
            duration = (datetime.now() - self.current_work.start_time).total_seconds()

            # Update stats
            if success:
                self.stats.successful_completions += 1
                self.log_activity(
                    "Work order completed successfully",
                    LogLevel.SUCCESS,
                    work_id=work_id,
                )
            else:
                self.stats.failed_attempts += 1
                self.log_activity("Work order failed", LogLevel.ERROR, work_id=work_id)

            # Update performance stats
            if duration > self.stats.longest_processing_time:
                self.stats.longest_processing_time = duration

            if self.stats.successful_completions > 0:
                self.stats.average_processing_time = (
                    self.stats.average_processing_time
                    * (self.stats.successful_completions - 1)
                    + duration
                ) / self.stats.successful_completions

            # Add to history
            self.work_history.append(
                {
                    "work_id": work_id,
                    "competition_id": self.current_work.competition_id,
                    "season_year": self.current_work.season_year,
                    "duration": duration,
                    "status": "completed" if success else "failed",
                    "matches_processed": metrics.get("matches_processed", 0),
                    "matchdays_processed": metrics.get("matchdays_processed", 0),
                    "end_time": datetime.now(),
                }
            )

            # Update totals
            self.stats.total_matches_processed += metrics.get("matches_processed", 0)
            self.stats.total_matchdays_processed += metrics.get(
                "matchdays_processed", 0
            )

            self.current_work = None

        self.state = WorkerState.IDLE
        self._idle_start = datetime.now()

    def run_worker_with_gui(self):
        """Run the worker with GUI integration"""
        try:
            self.worker = MatchDistributedWorker(
                repo_url=self.config.repo_url, environment=self.config.environment
            )

            self.state = WorkerState.IDLE
            self._idle_start = datetime.now()

            self.log_activity("Worker initialized successfully", LogLevel.SUCCESS)

            # Custom worker cycle with GUI integration
            self._run_enhanced_worker_cycle()

        except Exception as e:
            self.state = WorkerState.ERROR
            self.log_activity(
                f"Worker initialization failed: {str(e)}", LogLevel.CRITICAL
            )
            raise

    def _run_enhanced_worker_cycle(self):
        """Enhanced worker cycle with GUI integration"""
        processed_count = 0
        consecutive_failures = 0
        start_time = time.time()
        last_success_time = time.time()

        while (
            processed_count < self.config.max_work_orders
            and not self.shutdown_event.is_set()
        ):
            # Check pause state
            while self.pause_event.is_set() and not self.shutdown_event.is_set():
                self.state = WorkerState.PAUSED
                time.sleep(1)

            if self.shutdown_event.is_set():
                break

            # Check stop conditions
            idle_hours = (time.time() - last_success_time) / 3600

            if consecutive_failures >= self.config.max_consecutive_failures:
                self.log_activity(
                    f"Stopping: {consecutive_failures} consecutive failures",
                    LogLevel.WARNING,
                )
                break
            elif idle_hours >= self.config.max_idle_hours:
                self.log_activity(
                    f"Stopping: {idle_hours:.1f}h idle time", LogLevel.WARNING
                )
                break

            # Claim work
            self.state = WorkerState.CLAIMING_WORK
            self.log_activity("Looking for available work...", LogLevel.INFO)

            try:
                work_order = self.worker.github_bridge.claim_available_work(
                    self.worker.worker_id
                )

                # Filter for match work
                if work_order and work_order.get("work_type") != "match_data":
                    self.log_activity(
                        f"Skipping non-match work: {work_order.get('work_type')}",
                        LogLevel.INFO,
                    )
                    continue

                if not work_order:
                    consecutive_failures += 1
                    self.state = WorkerState.IDLE
                    wait_time = min(
                        self.config.work_claim_interval
                        * (1.5 ** min(consecutive_failures - 1, 5)),
                        300,
                    )
                    self.log_activity(
                        f"No work available, waiting {wait_time:.1f}s", LogLevel.INFO
                    )
                    time.sleep(wait_time)
                    continue

                # Process work order
                consecutive_failures = 0
                last_success_time = time.time()

                self.start_work_order(work_order)

                try:
                    # Process with progress tracking
                    results = self._process_work_with_progress_tracking(work_order)

                    self.state = WorkerState.SUBMITTING
                    self.worker.github_bridge.submit_completed_work(work_order, results)

                    # Complete successfully
                    metrics = {
                        "matches_processed": results.get("total_matches", 0),
                        "matchdays_processed": results.get("total_matchdays", 0),
                    }
                    self.complete_work_order(work_order["work_id"], True, **metrics)
                    processed_count += 1

                except Exception as e:
                    error_msg = f"Processing error: {str(e)}"
                    self.worker.github_bridge.submit_failed_work(work_order, error_msg)
                    self.complete_work_order(work_order["work_id"], False)
                    self.log_activity(
                        f"Work order failed: {error_msg}",
                        LogLevel.ERROR,
                        work_id=work_order["work_id"],
                    )
                    continue

                time.sleep(10)  # Brief pause between work orders

            except Exception as e:
                self.log_activity(f"Worker cycle error: {str(e)}", LogLevel.ERROR)
                consecutive_failures += 1
                time.sleep(30)

        # Final summary
        elapsed_hours = (time.time() - start_time) / 3600
        self.log_activity(
            f"Worker cycle completed: {processed_count} orders in {elapsed_hours:.1f}h",
            LogLevel.SUCCESS,
        )

    def _process_work_with_progress_tracking(self, work_order):
        """Process work order with progress tracking for GUI"""
        from selenium import webdriver

        driver = None
        try:
            # Initialize WebDriver
            self.update_work_progress(work_order["work_id"], stage="initializing")
            driver = webdriver.Chrome()

            # Process with enhanced tracking
            self.update_work_progress(work_order["work_id"], stage="processing")

            # Use a wrapper that provides progress callbacks
            results = self._process_with_callbacks(work_order, driver)

            self.update_work_progress(work_order["work_id"], stage="finalizing")
            return results

        finally:
            if driver:
                driver.quit()

    def _process_with_callbacks(self, work_order, driver):
        """Process work order with progress callbacks"""

        # Create a custom progress callback
        def progress_callback(matchday, matches_found=0, stage="processing"):
            self.update_work_progress(
                work_order["work_id"],
                current_matchday=matchday,
                matches_processed=matches_found,
                stage=stage,
            )

        # Process using the worker's method but with progress tracking
        return self.worker.process_match_work_order(work_order, driver)

    def export_session_data(self):
        """Export comprehensive session data"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = (
                Path(self.config.log_dir)
                / f"worker_session_{self.worker_id}_{timestamp}.json"
            )

            export_data = {
                "session_info": {
                    "worker_id": self.worker_id,
                    "hostname": socket.gethostname(),
                    "start_time": self.stats.session_start.isoformat(),
                    "export_time": datetime.now().isoformat(),
                    "config": asdict(self.config),
                },
                "statistics": asdict(self.stats),
                "work_history": self.work_history,
                "recent_activities": [
                    {
                        "timestamp": entry.timestamp.isoformat(),
                        "level": entry.level.value,
                        "message": entry.message,
                        "work_id": entry.work_id,
                    }
                    for entry in self.activity_history[-100:]
                ],
                "current_work": (
                    asdict(self.current_work) if self.current_work else None
                ),
            }

            with open(filename, "w") as f:
                json.dump(export_data, f, indent=2, default=str)

            self.log_activity(
                f"Session data exported to {filename.name}", LogLevel.SUCCESS
            )
            return filename

        except Exception as e:
            self.log_activity(f"Export failed: {str(e)}", LogLevel.ERROR)
            return None

    def graceful_shutdown(self):
        """Graceful shutdown with cleanup"""
        if self.state not in [WorkerState.STOPPING, WorkerState.STOPPED]:
            self.state = WorkerState.STOPPING
            self.log_activity("Initiating graceful shutdown...", LogLevel.WARNING)

            self.shutdown_event.set()

            # Export session data
            self.export_session_data()

            # Cleanup worker
            if self.worker:
                self.worker.shutdown()

    def run(self):
        """Main GUI execution"""
        try:
            # Start worker thread
            self.worker_thread = threading.Thread(
                target=self.run_worker_with_gui, daemon=True, name="WorkerThread"
            )

            if self.config.auto_start:
                self.worker_thread.start()
                self.log_activity("Auto-started worker thread", LogLevel.INFO)
            else:
                self.log_activity(
                    "Worker ready - start manually or auto-start disabled",
                    LogLevel.INFO,
                )

            # Start GUI
            with Live(
                self.layout, refresh_per_second=self.config.refresh_rate, screen=True
            ) as live:
                self.live = live

                while not self.shutdown_event.is_set():
                    try:
                        self.update_layout()
                        time.sleep(1.0 / self.config.refresh_rate)

                        # Start worker if not auto-started and idle too long
                        if (
                            not self.worker_thread.is_alive()
                            and not self.config.auto_start
                            and self.state == WorkerState.INITIALIZING
                        ):
                            # Could add manual start logic here
                            pass

                    except Exception as e:
                        self.logger.error(f"GUI update error: {e}")
                        time.sleep(1.0)

                # Final update
                self.update_layout()
                time.sleep(2)

        except KeyboardInterrupt:
            self.graceful_shutdown()
        except Exception as e:
            self.state = WorkerState.ERROR
            self.log_activity(f"Critical GUI error: {str(e)}", LogLevel.CRITICAL)
            raise
        finally:
            self.cleanup()

    def cleanup(self):
        """Cleanup resources"""
        try:
            self.log_activity("Cleaning up resources...", LogLevel.INFO)

            self.shutdown_event.set()

            # Wait for worker thread
            if self.worker_thread and self.worker_thread.is_alive():
                self.worker_thread.join(timeout=10)
                if self.worker_thread.is_alive():
                    self.logger.warning("Worker thread did not terminate gracefully")

            # Final export
            self.export_session_data()

            self.state = WorkerState.STOPPED
            self.logger.info("Worker GUI cleanup completed")

        except Exception as e:
            self.logger.error(f"Cleanup error: {e}")


def load_worker_config(profile: str = "standard") -> WorkerConfig:
    """Load worker configuration from shared configurations directory"""
    config_dir = Path("configurations")
    config_file = config_dir / f"worker_{profile}.json"

    # Create default config if it doesn't exist
    if not config_file.exists():
        config = WorkerConfig(worker_profile=profile)
        save_worker_config(config, profile)
        return config

    try:
        with open(config_file, "r") as f:
            config_data = json.load(f)
        return WorkerConfig(**config_data)
    except Exception as e:
        print(f"⚠️ Error loading config {config_file}: {e}")
        return WorkerConfig(worker_profile=profile)


def save_worker_config(config: WorkerConfig, profile: str):
    """Save worker configuration to shared directory"""
    config_dir = Path("configurations")
    config_dir.mkdir(exist_ok=True)

    config_file = config_dir / f"worker_{profile}.json"

    try:
        with open(config_file, "w") as f:
            json.dump(asdict(config), f, indent=2)
        print(f"✅ Saved config to {config_file}")
    except Exception as e:
        print(f"❌ Error saving config: {e}")


def create_default_profiles():
    """Create default worker configuration profiles"""
    profiles = {
        "development": WorkerConfig(
            environment="development",
            worker_profile="development",
            max_work_orders=3,
            max_idle_hours=0.5,
            show_debug=True,
            auto_start=False,
            refresh_rate=1.0,
        ),
        "testing": WorkerConfig(
            environment="testing",
            worker_profile="testing",
            max_work_orders=5,
            max_idle_hours=1.0,
            show_debug=True,
            auto_start=False,
            refresh_rate=2.0,
        ),
        "standard": WorkerConfig(
            environment="production",
            worker_profile="standard",
            max_work_orders=10,
            max_idle_hours=2.0,
            show_debug=False,
            auto_start=True,
            refresh_rate=2.0,
        ),
        "high_capacity": WorkerConfig(
            environment="production",
            worker_profile="high_capacity",
            max_work_orders=25,
            max_idle_hours=4.0,
            max_consecutive_failures=20,
            show_debug=False,
            auto_start=True,
            refresh_rate=1.5,
        ),
        "24x7": WorkerConfig(
            environment="production",
            worker_profile="24x7",
            max_work_orders=100,
            max_idle_hours=8.0,
            max_consecutive_failures=30,
            show_debug=False,
            auto_start=True,
            refresh_rate=1.0,
        ),
    }

    config_dir = Path("configurations")
    config_dir.mkdir(exist_ok=True)

    for profile_name, config in profiles.items():
        save_worker_config(config, profile_name)

    print("✅ Created default worker profiles:")
    for profile_name in profiles.keys():
        print(f"   • {profile_name}")


def main():
    """Enhanced main function with profile management"""
    parser = argparse.ArgumentParser(
        description="Production Match Worker GUI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Worker Profiles:
  development  - 3 jobs, 0.5h idle, debug on, manual start
  testing      - 5 jobs, 1h idle, debug on, manual start  
  standard     - 10 jobs, 2h idle, auto-start (default)
  high_capacity- 25 jobs, 4h idle, auto-start
  24x7         - 100 jobs, 8h idle, auto-start

Examples:
  %(prog)s --repo-url https://github.com/blecktita/savvyo-work-distribution.git
  %(prog)s --config-profile development --repo-url REPO_URL
  %(prog)s --config-profile high_capacity --repo-url REPO_URL --max-work 50
  %(prog)s --create-profiles  # Create default configuration profiles
        """,
    )

    # Core arguments
    parser.add_argument("--repo-url", help="GitHub repository URL")
    parser.add_argument(
        "--config-profile",
        default="standard",
        choices=["development", "testing", "standard", "high_capacity", "24x7"],
        help="Worker configuration profile",
    )

    # Profile management
    parser.add_argument(
        "--create-profiles",
        action="store_true",
        help="Create default configuration profiles and exit",
    )
    parser.add_argument(
        "--list-profiles",
        action="store_true",
        help="List available configuration profiles",
    )

    # Override options
    parser.add_argument(
        "--environment",
        choices=["development", "testing", "production"],
        help="Override environment setting",
    )
    parser.add_argument("--max-work", type=int, help="Override max work orders")
    parser.add_argument("--max-idle-hours", type=float, help="Override max idle hours")
    parser.add_argument("--debug", action="store_true", help="Enable debug mode")
    parser.add_argument(
        "--no-auto-start", action="store_true", help="Disable auto-start"
    )
    parser.add_argument("--refresh-rate", type=float, help="GUI refresh rate")

    args = parser.parse_args()

    # Handle profile management
    if args.create_profiles:
        create_default_profiles()
        sys.exit(0)

    if args.list_profiles:
        config_dir = Path("configurations")
        profiles = list(config_dir.glob("worker_*.json"))
        print("Available worker profiles:")
        for profile_path in profiles:
            profile_name = profile_path.stem.replace("worker_", "")
            print(f"  • {profile_name}")
        sys.exit(0)

    # Validate required arguments
    if not args.repo_url:
        parser.error("--repo-url is required")

    try:
        # Load configuration
        print(f"📋 Loading worker profile: {args.config_profile}")
        config = load_worker_config(args.config_profile)
        config.repo_url = args.repo_url

        # Apply command line overrides
        if args.environment:
            config.environment = args.environment
        if args.max_work:
            config.max_work_orders = args.max_work
        if args.max_idle_hours:
            config.max_idle_hours = args.max_idle_hours
        if args.debug:
            config.show_debug = True
        if args.no_auto_start:
            config.auto_start = False
        if args.refresh_rate:
            config.refresh_rate = args.refresh_rate

        # Display configuration
        print("🤖 Worker Configuration:")
        print(f"   • Environment: {config.environment}")
        print(f"   • Profile: {config.worker_profile}")
        print(f"   • Max work orders: {config.max_work_orders}")
        print(f"   • Max idle time: {config.max_idle_hours}h")
        print(f"   • Auto-start: {config.auto_start}")
        print(f"   • Debug mode: {config.show_debug}")

        # Create and run GUI
        print("🚀 Starting Worker GUI...")
        gui = MatchWorkerGUI(config)
        gui.run()

    except KeyboardInterrupt:
        print("\n⏹️ Worker interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        logging.critical(f"Worker GUI crashed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
