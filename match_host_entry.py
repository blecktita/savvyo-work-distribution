# match_work_gui.py
"""
Production-ready terminal GUI for match work processing with improved error handling,
logging, configuration management, and monitoring capabilities.

Run with: uv run --active match_host_entry.py --repo-url=https://github.com/blecktita/savvyo-work-distribution.git --environment production
"""

import argparse
import json
import logging
import queue
import signal
import sys
import threading
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Callable, Dict, List, Optional

from rich import box
from rich.align import Align
from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.logging import RichHandler
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

# Import your existing manager
from wms_match_host_work_manager import MatchHostWorkManager


class ActivityLevel(Enum):
    """Activity log levels"""

    DEBUG = "debug"
    INFO = "info"
    SUCCESS = "success"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class GUIState(Enum):
    """GUI operational states"""

    INITIALIZING = "initializing"
    RUNNING = "running"
    PAUSED = "paused"
    STOPPING = "stopping"
    STOPPED = "stopped"
    ERROR = "error"


@dataclass
class ActivityEntry:
    """Structured activity log entry"""

    timestamp: datetime
    level: ActivityLevel
    message: str
    details: Optional[str] = None
    source: str = "gui"

    def __post_init__(self):
        if isinstance(self.level, str):
            self.level = ActivityLevel(self.level)


@dataclass
class CycleMetrics:
    """Metrics for a single processing cycle"""

    cycle_number: int
    start_time: datetime
    end_time: Optional[datetime] = None
    created: int = 0
    processed: int = 0
    errors: int = 0
    available: int = 0
    claimed: int = 0
    completed: int = 0
    failed: int = 0
    duration_seconds: float = 0.0

    @property
    def throughput(self) -> float:
        """Items processed per second"""
        if self.duration_seconds > 0:
            return (self.created + self.processed) / self.duration_seconds
        return 0.0


@dataclass
class PerformanceStats:
    """Performance statistics tracking"""

    total_cycles: int = 0
    total_created: int = 0
    total_processed: int = 0
    total_errors: int = 0
    average_cycle_time: float = 0.0
    peak_throughput: float = 0.0
    uptime_seconds: float = 0.0
    error_rate: float = 0.0
    last_activity: Optional[datetime] = None


@dataclass
class GUIConfig:
    """GUI configuration settings"""

    refresh_rate: float = 2.0
    max_activity_history: int = 100
    max_cycle_history: int = 50
    auto_scroll: bool = True
    show_debug: bool = False
    log_file: Optional[str] = None
    export_metrics: bool = True
    alert_thresholds: Dict[str, int] = field(
        default_factory=lambda: {
            "error_rate": 10,
            "failed_jobs": 5,
            "stale_claimed": 20,
        }
    )


class MatchWorkGUI:
    """
    Production-ready Rich-based terminal GUI for monitoring match work processing
    with enhanced error handling, logging, and metrics collection.
    """

    def __init__(
        self, manager: MatchHostWorkManager, config: Optional[GUIConfig] = None
    ) -> None:
        self.manager = manager
        self.config = config or GUIConfig()
        self.console = Console()

        # State management
        self.state = GUIState.INITIALIZING
        self.shutdown_event = threading.Event()
        self.pause_event = threading.Event()

        # Threading
        self.work_thread: Optional[threading.Thread] = None
        self.monitor_thread: Optional[threading.Thread] = None
        self.activity_queue: queue.Queue = queue.Queue()

        # Metrics and tracking
        self.start_time: Optional[datetime] = None
        self.cycle_history: List[CycleMetrics] = []
        self.activity_history: List[ActivityEntry] = []
        self.performance_stats = PerformanceStats()
        self.current_cycle: Optional[CycleMetrics] = None

        # Status tracking
        self.last_status: Dict[str, int] = {}
        self.status_lock = threading.Lock()

        # Layout and UI
        self.layout = Layout()
        self.live: Optional[Live] = None

        # Setup logging
        self._setup_logging()

        # Setup signal handlers for graceful shutdown
        self._setup_signal_handlers()

        # Initialize layout
        self.setup_layout()

        self.logger.info("MatchWorkGUI initialized")

    def _setup_logging(self) -> None:
        """Setup structured logging"""
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG if self.config.show_debug else logging.INFO)

        # Console handler with Rich
        console_handler = RichHandler(
            console=self.console, show_time=False, show_path=False
        )
        console_handler.setLevel(logging.WARNING)  # Only warnings+ to console

        # File handler if configured
        handlers = [console_handler]
        if self.config.log_file:
            file_handler = logging.FileHandler(self.config.log_file)
            file_handler.setLevel(logging.DEBUG)
            file_handler.setFormatter(
                logging.Formatter(
                    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
                )
            )
            handlers.append(file_handler)

        for handler in handlers:
            self.logger.addHandler(handler)

    def _setup_signal_handlers(self) -> None:
        """Setup graceful shutdown signal handlers"""

        def signal_handler(signum, frame):
            self.logger.info(f"Received signal {signum}, initiating graceful shutdown")
            self.graceful_shutdown()

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    def setup_layout(self) -> None:
        """Setup the responsive layout structure"""
        self.layout.split(
            Layout(name="header", size=4),
            Layout(name="main", ratio=1),
            Layout(name="footer", size=2),
        )

        self.layout["main"].split_row(
            Layout(name="left", ratio=3), Layout(name="right", ratio=2)
        )

        self.layout["left"].split(
            Layout(name="status", size=10),
            Layout(name="progress", size=8),
            Layout(name="activity", ratio=1),
        )

        self.layout["right"].split(
            Layout(name="stats", ratio=1), Layout(name="alerts", size=6)
        )

    def log_activity(
        self,
        message: str,
        level: ActivityLevel = ActivityLevel.INFO,
        details: Optional[str] = None,
        source: str = "gui",
    ) -> None:
        """Thread-safe activity logging"""
        entry = ActivityEntry(
            timestamp=datetime.now(),
            level=level,
            message=message,
            details=details,
            source=source,
        )

        try:
            self.activity_queue.put(entry, timeout=1.0)
        except queue.Full:
            # Log to system logger if queue is full
            self.logger.warning("Activity queue full, dropping entry")

        # Also log to system logger based on level
        log_level = {
            ActivityLevel.DEBUG: logging.DEBUG,
            ActivityLevel.INFO: logging.INFO,
            ActivityLevel.SUCCESS: logging.INFO,
            ActivityLevel.WARNING: logging.WARNING,
            ActivityLevel.ERROR: logging.ERROR,
            ActivityLevel.CRITICAL: logging.CRITICAL,
        }.get(level, logging.INFO)

        self.logger.log(
            log_level, f"[{source}] {message}" + (f": {details}" if details else "")
        )

    def _process_activity_queue(self) -> None:
        """Process queued activity entries"""
        while not self.activity_queue.empty():
            try:
                entry = self.activity_queue.get_nowait()
                self.activity_history.append(entry)

                # Trim history
                if len(self.activity_history) > self.config.max_activity_history:
                    self.activity_history = self.activity_history[
                        -self.config.max_activity_history :
                    ]

            except queue.Empty:
                break

    @contextmanager
    def status_update(self):
        """Context manager for thread-safe status updates"""
        with self.status_lock:
            yield

    def update_status(self) -> bool:
        """Update work status from manager with error handling"""
        try:
            raw_status = self.manager.get_match_work_status()

            with self.status_update():
                if isinstance(raw_status, dict):
                    self.last_status = {
                        k: int(v)
                        for k, v in raw_status.items()
                        if isinstance(v, (int, float))
                    }

                    # Check for alerts
                    self._check_status_alerts()
                    return True
                else:
                    self.log_activity(
                        "Invalid status format received", ActivityLevel.WARNING
                    )
                    return False

        except Exception as e:
            self.log_activity(
                f"Status update failed: {str(e)}", ActivityLevel.ERROR, details=str(e)
            )
            return False

    def _check_status_alerts(self) -> None:
        """Check status for alert conditions"""
        failed = self.last_status.get("failed", 0)
        claimed = self.last_status.get("claimed", 0)

        # Failed jobs alert
        if failed >= self.config.alert_thresholds["failed_jobs"]:
            self.log_activity(f"High failed job count: {failed}", ActivityLevel.WARNING)

        # Stale claimed jobs (would need timestamp tracking for real implementation)
        if claimed >= self.config.alert_thresholds["stale_claimed"]:
            self.log_activity(
                f"High claimed job count: {claimed}", ActivityLevel.WARNING
            )

    def create_header(self) -> Panel:
        """Create enhanced header with state and metrics"""
        # State indicator
        state_colors = {
            GUIState.INITIALIZING: "yellow",
            GUIState.RUNNING: "green",
            GUIState.PAUSED: "yellow",
            GUIState.STOPPING: "red",
            GUIState.STOPPED: "red",
            GUIState.ERROR: "bright_red",
        }

        state_icons = {
            GUIState.INITIALIZING: "🔄",
            GUIState.RUNNING: "🟢",
            GUIState.PAUSED: "⏸️",
            GUIState.STOPPING: "⏹️",
            GUIState.STOPPED: "🔴",
            GUIState.ERROR: "❌",
        }

        state_text = f"{state_icons[self.state]} {self.state.value.upper()}"

        # Runtime and cycle info
        runtime = ""
        if self.start_time:
            elapsed = datetime.now() - self.start_time
            total_seconds = int(elapsed.total_seconds())
            runtime = f" | ⏱️ {total_seconds // 3600:02d}:{(total_seconds % 3600) // 60:02d}:{total_seconds % 60:02d}"

        cycle_info = ""
        if self.current_cycle:
            cycle_info = f" | 🔄 Cycle {self.current_cycle.cycle_number}"

        # Performance indicator
        perf_info = ""
        if self.performance_stats.peak_throughput > 0:
            perf_info = f" | ⚡ Peak: {self.performance_stats.peak_throughput:.1f}/s"

        header_text = Text.assemble(
            ("🏠 Match Host Work Manager", "bold cyan"),
            f" | [{state_colors[self.state]}]{state_text}[/]",
            runtime,
            cycle_info,
            perf_info,
            style="white",
        )

        return Panel(Align.center(header_text), box=box.ROUNDED, style="bright_blue")

    def create_status_panel(self) -> Panel:
        """Create enhanced status panel with trends"""
        with self.status_update():
            status = self.last_status.copy()

        table = Table(
            box=box.SIMPLE_HEAD, show_header=True, header_style="bold magenta"
        )
        table.add_column("Status", style="cyan", no_wrap=True)
        table.add_column("Count", justify="right", style="green", width=8)
        table.add_column("Trend", justify="center", width=6)
        table.add_column("Description", style="white")

        # Calculate trends (simplified - would need historical data)
        status_items = [
            ("Available", status.get("available", 0), "📈", "Ready to be claimed"),
            ("Claimed", status.get("claimed", 0), "➡️", "Currently being processed"),
            (
                "Completed",
                status.get("completed", 0),
                "📈",
                "Finished, awaiting processing",
            ),
            (
                "Failed",
                status.get("failed", 0),
                "⚠️" if status.get("failed", 0) > 0 else "✅",
                "Encountered errors",
            ),
            (
                "Processed",
                self.performance_stats.total_processed,
                "📈",
                "Successfully archived",
            ),
        ]

        for name, count, trend, desc in status_items:
            # Color coding
            if name == "Failed" and count > 0:
                count_style = "red"
            elif name == "Completed" and count > 0:
                count_style = "yellow"
            elif name == "Processed":
                count_style = "bright_green"
            else:
                count_style = "white"

            table.add_row(name, f"[{count_style}]{count:,}[/]", trend, desc)

        return Panel(
            table, title="📊 Work Status", box=box.ROUNDED, style="bright_green"
        )

    def create_progress_panel(self) -> Panel:
        """Create enhanced progress panel with performance metrics"""
        if not self.cycle_history:
            return Panel(
                Align.center("Initializing metrics...", vertical="middle"),
                title="📈 Performance Metrics",
                box=box.ROUNDED,
                style="bright_yellow",
            )

        content = []

        # Current cycle progress
        if self.current_cycle and self.current_cycle.start_time:
            elapsed = (datetime.now() - self.current_cycle.start_time).total_seconds()
            content.extend(
                [
                    f"🔄 Current Cycle: {self.current_cycle.cycle_number}",
                    f"⏱️  Duration: {elapsed:.1f}s",
                    f"📊 Throughput: {self.current_cycle.throughput:.2f}/s",
                    "",
                ]
            )

        # Overall statistics
        content.extend(
            [
                f"📋 Total Created: {self.performance_stats.total_created:,}",
                f"✅ Total Processed: {self.performance_stats.total_processed:,}",
                f"❌ Total Errors: {self.performance_stats.total_errors:,}",
                "",
            ]
        )

        # Performance metrics
        if len(self.cycle_history) >= 2:
            recent_cycles = self.cycle_history[-5:]
            avg_duration = sum(c.duration_seconds for c in recent_cycles) / len(
                recent_cycles
            )
            avg_throughput = sum(c.throughput for c in recent_cycles) / len(
                recent_cycles
            )

            content.extend(
                [
                    f"⚡ Avg Cycle Time: {avg_duration:.1f}s",
                    f"📊 Avg Throughput: {avg_throughput:.2f}/s",
                    f"🏆 Peak Throughput: {self.performance_stats.peak_throughput:.2f}/s",
                ]
            )

        return Panel(
            "\n".join(content),
            title="📈 Performance Metrics",
            box=box.ROUNDED,
            style="bright_yellow",
        )

    def create_activity_panel(self) -> Panel:
        """Create enhanced activity panel with filtering"""
        self._process_activity_queue()

        if not self.activity_history:
            return Panel(
                Align.center("No activity logged yet...", vertical="middle"),
                title="📝 Activity Log",
                box=box.ROUNDED,
                style="bright_cyan",
            )

        # Filter activities based on config
        activities = self.activity_history[-20:]  # Show last 20
        if not self.config.show_debug:
            activities = [a for a in activities if a.level != ActivityLevel.DEBUG]

        activity_lines = []
        for activity in reversed(activities):  # Most recent first
            timestamp = activity.timestamp.strftime("%H:%M:%S")

            # Level styling
            level_styles = {
                ActivityLevel.DEBUG: ("🔍", "dim"),
                ActivityLevel.INFO: ("ℹ️", "white"),
                ActivityLevel.SUCCESS: ("✅", "green"),
                ActivityLevel.WARNING: ("⚠️", "yellow"),
                ActivityLevel.ERROR: ("❌", "red"),
                ActivityLevel.CRITICAL: ("🚨", "bright_red"),
            }

            icon, style = level_styles.get(activity.level, ("ℹ️", "white"))

            # Truncate long messages
            message = activity.message
            if len(message) > 60:
                message = message[:57] + "..."

            activity_lines.append(f"[dim]{timestamp}[/] {icon} [{style}]{message}[/]")

        return Panel(
            "\n".join(activity_lines),
            title=f"📝 Activity Log ({len(self.activity_history)} total)",
            box=box.ROUNDED,
            style="bright_cyan",
        )

    def create_stats_panel(self) -> Panel:
        """Create detailed statistics panel"""
        content = [
            f"🌍 Environment: {self.manager.environment}",
            f"📡 State: {self.state.value}",
            "",
        ]

        # Uptime and performance
        if self.start_time:
            uptime = datetime.now() - self.start_time
            uptime_str = str(uptime).split(".")[0]  # Remove microseconds
            content.extend(
                [
                    f"⏱️  Uptime: {uptime_str}",
                    f"🔄 Total Cycles: {self.performance_stats.total_cycles}",
                    "",
                ]
            )

        # Error rate
        if self.performance_stats.total_processed > 0:
            error_rate = (
                self.performance_stats.total_errors
                / self.performance_stats.total_processed
                * 100
            )
            content.append(f"📊 Error Rate: {error_rate:.2f}%")

        # Memory usage (simplified)
        cycle_memory = len(self.cycle_history)
        activity_memory = len(self.activity_history)
        content.extend(
            [
                "",
                f"💾 Cycle History: {cycle_memory}/{self.config.max_cycle_history}",
                f"💾 Activity History: {activity_memory}/{self.config.max_activity_history}",
            ]
        )

        return Panel(
            "\n".join(content),
            title="📊 System Stats",
            box=box.ROUNDED,
            style="bright_magenta",
        )

    def create_alerts_panel(self) -> Panel:
        """Create alerts panel for important notifications"""
        alerts = []

        # Check for various alert conditions
        with self.status_update():
            failed_count = self.last_status.get("failed", 0)
            claimed_count = self.last_status.get("claimed", 0)

        if failed_count >= self.config.alert_thresholds["failed_jobs"]:
            alerts.append(f"⚠️  {failed_count} failed jobs need attention")

        if claimed_count >= self.config.alert_thresholds["stale_claimed"]:
            alerts.append(f"🕐 {claimed_count} jobs claimed (check for stale)")

        if (
            self.performance_stats.error_rate
            > self.config.alert_thresholds["error_rate"]
        ):
            alerts.append(
                f"📈 High error rate: {self.performance_stats.error_rate:.1f}%"
            )

        if not alerts:
            alerts.append("✅ All systems normal")

        content = "\n".join(alerts[-5:])  # Show last 5 alerts

        return Panel(
            content,
            title="🚨 Alerts",
            box=box.ROUNDED,
            style="bright_red" if len(alerts) > 1 else "green",
        )

    def create_footer(self) -> Panel:
        """Create footer with enhanced controls"""
        controls = []

        if self.state == GUIState.RUNNING:
            controls.extend([("P", "Pause"), ("Ctrl+C", "Stop")])
        elif self.state == GUIState.PAUSED:
            controls.extend([("R", "Resume"), ("Ctrl+C", "Stop")])
        else:
            controls.append(("Ctrl+C", "Exit"))

        controls.append(("E", "Export"))

        control_text = Text.assemble(
            *[
                item
                for key, desc in controls
                for item in [(key, "bold yellow"), (f" {desc} | ", "white")]
            ][:-1]
        )

        return Panel(Align.center(control_text), box=box.ROUNDED, style="bright_blue")

    def update_layout(self) -> None:
        """Update all layout components"""
        try:
            self.layout["header"].update(self.create_header())
            self.layout["status"].update(self.create_status_panel())
            self.layout["progress"].update(self.create_progress_panel())
            self.layout["activity"].update(self.create_activity_panel())
            self.layout["stats"].update(self.create_stats_panel())
            self.layout["alerts"].update(self.create_alerts_panel())
            self.layout["footer"].update(self.create_footer())
        except Exception as e:
            self.logger.error(f"Layout update failed: {e}")

    def export_metrics(self) -> None:
        """Export metrics to JSON file"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"match_work_metrics_{timestamp}.json"

            export_data = {
                "timestamp": datetime.now().isoformat(),
                "performance_stats": asdict(self.performance_stats),
                "cycle_history": [asdict(cycle) for cycle in self.cycle_history],
                "recent_activities": [
                    {
                        "timestamp": entry.timestamp.isoformat(),
                        "level": entry.level.value,
                        "message": entry.message,
                        "source": entry.source,
                    }
                    for entry in self.activity_history[-50:]
                ],
                "final_status": self.last_status,
            }

            with open(filename, "w") as f:
                json.dump(export_data, f, indent=2, default=str)

            self.log_activity(f"Metrics exported to {filename}", ActivityLevel.SUCCESS)

        except Exception as e:
            self.log_activity(f"Export failed: {str(e)}", ActivityLevel.ERROR)

    def graceful_shutdown(self) -> None:
        """Initiate graceful shutdown"""
        if self.state not in [GUIState.STOPPING, GUIState.STOPPED]:
            self.state = GUIState.STOPPING
            self.log_activity("Initiating graceful shutdown...", ActivityLevel.WARNING)
            self.shutdown_event.set()

            # Export metrics on shutdown
            if self.config.export_metrics:
                self.export_metrics()

    def run_work_cycles(self, max_cycles: int) -> None:
        """Enhanced work cycle execution with better error handling"""
        self.state = GUIState.RUNNING
        self.start_time = datetime.now()

        self.log_activity(f"Starting {max_cycles} work cycles", ActivityLevel.SUCCESS)

        try:
            for cycle_num in range(1, max_cycles + 1):
                if self.shutdown_event.is_set():
                    break

                # Handle pause
                while self.pause_event.is_set() and not self.shutdown_event.is_set():
                    self.state = GUIState.PAUSED
                    time.sleep(0.5)

                if self.shutdown_event.is_set():
                    break

                self.state = GUIState.RUNNING

                # Start new cycle
                cycle = CycleMetrics(cycle_number=cycle_num, start_time=datetime.now())
                self.current_cycle = cycle

                try:
                    self.log_activity(
                        f"Starting cycle {cycle_num}/{max_cycles}", ActivityLevel.INFO
                    )

                    # Update status
                    if not self.update_status():
                        cycle.errors += 1

                    # Work processing logic
                    with self.status_update():
                        available = self.last_status.get("available", 0)

                    # Create work if needed
                    if available < 5:
                        created = self.manager.create_match_work_orders(limit=10)
                        cycle.created = created
                        self.performance_stats.total_created += created

                        if created > 0:
                            self.log_activity(
                                f"Created {created} work orders", ActivityLevel.SUCCESS
                            )

                    # Process completed work
                    processed = self.manager.process_completed_match_work()
                    cycle.processed = processed
                    self.performance_stats.total_processed += processed

                    if processed > 0:
                        self.log_activity(
                            f"Processed {processed} completed items",
                            ActivityLevel.SUCCESS,
                        )

                    # Final status update
                    self.update_status()
                    with self.status_update():
                        cycle.available = self.last_status.get("available", 0)
                        cycle.claimed = self.last_status.get("claimed", 0)
                        cycle.completed = self.last_status.get("completed", 0)
                        cycle.failed = self.last_status.get("failed", 0)

                except Exception as e:
                    cycle.errors += 1
                    self.performance_stats.total_errors += 1
                    self.log_activity(
                        f"Cycle {cycle_num} error: {str(e)}",
                        ActivityLevel.ERROR,
                        details=str(e),
                    )

                finally:
                    # Complete cycle metrics
                    cycle.end_time = datetime.now()
                    cycle.duration_seconds = (
                        cycle.end_time - cycle.start_time
                    ).total_seconds()

                    # Update performance stats
                    self.performance_stats.total_cycles += 1
                    if cycle.throughput > self.performance_stats.peak_throughput:
                        self.performance_stats.peak_throughput = cycle.throughput

                    # Store cycle history
                    self.cycle_history.append(cycle)
                    if len(self.cycle_history) > self.config.max_cycle_history:
                        self.cycle_history = self.cycle_history[
                            -self.config.max_cycle_history :
                        ]

                    self.current_cycle = None

                # Check for completion
                if (
                    cycle.created == 0
                    and cycle.processed == 0
                    and cycle.claimed == 0
                    and cycle.available == 0
                ):
                    self.log_activity("All work completed!", ActivityLevel.SUCCESS)
                    break

                # Sleep between cycles
                time.sleep(1)

        finally:
            self.state = GUIState.STOPPED
            summary = (
                f"Completed: {self.performance_stats.total_created} created, "
                f"{self.performance_stats.total_processed} processed, "
                f"{self.performance_stats.total_errors} errors"
            )
            self.log_activity(summary, ActivityLevel.SUCCESS)

    def run(self, max_cycles: int = 100) -> None:
        """Main GUI execution with enhanced error handling"""
        try:
            # Start work thread
            self.work_thread = threading.Thread(
                target=self.run_work_cycles,
                args=(max_cycles,),
                daemon=True,
                name="WorkThread",
            )
            self.work_thread.start()

            # Start GUI
            with Live(
                self.layout, refresh_per_second=self.config.refresh_rate, screen=True
            ) as live:
                self.live = live

                while self.work_thread.is_alive() or self.state not in [
                    GUIState.STOPPED,
                    GUIState.ERROR,
                ]:
                    if self.shutdown_event.is_set():
                        break

                    try:
                        self.update_layout()
                        time.sleep(1.0 / self.config.refresh_rate)
                    except Exception as e:
                        self.logger.error(f"GUI update error: {e}")
                        time.sleep(1.0)

                # Final update
                self.update_layout()
                time.sleep(2)  # Show final state

        except KeyboardInterrupt:
            self.graceful_shutdown()
        except Exception as e:
            self.state = GUIState.ERROR
            self.log_activity(f"Critical GUI error: {str(e)}", ActivityLevel.CRITICAL)
            self.logger.critical(f"GUI crashed: {e}", exc_info=True)
        finally:
            self.cleanup()

    def cleanup(self) -> None:
        """Cleanup resources"""
        try:
            self.log_activity("Cleaning up resources...", ActivityLevel.INFO)

            # Stop threads
            self.shutdown_event.set()

            # Wait for work thread to complete
            if self.work_thread and self.work_thread.is_alive():
                self.work_thread.join(timeout=5)
                if self.work_thread.is_alive():
                    self.logger.warning("Work thread did not terminate gracefully")

            # Cleanup manager
            if hasattr(self.manager, "cleanup"):
                self.manager.cleanup()

            # Final metrics export
            if self.config.export_metrics and self.cycle_history:
                self.export_metrics()

            self.logger.info("Cleanup completed")

        except Exception as e:
            self.logger.error(f"Cleanup error: {e}")


def load_config(config_path: Optional[str] = None) -> GUIConfig:
    """Load configuration from file or create default"""
    if config_path and Path(config_path).exists():
        try:
            with open(config_path, "r") as f:
                config_data = json.load(f)
            return GUIConfig(**config_data)
        except Exception as e:
            logging.warning(f"Failed to load config from {config_path}: {e}")

    return GUIConfig()


def save_config(config: GUIConfig, config_path: str) -> None:
    """Save configuration to file"""
    try:
        with open(config_path, "w") as f:
            json.dump(asdict(config), f, indent=2)
    except Exception as e:
        logging.error(f"Failed to save config to {config_path}: {e}")


class HealthMonitor:
    """Health monitoring for the application"""

    def __init__(self, gui: MatchWorkGUI):
        self.gui = gui
        self.checks: Dict[str, Callable[[], bool]] = {
            "manager_responsive": self._check_manager_responsive,
            "status_updates": self._check_status_updates,
            "thread_health": self._check_thread_health,
            "memory_usage": self._check_memory_usage,
        }
        self.last_check = datetime.now()
        self.check_interval = timedelta(minutes=1)

    def _check_manager_responsive(self) -> bool:
        """Check if manager is responding"""
        try:
            self.gui.manager.get_match_work_status()
            return True
        except Exception:
            return False

    def _check_status_updates(self) -> bool:
        """Check if status is being updated regularly"""
        if not hasattr(self.gui, "performance_stats"):
            return True

        last_activity = self.gui.performance_stats.last_activity
        if last_activity:
            silence_duration = datetime.now() - last_activity
            return silence_duration < timedelta(minutes=5)
        return True

    def _check_thread_health(self) -> bool:
        """Check if threads are healthy"""
        return (
            not self.gui.work_thread
            or self.gui.work_thread.is_alive()
            or self.gui.state in [GUIState.STOPPED, GUIState.STOPPING]
        )

    def _check_memory_usage(self) -> bool:
        """Check memory usage (simplified)"""
        return (
            len(self.gui.cycle_history) <= self.gui.config.max_cycle_history
            and len(self.gui.activity_history) <= self.gui.config.max_activity_history
        )

    def run_checks(self) -> Dict[str, bool]:
        """Run all health checks"""
        if datetime.now() - self.last_check < self.check_interval:
            return {}

        results = {}
        for check_name, check_func in self.checks.items():
            try:
                results[check_name] = check_func()
                if not results[check_name]:
                    self.gui.log_activity(
                        f"Health check failed: {check_name}", ActivityLevel.WARNING
                    )
            except Exception as e:
                results[check_name] = False
                self.gui.log_activity(
                    f"Health check error {check_name}: {e}", ActivityLevel.ERROR
                )

        self.last_check = datetime.now()
        return results


def create_default_config_file() -> str:
    """Create a default configuration file"""
    config_path = "configurations/match_host_controller_config.json"
    default_config = GUIConfig(
        refresh_rate=2.0,
        max_activity_history=200,
        max_cycle_history=100,
        show_debug=False,
        log_file="logs/match_host_controller.log",
        export_metrics=True,
        alert_thresholds={"error_rate": 5, "failed_jobs": 10, "stale_claimed": 30},
    )

    save_config(default_config, config_path)
    return config_path


def main() -> None:
    """Enhanced main function with better argument handling and setup"""
    parser = argparse.ArgumentParser(
        description="Production-ready Match Host Work Manager GUI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --repo-url https://github.com/blecktita/savvyo-work-distribution.git --environment production
  %(prog)s --config custom_config.json --max-cycles 50 --debug
  %(prog)s --create-config  # Create default configuration file
        """,
    )

    # Core arguments
    parser.add_argument("--repo-url", help="GitHub repository URL")
    parser.add_argument(
        "--environment",
        default="production",
        choices=["development", "staging", "production"],
        help="Environment to run against",
    )
    parser.add_argument(
        "--max-cycles",
        type=int,
        default=100,
        help="Maximum number of processing cycles",
    )

    # Configuration arguments
    parser.add_argument("--config", help="Path to JSON configuration file")
    parser.add_argument(
        "--create-config",
        action="store_true",
        help="Create default configuration file and exit",
    )
    parser.add_argument(
        "--debug", action="store_true", help="Enable debug logging and display"
    )

    # GUI arguments
    parser.add_argument(
        "--refresh-rate", type=float, default=2.0, help="GUI refresh rate (Hz)"
    )
    parser.add_argument(
        "--no-export", action="store_true", help="Disable metrics export on shutdown"
    )
    parser.add_argument("--log-file", help="Log file path")

    args = parser.parse_args()

    # Handle config creation
    if args.create_config:
        config_path = create_default_config_file()
        print(f"Created default configuration file: {config_path}")
        sys.exit(0)

    # Load configuration
    config = load_config(args.config)

    # Override config with command line arguments
    if args.debug:
        config.show_debug = True
    if args.refresh_rate != 2.0:
        config.refresh_rate = args.refresh_rate
    if args.no_export:
        config.export_metrics = False
    if args.log_file:
        config.log_file = args.log_file

    # Validate required arguments
    if not args.repo_url:
        parser.error("--repo-url is required")

    try:
        # Create the manager
        print("Initializing Match Host Work Manager...")
        manager = MatchHostWorkManager(
            environment=args.environment, repo_url=args.repo_url
        )

        # Create and run GUI
        print("Starting GUI...")
        gui = MatchWorkGUI(manager, config)

        # Add health monitoring
        health_monitor = HealthMonitor(gui)

        # Start the GUI
        gui.run(args.max_cycles)

    except KeyboardInterrupt:
        print("\nShutdown requested by user")
        sys.exit(0)
    except Exception as e:
        print(f"Fatal error: {e}")
        logging.critical(f"Application crashed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
