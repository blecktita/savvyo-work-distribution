# logger/sourcing_logger.py

import logging
import logging.handlers
import os
import sys
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union


class IPRotationEvent:
    """
    Represents a VPN IP rotation event
    """

    def __init__(
        self,
        timestamp: str,
        old_ip: str,
        new_ip: str,
        request_count: int,
        rotation_forced: bool,
    ):
        self.timestamp = timestamp
        self.old_ip = old_ip
        self.new_ip = new_ip
        self.request_count = request_count
        self.rotation_forced = rotation_forced


class DataSourcingLogger:
    """
    Logger system for VPN-protected data sourcing operations
    """

    _instance: Optional["DataSourcingLogger"] = None
    _lock: threading.Lock = threading.Lock()

    def __new__(cls, *args: Any, **kwargs: Any) -> "DataSourcingLogger":
        """
        Singleton pattern - one logger instance per process
        """
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(DataSourcingLogger, cls).__new__(cls)
        return cls._instance

    def __init__(
        self,
        component_name: str = "ScraperSystem",
        console_level: str = "INFO",
        log_dir: str = "logs",
    ) -> None:
        if hasattr(self, "_initialized"):
            return

        self.component_name: str = component_name
        self.log_dir: str = log_dir
        self._start_time: float = time.time()
        self._operation_counters: Dict[str, int] = {}
        self._last_progress_time: Dict[str, float] = {}
        self._trace_counter: int = 0

        # VPN-specific tracking with proper types
        self._vpn_stats: Dict[
            str, Union[int, float, str, List[IPRotationEvent], None]
        ] = {
            "total_rotations": 0,
            "requests_on_current_ip": 0,
            "current_ip": None,
            "rotation_history": [],
            "success_rate": 100.0,
            "time_on_current_ip_start": None,
        }

        # Create logs directory
        os.makedirs(log_dir, exist_ok=True)

        # Setup dual logging system
        self._setup_console_logger(console_level)
        self._setup_structured_logger()
        self._initialized: bool = True

    def _setup_console_logger(self, console_level: str) -> None:
        """
        Setup clean console logging for human monitoring
        """
        self.console_logger: logging.Logger = logging.getLogger(
            f"{self.component_name}_console"
        )
        self.console_logger.setLevel(getattr(logging, console_level.upper()))
        self.console_logger.handlers.clear()

        console_handler: logging.StreamHandler[Any] = logging.StreamHandler(sys.stdout)
        console_formatter: logging.Formatter = logging.Formatter(
            "%(asctime)s | %(levelname)-5s | %(message)s", datefmt="%H:%M:%S"
        )
        console_handler.setFormatter(console_formatter)
        self.console_logger.addHandler(console_handler)

    def _setup_structured_logger(self) -> None:
        """
        Setup structured table logging for operational monitoring
        """
        self.structured_logger: logging.Logger = logging.getLogger(
            f"{self.component_name}_structured"
        )
        self.structured_logger.setLevel(logging.DEBUG)
        self.structured_logger.handlers.clear()

        # Main structured log file - neat tables
        file_handler: logging.handlers.RotatingFileHandler = (
            logging.handlers.RotatingFileHandler(
                filename=os.path.join(
                    self.log_dir, f"{self.component_name.lower()}_operations.log"
                ),
                maxBytes=100 * 1024 * 1024,  # 100MB
                backupCount=5,
            )
        )
        file_formatter: logging.Formatter = logging.Formatter(
            "%(message)s"
        )  # Raw table output
        file_handler.setFormatter(file_formatter)
        self.structured_logger.addHandler(file_handler)

        # VPN-specific operations log - detailed tables
        vpn_handler: logging.handlers.RotatingFileHandler = (
            logging.handlers.RotatingFileHandler(
                filename=os.path.join(
                    self.log_dir, f"{self.component_name.lower()}_vpn_detailed.log"
                ),
                maxBytes=50 * 1024 * 1024,
                backupCount=3,
            )
        )
        vpn_handler.setFormatter(file_formatter)
        self.structured_logger.addHandler(vpn_handler)

    def _generate_trace_id(self) -> str:
        """
        Generate trace ID for request tracking
        """
        self._trace_counter += 1
        return f"{self.component_name.lower()}_{int(time.time())}_{self._trace_counter}"

    def _log_structured(
        self, level: str, event_type: str, message: str, **data: Any
    ) -> None:
        """
        Log structured data as neat tables
        """
        timestamp: str = datetime.now(timezone.utc).strftime("%H:%M:%S")
        trace_id: str = data.get("trace_id", self._generate_trace_id())

        # Create a neat table entry
        table_entry: str = self._format_table_entry(
            timestamp, level, event_type, message, trace_id, **data
        )

        # Log to structured file
        self.structured_logger.info(table_entry)

    def _format_table_entry(
        self,
        timestamp: str,
        level: str,
        event_type: str,
        message: str,
        trace_id: str,
        **data: Any,
    ) -> str:
        """
        Format data as a neat table row
        """
        # Standard columns
        base_cols: str = f"{timestamp} | {level:<8} | {event_type:<25} | {message:<40}"

        # Add key data columns based on event type
        if event_type.startswith("vpn_"):
            return self._format_vpn_table_entry(base_cols, **data)
        elif event_type == "request_delay_applied":
            return self._format_request_table_entry(base_cols, **data)
        elif event_type == "processing_progress":
            return self._format_progress_table_entry(base_cols, **data)
        elif event_type == "statistics_collected":
            return self._format_stats_table_entry(base_cols, **data)
        else:
            # Generic format
            extra_data: str = " | ".join(
                f"{k}={v}"
                for k, v in data.items()
                if k not in ["trace_id"] and v is not None
            )
            return f"{base_cols} | {extra_data}" if extra_data else base_cols

    def _format_vpn_table_entry(self, base_cols: str, **data: Any) -> str:
        """
        Format VPN-specific table entries
        """
        vpn_cols: List[str] = []

        if "current_ip" in data and data["current_ip"]:
            vpn_cols.append(f"IP={data['current_ip']:<15}")
        if "requests_on_current_ip" in data:
            vpn_cols.append(f"Reqs={data['requests_on_current_ip']:>3}")
        if "duration_seconds" in data:
            vpn_cols.append(f"Duration={data['duration_seconds']:>6.1f}s")
        if "total_rotations" in data:
            vpn_cols.append(f"Rotations={data['total_rotations']:>3}")
        if "success_rate" in data:
            vpn_cols.append(f"Success={data['success_rate']:>5.1f}%")

        vpn_info: str = " | ".join(vpn_cols) if vpn_cols else ""
        return f"{base_cols} | {vpn_info}" if vpn_info else base_cols

    def _format_request_table_entry(self, base_cols: str, **data: Any) -> str:
        """
        Format request timing table entries
        """
        req_cols: List[str] = []

        if "operation" in data:
            req_cols.append(f"Op={data['operation']:<20}")
        if "delay_seconds" in data:
            req_cols.append(f"Delay={data['delay_seconds']:>5.1f}s")
        if "delay_type" in data:
            req_cols.append(f"Type={data['delay_type']:<10}")
        if "requests_on_current_ip" in data:
            req_cols.append(f"IP_Reqs={data['requests_on_current_ip']:>3}")

        req_info: str = " | ".join(req_cols) if req_cols else ""
        return f"{base_cols} | {req_info}" if req_info else base_cols

    def _format_progress_table_entry(self, base_cols: str, **data: Any) -> str:
        """
        Format progress tracking table entries
        """
        prog_cols: List[str] = []

        if "current" in data and "total" in data and data["total"]:
            pct: float = (data["current"] / data["total"]) * 100
            prog_cols.append(
                f"Progress={data['current']:>6}/{data['total']:<6} ({pct:>5.1f}%)"
            )
        elif "current" in data:
            prog_cols.append(f"Processed={data['current']:>6}")

        prog_info: str = " | ".join(prog_cols) if prog_cols else ""
        return f"{base_cols} | {prog_info}" if prog_info else base_cols

    def _format_stats_table_entry(self, base_cols: str, **data: Any) -> str:
        """
        Format statistics table entries with key metrics
        """
        if "statistics" in data and isinstance(data["statistics"], dict):
            stats: Dict[str, Any] = data["statistics"]  # type: ignore
            stat_cols: List[str] = []

            # Key VPN stats
            if "vpn_protection" in stats:
                stat_cols.append(f"VPN={stats['vpn_protection']:<8}")
            if "current_ip" in stats:
                stat_cols.append(f"IP={stats['current_ip']:<15}")
            if "requests_on_current_ip" in stats:
                stat_cols.append(f"Reqs={stats['requests_on_current_ip']:>3}")
            if "total_rotations" in stats:
                stat_cols.append(f"Rotations={stats['total_rotations']:>3}")
            if "rotation_success_rate" in stats:
                stat_cols.append(f"Success={stats['rotation_success_rate']:>5.1f}%")
            if "time_on_current_ip" in stats:
                stat_cols.append(f"IPTime={stats['time_on_current_ip']:>6.1f}s")

            stat_info: str = " | ".join(stat_cols) if stat_cols else ""
            return f"{base_cols} | {stat_info}" if stat_info else base_cols

        return base_cols

    def _log_console(self, level: str, message: str) -> None:
        """
        Log clean message to console
        """
        console_level: int = getattr(logging, level.upper())
        self.console_logger.log(console_level, message)

    # === SYSTEM LIFECYCLE ===

    def component_startup(
        self, config: Optional[Dict[str, Any]] = None, **context: Any
    ) -> None:
        """
        Log component startup (like your original)
        """
        startup_time: str = datetime.now(timezone.utc).isoformat()

        # Structured logging
        self._log_structured(
            "INFO",
            "component_startup",
            f"{self.component_name} starting up",
            config=config or {},
            startup_time=startup_time,
            **context,
        )

        # Clean console
        config_str: str = (
            f"env={config.get('environment', 'unknown')}" if config else ""
        )
        self._log_console(
            "INFO", f"🚀 {self.component_name} starting up | {config_str}"
        )

    def component_shutdown(
        self,
        reason: str = "normal",
        final_stats: Optional[Dict[str, Any]] = None,
        **context: Any,
    ) -> None:
        """
        Log component shutdown with full stats
        """
        uptime_seconds: float = time.time() - self._start_time

        # Structured logging
        self._log_structured(
            "INFO",
            "component_shutdown",
            f"{self.component_name} shutting down",
            reason=reason,
            uptime_seconds=uptime_seconds,
            final_stats=final_stats or {},
            **context,
        )

        # Clean console
        self._log_console(
            "INFO",
            f"🏁 {self.component_name} stopped | reason={reason} | uptime={uptime_seconds:.0f}s",
        )

    # === VPN OPERATIONS ===

    def vpn_initialization_started(self, config_use_vpn: bool, **context: Any) -> None:
        """
        Log VPN initialization start
        """
        self._log_structured(
            "INFO",
            "vpn_initialization_started",
            "VPN manager initialization started",
            use_vpn=config_use_vpn,
            **context,
        )

        self._log_console("INFO", f"🔄 VPN initializing | use_vpn={config_use_vpn}")

    def vpn_initialization_completed(
        self,
        vpn_enabled: bool,
        duration_seconds: float,
        current_ip: Optional[str] = None,
        **context: Any,
    ) -> None:
        """
        Log successful VPN initialization
        """
        self._vpn_stats["current_ip"] = current_ip
        self._vpn_stats["time_on_current_ip_start"] = time.time()

        self._log_structured(
            "INFO",
            "vpn_initialization_completed",
            "VPN manager initialized successfully",
            vpn_enabled=vpn_enabled,
            duration_seconds=duration_seconds,
            current_ip=current_ip,
            **context,
        )

        ip_str: str = f"ip={current_ip}" if current_ip else ""
        self._log_console(
            "INFO", f"✅ VPN connected | {ip_str} | {duration_seconds:.1f}s"
        )

    def vpn_rotation_started(
        self, trigger_reason: str, current_ip: str, request_count: int, **context: Any
    ) -> None:
        """
        Log VPN rotation start
        """
        self._log_structured(
            "INFO",
            "vpn_rotation_started",
            "VPN rotation started",
            trigger_reason=trigger_reason,
            current_ip=current_ip,
            requests_on_current_ip=request_count,
            **context,
        )

        reason_map: Dict[str, str] = {
            "quota_exceeded": "📊 Quota exceeded",
            "security_violation": "🚨 Security violation",
            "manual_rotation": "🔄 Manual rotation",
            "scheduled_rotation": "⏰ Scheduled rotation",
        }

        reason_emoji: str = reason_map.get(trigger_reason, "🔄 Rotation")
        self._log_console(
            "INFO", f"{reason_emoji} | from_ip={current_ip} | requests={request_count}"
        )

    def vpn_rotation_completed(
        self,
        duration_seconds: float,
        new_ip: str,
        old_ip: str,
        request_count: Optional[int] = None,
        **context: Any,
    ) -> None:
        """
        Log successful VPN rotation
        """
        # Update stats
        current_rotations = self._vpn_stats["total_rotations"]
        if isinstance(current_rotations, (int, float)):
            self._vpn_stats["total_rotations"] = int(current_rotations) + 1
        else:
            self._vpn_stats["total_rotations"] = (
                1  # fallback if somehow it's not a number
            )

        self._vpn_stats["current_ip"] = new_ip
        self._vpn_stats["requests_on_current_ip"] = 0
        self._vpn_stats["time_on_current_ip_start"] = time.time()

        # Track rotation history
        rotation_event = IPRotationEvent(
            timestamp=datetime.now(timezone.utc).isoformat(),
            old_ip=old_ip,
            new_ip=new_ip,
            request_count=request_count or 0,
            rotation_forced=True,
        )

        rotation_history = self._vpn_stats["rotation_history"]
        if isinstance(rotation_history, list):
            rotation_history.append(rotation_event)

            # Keep only last 10 rotations
            if len(rotation_history) > 10:
                self._vpn_stats["rotation_history"] = rotation_history[-10:]

        self._log_structured(
            "INFO",
            "vpn_rotation_completed",
            "VPN rotation completed successfully",
            duration_seconds=duration_seconds,
            new_ip=new_ip,
            old_ip=old_ip,
            request_count=request_count,
            total_rotations=self._vpn_stats["total_rotations"],
            **context,
        )

        self._log_console(
            "INFO", f"✅ VPN rotated | {old_ip} → {new_ip} | {duration_seconds:.1f}s"
        )

    def request_timing_started(
        self, operation: str, vpn_protection_active: bool, **context: Any
    ) -> None:
        """
        Log request timing check start
        """
        self._log_structured(
            "INFO",
            "request_timing_started",
            "Request timing check started",
            operation=operation,
            vpn_protection_active=vpn_protection_active,
            **context,
        )

        # Only log to console if VPN protection is inactive (potential issue)
        if not vpn_protection_active:
            self._log_console(
                "WARNING",
                f"⚠️ Request timing | operation={operation} | vpn_protection=FALSE",
            )

    def request_delay_applied(
        self, operation: str, delay_seconds: float, delay_type: str, **context: Any
    ) -> None:
        """
        Log request delay application
        """
        # Update request counter
        current_requests = self._vpn_stats["requests_on_current_ip"]
        if isinstance(current_requests, (int, float)):
            self._vpn_stats["requests_on_current_ip"] = int(current_requests) + 1
        else:
            self._vpn_stats["requests_on_current_ip"] = (
                1  # fallback if somehow it's not a number
            )

        level: str = "WARNING" if delay_seconds > 10 else "INFO"
        self._log_structured(
            level,
            "request_delay_applied",
            "Request delay applied",
            operation=operation,
            delay_seconds=delay_seconds,
            delay_type=delay_type,
            requests_on_current_ip=self._vpn_stats["requests_on_current_ip"],
            **context,
        )

        # Only show long delays in console
        if delay_seconds > 5:
            self._log_console(
                "WARNING", f"⏳ Delay {delay_seconds:.1f}s | {operation} | {delay_type}"
            )

    def ip_quota_exceeded(
        self, current_ip: str, request_count: int, max_requests: int, **context: Any
    ) -> None:
        """
        Log IP quota exceeded
        """
        quota_utilization: float = (
            request_count / max_requests if max_requests > 0 else 0
        )

        self._log_structured(
            "WARNING",
            "ip_quota_exceeded",
            "IP request quota exceeded - rotation needed",
            current_ip=current_ip,
            request_count=request_count,
            max_requests=max_requests,
            quota_utilization=quota_utilization,
            **context,
        )

        self._log_console(
            "WARNING",
            f"📊 Quota exceeded | {current_ip} | {request_count}/{max_requests}",
        )

    # === STATISTICS & MONITORING ===

    def rotation_success_rate_metric(
        self, success_rate: float, total_rotations: int, **context: Any
    ) -> None:
        """
        Log VPN rotation success rate
        """
        self._vpn_stats["success_rate"] = success_rate

        level: str = (
            "ERROR" if success_rate < 80 else "WARNING" if success_rate < 95 else "INFO"
        )
        self._log_structured(
            level,
            "rotation_success_rate_metric",
            "VPN rotation success rate metric",
            success_rate=success_rate,
            total_rotations=total_rotations,
            **context,
        )

        if success_rate < 95:  # Only show problems in console
            self._log_console(
                level,
                f"📈 VPN success rate: {success_rate:.1f}% | rotations={total_rotations}",
            )

    def statistics_collected(
        self, stats_type: str, statistics: Dict[str, Any], **context: Any
    ) -> None:
        """
        Log statistics collection with neat table format
        """
        # Always log the structured table entry
        self._log_structured(
            "INFO",
            "statistics_collected",
            f"Stats: {stats_type}",
            stats_type=stats_type,
            statistics=statistics,
            **context,
        )

        # Also create a detailed stats table for easy scanning
        self._log_detailed_stats_table(stats_type, statistics)

        # Console only shows issues
        if (
            stats_type == "basic_vpn_stats"
            and statistics.get("vpn_protection") != "ACTIVE"
        ):
            self._log_console(
                "ERROR",
                f"🚨 VPN protection: {statistics.get('vpn_protection', 'UNKNOWN')}",
            )

    def _log_detailed_stats_table(
        self, stats_type: str, statistics: Dict[str, Any]
    ) -> None:
        """
        Create a detailed, easy-to-scan stats table
        """
        timestamp: str = datetime.now(timezone.utc).strftime("%H:%M:%S")

        # Create a formatted stats table
        table: List[str] = [
            f"\n{'=' * 80}",
            f"STATS SNAPSHOT - {stats_type.upper()} - {timestamp}",
            f"{'=' * 80}",
        ]

        if stats_type == "basic_vpn_stats" or stats_type == "comprehensive_vpn_stats":
            table.extend(
                [
                    f"VPN Protection    : {statistics.get('vpn_protection', 'UNKNOWN'):<10}",
                    f"Current IP        : {statistics.get('current_ip', 'None'):<15}",
                    f"Requests on IP    : {statistics.get('requests_on_current_ip', 0):>3} / {statistics.get('max_requests_per_ip', 10):>3}",
                    f"Total Rotations   : {statistics.get('total_rotations', 0):>3}",
                    f"Success Rate      : {statistics.get('rotation_success_rate', 0):>5.1f}%",
                    f"Time on Current IP: {statistics.get('time_on_current_ip', 0):>6.1f}s",
                ]
            )

            # Add security details if comprehensive
            if (
                stats_type == "comprehensive_vpn_stats"
                and "security_details" in statistics
            ):
                sec: Dict[str, Any] = statistics["security_details"]
                table.extend(
                    [
                        f"{'─' * 40}",
                        f"Monitoring Active : {sec.get('monitoring_active', False)}",
                        f"Reboot Required   : {sec.get('reboot_required', False)}",
                        f"Recent Alerts     : {sec.get('recent_alerts', 0):>3}",
                        f"Rotation Config   : {sec.get('rotation_callback_configured', False)}",
                        f"Rotation InProgress: {sec.get('rotation_in_progress', False)}",
                    ]
                )
        else:
            # Generic stats format
            for key, value in statistics.items():
                table.append(f"{key:<18}: {value}")

        table.append(f"{'=' * 80}\n")

        # Log the complete table
        self.structured_logger.info("\n".join(table))

    def rotation_history_recorded(
        self,
        rotation_count: int,
        recent_rotations: Optional[List[Union[str, Dict[str, Any]]]] = None,
        **context: Any,
    ) -> None:
        """
        Log rotation history with neat table format
        """
        self._log_structured(
            "INFO",
            "rotation_history_recorded",
            "Rotation history recorded",
            total_rotations=rotation_count,
            recent_rotations_count=len(recent_rotations) if recent_rotations else 0,
            **context,
        )

        # Create a rotation history table
        if recent_rotations:
            self._log_rotation_history_table(recent_rotations)

    def _log_rotation_history_table(
        self, recent_rotations: List[Union[str, Dict[str, Any]]]
    ) -> None:
        """
        Create a detailed rotation history table
        """
        timestamp: str = datetime.now(timezone.utc).strftime("%H:%M:%S")

        table: List[str] = [
            f"\n{'=' * 100}",
            f"VPN ROTATION HISTORY - {timestamp}",
            f"{'=' * 100}",
            f"{'Time':<8} | {'From IP':<15} | {'To IP':<15} | {'Requests':<8} | {'Forced':<6} | {'Reason':<20}",
            f"{'-' * 100}",
        ]

        for rotation in recent_rotations[-10:]:  # Last 10 rotations
            if isinstance(rotation, str):
                # Parse string format from your original logger
                # Example: "IPRotationEvent(timestamp=datetime.datetime(2025, 8, 1, 10, 23, 43, 105318), old_ip='185.255.129.245', new_ip='103.1.212.84', request_count=8, rotation_forced=True)"
                import re

                try:
                    timestamp_match = re.search(
                        r"datetime\.datetime\([^)]+\)", rotation
                    )
                    old_ip_match = re.search(r"old_ip='([^']+)'", rotation)
                    new_ip_match = re.search(r"new_ip='([^']+)'", rotation)
                    count_match = re.search(r"request_count=(\d+)", rotation)
                    forced_match = re.search(r"rotation_forced=(\w+)", rotation)

                    time_str: str = "Unknown"
                    if timestamp_match:
                        # Extract just the time part
                        time_str = (
                            timestamp_match.group(0)[-8:-1]
                            if len(timestamp_match.group(0)) > 8
                            else "Unknown"
                        )

                    old_ip: str = old_ip_match.group(1) if old_ip_match else "Unknown"
                    new_ip: str = new_ip_match.group(1) if new_ip_match else "Unknown"
                    count: str = count_match.group(1) if count_match else "0"
                    forced: str = forced_match.group(1) if forced_match else "Unknown"

                    table.append(
                        f"{time_str:<8} | {old_ip:<15} | {new_ip:<15} | {count:<8} | {forced:<6} | {'Quota/Security':<20}"
                    )

                except Exception:
                    table.append(
                        f"{'Unknown':<8} | {'Parse Error':<15} | {'Parse Error':<15} | {'0':<8} | {'?':<6} | {'Parse Error':<20}"
                    )
            else:
                # Dictionary format
                time_str: str = (
                    rotation.get("timestamp", "Unknown")[-8:]
                    if rotation.get("timestamp")
                    else "Unknown"
                )
                old_ip: str = rotation.get("old_ip", "Unknown")
                new_ip: str = rotation.get("new_ip", "Unknown")
                count: str = str(rotation.get("request_count", 0))
                forced: str = str(rotation.get("rotation_forced", False))
                reason: str = rotation.get("reason", "Auto")

                table.append(
                    f"{time_str:<8} | {old_ip:<15} | {new_ip:<15} | {count:<8} | {forced:<6} | {reason:<20}"
                )

        table.append(f"{'=' * 100}\n")

        # Log the complete rotation history table
        self.structured_logger.info("\n".join(table))

    # === DATA SOURCING OPERATIONS ===

    def processing_progress(
        self,
        operation: str,
        current: int,
        total: Optional[int] = None,
        show_every: int = 1000,
        force_show: bool = False,
        **details: Any,
    ) -> None:
        """
        Smart progress logging with structured tracking
        """
        key: str = f"{operation}_{total or 'unknown'}"
        now: float = time.time()
        last_time: float = self._last_progress_time.get(key, 0)

        # Always log structured data for tracking
        self._log_structured(
            "DEBUG",
            "processing_progress",
            f"Processing progress: {operation}",
            operation=operation,
            current=current,
            total=total,
            **details,
        )

        # Smart console display
        should_show: bool = (
            force_show
            or current == 1
            or (total and current == total)
            or current % show_every == 0
            or (now - last_time) > 30
        )

        if should_show:
            self._last_progress_time[key] = now

            msg: str = f"📊 {operation}: {current:,}"
            if total:
                percentage: float = (current / total) * 100
                msg += f"/{total:,} ({percentage:.1f}%)"

            # Add rate and ETA
            if last_time > 0 and current > 1:
                time_diff: float = now - last_time
                count_diff: int = current - self._operation_counters.get(key, 0)
                if time_diff > 0:
                    rate: float = count_diff / time_diff
                    msg += f" [{rate:.1f}/sec]"

                    if total and rate > 0:
                        remaining: float = (total - current) / rate
                        if remaining > 60:
                            msg += f" ETA: {remaining / 60:.1f}min"

            # Add VPN info if available
            if self._vpn_stats["current_ip"]:
                msg += f" | ip={self._vpn_stats['current_ip']} | reqs={self._vpn_stats['requests_on_current_ip']}"

            self._log_console("INFO", msg)
            self._operation_counters[key] = current

    def vpn_cleanup_started(self, reason: str, **context: Any) -> None:
        """
        Log VPN cleanup start
        """
        self._log_structured(
            "INFO",
            "vpn_cleanup_started",
            "VPN resource cleanup started",
            reason=reason,
            **context,
        )

        self._log_console("INFO", f"🧹 VPN cleanup started | reason={reason}")

    def vpn_cleanup_completed(self, duration_seconds: float, **context: Any) -> None:
        """
        Log VPN cleanup completion
        """
        self._log_structured(
            "INFO",
            "vpn_cleanup_completed",
            "VPN resource cleanup completed",
            duration_seconds=duration_seconds,
            **context,
        )

        self._log_console("INFO", f"✅ VPN cleanup completed | {duration_seconds:.1f}s")

    # === UTILITY METHODS ===

    def get_vpn_stats(self) -> Dict[str, Any]:
        """
        Get current VPN statistics
        """
        current_time: float = time.time()
        start_time = self._vpn_stats["time_on_current_ip_start"]
        time_on_current_ip: float = (
            current_time - float(start_time)
            if start_time is not None and isinstance(start_time, (int, float))
            else 0.0
        )

        return {
            "vpn_protection": "ACTIVE" if self._vpn_stats["current_ip"] else "INACTIVE",
            "current_ip": self._vpn_stats["current_ip"],
            "requests_on_current_ip": self._vpn_stats["requests_on_current_ip"],
            "total_rotations": self._vpn_stats["total_rotations"],
            "time_on_current_ip": time_on_current_ip,
            "rotation_success_rate": self._vpn_stats["success_rate"],
            "rotation_history_count": (
                len(self._vpn_stats["rotation_history"])
                if isinstance(self._vpn_stats["rotation_history"], list)
                else 0
            ),
        }

    def health_check(self, **additional_stats: Any) -> Dict[str, Any]:
        """
        Comprehensive health check with all stats
        """
        stats: Dict[str, Any] = self.get_vpn_stats()
        stats.update(additional_stats)

        # Log comprehensive stats
        self.statistics_collected("comprehensive_vpn_stats", stats)

        # Console summary
        status: str = (
            "🟢 HEALTHY" if stats["vpn_protection"] == "ACTIVE" else "🔴 UNHEALTHY"
        )
        self._log_console(
            "INFO",
            f"{status} | rotations={stats['total_rotations']} | success_rate={stats['rotation_success_rate']:.1f}%",
        )

        return stats


# === GLOBAL LOGGER INSTANCE ===
logger: DataSourcingLogger = DataSourcingLogger()
