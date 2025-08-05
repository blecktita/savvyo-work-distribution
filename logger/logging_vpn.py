import json
import logging
import logging.handlers
import os
import queue
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List

import requests


class VpnHandlerLogger:
    """
    Tracks all operationally critical events for VPN management operations.
    """

    def __init__(
        self,
        component_name: str = "VpnHandler",
        log_dir: str = "logs/vpn_logs",
        loki_url: str = "http://localhost:3100/loki/api/v1/push",
    ):
        self.component_name = component_name
        self.loki_url = loki_url
        self.log_dir = log_dir

        # Ensure log directory exists
        os.makedirs(log_dir, exist_ok=True)

        # Setup rotating file handlers
        self._setup_file_logging()

        # Initialize async logging queue for Loki
        self.log_queue = queue.Queue(maxsize=1000)
        self.loki_thread = threading.Thread(target=self._loki_worker, daemon=True)
        self.loki_thread.start()

        # Track component state for health checks
        self._component_start_time = datetime.now(timezone.utc)
        self._last_health_check = None

    def _setup_file_logging(self):
        """
        Setup rotating file handlers with JSON formatting
        """
        self.logger = logging.getLogger(f"{self.component_name}_file")
        self.logger.setLevel(logging.INFO)

        self.logger.handlers.clear()

        file_handler = logging.handlers.RotatingFileHandler(
            filename=os.path.join(self.log_dir, f"{self.component_name.lower()}.log"),
            maxBytes=50 * 1024 * 1024,  # 50MB
            backupCount=3,
        )

        formatter = logging.Formatter("%(message)s")
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

    def _send_to_loki(self, log_entry: Dict[str, Any]):
        """
        Send log entry to Loki
        """
        try:
            payload = {
                "streams": [
                    {
                        "stream": {
                            "job": "python-scraper",
                            "component": self.component_name,
                            "level": log_entry.get("level", "INFO"),
                            "event_type": log_entry.get("event_type", "general"),
                            "environment": "production",
                        },
                        "values": [
                            [
                                str(
                                    int(
                                        datetime.now(timezone.utc).timestamp()
                                        * 1000000000
                                    )
                                ),
                                json.dumps(log_entry),
                            ]
                        ],
                    }
                ]
            }

            requests.post(self.loki_url, json=payload, timeout=2)
        except Exception:
            pass

    def _loki_worker(self):
        """
        Background worker to send logs to Loki
        """
        while True:
            try:
                log_entry = self.log_queue.get(timeout=1)
                self._send_to_loki(log_entry)
            except queue.Empty:
                continue
            except Exception:
                continue

    def _log_structured(self, level: str, event_type: str, message: str, **data):
        """
        Core logging method with Loki integration
        """
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "component": self.component_name,
            "level": level,
            "event_type": event_type,
            "message": message,
            "trace_id": data.get("trace_id", f"vpn_{int(time.time())}"),
            **data,
        }

        try:
            self.log_queue.put_nowait(log_entry)
        except queue.Full:
            pass

        self.logger.info(json.dumps(log_entry, default=str))

    # === LIFECYCLE METHODS ===
    def startup(self, config_dict: Dict[str, Any] = None, **context):
        """
        Log VPN handler initialization
        """
        self._log_structured(
            "INFO",
            "component_startup",
            f"{self.component_name} starting up",
            config=config_dict or {},
            startup_time=self._component_start_time.isoformat(),
            **context,
        )

    def shutdown(self, reason: str = "normal", final_stats: Dict = None, **context):
        """
        Log VPN handler shutdown
        """
        uptime_seconds = (
            datetime.now(timezone.utc) - self._component_start_time
        ).total_seconds()
        self._log_structured(
            "INFO",
            "component_shutdown",
            f"{self.component_name} shutting down",
            reason=reason,
            uptime_seconds=uptime_seconds,
            final_stats=final_stats or {},
            **context,
        )

    # === VPN INITIALIZATION & CONNECTION ===
    def vpn_initialization_started(self, config_use_vpn: bool, **context):
        """
        Log VPN manager initialization attempt
        """
        self._log_structured(
            "INFO",
            "vpn_initialization_started",
            "VPN manager initialization started",
            use_vpn=config_use_vpn,
            **context,
        )

    def vpn_initialization_completed(
        self, vpn_enabled: bool, duration_seconds: float, **context
    ):
        """
        Log successful VPN initialization
        """
        self._log_structured(
            "INFO",
            "vpn_initialization_completed",
            "VPN manager initialized successfully",
            vpn_enabled=vpn_enabled,
            duration_seconds=duration_seconds,
            **context,
        )

    def vpn_initialization_failed(
        self, error: str, error_type: str, duration_seconds: float, **context
    ):
        """
        Log VPN initialization failure
        """
        level = "CRITICAL" if error_type == "VpnRequiredError" else "ERROR"
        self._log_structured(
            level,
            "vpn_initialization_failed",
            "VPN manager initialization failed",
            error=str(error),
            error_type=error_type,
            duration_seconds=duration_seconds,
            **context,
        )

    # === VPN ROTATION OPERATIONS ===
    def vpn_rotation_started(
        self, trigger_reason: str, current_ip: str, request_count: int, **context
    ):
        """
        Log VPN rotation initiation
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

    def vpn_rotation_completed(
        self, duration_seconds: float, new_ip: str, old_ip: str, **context
    ):
        """
        Log successful VPN rotation
        """
        self._log_structured(
            "INFO",
            "vpn_rotation_completed",
            "VPN rotation completed successfully",
            duration_seconds=duration_seconds,
            new_ip=new_ip,
            old_ip=old_ip,
            **context,
        )

    def vpn_rotation_failed(
        self, error: str, duration_seconds: float, current_ip: str, **context
    ):
        """
        Log VPN rotation failure
        """
        self._log_structured(
            "ERROR",
            "vpn_rotation_failed",
            "VPN rotation failed",
            error=str(error),
            duration_seconds=duration_seconds,
            current_ip=current_ip,
            **context,
        )

    def tunnelblick_recovery_attempted(self, failure_reason: str, **context):
        """
        Log Tunnelblick recovery attempt
        """
        self._log_structured(
            "WARNING",
            "tunnelblick_recovery_attempted",
            "Attempting Tunnelblick recovery",
            failure_reason=failure_reason,
            **context,
        )

    def tunnelblick_recovery_succeeded(self, duration_seconds: float, **context):
        """
        Log successful Tunnelblick recovery
        """
        self._log_structured(
            "INFO",
            "tunnelblick_recovery_succeeded",
            "Tunnelblick recovery succeeded",
            duration_seconds=duration_seconds,
            **context,
        )

    def tunnelblick_recovery_failed(self, error: str, **context):
        """
        Log failed Tunnelblick recovery - critical issue
        """
        self._log_structured(
            "CRITICAL",
            "tunnelblick_recovery_failed",
            "Tunnelblick recovery failed - manual intervention required",
            error=str(error),
            **context,
        )

    # === SECURITY & IP MANAGEMENT ===
    def ip_quota_exceeded(
        self, current_ip: str, request_count: int, max_requests: int, **context
    ):
        """Log IP request quota exceeded"""
        self._log_structured(
            "WARNING",
            "ip_quota_exceeded",
            "IP request quota exceeded - rotation needed",
            current_ip=current_ip,
            request_count=request_count,
            max_requests=max_requests,
            quota_utilization=request_count / max_requests,
            **context,
        )

    def security_violation_detected(
        self, violation_type: str, current_ip: str, request_count: int, **context
    ):
        """Log security violation detection"""
        self._log_structured(
            "ERROR",
            "security_violation_detected",
            "Security violation detected",
            violation_type=violation_type,
            current_ip=current_ip,
            request_count=request_count,
            **context,
        )

    def ip_security_alert_sent(
        self, alert_message: str, current_ip: str, request_count: int, **context
    ):
        """Log critical security alert"""
        self._log_structured(
            "CRITICAL",
            "ip_security_alert_sent",
            "Critical security alert sent",
            alert_message=alert_message,
            current_ip=current_ip,
            request_count=request_count,
            **context,
        )

    def reboot_required_triggered(
        self, reason: str, rotation_failures: int = None, **context
    ):
        """Log system reboot requirement"""
        self._log_structured(
            "CRITICAL",
            "reboot_required_triggered",
            "System reboot required",
            reason=reason,
            rotation_failures=rotation_failures,
            **context,
        )

    # === REQUEST TIMING & DELAYS ===
    def request_timing_started(
        self, operation: str, vpn_protection_active: bool, **context
    ):
        """Log request timing initiation"""
        self._log_structured(
            "INFO",
            "request_timing_started",
            "Request timing check started",
            operation=operation,
            vpn_protection_active=vpn_protection_active,
            **context,
        )

    def request_delay_applied(
        self, operation: str, delay_seconds: float, delay_type: str, **context
    ):
        """Log request delay application"""
        level = "WARNING" if delay_seconds > 10 else "INFO"
        self._log_structured(
            level,
            "request_delay_applied",
            "Request delay applied",
            operation=operation,
            delay_seconds=delay_seconds,
            delay_type=delay_type,  # 'mandatory' or 'standard'
            **context,
        )

    def vpn_protection_violation(self, required: bool, active: bool, **context):
        """Log VPN protection requirement violation"""
        self._log_structured(
            "ERROR",
            "vpn_protection_violation",
            "VPN protection requirement violated",
            vpn_required=required,
            vpn_active=active,
            **context,
        )

    # === VPN TERMINATION & CLEANUP ===
    def vpn_termination_detected(self, error_message: str, **context):
        """Log VPN termination detection"""
        self._log_structured(
            "CRITICAL",
            "vpn_termination_detected",
            "VPN termination detected - critical failure",
            error_message=error_message,
            **context,
        )

    def vpn_cleanup_started(self, reason: str, **context):
        """Log VPN resource cleanup initiation"""
        self._log_structured(
            "INFO",
            "vpn_cleanup_started",
            "VPN resource cleanup started",
            reason=reason,
            **context,
        )

    def vpn_cleanup_completed(self, duration_seconds: float, **context):
        """Log successful VPN cleanup"""
        self._log_structured(
            "INFO",
            "vpn_cleanup_completed",
            "VPN resource cleanup completed",
            duration_seconds=duration_seconds,
            **context,
        )

    def vpn_cleanup_failed(self, error: str, **context):
        """Log VPN cleanup failure"""
        self._log_structured(
            "WARNING",
            "vpn_cleanup_failed",
            "VPN resource cleanup failed",
            error=str(error),
            **context,
        )

    # === PERFORMANCE & HEALTH MONITORING ===
    def performance_metric(
        self, metric_name: str, value: float, threshold: float = None, **context
    ):
        """Log performance metrics with threshold checking"""
        level = "WARNING" if threshold and value > threshold else "INFO"
        self._log_structured(
            level,
            "performance_metric",
            f"Performance metric: {metric_name}",
            metric_name=metric_name,
            value=value,
            threshold=threshold,
            threshold_exceeded=threshold and value > threshold,
            **context,
        )

    def rotation_success_rate_metric(
        self, success_rate: float, total_rotations: int, **context
    ):
        """Log VPN rotation success rate"""
        level = (
            "ERROR"
            if success_rate < 0.8
            else "WARNING"
            if success_rate < 0.95
            else "INFO"
        )
        self._log_structured(
            level,
            "rotation_success_rate_metric",
            "VPN rotation success rate metric",
            success_rate=success_rate,
            total_rotations=total_rotations,
            **context,
        )

    def ip_time_threshold_warning(
        self,
        current_ip: str,
        time_on_ip_seconds: float,
        threshold_seconds: float,
        **context,
    ):
        """Log warning when IP has been used too long"""
        self._log_structured(
            "WARNING",
            "ip_time_threshold_warning",
            "IP usage time threshold exceeded",
            current_ip=current_ip,
            time_on_ip_seconds=time_on_ip_seconds,
            threshold_seconds=threshold_seconds,
            **context,
        )

    def health_check(
        self,
        status: str,
        is_active: bool,
        vpn_protection_active: bool,
        monitoring_active: bool,
        details: Dict = None,
        **context,
    ):
        """Log component health status"""
        level = "ERROR" if status.lower() in ["failed", "unhealthy"] else "INFO"
        self._last_health_check = datetime.now(timezone.utc)

        self._log_structured(
            level,
            "health_check",
            f"Health check: {status}",
            status=status,
            is_active=is_active,
            vpn_protection_active=vpn_protection_active,
            monitoring_active=monitoring_active,
            details=details or {},
            **context,
        )

    # === STATISTICS & MONITORING ===
    def statistics_collected(self, stats_type: str, statistics: Dict, **context):
        """Log statistics collection"""
        self._log_structured(
            "INFO",
            "statistics_collected",
            f"Statistics collected: {stats_type}",
            stats_type=stats_type,
            statistics=statistics,
            **context,
        )

    def rotation_history_recorded(
        self, rotation_count: int, recent_rotations: List[Dict] = None, **context
    ):
        """Log rotation history update"""
        self._log_structured(
            "INFO",
            "rotation_history_recorded",
            "Rotation history recorded",
            total_rotations=rotation_count,
            recent_rotations=recent_rotations or [],
            **context,
        )

    # === ERROR HANDLING & RECOVERY ===
    def retry_attempt(
        self,
        operation: str,
        attempt: int,
        max_attempts: int,
        error: str = None,
        **context,
    ):
        """Log retry attempts for any operation"""
        self._log_structured(
            "WARNING",
            "retry_attempt",
            f"Retry attempt {attempt}/{max_attempts} for {operation}",
            operation=operation,
            attempt=attempt,
            max_attempts=max_attempts,
            error=str(error) if error else None,
            **context,
        )

    def recovery_succeeded(
        self, operation: str, attempts: int, total_duration_seconds: float, **context
    ):
        """Log successful recovery after retries"""
        self._log_structured(
            "INFO",
            "recovery_succeeded",
            f"Recovery succeeded for {operation} after {attempts} attempts",
            operation=operation,
            attempts=attempts,
            total_duration_seconds=total_duration_seconds,
            **context,
        )

    def recovery_failed(
        self, operation: str, final_error: str, total_attempts: int, **context
    ):
        """Log failed recovery - usually critical"""
        self._log_structured(
            "CRITICAL",
            "recovery_failed",
            f"Recovery failed for {operation} after {total_attempts} attempts",
            operation=operation,
            final_error=str(final_error),
            total_attempts=total_attempts,
            **context,
        )

    # === CONFIGURATION & ENVIRONMENT ===
    def configuration_changed(
        self, config_section: str, old_value: Any, new_value: Any, **context
    ):
        """Log configuration changes"""
        self._log_structured(
            "INFO",
            "configuration_changed",
            f"Configuration changed: {config_section}",
            config_section=config_section,
            old_value=old_value,
            new_value=new_value,
            **context,
        )

    def environment_validation_failed(
        self, validation_type: str, error: str, **context
    ):
        """Log environment validation failures"""
        self._log_structured(
            "ERROR",
            "environment_validation_failed",
            f"Environment validation failed: {validation_type}",
            validation_type=validation_type,
            error=str(error),
            **context,
        )

    # === UTILITY METHODS ===
    def custom_event(self, event_type: str, level: str, message: str, **context):
        """
        Log custom events for specific operational needs
        """
        self._log_structured(level, f"custom_{event_type}", message, **context)

    def get_logger_stats(self) -> Dict[str, Any]:
        """
        Get logger statistics for monitoring
        """
        uptime = (
            datetime.now(timezone.utc) - self._component_start_time
        ).total_seconds()
        return {
            "component_name": self.component_name,
            "uptime_seconds": uptime,
            "last_health_check": (
                self._last_health_check.isoformat() if self._last_health_check else None
            ),
            "log_queue_size": self.log_queue.qsize(),
            "loki_worker_alive": self.loki_thread.is_alive(),
        }
