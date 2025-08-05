# vpn_controls/vpn_manger.py

import logging
import threading
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

import requests

from configurations import get_config
from exceptions import IPSecurityViolationError, VpnConnectionError, VpnRequiredError
from logger.sourcing_logger import DataSourcingLogger
from security import IPSecurityManager
from security.models import SecurityAlert

from .vpn_source import RequestThrottler

logging.getLogger("VpnHandler_file").setLevel(logging.ERROR)


class VpnProtectionHandler:
    """
    VPN Handler
    """

    def __init__(
        self,
        config,
        logger_name: str = "VpnHandler",
        env_file_path: Optional[str] = None,
    ):
        # Use the passed config if provided, otherwise get config for environment
        if config is not None:
            self.config = config
            self.environment = getattr(
                config, "_environment", "development"
            )  # ← Use config's environment (production, development, testing)
        else:
            self.config = get_config("development")
            self.environment = "development"

        # ***> Initialize VPN manager <***
        self.vpn_manager: Optional[RequestThrottler] = None
        self.vpn_protection_active: bool = False
        self.vpn_rotating = False
        self.rotation_lock = threading.RLock()

        # Initialize new logging system
        self.logger = DataSourcingLogger(component_name=logger_name, log_dir="logs")

        # Log startup with configuration
        startup_context = {
            "use_vpn": self.config.use_vpn,
            "environment": self.environment,
            "mandatory_delay": getattr(self.config, "mandatory_delay", None),
            "request_delay": getattr(self.config, "request_delay", None),
        }

        self.logger.component_startup(config=startup_context)

        self.ip_security = IPSecurityManager(
            rotation_callback=self._perform_vpn_rotation, shared_lock=self.rotation_lock
        )

        self.ip_security._vpn_handler_instance = self

        if self.config.use_vpn:
            self._initialize_vpn_manager()

        self.ip_security.activate_monitoring()

    def _initialize_vpn_manager(self) -> None:
        """
        Initialize VPN manager with comprehensive logging
        """
        start_time = time.time()

        self.logger.vpn_initialization_started(config_use_vpn=self.config.use_vpn)

        try:
            self.vpn_manager = RequestThrottler(
                vpn_config=self.config.vpn,
                termination_callback=self._handle_vpn_termination,
            )

            duration = time.time() - start_time

            if self.vpn_manager.vpn_enabled:
                self.vpn_protection_active = True
                self.logger.vpn_initialization_completed(
                    vpn_enabled=True,
                    duration_seconds=duration,
                    current_ip=self.ip_security.current_ip,
                )
            else:
                error_msg = "VPN required error"
                self.logger._log_structured(
                    "ERROR",
                    "vpn_initialization_failed",
                    error_msg,
                    error_type="VpnRequiredError",
                    duration_seconds=duration,
                )
                raise VpnRequiredError(error_msg)

        except VpnRequiredError:
            raise
        except Exception as e:
            duration = time.time() - start_time
            self.logger._log_structured(
                "ERROR",
                "vpn_initialization_failed",
                f"VPN initialization failed: {e}",
                error=str(e),
                error_type=type(e).__name__,
                duration_seconds=duration,
            )
            raise VpnConnectionError(f"VPN initialization failed: {e}")

    def _handle_vpn_termination(self, error_message: str) -> None:
        """Handle VPN termination with detailed logging"""
        self.logger._log_structured(
            "CRITICAL",
            "vpn_termination_detected",
            "VPN termination detected",
            error_message=error_message,
            current_ip=self.ip_security.current_ip,
            request_count=self.ip_security.request_count,
        )

        self.ip_security.reboot_required = True

        # Log reboot requirement
        self.logger._log_structured(
            "ERROR",
            "reboot_required_triggered",
            "System reboot required due to VPN termination",
            reason="VPN termination",
            rotation_failures=0,
            current_ip=self.ip_security.current_ip,
        )

        try:
            self.ip_security._send_critical_alert(
                f"VPN termination: {error_message}",
                self.ip_security.current_ip,
                self.ip_security.request_count,
            )

            # Log the critical alert
            self.logger._log_structured(
                "INFO",
                "ip_security_alert_sent",
                "Critical security alert sent",
                alert_message=f"VPN termination: {error_message}",
                current_ip=self.ip_security.current_ip,
                request_count=self.ip_security.request_count,
            )

        except Exception as alert_error:
            self.logger._log_structured(
                "ERROR",
                "alert_send_failed",
                "Failed to send critical alert",
                error=str(alert_error),
                original_alert=error_message,
            )

        try:
            cleanup_start = time.time()
            self.logger.vpn_cleanup_started(reason="termination_handler")

            self._cleanup_vpn_resources()

            cleanup_duration = time.time() - cleanup_start
            self.logger.vpn_cleanup_completed(duration_seconds=cleanup_duration)

        except Exception as cleanup_error:
            self.logger._log_structured(
                "ERROR",
                "vpn_cleanup_failed",
                "VPN cleanup failed during termination handling",
                error=str(cleanup_error),
            )

        raise IPSecurityViolationError(f"VPN CRITICAL FAILURE: {error_message}")

    def ensure_vpn_protection(self) -> None:
        """Ensure VPN protection with violation logging"""
        if self.config.use_vpn and not self.vpn_protection_active:
            self.logger._log_structured(
                "ERROR",
                "vpn_protection_violation",
                "VPN security violation detected",
                required=self.config.use_vpn,
                active=self.vpn_protection_active,
                current_ip=self.ip_security.current_ip,
            )
            raise VpnRequiredError("VPN security violation")

    def handle_request_timing(self, operation: str = "request") -> None:
        """Handle request timing with detailed logging"""
        self.logger.request_timing_started(
            operation=operation, vpn_protection_active=self.vpn_protection_active
        )

        self.ensure_vpn_protection()

        try:
            self.ip_security.check_request()
        except IPSecurityViolationError as e:
            self.logger._log_structured(
                "ERROR",
                "security_violation_detected",
                "IP security violation detected",
                violation_type=type(e).__name__,
                current_ip=self.ip_security.current_ip,
                request_count=self.ip_security.request_count,
                operation=operation,
            )
            raise

        self._apply_request_delays(operation)

    def _apply_request_delays(self, operation: str) -> None:
        """Apply request delays with timing logging"""
        if self.vpn_manager and self.vpn_protection_active:
            delay = getattr(self.config.vpn, "mandatory_delay", 0)
            delay_type = "mandatory"
        else:
            delay = getattr(self.config, "request_delay", 0)
            delay_type = "standard"

        self.logger.request_delay_applied(
            operation=operation, delay_seconds=delay, delay_type=delay_type
        )

        time.sleep(delay)

    def _perform_vpn_rotation(self) -> None:
        """Perform VPN rotation with comprehensive logging"""
        if not (self.vpn_manager and self.vpn_protection_active):
            error_msg = "VPN manager not available for rotation"
            self.logger._log_structured(
                "ERROR",
                "vpn_rotation_failed",
                error_msg,
                duration_seconds=0,
                current_ip=self.ip_security.current_ip,
            )
            raise VpnConnectionError(error_msg)

        start_time = time.time()
        old_ip = self.ip_security.current_ip

        # Determine rotation trigger reason
        if self.ip_security.request_count >= self.ip_security.max_requests_per_ip:
            trigger_reason = "quota_exceeded"
        else:
            trigger_reason = "time_threshold"

        self.logger.vpn_rotation_started(
            trigger_reason=trigger_reason,
            current_ip=old_ip,
            request_count=self.ip_security.request_count,
        )

        try:
            rotation_success = self.vpn_manager.rotate_configuration()
            duration = time.time() - start_time

            if rotation_success:
                new_ip = self.ip_security.current_ip
                self._update_duckdns_ip(new_ip)
                self.logger.vpn_rotation_completed(
                    duration_seconds=duration,
                    new_ip=new_ip,
                    old_ip=old_ip,
                    request_count=self.ip_security.request_count,
                )
            else:
                # Attempt Tunnelblick recovery
                self.logger._log_structured(
                    "WARNING",
                    "tunnelblick_recovery_attempted",
                    "Attempting Tunnelblick recovery",
                    failure_reason="rotation_failed",
                    current_ip=old_ip,
                )

                recovery_start = time.time()
                if self.vpn_manager.recover_tunnelblick():
                    recovery_duration = time.time() - recovery_start
                    self.logger._log_structured(
                        "INFO",
                        "tunnelblick_recovery_succeeded",
                        "Tunnelblick recovery successful",
                        duration_seconds=recovery_duration,
                    )

                    # Try to establish connection after recovery
                    if not self.vpn_manager.establish_secure_connection():
                        total_duration = time.time() - start_time
                        error_msg = "Failed to establish connection after recovery"
                        self.logger._log_structured(
                            "ERROR",
                            "vpn_rotation_failed",
                            error_msg,
                            duration_seconds=total_duration,
                            current_ip=old_ip,
                        )
                        raise VpnConnectionError(error_msg)
                else:
                    error_msg = "Tunnelblick recovery failed"
                    self.logger._log_structured(
                        "ERROR",
                        "tunnelblick_recovery_failed",
                        error_msg,
                        current_ip=old_ip,
                    )

                    total_duration = time.time() - start_time
                    self.logger._log_structured(
                        "ERROR",
                        "vpn_rotation_failed",
                        error_msg,
                        duration_seconds=total_duration,
                        current_ip=old_ip,
                    )
                    raise VpnConnectionError(error_msg)

        except Exception as e:
            duration = time.time() - start_time
            self.logger._log_structured(
                "ERROR",
                "vpn_rotation_failed",
                f"VPN rotation failed: {e}",
                error=str(e),
                duration_seconds=duration,
                current_ip=old_ip,
            )
            raise VpnConnectionError(f"VPN rotation failed: {e}")

    def _update_duckdns_ip(self, new_ip: str) -> None:
        """Update DuckDNS with new IP after rotation"""
        try:
            # Your DuckDNS details
            domain = "savvyo"  # Your chosen subdomain
            token = "ac7179dc-2ea8-4779-b331-e602bd13c8f2"

            url = f"https://www.duckdns.org/update?domains={domain}&token={token}&ip={new_ip}"

            response = requests.get(url, timeout=10)

            if response.text.strip() == "OK":
                self.logger._log_structured(
                    "INFO",
                    "duckdns_update_success",
                    "DuckDNS updated successfully",
                    new_ip=new_ip,
                    domain=f"{domain}.duckdns.org",
                )
            else:
                self.logger._log_structured(
                    "ERROR",
                    "duckdns_update_failed",
                    f"DuckDNS update failed: {response.text}",
                    new_ip=new_ip,
                    response_text=response.text,
                )

        except Exception as e:
            self.logger._log_structured(
                "ERROR",
                "duckdns_update_error",
                f"DuckDNS update error: {str(e)}",
                new_ip=new_ip,
                error=str(e),
            )

    def get_vpn_statistics(self) -> Dict:
        """Get VPN statistics with logging"""
        if not (self.vpn_manager and self.vpn_protection_active):
            stats = {"vpn_protection": "DISABLED"}
            self.logger.statistics_collected(
                stats_type="basic_vpn_stats", statistics=stats
            )
            return stats

        security_status = self.ip_security.get_security_status()

        stats = {
            "vpn_protection": "ACTIVE",
            "current_ip": security_status["current_ip"],
            "requests_on_current_ip": security_status["request_count"],
            "max_requests_per_ip": security_status["max_requests_per_ip"],
            "total_rotations": security_status["total_rotations"],
            "time_on_current_ip": security_status["time_since_rotation"],
            "rotation_success_rate": security_status["rotation_success_rate"],
        }

        # Log performance metrics
        self.logger.rotation_success_rate_metric(
            success_rate=stats["rotation_success_rate"],
            total_rotations=stats["total_rotations"],
        )

        # Check for time threshold warnings
        time_on_ip = stats["time_on_current_ip"]
        if (
            isinstance(time_on_ip, (int, float)) and time_on_ip > 3600
        ):  # 1 hour threshold
            self.logger._log_structured(
                "WARNING",
                "ip_time_threshold_warning",
                "IP time threshold exceeded",
                current_ip=stats["current_ip"],
                time_on_ip_seconds=stats["time_on_current_ip"],
                threshold_seconds=3600,
            )

        # Check for quota warnings
        quota_utilization = (
            stats["requests_on_current_ip"] / stats["max_requests_per_ip"]
        )
        if quota_utilization >= 0.8:  # 80% threshold
            self.logger.ip_quota_exceeded(
                current_ip=stats["current_ip"],
                request_count=stats["requests_on_current_ip"],
                max_requests=stats["max_requests_per_ip"],
            )

        self.logger.statistics_collected(stats_type="basic_vpn_stats", statistics=stats)

        return stats

    def get_comprehensive_vpn_statistics(self) -> Dict:
        """Get comprehensive statistics with logging"""
        vpn_stats = self.get_vpn_statistics()
        security_stats = self.ip_security.get_security_status()
        rotation_history = self.ip_security.get_rotation_history()

        comprehensive_stats = {
            **vpn_stats,
            "security_details": {
                "monitoring_active": security_stats["monitoring_active"],
                "reboot_required": security_stats["reboot_required"],
                "recent_alerts": security_stats["recent_alerts"],
                "rotation_callback_configured": security_stats[
                    "rotation_callback_configured"
                ],
                "rotation_in_progress": security_stats["rotation_in_progress"],
            },
            "rotation_history_count": len(rotation_history),
        }

        # Log rotation history
        self.logger.rotation_history_recorded(
            rotation_count=len(rotation_history),
            recent_rotations=rotation_history[-5:] if rotation_history else [],
        )

        # Log comprehensive statistics
        self.logger.statistics_collected(
            stats_type="comprehensive_vpn_stats", statistics=comprehensive_stats
        )

        return comprehensive_stats

    def _cleanup_vpn_resources(self) -> None:
        """Cleanup VPN resources with logging"""
        try:
            if self.vpn_manager and self.vpn_protection_active:
                self.vpn_manager.disconnect_configurations()
        except Exception as e:
            self.logger._log_structured(
                "ERROR",
                "vpn_resource_cleanup_error",
                "Error during VPN resource cleanup",
                error=str(e),
            )

    def cleanup(self) -> None:
        """Enhanced cleanup with comprehensive logging"""
        try:
            final_stats = self.get_comprehensive_vpn_statistics()

            cleanup_start = time.time()
            self.logger.vpn_cleanup_started(reason="normal_shutdown")

            self._cleanup_vpn_resources()
            self.ip_security.cleanup()

            cleanup_duration = time.time() - cleanup_start
            self.logger.vpn_cleanup_completed(duration_seconds=cleanup_duration)

            # Log shutdown with final statistics using new system
            self.logger.component_shutdown(reason="normal", final_stats=final_stats)

        except Exception as e:
            self.logger._log_structured(
                "ERROR", "vpn_cleanup_failed", "VPN cleanup failed", error=str(e)
            )
            self.logger.component_shutdown(reason="error", error=str(e))

    @property
    def is_active(self) -> bool:
        """Check if VPN handler is active with health logging"""
        is_active = self.vpn_protection_active and self.ip_security.is_monitoring_active

        # Periodic health check logging (every 5 minutes)
        now = datetime.now(timezone.utc)
        if (
            not hasattr(self, "_last_health_log")
            or (now - self._last_health_log).total_seconds() > 300
        ):
            # Use the new health_check method
            additional_stats = {
                "is_active": is_active,
                "vpn_protection_active": self.vpn_protection_active,
                "monitoring_active": self.ip_security.is_monitoring_active,
                "status": "healthy" if is_active else "unhealthy",
                "current_ip": self.ip_security.current_ip,
                "request_count": self.ip_security.request_count,
                "security_status": self.security_status,
            }
            self.logger.health_check(**additional_stats)
            self._last_health_log = now

        return is_active

    @property
    def security_alerts(self) -> List[SecurityAlert]:
        """Get security alerts with enhanced logging context"""
        return self.ip_security.get_recent_alerts()

    @property
    def security_status(self) -> str:
        """Get security status with enhanced logging context"""
        status = self.ip_security.get_security_status()

        if status["reboot_required"]:
            status_text = "🚨 REBOOT REQUIRED - Rotation Failures"
        elif status["rotation_in_progress"]:
            status_text = "🔄 ROTATION IN PROGRESS"
        elif status.get("should_rotate", False):
            status_text = "🔄 ROTATION NEEDED"
        elif status["monitoring_active"]:
            status_text = "🔒 ACTIVE - Monitoring"
        else:
            status_text = "⚠️ INACTIVE - No Monitoring"

        return status_text

    @property
    def current_ip(self) -> str:
        return self.ip_security.current_ip

    @property
    def request_count(self) -> int:
        return self.ip_security.request_count


def main():
    from configurations.factory import get_config

    # Use a development config for demonstration
    config = get_config("development")  # This returns a ScraperConfig

    # Initialize VPN handler with logging
    vpn_handler = VpnProtectionHandler(config, logger_name="TestVpnHandler")

    try:
        # Simulate some operations
        for i in range(5):
            print(f"Request {i + 1}")
            vpn_handler.handle_request_timing(f"test_request_{i + 1}")

            # Get statistics periodically
            if i % 2 == 0:
                stats = vpn_handler.get_vpn_statistics()
                print(f"Stats: {stats}")

        # Get comprehensive statistics
        comprehensive_stats = vpn_handler.get_comprehensive_vpn_statistics()
        print(f"Comprehensive stats: {comprehensive_stats}")

    except Exception as e:
        print(f"Error during operation: {e}")

    finally:
        # Clean shutdown
        vpn_handler.cleanup()


if __name__ == "__main__":
    main()
