# security/security_manager.py
"""
Main IP Security Manager - orchestrates all security components.
"""

import threading
import time
from typing import TYPE_CHECKING, Callable, Dict, List, Optional

from configurations import EnvironmentVariables, ScraperConfig, get_config
from exceptions import IPSecurityViolationError

from .alert_system import AlertSystem
from .ip_detector import IPDetector
from .models import SecurityAlert, SecurityThreatLevel
from .rotation_monitor import RotationMonitor

if TYPE_CHECKING:
    from vpn_controls.vpn_manger import VpnProtectionHandler


class IPSecurityManager:
    def __init__(
        self,
        security_config: Optional[ScraperConfig] = None,
        env_config: Optional[EnvironmentVariables] = None,
        environment: str = "production",
        rotation_callback: Optional[Callable] = None,
        shared_lock: Optional[threading.RLock] = None,
    ):
        if security_config is None:
            security_config = get_config(environment)

        if not isinstance(security_config, ScraperConfig):
            raise ValueError("Config must be a ScraperConfig instance")

        self.security_config = security_config.security
        self.env_config = env_config or EnvironmentVariables()
        self.rotation_callback = rotation_callback
        self._vpn_handler_instance: Optional["VpnProtectionHandler"] = None

        if shared_lock:
            self.rotation_lock = shared_lock
        else:
            self.rotation_lock = threading.RLock()

        self.ip_detector = IPDetector(security_config=self.security_config)

        self.rotation_monitor = RotationMonitor(
            config=security_config,
            environment=environment,
            shared_ip_detector=self.ip_detector,
            shared_lock=self.rotation_lock,
        )

        self.alert_system = AlertSystem(env_file_path=self.env_config.env_file_path)

        self.reboot_required: bool = False
        self._rotation_in_progress: bool = False

    def activate_monitoring(self) -> None:
        try:
            current_ip = self.ip_detector.get_current_ip()
            if not current_ip:
                self.reboot_required = True
                raise IPSecurityViolationError("Could not detect current IP address")
            self.rotation_monitor.start_monitoring(current_ip)
        except Exception as e:
            error_msg = f"Failed to start monitoring: {e}"
            self._send_critical_alert(error_msg, "UNKNOWN", 0)
            raise IPSecurityViolationError(error_msg)

    def deactivate_monitoring(self) -> None:
        self.rotation_monitor.stop_monitoring()

    def _attempt_full_recovery(self) -> bool:
        try:
            if getattr(self, "_vpn_handler_instance", None) is not None:
                vpn_manager = getattr(self._vpn_handler_instance, "vpn_manager", None)
            else:
                return False

            if not vpn_manager:
                return False

            if vpn_manager.recover_tunnelblick():
                if vpn_manager.establish_secure_connection():
                    return True
                else:
                    return False
            else:
                return False

        except Exception as e:
            return False

    def check_request(self) -> None:
        """
        Main entry point for security checks.
        Implements complete security flow with rotation and verification.
        """
        if self.reboot_required:
            try:
                if self._attempt_full_recovery():
                    self.reboot_required = False
                else:
                    raise IPSecurityViolationError(
                        "Full recovery failed - manual intervention required"
                    )
            except Exception as e:
                raise IPSecurityViolationError(f"Recovery failed: {e}")

        with self.rotation_lock:
            try:
                self.rotation_monitor.apply_request_throttling()
                needs_rotation = self.rotation_monitor.evaluate_request()

                if needs_rotation:
                    reason = self.rotation_monitor.stop_reason
                    self._perform_rotation_with_verification(reason)

                self.rotation_monitor.register_request()

            except IPSecurityViolationError:
                raise
            except Exception as e:
                error_msg = f"Security check failed: {e}"
                self._send_critical_alert(
                    error_msg, self.current_ip, self.request_count
                )
                raise IPSecurityViolationError(error_msg)

    def _perform_rotation_with_verification(self, reason: str) -> None:
        if self._rotation_in_progress:
            raise IPSecurityViolationError("Rotation already in progress")

        old_ip = self.current_ip

        try:
            self._rotation_in_progress = True

            if not self.rotation_callback:
                raise Exception("No rotation callback configured")

            self.rotation_callback()

            time.sleep(self.security_config.wait_time)

            if self.rotation_monitor.verify_ip_rotation():
                new_ip = self.ip_detector.get_current_ip()
                self.rotation_monitor.reset_after_successful_rotation(new_ip)

                self.alert_system.send_alert(
                    SecurityThreatLevel.PROTECTED,
                    f"IP rotation successful: {reason}",
                    new_ip,
                    0,
                )
            else:
                self._handle_rotation_failure(old_ip, reason, "IP verification failed")

        except Exception as e:
            self._handle_rotation_failure(old_ip, reason, str(e))
        finally:
            self._rotation_in_progress = False

    def _handle_rotation_failure(self, old_ip: str, reason: str, error: str) -> None:
        self.reboot_required = True

        error_msg = (
            f"IP rotation failed ({reason}): {error}. Manual intervention required."
        )

        self._send_critical_alert(
            f"ROTATION FAILURE: {reason} - {error}", old_ip, self.request_count
        )

        raise IPSecurityViolationError(error_msg)

    def _send_critical_alert(self, message: str, ip: str, request_count: int) -> None:
        try:
            self.alert_system.send_alert(
                SecurityThreatLevel.CRITICAL, message, ip, request_count
            )
        except Exception:
            pass

    def get_security_status(self) -> Dict:
        try:
            monitor_status = self.rotation_monitor.get_status()
        except Exception as e:
            monitor_status = {
                "current_ip": getattr(self.rotation_monitor, "current_ip", "UNKNOWN"),
                "request_count": getattr(self.rotation_monitor, "request_count", 0),
                "max_requests_per_ip": self.security_config.max_requests_per_ip,
                "monitoring_active": getattr(
                    self.rotation_monitor, "is_monitoring", False
                ),
                "should_rotate": False,
                "total_rotations": len(
                    getattr(self.rotation_monitor, "rotation_history", [])
                ),
                "time_since_rotation": 0,
                "stop_reason": getattr(self.rotation_monitor, "stop_reason", "none"),
            }

        try:
            recent_alerts = len(self.alert_system.get_recent_alerts(hours=1))
        except Exception:
            recent_alerts = 0

        return {
            **monitor_status,
            "recent_alerts": recent_alerts,
            "email_enabled": self.alert_system.email_config is not None,
            "rotation_success_rate": self._calculate_success_rate(),
            "rotation_callback_configured": self.rotation_callback is not None,
            "reboot_required": self.reboot_required,
            "rotation_in_progress": self._rotation_in_progress,
            "monitoring_active": monitor_status.get("monitoring_active", False),
        }

    def get_recent_alerts(self) -> List[SecurityAlert]:
        return self.alert_system.get_recent_alerts(
            hours=self.security_config.security_alerts_timeframe_hours
        )

    def _calculate_success_rate(self) -> float:
        total_rotations = len(self.rotation_monitor.rotation_history)
        if total_rotations == 0:
            return 100.0

        successful_rotations = total_rotations
        return (successful_rotations / total_rotations) * 100.0

    def get_rotation_history(self) -> list:
        return self.rotation_monitor.rotation_history

    def cleanup(self) -> None:
        self.deactivate_monitoring()

    def test_email_config(self) -> bool:
        return self.alert_system.test_email_config()

    @property
    def current_ip(self) -> str:
        return self.rotation_monitor.current_ip or "UNKNOWN"

    @property
    def request_count(self) -> int:
        return self.rotation_monitor.request_count

    @property
    def total_rotations(self) -> int:
        return len(self.rotation_monitor.rotation_history)

    @property
    def is_monitoring_active(self) -> bool:
        return self.rotation_monitor.is_monitoring

    @property
    def max_requests_per_ip(self) -> int:
        return self.security_config.max_requests_per_ip
