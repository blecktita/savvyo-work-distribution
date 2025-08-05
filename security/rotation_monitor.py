# core/security/rotation_monitor.py
"""
IP rotation monitoring and enforcement
"""

import random
import threading
import time
from datetime import datetime
from typing import Dict, List, Optional

from configurations import ScraperConfig, get_config

from .ip_detector import IPDetector
from .models import IPRotationEvent


class RotationMonitor:
    """
    Monitors IP rotation and enforces security policies.
    """

    def __init__(
        self,
        config: Optional[ScraperConfig] = None,
        environment: str = "production",
        shared_ip_detector: Optional[IPDetector] = None,
        shared_lock: Optional[threading.RLock] = None,
    ):
        if config is None:
            config = get_config(environment)

        if not isinstance(config, ScraperConfig):
            raise ValueError("Config must be a ScraperConfig instance")

        self.scraper_config = config
        self.environment = environment
        self.config = config.security
        self.vpn_config = config.vpn

        if shared_ip_detector:
            self.ip_detector = shared_ip_detector
        else:
            self.ip_detector = IPDetector(self.config)

        self.lock = shared_lock if shared_lock else threading.RLock()

        self.max_requests_per_ip = self.config.max_requests_per_ip
        self.rotation_check_interval = self.config.rotation_check_interval
        self.max_time_per_ip = self.config.max_time_per_ip
        self.rotation_variance = self.vpn_config.rotation_variance
        self.min_requests_bound = max(1, int(self.max_requests_per_ip * 0.4))
        self.max_requests_bound = int(self.max_requests_per_ip * 1.5)

        self.min_delay = self.vpn_config.request_min_delay
        self.max_delay = self.vpn_config.request_max_delay
        self.min_jitter = self.vpn_config.request_min_jitter
        self.max_jitter = self.vpn_config.request_max_jitter

        # State tracking
        self.current_ip: Optional[str] = None
        self.current_target = self._generate_dynamic_target()
        self.request_count = 0
        self.last_request_time = 0
        self.last_rotation_time: Optional[datetime] = None
        self.rotation_history: List[IPRotationEvent] = []

        # Monitoring control
        self.is_monitoring = False
        self.should_rotate_execution = False
        self.stop_reason = ""
        self.monitor_thread: Optional[threading.Thread] = None

    def _generate_dynamic_target(self) -> int:
        """
        Generate safe dynamic target with proper bounds
        """
        variance = int(self.max_requests_per_ip * self.rotation_variance)
        min_target = max(self.min_requests_bound, self.max_requests_per_ip - variance)
        max_target = min(self.max_requests_per_ip, self.max_requests_per_ip + variance)

        if min_target > max_target:
            min_target = max_target

        target = random.randint(min_target, max_target)
        return target

    def start_monitoring(self, initial_ip: str) -> None:
        """
        Start continuous IP rotation monitoring
        """
        with self.lock:
            if self.is_monitoring:
                return

            self.current_ip = initial_ip
            self.last_rotation_time = datetime.now()
            self.is_monitoring = True
            self.should_rotate_execution = False
            self.stop_reason = ""

            self.monitor_thread = threading.Thread(
                target=self._monitoring_loop, daemon=True
            )
            self.monitor_thread.start()

    def _monitoring_loop(self) -> None:
        """
        loop to monitor events and check time limits
        """
        while True:
            if not self.is_monitoring:
                break

            try:
                time.sleep(self.rotation_check_interval)

                if not self.is_monitoring:
                    break
                if self.lock.acquire(blocking=False):
                    try:
                        self._check_time_limits_internal()
                    finally:
                        self.lock.release()

            except Exception:
                time.sleep(5)

    def stop_monitoring(self) -> None:
        """
        Stop IP rotation monitoring
        """
        with self.lock:
            self.is_monitoring = False

        if self.monitor_thread and self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=self.config.wait_time)

    def register_request(self) -> None:
        """
        Register request
        """
        if self.should_rotate_execution:
            return

        self.request_count += 1

    def evaluate_request(self) -> bool:
        """
        Check if rotation needed
        Returns True if rotation needed, False if request can proceed
        """
        if self.should_rotate_execution:
            return True

        # CHECK 1: Would next request exceed limit?
        if (self.request_count + 1) >= self.current_target:
            self.should_rotate_execution = True
            self.stop_reason = f"Next request would exceed limit ({self.request_count + 1}/{self.current_target})"
            return True

        # CHECK 2: Time limit exceeded?
        if self._is_time_limit_exceeded():
            self.should_rotate_execution = True
            time_elapsed = self._get_time_since_rotation()
            self.stop_reason = f"Time limit exceeded (active for: {time_elapsed})"
            return True

        return False

    def _get_time_since_rotation(self) -> str:
        """
        Get formatted time since last rotation
        """
        if not self.last_rotation_time:
            return "never"

        delta = datetime.now() - self.last_rotation_time
        total_seconds = int(delta.total_seconds())
        minutes = total_seconds // 60
        seconds = total_seconds % 60
        return f"{minutes}m:{seconds}s"

    def _check_time_limits_internal(self) -> None:
        """
        Internal time limit check
        """
        if not self.last_rotation_time or self.should_rotate_execution:
            return

        time_since_rotation = datetime.now() - self.last_rotation_time

        if time_since_rotation.total_seconds() >= self.max_time_per_ip:
            self.should_rotate_execution = True
            self.stop_reason = f"Time limit exceeded: {time_since_rotation.total_seconds():.0f}s (limit: {self.max_time_per_ip}s)"

    def _is_time_limit_exceeded(self) -> bool:
        """
        Helper to check time limits
        """
        if not self.last_rotation_time:
            return False

        time_since_rotation = datetime.now() - self.last_rotation_time
        return time_since_rotation.total_seconds() >= self.max_time_per_ip

    def apply_request_throttling(self) -> None:
        """
        Apply delays between requests
        """
        if self.last_request_time > 0:
            elapsed = time.time() - self.last_request_time
            base_delay = random.uniform(self.min_delay, self.max_delay)
            jitter = random.uniform(self.min_jitter, self.max_jitter)
            delay = base_delay + jitter

            wait_time = max(0, delay - elapsed)

            if wait_time > 0:
                time.sleep(wait_time)

        self.last_request_time = time.time()

    def check_ip_change(self, new_ip: str) -> bool:
        """
        Check if IP changed
        """
        if new_ip != self.current_ip and self.current_ip is not None:
            self._record_rotation(self.current_ip, new_ip, forced=False)
            return True
        return False

    def _record_rotation(self, old_ip: str, new_ip: str, forced: bool) -> None:
        """
        Record successful IP rotation
        """
        rotation_event = IPRotationEvent(
            timestamp=datetime.now(),
            old_ip=old_ip,
            new_ip=new_ip,
            request_count=self.request_count,
            rotation_forced=forced,
        )

        self.rotation_history.append(rotation_event)
        self.current_ip = new_ip
        self.request_count = 0
        self.last_rotation_time = rotation_event.timestamp

    def verify_ip_rotation(
        self, expected_new_ip: Optional[str] = None, max_retries: int = 3
    ) -> bool:
        """
        Verify IP rotation
        """
        old_ip = self.current_ip

        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    time.sleep(2)

                current_ip = self.ip_detector.get_current_ip()

                if current_ip and current_ip != old_ip:
                    return True

            except Exception:
                pass

        return False

    def reset_after_successful_rotation(self, new_ip: str) -> None:
        """
        Reset after rotation
        """
        if self.current_ip:
            self._record_rotation(self.current_ip, new_ip, forced=True)
        else:
            self.current_ip = new_ip
            self.last_rotation_time = datetime.now()

        self.should_rotate_execution = False
        self.stop_reason = ""
        self.request_count = 0
        self.last_request_time = 0
        self.current_target = self._generate_dynamic_target()

    def get_status(self) -> Dict:
        """
        Get status
        """
        if self.last_rotation_time:
            time_since_seconds = (
                datetime.now() - self.last_rotation_time
            ).total_seconds()
        else:
            time_since_seconds = 0

        return {
            "current_ip": self.current_ip,
            "request_count": self.request_count,
            "max_requests_per_ip": self.current_target,
            "last_rotation": self.last_rotation_time,
            "time_since_rotation": time_since_seconds,
            "total_rotations": len(self.rotation_history),
            "monitoring_active": self.is_monitoring,
            "should_rotate": self.should_rotate_execution,
            "stop_reason": self.stop_reason,
        }

    def needs_rotation(self) -> bool:
        """
        Check if rotation needed
        """
        return self.should_rotate_execution
