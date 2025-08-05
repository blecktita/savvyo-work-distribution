# security/alert_system.py
"""
Security alert and notification system.
"""

import os
import smtplib
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Dict, List, Optional

from dotenv import load_dotenv

from exceptions import EmailConfigError, EmailSendingError

from .models import IPRotationEvent, SecurityAlert, SecurityThreatLevel


class AlertSystem:
    """
    Handles security alerts and notifications.
    """

    def __init__(self, env_file_path: Optional[str] = None):
        self.env_file_path = env_file_path or ".env"
        self.email_config = self._load_email_config()
        self.security_alerts: List[SecurityAlert] = []

    def send_alert(
        self,
        threat_level: SecurityThreatLevel,
        message: str,
        current_ip: str,
        request_count: int,
        last_rotation: Optional[datetime] = None,
        rotation_history: Optional[List[IPRotationEvent]] = None,
    ) -> None:
        """
        Send security alert.
        """
        alert = SecurityAlert(
            timestamp=datetime.now(),
            threat_level=threat_level,
            message=message,
            current_ip=current_ip,
            request_count=request_count,
            last_rotation=last_rotation,
        )

        self.security_alerts.append(alert)

        if self.email_config and threat_level in [
            SecurityThreatLevel.HIGH,
            SecurityThreatLevel.CRITICAL,
        ]:
            self._send_email_alert(alert, rotation_history or [])

    def _load_email_config(self) -> Optional[Dict[str, str]]:
        """
        Load email configuration from environment.
        """
        try:
            if not os.path.exists(self.env_file_path):
                return None

            load_dotenv(self.env_file_path)

            required_vars = ["EMAIL_FROM_ADDRESS", "EMAIL_TO_ADDRESS", "EMAIL_PASSWORD"]
            config_values = {var: os.getenv(var) for var in required_vars}

            if not all(config_values.values()):
                return None

            return {
                "smtp_server": str(os.getenv("EMAIL_SMTP_SERVER", "smtp.gmail.com")),
                "smtp_port": str(os.getenv("EMAIL_PORT", "587")),
                "from_email": str(config_values["EMAIL_FROM_ADDRESS"]),
                "to_email": str(config_values["EMAIL_TO_ADDRESS"]),
                "password": str(config_values["EMAIL_PASSWORD"]),
            }

        except Exception as e:
            raise EmailConfigError(f"Failed to load email configuration: {e}")
            return None

    def _send_email_alert(
        self, alert: SecurityAlert, rotation_history: List[IPRotationEvent]
    ) -> None:
        """
        Send email alert for high-severity events.
        """
        if not self.email_config:
            raise EmailConfigError("Email configuration not loaded")

        try:
            msg = MIMEMultipart()
            msg["From"] = self.email_config["from_email"]
            msg["To"] = self.email_config["to_email"]
            msg["Subject"] = f"🚨 {alert.threat_level.value} IP Security Alert"

            body = self._format_email_body(alert, rotation_history)
            msg.attach(MIMEText(body, "plain"))

            with smtplib.SMTP(
                self.email_config["smtp_server"], int(self.email_config["smtp_port"])
            ) as server:
                server.starttls()
                server.login(
                    self.email_config["from_email"], self.email_config["password"]
                )
                server.send_message(msg)

        except Exception as e:
            raise EmailSendingError(f"Failed to send email alert: {e}")

    def _format_email_body(
        self, alert: SecurityAlert, rotation_history: List[IPRotationEvent]
    ) -> str:
        """
        Format email body for security alert.
        """
        return f"""
                SECURITY ALERT DETAILS:
                =======================
                Timestamp: {alert.timestamp}
                Threat Level: {alert.threat_level.value}
                Current IP: {alert.current_ip}
                Request Count: {alert.request_count}
                Last Rotation: {alert.last_rotation or "Never"}

                MESSAGE:
                {alert.message}

                ROTATION HISTORY (Last 5):
                {self._format_rotation_history(rotation_history[-5:])}

                This is an automated security alert from IP Security Manager.
                Please investigate immediately if this is a HIGH or CRITICAL alert.
                        """.strip()

    def _format_rotation_history(self, rotations: List[IPRotationEvent]) -> str:
        """
        Format rotation history for display.
        """
        if not rotations:
            return "No rotations recorded"

        formatted = []
        for rotation in rotations:
            formatted.append(
                f"{rotation.timestamp.strftime('%H:%M:%S')}: "
                f"{rotation.old_ip} → {rotation.new_ip} "
                f"({rotation.request_count} reqs, {'FORCED' if rotation.rotation_forced else 'NATURAL'})"
            )

        return "\n".join(formatted)

    def test_email_config(self) -> bool:
        """
        Test email configuration.
        """
        if not self.email_config:
            return False

        try:
            msg = MIMEText("🧪 Email configuration test successful!")
            msg["Subject"] = "🧪 IP Security Test Email"
            msg["From"] = self.email_config["from_email"]
            msg["To"] = self.email_config["to_email"]

            with smtplib.SMTP(
                self.email_config["smtp_server"], int(self.email_config["smtp_port"])
            ) as server:
                server.starttls()
                server.login(
                    self.email_config["from_email"], self.email_config["password"]
                )
                server.send_message(msg)

            return True

        except Exception as e:
            raise EmailSendingError(f"Failed to send email configuration test: {e}")
            return False

    def get_recent_alerts(self, hours: int = 1) -> List[SecurityAlert]:
        """
        Get alerts from the last N hours.
        """
        cutoff = datetime.now() - timedelta(hours=hours)
        return [alert for alert in self.security_alerts if alert.timestamp >= cutoff]
