# security/ip_detector.py
"""
IP detection service
"""

from typing import Optional

import requests

from configurations import IpSecurityConfig, VpnConfig
from exceptions import IPDetectionError


class IPDetector:
    """
    Handles IP address detection with multiple service fallbacks
    """

    def __init__(self, security_config: Optional[IpSecurityConfig] = None):
        self.security_config = security_config or IpSecurityConfig(
            vpn_config=VpnConfig()
        )

        self.services = [
            ("https://api.ipify.org", self._parse_plain_text),
            ("https://httpbin.org/ip", self._parse_httpbin),
            ("https://icanhazip.com", self._parse_plain_text),
            ("https://ifconfig.me/ip", self._parse_plain_text),
            ("https://api.myip.com", self._parse_plain_text),
        ]

    def get_current_ip(self) -> str:
        """
        Get current IP address with multiple service fallbacks.

        Returns:
            Current IP address

        Raises:
            IPDetectionError: If unable to detect IP from any service
        """
        for service_url, parser in self.services:
            try:
                response = requests.get(
                    service_url, timeout=self.security_config.wait_time
                )
                response.raise_for_status()

                ip = parser(response)
                if self._is_valid_ip(ip):
                    return ip

            except Exception as e:
                raise IPDetectionError(f"IP detection failed: {e}")

        raise IPDetectionError("Unable to detect IP address - all services failed")

    def _parse_plain_text(self, response: requests.Response) -> str:
        """
        Parse plain text IP response
        """
        return response.text.strip()

    def _parse_httpbin(self, response: requests.Response) -> str:
        """
        Parse httpbin JSON response
        """
        data = response.json()
        return data["origin"].split(",")[0].strip()

    def _is_valid_ip(self, ip: str) -> bool:
        """
        Basic IP validation
        """
        if not ip:
            return False

        parts = ip.split(".")
        if len(parts) != 4:
            return False

        try:
            return all(0 <= int(part) <= 255 for part in parts)
        except ValueError:
            return False
