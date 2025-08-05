# vpn_controls/vpn_source.py
import os
import random
import re
import subprocess
import sys
import time
from subprocess import PIPE, Popen
from typing import Callable, Dict, Optional

from configurations import VpnConfig
from exceptions import TunnelblickRecoveryError, VpnConnectionError
from security import IPDetector


class RequestThrottler:
    """
    Manages the rate of web requests to avoid overloading the server
    and to mimic human browsing behavior, with VPN rotation
    """

    def __init__(
        self,
        vpn_config: VpnConfig,
        termination_callback: Optional[Callable[[str], None]] = None,
    ):
        """
        Initialize VPN connection manager

        Args:
            vpn_config: VPN configuration settings
            termination_callback: Called on critical failures
        """
        self.vpn_config = vpn_config
        self.termination_callback = termination_callback
        self.mandatory_delay = self.vpn_config.mandatory_delay

        # VPN connection state
        self.vpn_enabled = False
        self.current_vpn = None
        self.connection_list = []
        self.recent_configurations = []
        self.avoid_recent_count = 3

        # Recovery tracking
        self.recovery_attempts = 0
        self.max_recovery_attempts = self.vpn_config.max_recovery_attempts

        # VPN credentials
        self.username = None
        self.password = None

        # IP detection for VPN verification
        self.ip_detector = IPDetector()
        self.initial_ip = None

        try:
            self._initialize_vpn()
        except Exception as e:
            self._handle_critical_error(
                "VPN initialization failed. Application cannot continue without VPN."
            )

    def _initialize_vpn(self) -> None:
        """
        Initialize VPN settings and establish initial connection
        """
        # Load VPN credentials
        self._load_vpn_credentials()

        # Get available VPN configurations
        self._load_vpn_configurations()

        # Get initial IP for comparison
        try:
            self.initial_ip = self.ip_detector.get_current_ip()
        except Exception as e:
            self._handle_critical_error("IP detection failed")

        if not self.establish_secure_connection():
            self._handle_critical_error("Failed to establish initial VPN connection")

        self.vpn_enabled = True

    def _load_vpn_credentials(self) -> None:
        """
        Load VPN credentials from environment
        """
        dotenv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")

        if os.path.exists(dotenv_path):
            from dotenv import load_dotenv

            load_dotenv(dotenv_path)

        try:
            with open(dotenv_path, "r") as env_file:
                env_contents = env_file.read()
                for line in env_contents.split("\n"):
                    if "=" in line and not line.strip().startswith("#"):
                        key, value = line.strip().split("=", 1)
                        value = value.strip("'\"")
                        if key == "NORD_USER":
                            self.username = value
                        elif key == "NORD_PASS":
                            self.password = value
        except Exception:
            self.username = os.getenv("NORD_USER")
            self.password = os.getenv("NORD_PASS")

        if not self.username or not self.password:
            self._handle_critical_error("VPN credentials missing")

    def _load_vpn_configurations(self) -> None:
        """
        Load available VPN configurations from Tunnelblick
        """
        raw_configurations = self.execute_applescript(
            'tell application "/Applications/Tunnelblick.app" to get configurations'
        )

        if not raw_configurations:
            self._handle_critical_error("Could not retrieve VPN configurations")

        self.connection_list = [
            x.replace("configuration ", "").strip()
            for x in raw_configurations.split(",")
        ]

        if not self.connection_list:
            self._handle_critical_error("No VPN configurations available")

    def _terminate_application(self, message):
        """
        Handle critical errors - delegate to callback if available
        """
        print(f"\n[CRITICAL ERROR] {message}", file=sys.stderr)

        if self.termination_callback:
            self.termination_callback(message)
        else:
            sys.exit(1)

    def _handle_critical_error(self, message: str):
        """
        Handle critical errors that may require application termination
        """
        if self.termination_callback:
            self.termination_callback(message)
        else:
            self._terminate_application(message)

    def check_tunnelblick_status(self):
        """
        Check if Tunnelblick is running
        Returns (is_running: bool, pids: list)
        """
        try:
            result = subprocess.run(
                ["pgrep", "-f", "Tunnelblick"], capture_output=True, text=True
            )
            if result.returncode == 0:
                pids = result.stdout.strip().split("\n")
                return True, pids
            else:
                return False, []
        except Exception:
            return False, []

    def kill_tunnelblick(self):
        """
        Kill Tunnelblick application
        Returns True if successful, False otherwise
        """
        is_running, pids = self.check_tunnelblick_status()

        if not is_running:
            return True

        try:
            # Try graceful quit first
            graceful_quit = subprocess.run(
                ["osascript", "-e", 'tell application "Tunnelblick" to quit'],
                capture_output=True,
                text=True,
                timeout=30,
            )

            if graceful_quit.returncode == 0:
                time.sleep(5)  # Wait for graceful shutdown

                # Check if it actually quit
                is_running, remaining_pids = self.check_tunnelblick_status()
                if not is_running:
                    return True
        except subprocess.TimeoutExpired:
            pass
        except Exception:
            pass

        # Force kill if graceful quit didn't work
        try:
            # Kill by process name
            subprocess.run(
                ["pkill", "-f", "Tunnelblick"],
                capture_output=True,
                text=True,
                timeout=30,
            )

            time.sleep(10)  # Wait for processes to die

            # Check what's left
            is_running, remaining_pids = self.check_tunnelblick_status()
            if not is_running:
                return True

            # Kill remaining processes individually
            for pid in remaining_pids:
                try:
                    subprocess.run(
                        ["kill", "-9", pid], capture_output=True, text=True, timeout=5
                    )
                except Exception:
                    pass

            time.sleep(2)

            # Final check
            is_running, final_pids = self.check_tunnelblick_status()
            return True  # Consider it success for system daemons
        except Exception:
            return False

    def open_tunnelblick(self):
        """
        Open Tunnelblick application and wait for it to be ready
        Returns True if successful, False otherwise
        """
        # Check if already running
        is_running, pids = self.check_tunnelblick_status()
        if is_running:
            return True

        try:
            # Launch Tunnelblick
            result = subprocess.run(
                ["open", "-a", "Tunnelblick"],
                capture_output=True,
                text=True,
                timeout=15,
            )

            if result.returncode != 0:
                return False

            # Wait for Tunnelblick to start (up to 15 seconds)
            for i in range(15):
                time.sleep(1)
                is_running, pids = self.check_tunnelblick_status()
                if is_running:
                    # Give it a bit more time to fully initialize
                    time.sleep(3)
                    return True

            return False

        except subprocess.TimeoutExpired:
            return False
        except Exception:
            return False

    def recover_tunnelblick(self):
        """
        Complete Tunnelblick recovery process: kill -> restart -> reinitialize
        Returns True if successful, False if recovery failed
        """
        self.recovery_attempts += 1

        if self.recovery_attempts > self.max_recovery_attempts:
            return False

        # Step 1: Kill Tunnelblick
        if not self.kill_tunnelblick():
            return False

        # Step 2: Wait a moment for cleanup
        time.sleep(5)

        # Step 3: Restart Tunnelblick
        if not self.open_tunnelblick():
            return False

        # Step 4: Reinitialize VPN configurations
        try:
            # Get fresh configuration list
            raw_configurations = self.execute_applescript_safe(
                'tell application "/Applications/Tunnelblick.app" to get configurations'
            )

            if not raw_configurations:
                return False

            # Update connection list
            self.connection_list = [
                x.replace("configuration ", "").strip()
                for x in raw_configurations.split(",")
            ]

            # Reset current VPN since old connection is invalid
            self.current_vpn = None

            return True

        except Exception:
            return False

    def execute_applescript_safe(self, scpt, args=[], timeout_seconds=30):
        """
        Execute AppleScript with timeout and recovery mechanism
        If it fails with timeout, trigger Tunnelblick recovery
        """
        try:
            # Add timeout to the AppleScript itself
            scpt_with_timeout = (
                f"with timeout of {timeout_seconds} seconds\n{scpt}\nend timeout"
            )

            p = Popen(
                ["osascript", "-"] + args,
                stdin=PIPE,
                stdout=PIPE,
                stderr=PIPE,
                universal_newlines=True,
            )
            stdout, stderr = p.communicate(
                scpt_with_timeout, timeout=timeout_seconds + 10
            )

            if stderr:
                # Check for timeout error specifically
                if "timed out" in stderr.lower() or "-1712" in stderr:
                    raise TunnelblickRecoveryError(
                        "AppleScript timeout - Tunnelblick unresponsive"
                    )

            return stdout
        except subprocess.TimeoutExpired:
            raise TunnelblickRecoveryError("AppleScript subprocess timeout")
        except TunnelblickRecoveryError:
            raise
        except Exception as e:
            raise

    def execute_applescript(self, scpt, args=[]):
        """
        Enhanced execute_applescript with automatic recovery
        """
        try:
            return self.execute_applescript_safe(scpt, args)
        except TunnelblickRecoveryError:
            if self.recover_tunnelblick():
                try:
                    return self.execute_applescript_safe(scpt, args)
                except Exception as retry_error:
                    raise VpnConnectionError(
                        f"Command failed even after Tunnelblick recovery: {retry_error}"
                    )
            else:
                raise VpnConnectionError(
                    "Tunnelblick recovery failed - cannot continue"
                )

    def _verify_no_active_connections(self):
        """Verify no VPN connections are currently active"""
        try:
            result = self.execute_applescript(
                'tell application "/Applications/Tunnelblick.app" to get state of configurations'
            )
            # Check if any connections show as "CONNECTED"
            return "CONNECTED" not in result.upper()
        except Exception:
            return False

    def _force_disconnect_all(self):
        """Force disconnect with multiple attempts"""
        for attempt in range(3):
            self.disconnect_configurations()
            time.sleep(3)
            if self._verify_no_active_connections():
                return True
        return False

    def establish_secure_connection(self):
        """
        establish_secure_connection with recovery mechanism
        """
        try:
            # Reset recovery attempts counter for new connection attempt
            if self.recovery_attempts > 0:
                self.recovery_attempts = 0

            # Create a list of configurations to try, excluding recently used ones
            available_configs = [
                cfg
                for cfg in self.connection_list
                if cfg not in self.recent_configurations[-self.avoid_recent_count :]
            ]

            if not available_configs:
                available_configs = self.connection_list.copy()

            # Shuffle the list to randomize connection attempts
            random.shuffle(available_configs)

            # Try configurations until one works or we run out
            for config in available_configs:
                try:
                    # Disconnect any existing connections first
                    self.disconnect_configurations()

                    if not self._verify_no_active_connections():
                        self._force_disconnect_all()

                    # Set authentication for this configuration
                    if not self.set_configuration_auth(
                        config, self.username, self.password
                    ):
                        continue

                    time.sleep(5)

                    # Try to connect
                    if self.connect_to_configuration_by_name(config):
                        # Success! Add to recent configurations
                        self.recent_configurations.append(config)
                        if len(self.recent_configurations) > self.avoid_recent_count:
                            self.recent_configurations.pop(0)
                        return True

                except TunnelblickRecoveryError:
                    continue
                except Exception:
                    continue

            return False
        except Exception:
            return False

    def rotate_configuration(self) -> bool:
        """
        Rotate to a new VPN configuration.
        ONLY handles VPN connection changes.

        Returns:
            bool: True if rotation successful, False otherwise
        """
        if not self.vpn_enabled:
            return False

        # Try to establish new connection with retries
        for attempt in range(self.vpn_config.max_recovery_attempts):
            try:
                if self.establish_secure_connection():
                    return True
            except Exception:
                pass

            if attempt < self.vpn_config.max_recovery_attempts - 1:
                time.sleep(5)

        return False

    def connect_to_configuration_by_name(self, configuration_name):
        """
        Connect to a specific VPN configuration
        Returns True if connection successful, False otherwise
        """
        result = self.execute_applescript(
            'tell application "/Applications/Tunnelblick.app" to connect "'
            + configuration_name
            + '"'
        )

        # Set current_vpn immediately so verify_vpn_connection() can check it
        self.current_vpn = configuration_name

        # Give the VPN time to establish connection (increase timeout)
        time.sleep(10)

        # Verify connection is actually working
        if not self.verify_vpn_connection():
            self.current_vpn = None  # Reset since connection failed
            return False

        return True

    def verify_vpn_connection(self) -> bool:
        """
        Comprehensive VPN connection verification.
        Checks multiple indicators to ensure VPN is actually working.

        Returns:
            bool: True if VPN connection verified, False otherwise
        """
        try:
            verification_methods = {
                "process": False,
                "interface": False,
                "routes": False,
                "external_ip": False,
            }

            # Method 1: Check for VPN process
            ps_result = subprocess.run(
                ["ps", "aux"], capture_output=True, text=True, timeout=10
            )
            processes = ps_result.stdout

            vpn_count = 0
            for line in processes.split("\n"):
                if "openvpn" in line.lower() and "nordvpn" in line.lower():
                    vpn_count += 1

            if vpn_count > 1:
                # Fix the problem directly
                if self.recover_tunnelblick():
                    # Recheck process count after recovery
                    time.sleep(10)  # Let processes settle
                    ps_result = subprocess.run(
                        ["ps", "aux"], capture_output=True, text=True, timeout=10
                    )
                    processes = ps_result.stdout

                    vpn_count = 0  # Recount
                    for line in processes.split("\n"):
                        if "openvpn" in line.lower() and "nordvpn" in line.lower():
                            vpn_count += 1

                    if vpn_count == 1:
                        verification_methods["process"] = True
                    else:
                        return False
                else:
                    return False
            elif vpn_count == 1:
                verification_methods["process"] = True
            else:
                return False

            # Method 2: Check VPN interface
            try:
                ifconfig_result = subprocess.run(
                    ["ifconfig"], capture_output=True, text=True, timeout=10
                )
                interfaces = ifconfig_result.stdout

                for line in interfaces.split("\n"):
                    if "inet 10." in line and "netmask" in line:
                        ip_match = re.search(r"inet (10\.\d+\.\d+\.\d+)", line)
                        if ip_match:
                            verification_methods["interface"] = True
                            break
            except Exception:
                pass

            # Method 3: Check VPN routes
            try:
                route_result = subprocess.run(
                    ["netstat", "-rn"], capture_output=True, text=True, timeout=10
                )
                routes = route_result.stdout

                for line in routes.split("\n"):
                    if "utun" in line and ("10." in line or "0/1" in line):
                        verification_methods["routes"] = True
                        break
            except Exception:
                pass

            # Method 4: External IP verification
            try:
                ip_check = subprocess.run(
                    ["curl", "--silent", "--max-time", "10", "https://api.ipify.org"],
                    capture_output=True,
                    text=True,
                )

                if ip_check.returncode == 0 and ip_check.stdout.strip():
                    current_external_ip = ip_check.stdout.strip()

                    if hasattr(self, "initial_ip") and self.initial_ip:
                        if self.initial_ip != current_external_ip:
                            verification_methods["external_ip"] = True
                        else:
                            pass
                    else:
                        verification_methods["external_ip"] = True
                else:
                    pass
            except Exception:
                pass

            # Evaluate verification results
            passed_methods = sum(verification_methods.values())

            # Require at least 3 methods to pass for high confidence
            if passed_methods >= 3:
                return True
            else:
                return False

        except Exception:
            return False

    def get_connected_vpn_name(self):
        """
        Helper method to get the name of currently connected VPN
        Returns VPN name or None if no VPN connected
        """
        try:
            # Check process list for running VPN
            ps_result = subprocess.run(
                ["ps", "aux"], capture_output=True, text=True, timeout=10
            )
            processes = ps_result.stdout

            for line in processes.split("\n"):
                if "openvpn" in line.lower() and "nordvpn" in line.lower():
                    if "--config" in line:
                        config_match = re.search(
                            r"--config.*?([a-z]{2}\d+\.nordvpn\.com\.udp)", line
                        )
                        if config_match:
                            return config_match.group(1)

            return None
        except Exception:
            return None

    def disconnect_configurations(self):
        """
        Disconnect all VPN connections
        """
        result = self.execute_applescript(
            'tell application "/Applications/Tunnelblick.app" to disconnect all'
        )
        time.sleep(self.mandatory_delay)
        result_clean = result.strip()

        try:
            disconnected_count = int(result_clean)
            self.current_vpn = None
            return True
        except ValueError:
            return False

    def set_configuration_auth(self, configuration_name, username, password):
        """
        Set authentication for a VPN configuration
        """
        name = self.execute_applescript(
            'tell application "/Applications/Tunnelblick.app" to save username "'
            + username
            + '" for "'
            + configuration_name
            + '"'
        )
        cred = self.execute_applescript(
            'tell application "/Applications/Tunnelblick.app" to save password "'
            + password
            + '" for "'
            + configuration_name
            + '"'
        )

        if name.strip().lower() == "true" and cred.strip().lower() == "true":
            return True
        else:
            return False

    def get_vpn_status(self) -> Dict:
        """
        Get current VPN status information.
        ONLY VPN-specific information, no request counting.

        Returns:
            dict: VPN status information
        """
        return {
            "vpn_enabled": self.vpn_enabled,
            "current_vpn": self.current_vpn,
            "available_configurations": len(self.connection_list),
            "recent_configurations": len(self.recent_configurations),
            "recovery_attempts": self.recovery_attempts,
            "max_recovery_attempts": self.max_recovery_attempts,
            "initial_ip": self.initial_ip,
        }
