# vpn_controls/vpn_root.py
import os
import time
import random
import sys
import re
import subprocess
from typing import Optional, Callable, Dict
from subprocess import Popen, PIPE
from utils.logging.logger import setup_logger
from utils.configurations import VpnConfig
from core.security.ip_detector import IPDetector

logger = setup_logger("VpnManager", "logs/vpn/RootVpn.log")

class VpnConnectionError(Exception):
    """Exception raised when VPN connection fails"""
    pass

class TunnelblickRecoveryError(Exception):
    """Exception raised when VPN connection fails"""
    pass

class RequestThrottler:
    """
    Manages the rate of web requests to avoid overloading the server
    and to mimic human browsing behavior, with VPN rotation
    """

    def __init__(self,
                 vpn_config: VpnConfig,
                 termination_callback: Optional[Callable[[str], None]] = None):
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
            logger.critical(f"Fatal error initializing VPN: {e}")
            self._handle_critical_error("VPN initialization failed. Application cannot continue without VPN.")

    def _initialize_vpn(self) -> None:
        """
        Initialize VPN settings and establish initial connection
        """
        logger.info("Initializing VPN connection manager...")
        
        # Load VPN credentials
        self._load_vpn_credentials()
        
        # Get available VPN configurations
        self._load_vpn_configurations()
        
        # Get initial IP for comparison
        try:
            self.initial_ip = self.ip_detector.get_current_ip()
            logger.info(f"Initial IP (without VPN): {self.initial_ip}")
        except Exception as e:
            logger.critical(f"Could not determine initial IP: {e}")
            self._handle_critical_error("IP detection failed")

        if not self.establish_secure_connection():
            self._handle_critical_error("Failed to establish initial VPN connection")
        
        self.vpn_enabled = True
        logger.info("✅ VPN connection manager initialized successfully")

    def _load_vpn_credentials(self) -> None:
        """
        Load VPN credentials from environment
        """
        dotenv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
        
        if os.path.exists(dotenv_path):
            from dotenv import load_dotenv
            load_dotenv(dotenv_path)
            try:
                relative_path = os.path.relpath(dotenv_path)
                logger.info(f"Loaded .env file from {relative_path}")
            except ValueError:
                logger.info("Loaded .env file successfully")

        try:
            with open(dotenv_path, 'r') as env_file:
                env_contents = env_file.read()
                for line in env_contents.split('\n'):
                    if '=' in line and not line.strip().startswith('#'):
                        key, value = line.strip().split('=', 1)
                        value = value.strip('\'"')
                        if key == 'NORD_USER':
                            self.username = value
                        elif key == 'NORD_PASS':
                            self.password = value
        except Exception as e:
            logger.error(f"Error reading .env file: {e}")
            self.username = os.getenv('NORD_USER')
            self.password = os.getenv('NORD_PASS')

        if not self.username or not self.password:
            self._handle_critical_error("VPN credentials missing")
        
        logger.info(f"VPN credentials loaded: {self.username[:2]}***")

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
        
        logger.info(f"{len(self.connection_list)} VPN configurations found")
        
        if not self.connection_list:
            self._handle_critical_error("No VPN configurations available")

    def _terminate_application(self, message):
        """
        Handle critical errors - delegate to callback if available
        """
        logger.critical(message)
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
            result = subprocess.run(['pgrep', '-f', 'Tunnelblick'], 
                                  capture_output=True, text=True)
            if result.returncode == 0:
                pids = result.stdout.strip().split('\n')
                return True, pids
            else:
                return False, []
        except Exception as error:
            logger.error(f"Error checking Tunnelblick status: {error}")
            return False, []
    
    def kill_tunnelblick(self):
        """
        Kill Tunnelblick application
        Returns True if successful, False otherwise
        """
        logger.warning("🔧 Tunnelblick appears unresponsive - attempting recovery...")
        
        is_running, pids = self.check_tunnelblick_status()
        
        if not is_running:
            logger.info("Tunnelblick is not running.")
            return True
        
        logger.info(f"Tunnelblick is running with PID(s): {', '.join(pids)}")
        logger.info("Attempting graceful quit...")
        
        try:
            # Try graceful quit first
            graceful_quit = subprocess.run([
                'osascript', '-e', 
                'tell application "Tunnelblick" to quit'
            ], capture_output=True, text=True, timeout=30)
            
            if graceful_quit.returncode == 0:
                logger.info("Sent quit command to Tunnelblick.")
                time.sleep(5)  # Wait for graceful shutdown

                # Check if it actually quit
                is_running, remaining_pids = self.check_tunnelblick_status()
                if not is_running:
                    logger.info("✅ Tunnelblick quit gracefully.")
                    return True
                else:
                    logger.warning(f"Some processes still running: {remaining_pids}")
        except subprocess.TimeoutExpired:
            logger.warning("Graceful quit timed out.")
        except Exception as error:
            logger.warning(f"Graceful quit failed: {error}")
        
        # Force kill if graceful quit didn't work
        logger.info("Attempting force kill...")
        try:
            # Kill by process name
            subprocess.run(['pkill', '-f', 'Tunnelblick'], 
                          capture_output=True, text=True, timeout=30)
            
            time.sleep(10)  # Wait for processes to die
            
            # Check what's left
            is_running, remaining_pids = self.check_tunnelblick_status()
            if not is_running:
                logger.info("✅ Tunnelblick killed successfully.")
                return True
            
            # Kill remaining processes individually
            logger.info(f"Killing remaining processes: {remaining_pids}")
            for pid in remaining_pids:
                try:
                    subprocess.run(['kill', '-9', pid], capture_output=True, text=True, timeout=5)
                except Exception:
                    pass
            
            time.sleep(2)
            
            # Final check
            is_running, final_pids = self.check_tunnelblick_status()
            if not is_running:
                logger.info("✅ All Tunnelblick processes killed.")
                return True
            else:
                logger.warning(f"Some processes may still be running: {final_pids}")
                return True  # Consider it success for system daemons  
        except Exception as e:
            logger.error(f"❌ Error during force kill: {e}")
            return False

    def open_tunnelblick(self):
        """
        Open Tunnelblick application and wait for it to be ready
        Returns True if successful, False otherwise
        """
        logger.info("Opening Tunnelblick...")
        
        # Check if already running
        is_running, pids = self.check_tunnelblick_status()
        if is_running:
            logger.info(f"Tunnelblick is already running with PID(s): {', '.join(pids)}")
            return True
        
        try:
            # Launch Tunnelblick
            result = subprocess.run(['open', '-a', 'Tunnelblick'], 
                                  capture_output=True, text=True, timeout=15)
            
            if result.returncode != 0:
                logger.error(f"Failed to launch Tunnelblick: {result.stderr}")
                return False
            
            logger.info("Tunnelblick launch command sent, waiting for startup...")
            
            # Wait for Tunnelblick to start (up to 15 seconds)
            for i in range(15):
                time.sleep(1)
                is_running, pids = self.check_tunnelblick_status()
                if is_running:
                    logger.info(f"✅ Tunnelblick started with PID(s): {', '.join(pids)}")
                    # Give it a bit more time to fully initialize
                    time.sleep(3)
                    return True
                
                if i % 3 == 0:  # Log every 3 seconds
                    logger.info(f"Waiting for Tunnelblick to start... ({i+1}/15)")
            
            logger.error("❌ Tunnelblick failed to start within 15 seconds")
            return False
            
        except subprocess.TimeoutExpired:
            logger.error("❌ Tunnelblick launch timed out")
            return False
        except Exception as e:
            logger.error(f"❌ Error opening Tunnelblick: {e}")
            return False

    def recover_tunnelblick(self):
        """
        Complete Tunnelblick recovery process: kill -> restart -> reinitialize
        Returns True if successful, False if recovery failed
        """
        self.recovery_attempts += 1
        
        if self.recovery_attempts > self.max_recovery_attempts:
            logger.critical(f"❌ Maximum recovery attempts ({self.max_recovery_attempts}) exceeded")
            return False
        
        logger.warning(f"🔧 Starting Tunnelblick recovery attempt {self.recovery_attempts}/{self.max_recovery_attempts}")
        
        # Step 1: Kill Tunnelblick
        if not self.kill_tunnelblick():
            logger.error("❌ Failed to kill Tunnelblick during recovery")
            return False
        
        # Step 2: Wait a moment for cleanup
        logger.info("Waiting for system cleanup...")
        time.sleep(5)
        
        # Step 3: Restart Tunnelblick
        if not self.open_tunnelblick():
            logger.error("❌ Failed to restart Tunnelblick during recovery")
            return False
        
        # Step 4: Reinitialize VPN configurations
        try:
            logger.info("Reinitializing VPN configurations...")
            
            # Get fresh configuration list
            raw_configurations = self.execute_applescript_safe(
                'tell application "/Applications/Tunnelblick.app" to get configurations'
            )
            
            if not raw_configurations:
                logger.error("❌ Failed to get VPN configurations after recovery")
                return False
            
            # Update connection list
            self.connection_list = [x.replace("configuration ", "").strip() 
                                  for x in raw_configurations.split(",")]
            logger.info(f"✅ Reloaded {len(self.connection_list)} VPN configurations")
            
            # Reset current VPN since old connection is invalid
            self.current_vpn = None
            
            logger.info("✅ Tunnelblick recovery completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error during VPN reinitialization: {e}")
            return False

    def execute_applescript_safe(self, scpt, args=[], timeout_seconds=30):
        """
        Execute AppleScript with timeout and recovery mechanism
        If it fails with timeout, trigger Tunnelblick recovery
        """
        try:
            # Add timeout to the AppleScript itself
            scpt_with_timeout = f'with timeout of {timeout_seconds} seconds\n{scpt}\nend timeout'
            
            p = Popen(['osascript', '-'] + args, stdin=PIPE, stdout=PIPE, stderr=PIPE, universal_newlines=True)
            stdout, stderr = p.communicate(scpt_with_timeout, timeout=timeout_seconds + 10)

            if stderr:
                logger.error(f"AppleScript stderr: {stderr}")

                # Check for timeout error specifically
                if "timed out" in stderr.lower() or "-1712" in stderr:
                    logger.error("🚨 AppleScript timeout detected - triggering Tunnelblick recovery")
                    raise TunnelblickRecoveryError("AppleScript timeout - Tunnelblick unresponsive")
            
            return stdout
        except subprocess.TimeoutExpired:
            logger.error(f"AppleScript command timed out after {timeout_seconds} seconds")
            raise TunnelblickRecoveryError("AppleScript subprocess timeout")
        except TunnelblickRecoveryError:
            raise
        except Exception as e:
            logger.error(f"AppleScript execution error: {e}")
            raise

    def execute_applescript(self, scpt, args=[]):
        """
        Enhanced execute_applescript with automatic recovery
        """
        try:
            return self.execute_applescript_safe(scpt, args)
        except TunnelblickRecoveryError as e:
            logger.warning(f"Tunnelblick recovery needed: {e}")

            if self.recover_tunnelblick():
                logger.info("🔄 Retrying AppleScript command after successful recovery...")
                try:
                    return self.execute_applescript_safe(scpt, args)
                except Exception as retry_error:
                    logger.error(f"❌ Command still failed after recovery: {retry_error}")
                    raise VpnConnectionError(f"Command failed even after Tunnelblick recovery: {retry_error}")
            else:
                logger.critical("❌ Tunnelblick recovery failed")
                raise VpnConnectionError("Tunnelblick recovery failed - cannot continue")
    
    def _verify_no_active_connections(self):
        """Verify no VPN connections are currently active"""
        try:
            result = self.execute_applescript(
                'tell application "/Applications/Tunnelblick.app" to get state of configurations'
            )
            # Check if any connections show as "CONNECTED"
            return "CONNECTED" not in result.upper()
        except Exception as e:
            logger.warning(f"Could not verify connection state: {e}")
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
                logger.info(f"Resetting recovery attempts counter was {self.recovery_attempts})")
                self.recovery_attempts = 0
            
            # Create a list of configurations to try, excluding recently used ones
            available_configs = [cfg for cfg in self.connection_list 
                            if cfg not in self.recent_configurations[-self.avoid_recent_count:]]

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
                        logger.warning("Some connections still active, forcing disconnect")
                        self._force_disconnect_all()
                    
                    logger.info(f"Attempting to connect to {config}")
                    # Set authentication for this configuration
                    if not self.set_configuration_auth(config, self.username, self.password):
                        logger.warning(f"Could not set auth for {config}, trying next configuration")
                        continue
                    
                    time.sleep(5)
                    
                    # Try to connect
                    if self.connect_to_configuration_by_name(config):
                        # Success! Add to recent configurations
                        self.recent_configurations.append(config)
                        if len(self.recent_configurations) > self.avoid_recent_count:
                            self.recent_configurations.pop(0)
                        logger.info(f"✅ Successfully connected to {config}")
                        return True
                    
                    logger.warning(f"Connection to {config} failed, trying next configuration")
                    
                except TunnelblickRecoveryError:
                    logger.warning(f"Skipping {config} due to Tunnelblick issues, trying next...")
                    continue
                except Exception as e:
                    logger.warning(f"Unexpected error with {config}: {e}, trying next...")
                    continue

            logger.critical("❌ All VPN connection attempts failed")
            return False
        except Exception as e:
            logger.error(f"❌ Critical error in establish_secure_connection: {e}")
            return False

    def rotate_configuration(self) -> bool:
        """
        Rotate to a new VPN configuration.
        ONLY handles VPN connection changes.
        
        Returns:
            bool: True if rotation successful, False otherwise
        """
        if not self.vpn_enabled:
            logger.error("VPN rotation requested but VPN not enabled")
            return False

        logger.info("🔄 Starting VPN configuration rotation...")
        
        # Try to establish new connection with retries
        for attempt in range(self.vpn_config.max_recovery_attempts):
            logger.info(f"VPN rotation attempt {attempt+1}/{self.vpn_config.max_recovery_attempts}")
            
            try:
                if self.establish_secure_connection():
                    logger.info(f"✅ VPN rotation successful to {self.current_vpn}")
                    return True
                
                logger.warning(f"Rotation attempt {attempt+1} failed")
                
            except Exception as e:
                logger.error(f"Error during rotation attempt {attempt+1}: {e}")
            
            if attempt < self.vpn_config.max_recovery_attempts - 1:
                time.sleep(5)

        logger.error("❌ All VPN rotation attempts failed")
        return False

    def connect_to_configuration_by_name(self, configuration_name):
        """
        Connect to a specific VPN configuration
        Returns True if connection successful, False otherwise
        """
        result = self.execute_applescript('tell application "/Applications/Tunnelblick.app" to connect "' + configuration_name + '"')
        logger.info(f"Connection result for {configuration_name}: {result}")

        # Set current_vpn immediately so verify_vpn_connection() can check it
        self.current_vpn = configuration_name
        
        # Give the VPN time to establish connection (increase timeout)
        logger.info("Waiting for VPN connection to establish...")
        time.sleep(10)
        
        # Verify connection is actually working
        if not self.verify_vpn_connection():
            logger.error(f"VPN connection to {configuration_name} failed verification")
            self.current_vpn = None  # Reset since connection failed
            return False
        
        logger.info(f"VPN connection to {configuration_name} established and verified")
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
                'process': False,
                'interface': False,
                'routes': False,
                'external_ip': False
            }
            
            # Method 1: Check for VPN process
            ps_result = subprocess.run(['ps', 'aux'], capture_output=True, text=True, timeout=10)
            processes = ps_result.stdout
            
            vpn_count = 0
            for line in processes.split('\n'):
                if 'openvpn' in line.lower() and 'nordvpn' in line.lower():
                    vpn_count += 1
            
            if vpn_count > 1:
                logger.critical(f"🚨 Multiple VPN processes detected: {vpn_count}")
                logger.critical("🔄 Attempting recovery...")
                
                # Fix the problem directly
                if self.recover_tunnelblick():
                    logger.info("✅ Recovery successful - rechecking processes...")
                    
                    # Recheck process count after recovery
                    time.sleep(10)  # Let processes settle
                    ps_result = subprocess.run(['ps', 'aux'], capture_output=True, text=True, timeout=10)
                    processes = ps_result.stdout
                    
                    vpn_count = 0  # Recount
                    for line in processes.split('\n'):
                        if 'openvpn' in line.lower() and 'nordvpn' in line.lower():
                            vpn_count += 1
                    
                    if vpn_count == 1:
                        verification_methods['process'] = True
                        logger.info("✅ Single VPN process after recovery")
                    else:
                        logger.error(f"❌ Still {vpn_count} VPN processes after recovery")
                        return False
                else:
                    logger.error("❌ Recovery failed")
                    return False
            elif vpn_count == 1:
                verification_methods['process'] = True
                logger.info("✅ Single VPN process detected")
            else:
                logger.error("❌ No VPN process detected")
                return False
            
            # Method 2: Check VPN interface
            try:
                ifconfig_result = subprocess.run(['ifconfig'], capture_output=True, text=True, timeout=10)
                interfaces = ifconfig_result.stdout
                
                for line in interfaces.split('\n'):
                    if 'inet 10.' in line and 'netmask' in line:
                        ip_match = re.search(r'inet (10\.\d+\.\d+\.\d+)', line)
                        if ip_match:
                            vpn_ip = ip_match.group(1)
                            verification_methods['interface'] = True
                            logger.info(f"✅ VPN interface detected: {vpn_ip}")
                            break
            except Exception as e:
                logger.warning(f"Could not check VPN interfaces: {e}")
            
            # Method 3: Check VPN routes
            try:
                route_result = subprocess.run(['netstat', '-rn'], capture_output=True, text=True, timeout=10)
                routes = route_result.stdout
                
                for line in routes.split('\n'):
                    if 'utun' in line and ('10.' in line or '0/1' in line):
                        verification_methods['routes'] = True
                        logger.info("✅ VPN routes detected")
                        break
            except Exception as e:
                logger.warning(f"Could not check routing table: {e}")
            
            # Method 4: External IP verification
            try:
                ip_check = subprocess.run(
                    ['curl', '--silent', '--max-time', '10', 'https://api.ipify.org'], 
                    capture_output=True, text=True
                )
                
                if ip_check.returncode == 0 and ip_check.stdout.strip():
                    current_external_ip = ip_check.stdout.strip()
                    logger.info(f"Current external IP: {current_external_ip}")
                    
                    if hasattr(self, 'initial_ip') and self.initial_ip:
                        if self.initial_ip != current_external_ip:
                            verification_methods['external_ip'] = True
                            logger.info(f"✅ IP changed: {self.initial_ip} → {current_external_ip}")
                        else:
                            logger.error(f"❌ IP unchanged: {current_external_ip}")
                    else:
                        verification_methods['external_ip'] = True
                        logger.info("✅ External IP check passed (no baseline)")
                else:
                    logger.error("❌ Could not verify external IP")
            except Exception as e:
                logger.error(f"External IP verification error: {e}")
            
            # Evaluate verification results
            passed_methods = sum(verification_methods.values())
            logger.info(f"VPN verification: {passed_methods}/4 methods passed")
            
            # Require at least 3 methods to pass for high confidence
            if passed_methods >= 3:
                logger.info("✅ VPN connection fully verified")
                return True
            else:
                logger.error(f"❌ VPN verification failed: only {passed_methods}/4 methods passed")
                return False
                
        except Exception as e:
            logger.error(f"VPN verification error: {e}")
            return False

    def get_connected_vpn_name(self):
        """
        Helper method to get the name of currently connected VPN
        Returns VPN name or None if no VPN connected
        """
        try:
            # Check process list for running VPN
            ps_result = subprocess.run(['ps', 'aux'], capture_output=True, text=True, timeout=10)
            processes = ps_result.stdout
            
            for line in processes.split('\n'):
                if 'openvpn' in line.lower() and 'nordvpn' in line.lower():
                    if '--config' in line:
                        config_match = re.search(r'--config.*?([a-z]{2}\d+\.nordvpn\.com\.udp)', line)
                        if config_match:
                            return config_match.group(1)
            
            return None
        except Exception as e:
            logger.warning(f"Could not determine connected VPN name: {e}")
            return None

    def disconnect_configurations(self):
        """
        Disconnect all VPN connections
        """
        result = self.execute_applescript('tell application "/Applications/Tunnelblick.app" to disconnect all')
        time.sleep(self.mandatory_delay)
        result_clean = result.strip()
        
        try:
            disconnected_count = int(result_clean)
            logger.info(f"Disconnected VPN successfully ({disconnected_count} connections were active)")
            self.current_vpn = None
            return True
        except ValueError:
            logger.error(f"Failed to disconnect VPN. Tunnelblick returned non-numeric result: '{result_clean}'")
            return False

    def set_configuration_auth(self, configuration_name, username, password):
        """
        Set authentication for a VPN configuration
        """
        logger.info(f"Setting auth for {configuration_name} with username {username[:2]}***")
        name = self.execute_applescript('tell application "/Applications/Tunnelblick.app" to save username "' + username + '" for "' + configuration_name + '"')
        cred = self.execute_applescript('tell application "/Applications/Tunnelblick.app" to save password "' + password + '" for "' + configuration_name + '"')

        if name.strip().lower() == "true" and cred.strip().lower() == "true":
            logger.info(f"{configuration_name} credentials activated")
            return True
        else:
            logger.error(f"Failed to activate credentials for {configuration_name}. Please check your configuration.")
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
            "initial_ip": self.initial_ip
        }
