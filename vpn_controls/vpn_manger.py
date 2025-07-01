# core/scrapers/vpn_handler.py
"""
VPN management and request timing handler.
"""

import time
from typing import Optional, Dict
import threading

from utils.configurations import VpnConfig
from .vpn_root import RequestThrottler
from .constants import LoggingConstants
from .exceptions import VpnRequiredError, VpnConnectionError
from utils.logging.logger import setup_logger
from core.security.security_manager import IPSecurityManager, IPSecurityViolationError


class VpnProtectionHandler:
    """
    Handles VPN protection, timing, and rotation logic
    """
    def __init__(self,
                 config: VpnConfig,
                 logger_name: str = "VpnHandler",
                 env_file_path: Optional[str] = None):
        """
        Initialize VPN protection handler
        """
        self.config = config
        self.logger = setup_logger("VpnHandler", "logs/vpn/vpn_handler.log")
        self.vpn_manager: Optional[RequestThrottler] = None
        self.vpn_protection_active: bool = False
        self.vpn_rotating = False
        self.rotation_lock = threading.Lock()

        # Initialize SecurityManager
        self.ip_security = IPSecurityManager(
            rotation_callback=self._perform_vpn_rotation,
            shared_lock=self.rotation_lock
        )
        self.ip_security._vpn_handler_instance = self
        
        if self.config.use_vpn:
            self._initialize_vpn_manager()
        else:
            self.logger.warning(LoggingConstants.VPN_DISABLED_MSG)

        self.ip_security.activate_monitoring()
    
    def _initialize_vpn_manager(self) -> None:
        """
        Initialize VPN manager.
        """
        self.logger.info("VPN protection is REQUIRED - initializing...")
        
        try:
            self.vpn_manager = RequestThrottler(
                vpn_config=self.config,
                termination_callback=self._handle_vpn_termination
            )

            if self.vpn_manager.vpn_enabled:
                self.vpn_protection_active = True
                self.logger.info("VPN is ACTIVE - protection enabled")
            else:
                error_msg = LoggingConstants.VPN_REQUIRED_ERROR_MSG
                self.logger.error(error_msg)
                raise VpnRequiredError(error_msg)
        except VpnRequiredError:
            raise
        except Exception as e:
            error_msg = f"VPN initialization failed: {e}"
            self.logger.error(error_msg)
            raise VpnConnectionError(error_msg)

    def _handle_vpn_termination(self, error_message: str) -> None:
        """
        Handle critical VPN failures from RequestThrottler.
        """
        self.logger.critical(f"🚨 VPN CRITICAL ERROR: {error_message}")
        
        # Set reboot flag in SecurityManager (single source of truth)
        self.ip_security.reboot_required = True
        
        # Send alert through SecurityManager's alert system
        try:
            self.ip_security._send_critical_alert(
                f"VPN termination: {error_message}",
                self.ip_security.current_ip,
                self.ip_security.request_count
            )
        except Exception as alert_error:
            self.logger.error(f"Failed to send termination alert: {alert_error}")
        
        # Clean up resources
        try:
            self._cleanup_vpn_resources()
        except Exception as cleanup_error:
            self.logger.error(f"Error during emergency cleanup: {cleanup_error}")
        
        # Raise exception to bubble up
        raise IPSecurityViolationError(f"VPN CRITICAL FAILURE: {error_message}")

    def ensure_vpn_protection(self) -> None:
        """
        Verify VPN protection is active - ONLY VPN status checking.
        SecurityManager handles reboot flag checking.
        """
        if self.config.use_vpn and not self.vpn_protection_active:
            error_msg = LoggingConstants.VPN_SECURITY_VIOLATION_MSG
            self.logger.error(error_msg)
            raise VpnRequiredError(error_msg)
    
    def handle_request_timing(self, operation: str = "request") -> None:
        """
        MAIN METHOD - Handle request timing with clear responsibility separation:
        1. VPN status checking (this handler)
        2. Security decisions (SecurityManager)
        3. VPN timing delays (this handler)
        """
        # STEP 1: VPN status validation
        self.ensure_vpn_protection()
        
        # STEP 2: Delegate ALL security decisions to SecurityManager
        # This handles: rotation evaluation, rotation execution, request counting
        try:
            self.ip_security.check_request()
        except IPSecurityViolationError as e:
            self.logger.error(f"🚨 Security violation during {operation}: {e}")
            raise

        # STEP 3: Apply VPN-specific timing delays
        self._apply_request_delays(operation)
    
    def _apply_request_delays(self, operation: str) -> None:
        """
        Apply VPN-specific timing delays - ONLY timing logic
        """
        if self.vpn_manager and self.vpn_protection_active:
            delay = self.config.mandatory_delay
            self.logger.debug(f"Applying VPN delay of {delay}s for {operation}")
            time.sleep(delay)
        else:
            # Fallback delay when VPN not required
            delay = self.config.request_delay
            self.logger.debug(f"Applying fallback delay of {delay}s for {operation}")
            time.sleep(delay)
    
    def _perform_vpn_rotation(self) -> None:
        """
        Called by SecurityManager when rotation is needed.
        ONLY handles VPN operations - NO security logic.
        """
        self.logger.info("🔄 Executing VPN rotation...")
        
        if not (self.vpn_manager and self.vpn_protection_active):
            raise VpnConnectionError("VPN manager not available for rotation")
        
        try:
            # ONLY VPN operations - RequestThrottler handles the details
            rotation_success = self.vpn_manager.rotate_configuration()
            
            if rotation_success:
                self.logger.info("✅ VPN rotation completed successfully")
            else:
                # Attempt recovery
                self.logger.warning("❌ VPN rotation failed - attempting recovery")
                
                if self.vpn_manager.recover_tunnelblick():
                    self.logger.info("🔄 Tunnelblick recovery completed")
                    
                    if self.vpn_manager.establish_secure_connection():
                        self.logger.info("✅ VPN recovery and reconnection successful")
                    else:
                        raise VpnConnectionError("Failed to establish connection after recovery")
                else:
                    raise VpnConnectionError("Tunnelblick recovery failed")
                    
        except Exception as e:
            self.logger.error(f"❌ VPN rotation failed: {e}")
            raise VpnConnectionError(f"VPN rotation failed: {e}")
    
    def get_vpn_statistics(self) -> Dict:
        """
        Get VPN statistics - queries SecurityManager for authoritative counts
        """
        if not (self.vpn_manager and self.vpn_protection_active):
            return {"vpn_protection": "DISABLED"}
        
        # Get authoritative data from SecurityManager
        security_status = self.ip_security.get_security_status()
        
        return {
            "vpn_protection": "ACTIVE",
            "current_ip": security_status['current_ip'],
            "requests_on_current_ip": security_status['request_count'],
            "max_requests_per_ip": security_status['max_requests_per_ip'],
            "total_rotations": security_status['total_rotations'],
            "time_on_current_ip": security_status['time_since_rotation'],
            "rotation_success_rate": security_status['rotation_success_rate']
        }
    
    def get_comprehensive_vpn_statistics(self) -> Dict:
        """
        Get comprehensive statistics combining VPN and security data
        """
        vpn_stats = self.get_vpn_statistics()
        security_stats = self.ip_security.get_security_status()
        
        return {
            **vpn_stats,
            "security_details": {
                "monitoring_active": security_stats['monitoring_active'],
                "reboot_required": security_stats['reboot_required'],
                "recent_alerts": security_stats['recent_alerts'],
                "rotation_callback_configured": security_stats['rotation_callback_configured'],
                "rotation_in_progress": security_stats['rotation_in_progress']
            },
            "rotation_history_count": len(self.ip_security.get_rotation_history())
        }

    def _cleanup_vpn_resources(self) -> None:
        """
        Clean up VPN-specific resources
        """
        try:
            if self.vpn_manager and self.vpn_protection_active:
                self.vpn_manager.disconnect_configurations()
                self.logger.info("VPN connections disconnected")
        except Exception as e:
            self.logger.error(f"Error disconnecting VPN: {e}")
    
    def cleanup(self) -> None:
        """
        Clean up all resources - delegates to appropriate managers
        """
        self.logger.info("🔒 Starting VPN handler cleanup...")
        
        try:
            # Get final statistics before cleanup
            final_stats = self.get_comprehensive_vpn_statistics()
            
            # Clean up VPN resources
            self._cleanup_vpn_resources()
            
            # Delegate security cleanup to SecurityManager
            self.ip_security.cleanup()
            
            # Log final statistics
            self.logger.info("🔒 FINAL VPN STATISTICS:")
            self.logger.info(f"   VPN Protection: {final_stats.get('vpn_protection', 'N/A')}")
            self.logger.info(f"   Total Requests: {final_stats.get('requests_on_current_ip', 0)}")
            self.logger.info(f"   Total Rotations: {final_stats.get('total_rotations', 0)}")
            self.logger.info(f"   Recent Alerts: {final_stats.get('security_details', {}).get('recent_alerts', 0)}")
            self.logger.info(f"   Success Rate: {final_stats.get('rotation_success_rate', 0):.1f}%")

            self.logger.info("✅ VPN handler cleanup completed")
            
        except Exception as e:
            self.logger.error(f"Error during VPN cleanup: {e}")

    # Properties for external access - delegate to SecurityManager
    @property
    def is_active(self) -> bool:
        """
        Check if VPN protection is active
        """
        return (
            self.vpn_protection_active and 
            self.ip_security.is_monitoring_active
        )

    @property
    def security_status(self) -> str:
        """
        Get current security status as string
        """
        status = self.ip_security.get_security_status()
        
        if status['reboot_required']:
            return "🚨 REBOOT REQUIRED - Rotation Failures"
        elif status['rotation_in_progress']:
            return "🔄 ROTATION IN PROGRESS"
        elif status.get('should_rotate', False):
            return "🔄 ROTATION NEEDED"
        elif status['monitoring_active']:
            return "🔒 ACTIVE - Monitoring"
        else:
            return "⚠️ INACTIVE - No Monitoring"
    
    @property
    def current_ip(self) -> str:
        """
        Get current IP from SecurityManager
        """
        return self.ip_security.current_ip
    
    @property
    def request_count(self) -> int:
        """
        Get current request count from SecurityManager
        """
        return self.ip_security.request_count