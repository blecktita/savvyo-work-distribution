#!/usr/bin/env python3
"""
Main script with macOS sleep prevention for M4 Mac
"""
import subprocess
import signal
import sys
import atexit
import logging

# ✅ APPROACH 1: Set VPN logging level IMMEDIATELY at import time
logging.getLogger("VpnHandler_file").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("selenium").setLevel(logging.WARNING)

from pipelines.run_competition_club_collection import scrape_club_data
from selenium import webdriver

def setup_production_logging():
    """Setup clean logging for production runs - Enhanced version"""
    
    # Multiple approaches to ensure VPN noise is suppressed
    vpn_logger = logging.getLogger("VpnHandler_file")
    vpn_logger.setLevel(logging.ERROR)
    vpn_logger.propagate = False  # ✅ NEW: Prevent propagation to parent loggers
    
    # Clear any existing handlers and add a quiet one
    if vpn_logger.handlers:
        vpn_logger.handlers.clear()
    
    # Create a null handler to completely suppress output
    null_handler = logging.NullHandler()
    vpn_logger.addHandler(null_handler)
    
    # Also try to get the root VpnHandler logger
    for logger_name in ["VpnHandler", "VpnHandler_file", "VpnHandlerLogger"]:
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.ERROR)
        logger.propagate = False
    
    # Reduce other noisy loggers
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("selenium").setLevel(logging.WARNING)
    
    print("🔧 Enhanced production logging configured - VPN noise should be eliminated")

class MacOSSleepPrevention:
    """
    Prevents macOS from sleeping using caffeinate command
    """
    
    def __init__(self):
        self.caffeinate_process = None
        self.is_active = False
    
    def start_prevention(self):
        """
        Start preventing sleep using caffeinate
        """
        try:
            # Use caffeinate to prevent system sleep, display sleep, and disk sleep
            # -i: prevent system sleep when system is idle
            # -d: prevent display from sleeping  
            # -m: prevent disk from sleeping
            # -u: declare that user is active (prevents idle sleep)
            self.caffeinate_process = subprocess.Popen([
                'caffeinate', 
                '-i', '-d', '-m', '-u'
            ])
            
            self.is_active = True
            print("🚫💤 Sleep prevention activated (caffeinate started)")
            print(f"   Process PID: {self.caffeinate_process.pid}")
            
            # Register cleanup functions
            atexit.register(self.stop_prevention)
            signal.signal(signal.SIGINT, self._signal_handler)
            signal.signal(signal.SIGTERM, self._signal_handler)
            
        except FileNotFoundError:
            print("❌ caffeinate command not found - sleep prevention unavailable")
            self.is_active = False
        except Exception as e:
            print(f"❌ Failed to start sleep prevention: {e}")
            self.is_active = False
    
    def stop_prevention(self):
        """
        Stop sleep prevention and allow normal sleep behavior
        """
        if self.caffeinate_process and self.is_active:
            try:
                self.caffeinate_process.terminate()
                self.caffeinate_process.wait(timeout=5)
                print("✅💤 Sleep prevention deactivated (caffeinate stopped)")
            except subprocess.TimeoutExpired:
                # Force kill if it doesn't terminate gracefully
                self.caffeinate_process.kill()
                print("🔨💤 Sleep prevention force-stopped")
            except Exception as e:
                print(f"⚠️ Error stopping sleep prevention: {e}")
            finally:
                self.caffeinate_process = None
                self.is_active = False
    
    def _signal_handler(self, signum, frame):
        """
        Handle interrupt signals to cleanup properly
        """
        print(f"\n🛑 Received signal {signum}, cleaning up...")
        self.stop_prevention()
        sys.exit(0)
    
    def __enter__(self):
        """
        Context manager entry
        """
        self.start_prevention()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Context manager exit - ensures cleanup
        """
        self.stop_prevention()

def check_system_info():
    """
    Display system information for debugging
    """
    try:
        # Get macOS version
        result = subprocess.run(['sw_vers'], capture_output=True, text=True)
        if result.returncode == 0:
            print("🖥️ System Information:")
            for line in result.stdout.strip().split('\n'):
                print(f"   {line}")
        
        # Check if we're on Apple Silicon
        result = subprocess.run(['uname', '-m'], capture_output=True, text=True)
        if result.returncode == 0:
            architecture = result.stdout.strip()
            print(f"   Architecture: {architecture}")
            if architecture == 'arm64':
                print("   ✅ Apple Silicon (M-series) detected")
            else:
                print("   ℹ️ Intel architecture")
        
        print()
        
    except Exception as e:
        print(f"⚠️ Could not get system info: {e}")

if __name__ == "__main__":
    # ✅ Multiple approaches to suppress VPN logs
    setup_production_logging()
    
    # ✅ Also try to suppress after imports
    logging.getLogger("VpnHandler_file").setLevel(logging.CRITICAL)
    
    print("🚀 Starting scraper with enhanced log suppression...")
    
    with MacOSSleepPrevention():
        driver = webdriver.Chrome()
        scrape_club_data(driver=driver, environment="production", use_vpn=True)