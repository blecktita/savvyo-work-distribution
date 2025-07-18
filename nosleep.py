#!/usr/bin/env python3
"""
Simple macOS sleep prevention script.
Keeps your Mac awake while data processing is running.
"""

import subprocess
import time
import signal
import sys
from datetime import datetime


class MacSleepPreventer:
    """Prevent macOS from sleeping."""
    
    def __init__(self):
        self.caffeinate_process = None
        self.start_time = None
        
    def start_prevention(self):
        """Start preventing sleep."""
        try:
            # Use caffeinate to prevent sleep
            # -d: prevent display sleep
            # -i: prevent idle sleep
            # -s: prevent system sleep
            self.caffeinate_process = subprocess.Popen([
                'caffeinate', '-d', '-i', '-s'
            ])
            
            self.start_time = datetime.now()
            print(f"🔋 Sleep prevention started at {self.start_time.strftime('%H:%M:%S')}")
            print("💻 Your Mac will stay awake until you stop this script")
            print("⏹️  Press Ctrl+C to stop and allow sleep again")
            
        except Exception as e:
            print(f"❌ Failed to start sleep prevention: {e}")
            sys.exit(1)
    
    def stop_prevention(self):
        """Stop preventing sleep."""
        if self.caffeinate_process:
            try:
                self.caffeinate_process.terminate()
                self.caffeinate_process.wait(timeout=5)
                
                duration = datetime.now() - self.start_time if self.start_time else None
                if duration:
                    hours = int(duration.total_seconds() // 3600)
                    minutes = int((duration.total_seconds() % 3600) // 60)
                    print(f"\n💤 Sleep prevention stopped after {hours}h {minutes}m")
                else:
                    print("\n💤 Sleep prevention stopped")
                    
                print("🔋 Your Mac can now sleep normally")
                
            except subprocess.TimeoutExpired:
                self.caffeinate_process.kill()
                print("\n🔋 Force stopped sleep prevention")
            except Exception as e:
                print(f"\n⚠️ Error stopping sleep prevention: {e}")
    
    def run_with_timer(self, hours: float = None):
        """Run sleep prevention for a specific duration."""
        if hours:
            print(f"⏰ Will prevent sleep for {hours} hours")
            
        self.start_prevention()
        
        try:
            if hours:
                # Sleep for specified hours
                time.sleep(hours * 3600)
                print(f"\n⏰ Timer finished ({hours} hours)")
            else:
                # Run indefinitely until interrupted
                while True:
                    time.sleep(60)  # Check every minute
                    
        except KeyboardInterrupt:
            pass
        finally:
            self.stop_prevention()


def signal_handler(signum, frame):
    """Handle Ctrl+C gracefully."""
    print("\n⏹️ Stopping sleep prevention...")
    sys.exit(0)


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Prevent macOS from sleeping")
    parser.add_argument("--hours", type=float, help="Hours to prevent sleep (default: indefinite)")
    parser.add_argument("--status", action="store_true", help="Check current sleep prevention status")
    
    args = parser.parse_args()
    
    # Check if caffeinate is available
    try:
        subprocess.run(['which', 'caffeinate'], check=True, capture_output=True)
    except subprocess.CalledProcessError:
        print("❌ caffeinate command not found. This script requires macOS.")
        sys.exit(1)
    
    if args.status:
        # Check if caffeinate is already running
        try:
            result = subprocess.run(['pgrep', 'caffeinate'], capture_output=True, text=True)
            if result.returncode == 0:
                pids = result.stdout.strip().split('\n')
                print(f"🔋 Sleep prevention is active (PIDs: {', '.join(pids)})")
            else:
                print("💤 No sleep prevention currently active")
        except Exception as e:
            print(f"⚠️ Could not check status: {e}")
        return
    
    # Set up signal handler for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    
    # Create and run sleep preventer
    preventer = MacSleepPreventer()
    
    try:
        preventer.run_with_timer(args.hours)
    except Exception as e:
        print(f"❌ Error: {e}")
        preventer.stop_prevention()
    except SystemExit:
        preventer.stop_prevention()


if __name__ == "__main__":
    main()