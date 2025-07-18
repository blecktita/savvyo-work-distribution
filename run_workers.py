#!/usr/bin/env python3
"""
Worker Machine Runner - Orchestrates worker workflow.
Prevents sleep + runs multiple workers + handles VPN.
"""

# python run_workers.py --repo-url https://github.com/blecktita/savvyo-work-distribution.git --max-work 1500 --hours 8 --check-vpn

import subprocess
import time
import sys
import threading
import argparse
from datetime import datetime, timedelta
from pathlib import Path


class SingleWorkerRunner:
    """Single worker machine orchestrator with VPN control."""
    
    def __init__(self, repo_url: str, environment: str = "production", 
                 max_work: int = 100, max_hours: float = 8.0):
        self.repo_url = repo_url
        self.environment = environment
        self.max_work = max_work
        self.max_hours = max_hours
        
        # Process tracking
        self.caffeinate_process = None
        self.worker_process = None
        self.start_time = None
        self.should_stop = False
        
        print("🤖 Single Worker Runner initialized")
        print(f"📋 Repo: {repo_url}")
        print(f"🔧 Environment: {environment}")
        print(f"📊 Max work orders: {max_work}")
        print(f"⏰ Max duration: {max_hours} hours")
        print("🔒 VPN: Single worker controls VPN rotation")
    
    def start_sleep_prevention(self):
        """Start preventing Mac from sleeping."""
        try:
            self.caffeinate_process = subprocess.Popen([
                'caffeinate', '-d', '-i', '-s'
            ])
            print("🔋 Sleep prevention started")
            return True
        except Exception as e:
            print(f"❌ Failed to start sleep prevention: {e}")
            return False
    
    def stop_sleep_prevention(self):
        """Stop sleep prevention."""
        if self.caffeinate_process:
            try:
                self.caffeinate_process.terminate()
                self.caffeinate_process.wait(timeout=5)
                print("💤 Sleep prevention stopped")
            except subprocess.TimeoutExpired:
                self.caffeinate_process.kill()
                print("🔋 Force stopped sleep prevention")
            except Exception as e:
                print(f"⚠️ Error stopping sleep prevention: {e}")
    
    def start_single_worker(self):
        """Start the single worker process with VPN control."""
        try:
            cmd = [
                'uv run --active', 'worker_main.py',
                '--repo-url', self.repo_url,
                '--environment', self.environment,
                '--max-work', str(self.max_work)
            ]
            
            self.worker_process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                universal_newlines=True
            )
            
            print(f"🤖 Worker started (PID: {self.worker_process.pid})")
            print("🔒 VPN rotation will be handled by this worker")
            return True
            
        except Exception as e:
            print(f"❌ Failed to start worker: {e}")
            return False
    
    def monitor_worker(self):
        """Monitor the single worker process."""
        def monitor_output():
            if self.worker_process and self.worker_process.stdout:
                for line in iter(self.worker_process.stdout.readline, ''):
                    if self.should_stop:
                        break
                    print(f"🤖 WORKER: {line.strip()}")
        
        # Start monitoring thread
        if self.worker_process:
            worker_thread = threading.Thread(target=monitor_output, daemon=True)
            worker_thread.start()
            return worker_thread
        return None
    
    def stop_worker(self):
        """Stop the worker process."""
        self.should_stop = True
        
        print("\n🛑 Stopping worker...")
        
        if self.worker_process:
            try:
                self.worker_process.terminate()
                self.worker_process.wait(timeout=15)  # Give worker time to finish current work
                print("🤖 Worker stopped gracefully")
            except subprocess.TimeoutExpired:
                self.worker_process.kill()
                print("🔥 Worker force killed")
            except:
                pass
        
        # Stop sleep prevention
        self.stop_sleep_prevention()
    
    def get_worker_status(self):
        """Get status of the worker."""
        if self.worker_process:
            if self.worker_process.poll() is None:
                return "running"
            else:
                return "finished"
        return "not_started"
    
    def show_status(self):
        """Show current worker status."""
        if self.start_time:
            elapsed = datetime.now() - self.start_time
            remaining = timedelta(hours=self.max_hours) - elapsed
            worker_status = self.get_worker_status()
            
            print("\n📊 WORKER STATUS:")
            print(f"⏰ Started: {self.start_time.strftime('%H:%M:%S')}")
            print(f"⏳ Elapsed: {str(elapsed).split('.')[0]}")
            print(f"⏰ Remaining: {str(remaining).split('.')[0]}")
            print(f"🔋 Sleep prevention: {'Active' if self.caffeinate_process else 'Inactive'}")
            print(f"🤖 Worker status: {worker_status}")
            print("🔒 VPN control: Handled by single worker")
    
    def run_single_worker(self):
        """Run the single worker with VPN control."""
        self.start_time = datetime.now()
        end_time = self.start_time + timedelta(hours=self.max_hours)
        
        print("\n🚀 STARTING SINGLE WORKER")
        print(f"⏰ Start time: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"⏰ End time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print("🔒 VPN rotation: Controlled by this worker")
        print("=" * 60)
        
        try:
            # 1. Start sleep prevention
            if not self.start_sleep_prevention():
                return False
            
            # 2. Start single worker
            if not self.start_single_worker():
                return False
            
            # 3. Start monitoring
            monitor_thread = self.monitor_worker()
            
            # 4. Main monitoring loop
            print("\n🏃 Worker running... (will stop at {end_time.strftime('%H:%M:%S')})")
            
            while datetime.now() < end_time and not self.should_stop:
                worker_status = self.get_worker_status()
                
                # If worker finished its work, we're done
                if worker_status == "finished":
                    print("\n🎉 Worker completed all assigned work!")
                    break
                
                # Show status every 10 minutes
                if int(time.time()) % 600 == 0:
                    self.show_status()
                
                time.sleep(30)  # Check every 30 seconds
            
            if datetime.now() >= end_time:
                print(f"\n⏰ Time limit reached ({self.max_hours} hours)")
            
            return True
            
        except KeyboardInterrupt:
            print("\n⏹️ Worker interrupted by user")
            return True
        except Exception as e:
            print(f"\n❌ Worker error: {e}")
            return False
        finally:
            self.stop_worker()
            
            if self.start_time:
                duration = datetime.now() - self.start_time
                hours = int(duration.total_seconds() // 3600)
                minutes = int((duration.total_seconds() % 3600) // 60)
                print(f"\n🏁 Worker completed after {hours}h {minutes}m")


def check_vpn_status():
    """Check if VPN is connected (basic check)."""
    try:
        # This is a simple check - you might want to customize for your VPN
        result = subprocess.run(['ifconfig'], capture_output=True, text=True)
        if 'tun' in result.stdout or 'utun' in result.stdout:
            print("🔒 VPN appears to be connected")
            return True
        else:
            print("⚠️ VPN might not be connected")
            return False
    except:
        print("❓ Could not check VPN status")
        return None


def main():
    parser = argparse.ArgumentParser(description="Single Worker Machine Runner")
    parser.add_argument("--repo-url", required=True, help="GitHub repository URL")
    parser.add_argument("--environment", default="production", help="Environment (production/testing/development)")
    parser.add_argument("--max-work", type=int, default=100, help="Max work orders for this worker")
    parser.add_argument("--hours", type=float, default=8.0, help="Maximum hours to run")
    parser.add_argument("--status", action="store_true", help="Check if worker is running")
    parser.add_argument("--check-vpn", action="store_true", help="Check VPN status before starting")
    
    args = parser.parse_args()
    
    if args.status:
        # Check if worker process is running
        try:
            result = subprocess.run(['pgrep', '-f', 'worker_main.py'], capture_output=True, text=True)
            if result.returncode == 0:
                pids = result.stdout.strip().split('\n')
                print(f"🤖 Worker is running (PID: {pids[0]})")
                if len(pids) > 1:
                    print(f"⚠️ Warning: Multiple workers detected ({len(pids)}) - VPN conflicts possible!")
            else:
                print("💤 No worker is running")
                
            result = subprocess.run(['pgrep', 'caffeinate'], capture_output=True, text=True)
            if result.returncode == 0:
                print("🔋 Sleep prevention is active")
            else:
                print("💤 Sleep prevention is not active")
                
        except Exception as e:
            print(f"⚠️ Could not check status: {e}")
        return
    
    # Check VPN if requested
    if args.check_vpn:
        vpn_status = check_vpn_status()
        if vpn_status is False:
            response = input("Continue without VPN? (y/N): ")
            if response.lower() != 'y':
                print("⏹️ Aborted by user")
                sys.exit(1)
    
    # Validate required files exist
    if not Path('worker_main.py').exists():
        print("❌ Required file not found: worker_main.py")
        sys.exit(1)
    
    # Create and run single worker
    runner = SingleWorkerRunner(
        repo_url=args.repo_url,
        environment=args.environment,
        max_work=args.max_work,
        max_hours=args.hours
    )
    
    success = runner.run_single_worker()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()