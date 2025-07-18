#!/usr/bin/env python3
"""
Host Machine Runner - Orchestrates the complete host workflow.
Prevents sleep + runs host manager + optional local workers.
"""

# python run_host.py --repo-url https://github.com/blecktita/savvyo-work-distribution.git --local-workers 1 --hours 8

import subprocess
import time
import sys
import threading
import argparse
from datetime import datetime, timedelta
from pathlib import Path


class HostWorkflowRunner:
    """Complete host machine workflow orchestrator."""
    
    def __init__(self, repo_url: str, environment: str = "production", 
                 max_cycles: int = 1500, local_workers: int = 1, max_hours: float = 8.0):
        self.repo_url = repo_url
        self.environment = environment
        self.max_cycles = max_cycles
        self.local_workers = local_workers
        self.max_hours = max_hours
        
        # Process tracking
        self.caffeinate_process = None
        self.host_process = None
        self.worker_processes = []
        self.start_time = None
        self.should_stop = False
        
        print("🏠 Host Workflow Runner initialized")
        print(f"📋 Repo: {repo_url}")
        print(f"🔧 Environment: {environment}")
        print(f"👥 Local workers: {local_workers}")
        print(f"⏰ Max duration: {max_hours} hours")
    
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
    
    def start_host_manager(self):
        """Start the host work manager."""
        try:
            cmd = [
                'uv run --active', 'host_work_manager.py',
                '--repo-url', self.repo_url,
                '--environment', self.environment,
                '--max-cycles', str(self.max_cycles)
            ]
            
            self.host_process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                universal_newlines=True
            )
            
            print(f"🏠 Host manager started (PID: {self.host_process.pid})")
            return True
            
        except Exception as e:
            print(f"❌ Failed to start host manager: {e}")
            return False
    
    def start_local_workers(self):
        """Start local worker processes."""
        for i in range(self.local_workers):
            try:
                cmd = [
                    'uv run --active', 'worker_main.py',
                    '--repo-url', self.repo_url,
                    '--environment', self.environment,
                    '--max-work', '1000'  # Higher limit for local workers
                ]
                
                worker_process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1,
                    universal_newlines=True
                )
                
                self.worker_processes.append(worker_process)
                print(f"🤖 Local worker {i+1} started (PID: {worker_process.pid})")
                
                # Small delay between worker starts
                time.sleep(2)
                
            except Exception as e:
                print(f"❌ Failed to start worker {i+1}: {e}")
    
    def monitor_processes(self):
        """Monitor all processes and handle output."""
        def monitor_host():
            if self.host_process:
                if self.host_process.stdout:
                    for line in iter(self.host_process.stdout.readline, ''):
                        if self.should_stop:
                            break
                        print(f"🏠 HOST: {line.strip()}")
        
        def monitor_worker(worker_id, process):
            try:
                for line in iter(process.stdout.readline, ''):
                    if self.should_stop:
                        break
                    print(f"🤖 WORKER-{worker_id}: {line.strip()}")
            except:
                pass
        
        # Start monitoring threads
        threads = []
        
        if self.host_process:
            host_thread = threading.Thread(target=monitor_host, daemon=True)
            host_thread.start()
            threads.append(host_thread)
        
        for i, worker in enumerate(self.worker_processes):
            worker_thread = threading.Thread(
                target=monitor_worker, 
                args=(i+1, worker), 
                daemon=True
            )
            worker_thread.start()
            threads.append(worker_thread)
        
        return threads
    
    def stop_all_processes(self):
        """Stop all running processes."""
        self.should_stop = True
        
        print("\n🛑 Stopping all processes...")
        
        # Stop workers first
        for i, worker in enumerate(self.worker_processes):
            try:
                worker.terminate()
                worker.wait(timeout=10)
                print(f"🤖 Worker {i+1} stopped")
            except subprocess.TimeoutExpired:
                worker.kill()
                print(f"🔥 Worker {i+1} force killed")
            except:
                pass
        
        # Stop host manager
        if self.host_process:
            try:
                self.host_process.terminate()
                self.host_process.wait(timeout=10)
                print("🏠 Host manager stopped")
            except subprocess.TimeoutExpired:
                self.host_process.kill()
                print("🔥 Host manager force killed")
            except:
                pass
        
        # Stop sleep prevention
        self.stop_sleep_prevention()
    
    def show_status(self):
        """Show current status."""
        if self.start_time:
            elapsed = datetime.now() - self.start_time
            remaining = timedelta(hours=self.max_hours) - elapsed
            
            print("\n📊 WORKFLOW STATUS:")
            print(f"⏰ Started: {self.start_time.strftime('%H:%M:%S')}")
            print(f"⏳ Elapsed: {str(elapsed).split('.')[0]}")
            print(f"⏰ Remaining: {str(remaining).split('.')[0]}")
            print(f"🔋 Sleep prevention: {'Active' if self.caffeinate_process else 'Inactive'}")
            print(f"🏠 Host manager: {'Running' if self.host_process and self.host_process.poll() is None else 'Stopped'}")
            print(f"🤖 Local workers: {len([w for w in self.worker_processes if w.poll() is None])}/{len(self.worker_processes)}")
    
    def run_workflow(self):
        """Run the complete workflow."""
        self.start_time = datetime.now()
        end_time = self.start_time + timedelta(hours=self.max_hours)
        
        print("\n🚀 STARTING HOST WORKFLOW")
        print(f"⏰ Start time: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"⏰ End time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)
        
        try:
            # 1. Start sleep prevention
            if not self.start_sleep_prevention():
                return False
            
            # 2. Start host manager
            if not self.start_host_manager():
                return False
            
            # Wait a bit for host to initialize
            time.sleep(5)
            
            # 3. Start local workers
            if self.local_workers > 0:
                self.start_local_workers()
            
            # 4. Start monitoring
            monitor_threads = self.monitor_processes()
            
            # 5. Main loop with time limit
            print(f"\n🏃 Workflow running... (will stop at {end_time.strftime('%H:%M:%S')})")
            
            while datetime.now() < end_time and not self.should_stop:
                # Check if host process is still running
                if self.host_process and self.host_process.poll() is not None:
                    print("🏠 Host manager finished")
                    break
                
                # Show status every 10 minutes
                if int(time.time()) % 600 == 0:
                    self.show_status()
                
                time.sleep(30)  # Check every 30 seconds
            
            if datetime.now() >= end_time:
                print(f"\n⏰ Time limit reached ({self.max_hours} hours)")
            
            return True
            
        except KeyboardInterrupt:
            print("\n⏹️ Workflow interrupted by user")
            return True
        except Exception as e:
            print(f"\n❌ Workflow error: {e}")
            return False
        finally:
            self.stop_all_processes()
            
            if self.start_time:
                duration = datetime.now() - self.start_time
                hours = int(duration.total_seconds() // 3600)
                minutes = int((duration.total_seconds() % 3600) // 60)
                print(f"\n🏁 Workflow completed after {hours}h {minutes}m")


def main():
    parser = argparse.ArgumentParser(description="Host Machine Workflow Runner")
    parser.add_argument("--repo-url", required=True, help="GitHub repository URL")
    parser.add_argument("--environment", default="production", help="Environment (production/testing/development)")
    parser.add_argument("--max-cycles", type=int, default=1500, help="Max cycles for host manager")
    parser.add_argument("--local-workers", type=int, default=1, help="Number of local workers to start")
    parser.add_argument("--hours", type=float, default=8.0, help="Maximum hours to run")
    parser.add_argument("--status", action="store_true", help="Check if workflow is running")
    
    args = parser.parse_args()
    
    if args.status:
        # Check if processes are running
        try:
            result = subprocess.run(['pgrep', '-f', 'host_work_manager.py'], capture_output=True, text=True)
            if result.returncode == 0:
                print("🏠 Host manager is running")
            else:
                print("💤 Host manager is not running")
            
            result = subprocess.run(['pgrep', '-f', 'worker_main.py'], capture_output=True, text=True)
            if result.returncode == 0:
                pids = result.stdout.strip().split('\n')
                print(f"🤖 {len(pids)} workers are running")
            else:
                print("💤 No workers are running")
                
            result = subprocess.run(['pgrep', 'caffeinate'], capture_output=True, text=True)
            if result.returncode == 0:
                print("🔋 Sleep prevention is active")
            else:
                print("💤 Sleep prevention is not active")
                
        except Exception as e:
            print(f"⚠️ Could not check status: {e}")
        return
    
    # Validate required files exist
    required_files = ['host_work_manager.py', 'worker_main.py']
    for file in required_files:
        if not Path(file).exists():
            print(f"❌ Required file not found: {file}")
            sys.exit(1)
    
    # Create and run workflow
    runner = HostWorkflowRunner(
        repo_url=args.repo_url,
        environment=args.environment,
        max_cycles=args.max_cycles,
        local_workers=args.local_workers,
        max_hours=args.hours
    )
    
    success = runner.run_workflow()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()