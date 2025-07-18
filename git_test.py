#!/usr/bin/env python3
"""
Test script to verify atomic git operations work properly.
Run this on ALL machines before starting production workflow.
"""

import subprocess
import time
import uuid
import socket
import os
import json
from pathlib import Path
from datetime import datetime
from coordination.github_bridge import GitHubWorkBridge


class GitConflictTester:
    """Test atomic git operations to prevent conflicts."""
    
    def __init__(self, repo_url: str):
        self.repo_url = repo_url
        self.machine_id = f"{socket.gethostname()}_{os.getpid()}_{uuid.uuid4().hex[:8]}"
        self.test_repo_path = Path("./test-git-operations")
        
    def setup_test_environment(self):
        """Setup test environment."""
        print(f"🧪 Setting up test environment on {self.machine_id}")
        
        # Clean up any existing test
        if self.test_repo_path.exists():
            subprocess.run(['rm', '-rf', str(self.test_repo_path)], check=True)
        
        # Create fresh test environment
        self.github_bridge = GitHubWorkBridge(
            repo_path=str(self.test_repo_path),
            repo_url=self.repo_url
        )
        
        print("✅ Test environment ready")
    
    def test_single_operation(self):
        """Test a single git operation."""
        print(f"\n🔧 Testing single git operation...")
        
        # Create a test file
        test_file = self.test_repo_path / f"test_{self.machine_id}.json"
        test_data = {
            "machine_id": self.machine_id,
            "timestamp": datetime.now().isoformat(),
            "test_type": "single_operation",
            "operation_number": 1
        }
        
        with open(test_file, 'w') as f:
            json.dump(test_data, f, indent=2)
        
        # Test the atomic git operation
        try:
            self.github_bridge._git_add_commit_push(f"Test single operation: {self.machine_id}")
            print("✅ Single operation successful")
            return True
        except Exception as e:
            print(f"❌ Single operation failed: {e}")
            return False
    
    def test_rapid_operations(self, num_operations: int = 5):
        """Test rapid consecutive git operations."""
        print(f"\n⚡ Testing {num_operations} rapid consecutive operations...")
        
        success_count = 0
        fail_count = 0
        
        for i in range(num_operations):
            test_file = self.test_repo_path / f"rapid_test_{self.machine_id}_{i}.json"
            test_data = {
                "machine_id": self.machine_id,
                "timestamp": datetime.now().isoformat(),
                "test_type": "rapid_operation",
                "operation_number": i + 1
            }
            
            with open(test_file, 'w') as f:
                json.dump(test_data, f, indent=2)
            
            try:
                self.github_bridge._git_add_commit_push(f"Rapid test {i+1}: {self.machine_id}")
                success_count += 1
                print(f"  ✅ Operation {i+1} successful")
            except Exception as e:
                fail_count += 1
                print(f"  ❌ Operation {i+1} failed: {e}")
            
            # Small delay between operations
            time.sleep(1)
        
        print(f"📊 Rapid test results: {success_count} success, {fail_count} failures")
        return fail_count == 0
    
    def test_simulated_worker_operations(self):
        """Test operations that simulate real worker behavior."""
        print(f"\n🤖 Testing simulated worker operations...")
        
        # Simulate claiming work
        claimed_work = {
            "work_id": f"test_work_{uuid.uuid4().hex[:8]}",
            "machine_id": self.machine_id,
            "claimed_at": datetime.now().isoformat(),
            "status": "claimed"
        }
        
        claimed_file = self.test_repo_path / "claimed_competitions" / f"{claimed_work['work_id']}.json"
        claimed_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(claimed_file, 'w') as f:
            json.dump(claimed_work, f, indent=2)
        
        try:
            self.github_bridge._git_add_commit_push(f"Claim test work: {claimed_work['work_id']}")
            print("  ✅ Claim work operation successful")
        except Exception as e:
            print(f"  ❌ Claim work operation failed: {e}")
            return False
        
        # Simulate completing work
        completed_work = {
            **claimed_work,
            "completed_at": datetime.now().isoformat(),
            "status": "completed",
            "clubs_scraped": 42,
            "test_result": "success"
        }
        
        completed_file = self.test_repo_path / "completed_competitions" / f"{claimed_work['work_id']}.json"
        completed_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(completed_file, 'w') as f:
            json.dump(completed_work, f, indent=2)
        
        # Remove claimed file
        if claimed_file.exists():
            claimed_file.unlink()
        
        try:
            self.github_bridge._git_add_commit_push(f"Complete test work: {claimed_work['work_id']}")
            print("  ✅ Complete work operation successful")
            return True
        except Exception as e:
            print(f"  ❌ Complete work operation failed: {e}")
            return False
    
    def test_git_configuration(self):
        """Test git configuration."""
        print(f"\n⚙️ Testing git configuration...")
        
        config_checks = {
            "pull.rebase": "false",
            "core.editor": "echo"
        }
        
        all_good = True
        
        for config_key, expected_value in config_checks.items():
            try:
                result = subprocess.run(
                    ['git', 'config', config_key], 
                    cwd=self.test_repo_path, 
                    capture_output=True, 
                    text=True
                )
                
                if result.returncode == 0:
                    actual_value = result.stdout.strip()
                    if actual_value == expected_value:
                        print(f"  ✅ {config_key} = {actual_value}")
                    else:
                        print(f"  ⚠️ {config_key} = {actual_value} (expected: {expected_value})")
                        all_good = False
                else:
                    print(f"  ❌ {config_key} not set")
                    all_good = False
                    
            except Exception as e:
                print(f"  ❌ Error checking {config_key}: {e}")
                all_good = False
        
        return all_good
    
    def run_full_test(self):
        """Run comprehensive git test."""
        print(f"🧪 COMPREHENSIVE GIT TEST - {self.machine_id}")
        print("=" * 60)
        
        results = {}
        
        try:
            # Setup
            self.setup_test_environment()
            
            # Test git configuration
            results['config'] = self.test_git_configuration()
            
            # Test single operation
            results['single'] = self.test_single_operation()
            
            # Test rapid operations
            results['rapid'] = self.test_rapid_operations(3)
            
            # Test worker simulation
            results['worker_sim'] = self.test_simulated_worker_operations()
            
            # Summary
            print(f"\n📋 TEST SUMMARY FOR {self.machine_id}:")
            print("=" * 40)
            
            all_passed = True
            for test_name, passed in results.items():
                status = "✅ PASS" if passed else "❌ FAIL"
                print(f"  {test_name.upper()}: {status}")
                if not passed:
                    all_passed = False
            
            print(f"\n🎯 OVERALL RESULT: {'✅ ALL TESTS PASSED' if all_passed else '❌ SOME TESTS FAILED'}")
            
            if all_passed:
                print("🚀 This machine is ready for production!")
            else:
                print("⚠️ Fix issues before running production workflow!")
            
            return all_passed
            
        except Exception as e:
            print(f"❌ Test setup failed: {e}")
            return False
    
    def cleanup(self):
        """Clean up test environment."""
        try:
            if self.test_repo_path.exists():
                subprocess.run(['rm', '-rf', str(self.test_repo_path)], check=True)
                print("🧹 Cleaned up test environment")
        except Exception as e:
            print(f"⚠️ Cleanup failed: {e}")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Test Git Conflict Prevention")
    parser.add_argument("--repo-url", required=True, help="GitHub repository URL")
    parser.add_argument("--quick", action="store_true", help="Run quick test only")
    
    args = parser.parse_args()
    
    tester = GitConflictTester(args.repo_url)
    
    try:
        if args.quick:
            tester.setup_test_environment()
            success = tester.test_single_operation()
            print(f"\n🎯 QUICK TEST: {'✅ PASSED' if success else '❌ FAILED'}")
        else:
            success = tester.run_full_test()
        
        return 0 if success else 1
        
    except KeyboardInterrupt:
        print("\n⏹️ Test interrupted")
        return 1
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return 1
    finally:
        tester.cleanup()


if __name__ == "__main__":
    exit(main())