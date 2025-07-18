# VPN Controls API Documentation

## Overview

The `vpn_controls` module provides comprehensive VPN management functionality for secure web scraping and request handling. It includes automatic VPN rotation, connection recovery, request throttling, and security monitoring.

## Table of Contents

- [Installation & Setup](#installation--setup)
- [Quick Start](#quick-start)
- [Public API Reference](#public-api-reference)
  - [VpnProtectionHandler](#vpnprotectionhandler)
  - [RequestThrottler](#requestthrottler)
  - [Exception Classes](#exception-classes)
- [Configuration](#configuration)
- [Examples](#examples)
- [Best Practices](#best-practices)
- [Troubleshooting](#troubleshooting)

## Installation & Setup

### Prerequisites

- macOS with Tunnelblick VPN client installed
- NordVPN credentials
- Python 3.7+

### Environment Setup

1. Create a `.env` file in your project root:
```bash
NORD_USER=your_nordvpn_username
NORD_PASS=your_nordvpn_password
```

2. Install Tunnelblick and configure NordVPN connections

3. Ensure proper logging directory structure:
```
logs/
├── vpn/
│   ├── vpn_handler.log
│   └── RootVpn.log
```

## Quick Start

```python
from vpn_controls import VpnProtectionHandler
from utils.configurations import VpnConfig

# Create VPN configuration
config = VpnConfig(
    use_vpn=True,
    mandatory_delay=2.0,
    request_delay=1.0,
    max_recovery_attempts=3
)

# Initialize VPN handler
vpn_handler = VpnProtectionHandler(config)

# Use in your application
try:
    # Before making requests
    vpn_handler.handle_request_timing("web_scraping")
    
    # Your request code here
    response = requests.get("https://example.com")
    
    # Get statistics
    stats = vpn_handler.get_vpn_statistics()
    print(f"Current IP: {stats['current_ip']}")
    
finally:
    # Cleanup when done
    vpn_handler.cleanup()
```

## Public API Reference

### VpnProtectionHandler

The main interface for VPN protection and request management.

#### Constructor

```python
def __init__(self,
             config: VpnConfig,
             logger_name: str = "VpnHandler",
             env_file_path: Optional[str] = None)
```

**Parameters:**
- `config` (VpnConfig): VPN configuration object
- `logger_name` (str): Name for the logger instance
- `env_file_path` (Optional[str]): Path to environment file

**Example:**
```python
from vpn_controls import VpnProtectionHandler
from utils.configurations import VpnConfig

config = VpnConfig(use_vpn=True, mandatory_delay=2.0)
handler = VpnProtectionHandler(config, logger_name="MyApp")
```

#### Core Methods

##### handle_request_timing()

Main method for handling request timing with VPN protection.

```python
def handle_request_timing(self, operation: str = "request") -> None
```

**Parameters:**
- `operation` (str): Description of the operation being performed

**Raises:**
- `VpnRequiredError`: When VPN is required but not active
- `IPSecurityViolationError`: When security violations are detected

**Example:**
```python
# Before making any web request
vpn_handler.handle_request_timing("API_call")

# Before web scraping
vpn_handler.handle_request_timing("scraping_products")
```

##### ensure_vpn_protection()

Verifies that VPN protection is active.

```python
def ensure_vpn_protection(self) -> None
```

**Raises:**
- `VpnRequiredError`: When VPN is required but not active

**Example:**
```python
try:
    vpn_handler.ensure_vpn_protection()
    print("VPN protection is active")
except VpnRequiredError:
    print("VPN protection required but not active")
```

##### get_vpn_statistics()

Returns current VPN statistics and status information.

```python
def get_vpn_statistics(self) -> Dict
```

**Returns:**
- `Dict`: Dictionary containing VPN statistics

**Response Format:**
```python
{
    "vpn_protection": "ACTIVE" | "DISABLED",
    "current_ip": "192.168.1.100",
    "requests_on_current_ip": 45,
    "max_requests_per_ip": 100,
    "total_rotations": 3,
    "time_on_current_ip": "00:15:30",
    "rotation_success_rate": 95.5
}
```

**Example:**
```python
stats = vpn_handler.get_vpn_statistics()
print(f"Current IP: {stats['current_ip']}")
print(f"Requests made: {stats['requests_on_current_ip']}")
print(f"Rotation success rate: {stats['rotation_success_rate']}%")
```

##### get_comprehensive_vpn_statistics()

Returns detailed statistics including security monitoring data.

```python
def get_comprehensive_vpn_statistics(self) -> Dict
```

**Returns:**
- `Dict`: Comprehensive statistics including security details

**Response Format:**
```python
{
    "vpn_protection": "ACTIVE",
    "current_ip": "192.168.1.100",
    "requests_on_current_ip": 45,
    "max_requests_per_ip": 100,
    "total_rotations": 3,
    "time_on_current_ip": "00:15:30",
    "rotation_success_rate": 95.5,
    "security_details": {
        "monitoring_active": True,
        "reboot_required": False,
        "recent_alerts": 0,
        "rotation_callback_configured": True,
        "rotation_in_progress": False
    },
    "rotation_history_count": 10
}
```

##### cleanup()

Performs cleanup of all VPN and security resources.

```python
def cleanup(self) -> None
```

**Example:**
```python
try:
    # Your application code
    pass
finally:
    vpn_handler.cleanup()  # Always cleanup
```

#### Properties

##### is_active

```python
@property
def is_active(self) -> bool
```

Returns `True` if VPN protection is active and monitoring is enabled.

**Example:**
```python
if vpn_handler.is_active:
    print("VPN protection is fully active")
```

##### security_status

```python
@property
def security_status(self) -> str
```

Returns current security status as a descriptive string.

**Possible Values:**
- `"🚨 REBOOT REQUIRED - Rotation Failures"`
- `"🔄 ROTATION IN PROGRESS"`
- `"🔄 ROTATION NEEDED"`
- `"🔒 ACTIVE - Monitoring"`
- `"⚠️ INACTIVE - No Monitoring"`

**Example:**
```python
print(f"Security Status: {vpn_handler.security_status}")
```

##### current_ip

```python
@property
def current_ip(self) -> str
```

Returns the current external IP address.

**Example:**
```python
print(f"Current IP: {vpn_handler.current_ip}")
```

##### request_count

```python
@property
def request_count(self) -> int
```

Returns the number of requests made on the current IP.

**Example:**
```python
print(f"Requests on current IP: {vpn_handler.request_count}")
```

### RequestThrottler

Core VPN connection manager that handles Tunnelblick operations.

#### Constructor

```python
def __init__(self,
             vpn_config: VpnConfig,
             termination_callback: Optional[Callable[[str], None]] = None)
```

**Parameters:**
- `vpn_config` (VpnConfig): VPN configuration settings
- `termination_callback` (Optional[Callable]): Callback for critical failures

#### Core Methods

##### establish_secure_connection()

Establishes a secure VPN connection with automatic configuration selection.

```python
def establish_secure_connection(self) -> bool
```

**Returns:**
- `bool`: True if connection established successfully

**Example:**
```python
throttler = RequestThrottler(vpn_config)
if throttler.establish_secure_connection():
    print("VPN connection established")
```

##### rotate_configuration()

Rotates to a new VPN configuration for IP address change.

```python
def rotate_configuration(self) -> bool
```

**Returns:**
- `bool`: True if rotation successful

**Example:**
```python
if throttler.rotate_configuration():
    print("Successfully rotated to new VPN server")
    print(f"New server: {throttler.current_vpn}")
```

##### verify_vpn_connection()

Performs comprehensive verification of VPN connection status.

```python
def verify_vpn_connection(self) -> bool
```

**Returns:**
- `bool`: True if VPN connection is verified and working

**Verification Methods:**
1. Process verification (checks for VPN processes)
2. Network interface verification (checks for VPN interfaces)
3. Routing table verification (checks for VPN routes)
4. External IP verification (confirms IP change)

**Example:**
```python
if throttler.verify_vpn_connection():
    print("VPN connection verified and working")
else:
    print("VPN connection verification failed")
```

##### disconnect_configurations()

Disconnects all active VPN connections.

```python
def disconnect_configurations(self) -> bool
```

**Returns:**
- `bool`: True if disconnection successful

**Example:**
```python
if throttler.disconnect_configurations():
    print("All VPN connections disconnected")
```

##### recover_tunnelblick()

Performs complete Tunnelblick recovery (kill, restart, reinitialize).

```python
def recover_tunnelblick(self) -> bool
```

**Returns:**
- `bool`: True if recovery successful

**Example:**
```python
if throttler.recover_tunnelblick():
    print("Tunnelblick recovery completed successfully")
```

##### get_vpn_status()

Returns current VPN status information.

```python
def get_vpn_status(self) -> Dict
```

**Returns:**
- `Dict`: VPN status information

**Response Format:**
```python
{
    "vpn_enabled": True,
    "current_vpn": "us1234.nordvpn.com.udp",
    "available_configurations": 25,
    "recent_configurations": 3,
    "recovery_attempts": 0,
    "max_recovery_attempts": 3,
    "initial_ip": "203.0.113.1"
}
```

#### Utility Methods

##### connect_to_configuration_by_name()

Connects to a specific VPN configuration by name.

```python
def connect_to_configuration_by_name(self, configuration_name: str) -> bool
```

**Parameters:**
- `configuration_name` (str): Name of the VPN configuration

**Returns:**
- `bool`: True if connection successful

##### set_configuration_auth()

Sets authentication credentials for a VPN configuration.

```python
def set_configuration_auth(self, 
                          configuration_name: str, 
                          username: str, 
                          password: str) -> bool
```

**Parameters:**
- `configuration_name` (str): Name of the VPN configuration
- `username` (str): VPN username
- `password` (str): VPN password

**Returns:**
- `bool`: True if authentication set successfully

### Exception Classes

#### VpnConnectionError

Raised when VPN connection operations fail.

```python
class VpnConnectionError(Exception):
    """Exception raised when VPN connection fails"""
    pass
```

**Common Scenarios:**
- VPN connection establishment failure
- VPN rotation failure
- Tunnelblick communication error

**Example:**
```python
try:
    vpn_handler.handle_request_timing()
except VpnConnectionError as e:
    print(f"VPN connection error: {e}")
    # Implement retry logic or fallback
```

#### TunnelblickRecoveryError

Raised when Tunnelblick recovery operations are needed.

```python
class TunnelblickRecoveryError(Exception):
    """Exception raised when Tunnelblick recovery is needed"""
    pass
```

**Common Scenarios:**
- AppleScript timeout
- Tunnelblick process unresponsive
- Configuration access failure

**Example:**
```python
try:
    throttler.execute_applescript(script)
except TunnelblickRecoveryError as e:
    print(f"Tunnelblick recovery needed: {e}")
    if throttler.recover_tunnelblick():
        print("Recovery successful, retrying operation")
```

## Configuration

### VpnConfig Class

The VPN configuration object should include these key settings:

```python
class VpnConfig:
    def __init__(self):
        self.use_vpn = True                    # Enable/disable VPN
        self.mandatory_delay = 2.0             # Delay between requests (seconds)
        self.request_delay = 1.0               # Fallback delay when VPN disabled
        self.max_recovery_attempts = 3         # Max recovery attempts
        self.rotation_threshold = 100          # Requests before rotation
        self.rotation_time_limit = 3600        # Time limit before rotation (seconds)
```

### Environment Variables

Required environment variables in `.env` file:

```bash
# NordVPN Credentials
NORD_USER=your_nordvpn_username
NORD_PASS=your_nordvpn_password

# Optional: Logging configuration
LOG_LEVEL=INFO
VPN_LOG_PATH=logs/vpn/
```

## Examples

### Basic Usage

```python
from vpn_controls import VpnProtectionHandler
from utils.configurations import VpnConfig
import requests

# Setup
config = VpnConfig(use_vpn=True, mandatory_delay=2.0)
vpn_handler = VpnProtectionHandler(config)

try:
    # Make requests with VPN protection
    for url in urls_to_scrape:
        vpn_handler.handle_request_timing("scraping")
        response = requests.get(url)
        print(f"Scraped {url} via IP {vpn_handler.current_ip}")
        
finally:
    vpn_handler.cleanup()
```

### Advanced Usage with Statistics Monitoring

```python
from vpn_controls import VpnProtectionHandler
from utils.configurations import VpnConfig
import requests
import time

config = VpnConfig(use_vpn=True, mandatory_delay=1.5)
vpn_handler = VpnProtectionHandler(config)

try:
    request_count = 0
    
    for batch in data_batches:
        # Process batch
        for item in batch:
            vpn_handler.handle_request_timing(f"processing_item_{item['id']}")
            response = requests.get(item['url'])
            request_count += 1
            
            # Monitor statistics every 10 requests
            if request_count % 10 == 0:
                stats = vpn_handler.get_comprehensive_vpn_statistics()
                print(f"Progress: {request_count} requests completed")
                print(f"Current IP: {stats['current_ip']}")
                print(f"Rotation success rate: {stats['rotation_success_rate']}%")
                print(f"Security status: {vpn_handler.security_status}")
        
        # Brief pause between batches
        time.sleep(5)
        
finally:
    # Get final statistics
    final_stats = vpn_handler.get_comprehensive_vpn_statistics()
    print("\n=== Final Statistics ===")
    print(f"Total requests: {request_count}")
    print(f"Total rotations: {final_stats['total_rotations']}")
    print(f"Security alerts: {final_stats['security_details']['recent_alerts']}")
    
    vpn_handler.cleanup()
```

### Error Handling and Recovery

```python
from vpn_controls import VpnProtectionHandler, VpnConnectionError
from core.security.security_manager import IPSecurityViolationError
import requests
import time

config = VpnConfig(use_vpn=True)
vpn_handler = VpnProtectionHandler(config)

def robust_request(url, max_retries=3):
    """Make a request with robust error handling"""
    
    for attempt in range(max_retries):
        try:
            # Handle VPN timing and security
            vpn_handler.handle_request_timing("web_request")
            
            # Make the actual request
            response = requests.get(url, timeout=30)
            return response
            
        except VpnConnectionError as e:
            print(f"VPN connection error (attempt {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                print("Waiting before retry...")
                time.sleep(10)
            else:
                raise
                
        except IPSecurityViolationError as e:
            print(f"Security violation (attempt {attempt + 1}): {e}")
            if "REBOOT REQUIRED" in str(e):
                print("System reboot required - stopping operations")
                raise
            elif attempt < max_retries - 1:
                print("Waiting for security recovery...")
                time.sleep(30)
            else:
                raise
    
    raise Exception(f"Failed to complete request after {max_retries} attempts")

# Usage
try:
    urls = ["https://example1.com", "https://example2.com"]
    
    for url in urls:
        try:
            response = robust_request(url)
            print(f"Successfully processed {url}")
        except Exception as e:
            print(f"Failed to process {url}: {e}")
            
finally:
    vpn_handler.cleanup()
```

### Manual VPN Management

```python
from vpn_controls.vpn_root import RequestThrottler
from utils.configurations import VpnConfig

# Direct VPN management (advanced usage)
config = VpnConfig()
throttler = RequestThrottler(config)

try:
    # Check current status
    status = throttler.get_vpn_status()
    print(f"Current VPN: {status['current_vpn']}")
    
    # Manual rotation
    if throttler.rotate_configuration():
        print("Rotation successful")
        
        # Verify new connection
        if throttler.verify_vpn_connection():
            print("New connection verified")
            new_status = throttler.get_vpn_status()
            print(f"New VPN: {new_status['current_vpn']}")
    
    # Manual recovery if needed
    if not throttler.verify_vpn_connection():
        print("Connection issues detected, attempting recovery...")
        if throttler.recover_tunnelblick():
            print("Recovery completed")
            if throttler.establish_secure_connection():
                print("New connection established")
    
finally:
    throttler.disconnect_configurations()
```

## Best Practices

### 1. Resource Management

Always use proper resource management:

```python
# Use try/finally for cleanup
vpn_handler = VpnProtectionHandler(config)
try:
    # Your code here
    pass
finally:
    vpn_handler.cleanup()

# Or use context manager pattern if available
with VpnProtectionHandler(config) as vpn_handler:
    # Your code here
    pass  # Automatic cleanup
```

### 2. Error Handling Strategy

Implement comprehensive error handling:

```python
def handle_vpn_errors(func):
    """Decorator for VPN error handling"""
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except VpnConnectionError as e:
            logger.error(f"VPN connection error: {e}")
            # Implement retry logic
        except IPSecurityViolationError as e:
            logger.critical(f"Security violation: {e}")
            # Implement security response
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            raise
    return wrapper
```

### 3. Monitoring and Logging

Implement proper monitoring:

```python
# Regular statistics monitoring
def monitor_vpn_health(vpn_handler, check_interval=60):
    """Monitor VPN health periodically"""
    while True:
        try:
            stats = vpn_handler.get_comprehensive_vpn_statistics()
            
            # Check for issues
            if stats['security_details']['reboot_required']:
                logger.critical("System reboot required!")
                break
                
            if stats['rotation_success_rate'] < 80:
                logger.warning(f"Low rotation success rate: {stats['rotation_success_rate']}%")
            
            logger.info(f"VPN Health: {vpn_handler.security_status}")
            
        except Exception as e:
            logger.error(f"Health check error: {e}")
            
        time.sleep(check_interval)
```

### 4. Configuration Management

Use environment-specific configurations:

```python
# config.py
import os

class VpnConfig:
    def __init__(self, environment="production"):
        if environment == "development":
            self.use_vpn = False
            self.mandatory_delay = 0.5
            self.max_recovery_attempts = 1
        elif environment == "testing":
            self.use_vpn = True
            self.mandatory_delay = 1.0
            self.max_recovery_attempts = 2
        else:  # production
            self.use_vpn = True
            self.mandatory_delay = 2.0
            self.max_recovery_attempts = 3

# Usage
env = os.getenv('ENVIRONMENT', 'production')
config = VpnConfig(environment=env)
```

## Troubleshooting

### Common Issues

#### 1. VPN Connection Fails

**Symptoms:** `VpnConnectionError` raised during initialization

**Solutions:**
- Verify Tunnelblick is installed and running
- Check NordVPN credentials in `.env` file
- Ensure VPN configurations are properly loaded in Tunnelblick
- Check network connectivity

```python
# Debug connection issues
try:
    vpn_handler = VpnProtectionHandler(config)
except VpnConnectionError as e:
    print(f"Connection failed: {e}")
    
    # Check Tunnelblick status
    throttler = RequestThrottler(config)
    is_running, pids = throttler.check_tunnelblick_status()
    print(f"Tunnelblick running: {is_running}, PIDs: {pids}")
```

#### 2. Frequent Rotation Failures

**Symptoms:** Low rotation success rate in statistics

**Solutions:**
- Increase recovery attempts in configuration
- Check Tunnelblick configuration quality
- Verify network stability
- Monitor system resources

```python
# Monitor rotation issues
stats = vpn_handler.get_comprehensive_vpn_statistics()
if stats['rotation_success_rate'] < 80:
    print("Investigating rotation issues...")
    print(f"Recent alerts: {stats['security_details']['recent_alerts']}")
    print(f"Recovery attempts: {stats['recovery_attempts']}")
```

#### 3. AppleScript Timeouts

**Symptoms:** `TunnelblickRecoveryError` with timeout messages

**Solutions:**
- Increase AppleScript timeout values
- Check system load and performance
- Restart Tunnelblick manually
- Verify macOS permissions

```python
# Handle AppleScript issues
try:
    throttler.execute_applescript(script)
except TunnelblickRecoveryError as e:
    if "timeout" in str(e).lower():
        print("AppleScript timeout detected")
        if throttler.recover_tunnelblick():
            print("Recovery completed, retry operation")
```

#### 4. Security Violations

**Symptoms:** `IPSecurityViolationError` raised frequently

**Solutions:**
- Review request patterns and frequency
- Adjust rotation thresholds
- Check for IP-based blocking
- Implement longer delays between requests

```python
# Handle security violations
try:
    vpn_handler.handle_request_timing()
except IPSecurityViolationError as e:
    if "REBOOT REQUIRED" in str(e):
        print("Critical security issue - system reboot needed")
        # Implement graceful shutdown
    else:
        print(f"Security violation: {e}")
        # Implement temporary backoff
```

### Debug Mode

Enable debug logging for troubleshooting:

```python
import logging

# Enable debug logging
logging.getLogger("VpnHandler").setLevel(logging.DEBUG)
logging.getLogger("VpnManager").setLevel(logging.DEBUG)

# Use debug configuration
config = VpnConfig(
    use_vpn=True,
    mandatory_delay=0.5,  # Faster for debugging
    max_recovery_attempts=1  # Quick failure for debugging
)
```

### Performance Optimization

For high-volume applications:

```python
# Optimized configuration
config = VpnConfig(
    use_vpn=True,
    mandatory_delay=1.0,        # Balanced delay
    request_delay=0.5,          # Faster fallback
    max_recovery_attempts=2,     # Quick recovery
    rotation_threshold=200,      # More requests per IP
    rotation_time_limit=7200     # Longer time per IP
)

# Batch processing for efficiency
def process_batch(items, batch_size=50):
    """Process items in batches to optimize VPN usage"""
    for i in range(0, len(items), batch_size):
        batch = items[i:i + batch_size]
        
        # Process batch with single VPN setup
        for item in batch:
            vpn_handler.handle_request_timing("batch_processing")
            process_item(item)
        
        # Brief pause between batches
        time.sleep(2)
```

## Support and Contributing

For issues, questions, or contributions:

1. Check this documentation first
2. Review log files in `logs/vpn/` for error details
3. Ensure all dependencies are properly installed
4. Test with minimal configuration first
5. Report issues with complete error logs and configuration details

---

*This documentation covers all public APIs and functionality of the vpn_controls module. For internal implementation details, refer to the source code comments and docstrings.*