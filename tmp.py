#!/usr/bin/env python3
"""
Simple DuckDNS IP Updater Test Script
Run this to test DuckDNS updates manually
"""

import requests
import socket
import sys
from datetime import datetime


def get_current_ip():
    """Get current public IP address"""
    try:
        # Method 1: Using ipify
        response = requests.get('https://api.ipify.org', timeout=10)
        if response.status_code == 200:
            return response.text.strip()
    except:
        pass
    
    try:
        # Method 2: Using httpbin
        response = requests.get('https://httpbin.org/ip', timeout=10)
        if response.status_code == 200:
            return response.json()['origin']
    except:
        pass
    
    try:
        # Method 3: Using icanhazip
        response = requests.get('https://icanhazip.com', timeout=10)
        if response.status_code == 200:
            return response.text.strip()
    except:
        pass
    
    return None


def update_duckdns_ip(domain, token, ip=None):
    """
    Update DuckDNS with current or specified IP
    
    Args:
        domain: Your DuckDNS subdomain (without .duckdns.org)
        token: Your DuckDNS token
        ip: IP address to set (if None, uses current public IP)
    
    Returns:
        dict: Result of the update operation
    """
    
    if ip is None:
        print("🔍 Getting current public IP...")
        ip = get_current_ip()
        if ip is None:
            return {
                'success': False,
                'error': 'Could not determine current public IP',
                'ip': None,
                'domain': f"{domain}.duckdns.org"
            }
        print(f"📍 Current IP: {ip}")
    
    print(f"🚀 Updating DuckDNS: {domain}.duckdns.org -> {ip}")
    
    try:
        # Build DuckDNS update URL
        url = f"https://www.duckdns.org/update?domains={domain}&token={token}&ip={ip}"
        
        # Make the request
        response = requests.get(url, timeout=10)
        
        result = {
            'timestamp': datetime.now().isoformat(),
            'ip': ip,
            'domain': f"{domain}.duckdns.org",
            'response_text': response.text.strip(),
            'status_code': response.status_code
        }
        
        if response.status_code == 200 and response.text.strip() == "OK":
            result['success'] = True
            result['message'] = "DuckDNS updated successfully"
            print("✅ SUCCESS: DuckDNS updated successfully")
        else:
            result['success'] = False
            result['error'] = f"DuckDNS update failed: {response.text.strip()}"
            print(f"❌ FAILED: {result['error']}")
        
        return result
        
    except requests.exceptions.Timeout:
        result = {
            'success': False,
            'error': 'Request timeout - check internet connection',
            'ip': ip,
            'domain': f"{domain}.duckdns.org",
            'timestamp': datetime.now().isoformat()
        }
        print(f"⏰ TIMEOUT: {result['error']}")
        return result
        
    except Exception as e:
        result = {
            'success': False,
            'error': f"Unexpected error: {str(e)}",
            'ip': ip,
            'domain': f"{domain}.duckdns.org",
            'timestamp': datetime.now().isoformat()
        }
        print(f"💥 ERROR: {result['error']}")
        return result


def test_domain_resolution(domain):
    """Test if domain resolves correctly"""
    full_domain = f"{domain}.duckdns.org"
    print(f"\n🔍 Testing domain resolution: {full_domain}")
    
    try:
        resolved_ip = socket.gethostbyname(full_domain)
        print(f"✅ Domain resolves to: {resolved_ip}")
        return resolved_ip
    except socket.gaierror as e:
        print(f"❌ Domain resolution failed: {e}")
        return None


def main():
    """Main test function"""
    print("🦆 DuckDNS IP Updater Test Script")
    print("=" * 50)
    
    # CONFIGURE THESE VALUES:
    DOMAIN = "savvyo"  # ← Change this to your DuckDNS subdomain
    TOKEN = "ac7179dc-2ea8-4779-b331-e602bd13c8f2"  # ← Change this to your DuckDNS token
    
    # Check if user configured the values
    if DOMAIN == "yourname-db" or TOKEN == "your_duckdns_token":
        print("❌ ERROR: Please configure DOMAIN and TOKEN in the script first!")
        print("   1. Go to https://www.duckdns.org")
        print("   2. Sign in and create a subdomain")
        print("   3. Copy your token")
        print("   4. Update DOMAIN and TOKEN variables in this script")
        sys.exit(1)
    
    print(f"📋 Configuration:")
    print(f"   Domain: {DOMAIN}.duckdns.org")
    print(f"   Token: {TOKEN[:8]}{'*' * (len(TOKEN) - 8)}")
    
    # Test 1: Update with current IP
    print(f"\n🧪 TEST 1: Update DuckDNS with current IP")
    result = update_duckdns_ip(DOMAIN, TOKEN)
    
    if result['success']:
        print(f"✅ Update successful!")
        
        # Test 2: Verify domain resolution
        print(f"\n🧪 TEST 2: Verify domain resolution")
        resolved_ip = test_domain_resolution(DOMAIN)
        
        if resolved_ip == result['ip']:
            print("✅ Domain resolves to correct IP!")
        else:
            print(f"⚠️  Domain resolves to different IP (may take a few minutes to propagate)")
            print(f"   Expected: {result['ip']}")
            print(f"   Got: {resolved_ip}")
    
    # Show full result
    print(f"\n📊 Full Result:")
    for key, value in result.items():
        print(f"   {key}: {value}")
    
    print(f"\n💡 Next Steps:")
    if result['success']:
        print("   1. ✅ DuckDNS is working!")
        print("   2. Connect/disconnect VPN manually")
        print("   3. Run this script again to test IP updates")
        print("   4. Update your coordinator script to use:")
        print(f"      DATABASE_URL = 'postgresql://user:pass@{DOMAIN}.duckdns.org:5432/db'")
    else:
        print("   1. ❌ Fix the DuckDNS configuration")
        print("   2. Check domain and token are correct")
        print("   3. Try again")


if __name__ == "__main__":
    main()