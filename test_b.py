#!/usr/bin/env python3
"""
Simple Database Connection Test for Machine B
Tests if Machine B can connect to Database H via DuckDNS
"""

import sys
from datetime import datetime

def test_basic_connection():
    """Test basic network connectivity to the domain"""
    print("🌐 Testing basic network connectivity...")
    
    try:
        import socket
        
        # Test domain resolution
        domain = "savvyo.duckdns.org"
        port = 5432
        
        print(f"🔍 Resolving {domain}...")
        ip = socket.gethostbyname(domain)
        print(f"✅ Domain resolves to: {ip}")
        
        # Test port connectivity
        print(f"🔌 Testing port {port} connectivity...")
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10)
        
        result = sock.connect_ex((domain, port))
        sock.close()
        
        if result == 0:
            print(f"✅ Port {port} is open and accepting connections!")
            return True
        else:
            print(f"❌ Cannot connect to port {port}")
            return False
            
    except socket.gaierror as e:
        print(f"❌ Domain resolution failed: {e}")
        return False
    except Exception as e:
        print(f"❌ Connection test failed: {e}")
        return False

def test_postgres_connection():
    """Test actual PostgreSQL connection"""
    print("\n🐘 Testing PostgreSQL connection...")
    
    try:
        import psycopg2
    except ImportError:
        print("❌ psycopg2 not installed. Install with:")
        print("   pip install psycopg2-binary")
        return False
    
    # CONFIGURE THESE VALUES:
    DB_CONFIG = {
        'host': 'savvyo.duckdns.org',
        'port': 5432,
        'database': 'your_database_name',  # ← Change this
        'user': 'your_username',          # ← Change this
        'password': 'your_password'       # ← Change this
    }
    
    # Check if user configured the values
    if (DB_CONFIG['database'] == 'your_database_name' or 
        DB_CONFIG['user'] == 'your_username' or 
        DB_CONFIG['password'] == 'your_password'):
        print("⚠️  Please configure database credentials in the script first!")
        print("   Update DB_CONFIG dictionary with your actual values")
        return False
    
    try:
        print(f"🔐 Connecting to {DB_CONFIG['host']}:{DB_CONFIG['port']}")
        print(f"📊 Database: {DB_CONFIG['database']}")
        print(f"👤 User: {DB_CONFIG['user']}")
        
        # Attempt connection
        conn = psycopg2.connect(**DB_CONFIG)
        
        # Test basic query
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        
        cursor.execute("SELECT current_database();")
        current_db = cursor.fetchone()[0]
        
        print(f"✅ PostgreSQL connection successful!")
        print(f"📋 Database: {current_db}")
        print(f"🔧 Version: {version[:50]}...")
        
        # Test if coordination tables exist (from your script)
        try:
            cursor.execute("SELECT COUNT(*) FROM competition_progress;")
            comp_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM season_progress;")
            season_count = cursor.fetchone()[0]
            
            print(f"🎯 Coordination tables found!")
            print(f"   Competitions: {comp_count}")
            print(f"   Seasons: {season_count}")
            
        except psycopg2.Error:
            print("⚠️  Coordination tables not found (may need to run coordinator script first)")
        
        cursor.close()
        conn.close()
        
        return True
        
    except psycopg2.OperationalError as e:
        if "could not connect" in str(e).lower():
            print(f"❌ Cannot reach database server: {e}")
        elif "authentication failed" in str(e).lower():
            print(f"❌ Authentication failed: {e}")
        elif "does not exist" in str(e).lower():
            print(f"❌ Database does not exist: {e}")
        else:
            print(f"❌ Connection error: {e}")
        return False
        
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False

def test_with_psql():
    """Show psql command for manual testing"""
    print("\n🔧 Manual testing with psql:")
    print("If you have psql installed, try this command:")
    print()
    print("psql -h savvyo.duckdns.org -p 5432 -U your_username -d your_database")
    print()
    print("This will prompt for password and connect directly")

def main():
    """Run all connection tests"""
    print("🧪 Database Connection Test for Machine B")
    print("=" * 60)
    print(f"⏰ Test time: {datetime.now()}")
    print()
    
    # Test 1: Basic connectivity
    basic_ok = test_basic_connection()
    
    if not basic_ok:
        print("\n💡 Troubleshooting:")
        print("   1. Check if Machine A is running and VPN is connected")
        print("   2. Verify DuckDNS is updating correctly")
        print("   3. Check if PostgreSQL is running in Docker on Machine A")
        print("   4. Verify Docker port mapping: -p 5432:5432")
        return
    
    # Test 2: PostgreSQL connection
    db_ok = test_postgres_connection()
    
    # Test 3: Manual alternative
    test_with_psql()
    
    print("\n📊 Test Summary:")
    print(f"   Network connectivity: {'✅ PASS' if basic_ok else '❌ FAIL'}")
    print(f"   PostgreSQL connection: {'✅ PASS' if db_ok else '❌ FAIL'}")
    
    if basic_ok and db_ok:
        print("\n🎉 SUCCESS! Machine B can connect to Database H!")
        print("   Your coordinator script should work perfectly!")
    elif basic_ok:
        print("\n⚠️  Network is good, but check database credentials/setup")
    else:
        print("\n❌ Network connectivity issues - check VPN and DuckDNS")

if __name__ == "__main__":
    main()