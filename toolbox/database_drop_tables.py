#!/usr/bin/env python3
"""
Drop database tables - USE WITH EXTREME CAUTION!
"""

import os
import sys

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
    print("✅ Loaded .env file")
except ImportError:
    print("⚠️ python-dotenv not installed, skipping .env file loading")
except Exception as e:
    print(f"⚠️ Error loading .env file: {e}")

def get_database_connection():
    """Get database connection using environment variables"""
    try:
        import psycopg2
        
        conn_params = {
            'host': os.getenv('POSTGRES_HOST', 'localhost'),
            'port': int(os.getenv('POSTGRES_PORT', 5432)),
            'user': os.getenv('POSTGRES_USER'),
            'password': os.getenv('POSTGRES_PASSWORD'),
            'database': os.getenv('POSTGRES_DB')
        }
        
        return psycopg2.connect(**conn_params)
    except ImportError:
        print("❌ psycopg2 not installed. Install with: pip install psycopg2-binary")
        return None
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return None

def list_existing_tables():
    """List all existing tables in the database"""
    print("\n📋 Current Tables in Database")
    print("=" * 50)
    
    conn = get_database_connection()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """)
        tables = cursor.fetchall()
        
        if tables:
            existing_tables = [table[0] for table in tables]
            for i, table in enumerate(existing_tables, 1):
                print(f"{i:2d}. {table}")
            cursor.close()
            conn.close()
            return existing_tables
        else:
            print("No tables found in database")
            cursor.close()
            conn.close()
            return []
            
    except Exception as e:
        print(f"❌ Error listing tables: {e}")
        conn.close()
        return []

def drop_specific_tables(tables_to_drop):
    """Drop specific tables from the database"""
    if not tables_to_drop:
        print("No tables specified to drop")
        return False
    
    print(f"\n🗑️  Dropping {len(tables_to_drop)} Tables")
    print("=" * 50)
    
    conn = get_database_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
        # Drop each table
        for table in tables_to_drop:
            print(f"🔄 Dropping table: {table}")
            cursor.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")
            print(f"✅ Dropped table: {table}")
        
        # Commit the changes
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"\n🎉 Successfully dropped {len(tables_to_drop)} tables!")
        return True
        
    except Exception as e:
        print(f"❌ Error dropping tables: {e}")
        conn.rollback()
        cursor.close()
        conn.close()
        return False

def drop_your_specific_tables():
    """Drop the specific tables you mentioned"""
    tables_to_drop = [
        'cards',
        'goals', 
        'match_lineups',
        'match_lineups_safe',
        'matchday_info',
        'matches',
        'players',
        'substitutions',
        'work_queue'
    ]
    
    print("🎯 Tables to be dropped:")
    for i, table in enumerate(tables_to_drop, 1):
        print(f"   {i}. {table}")
    
    # Safety confirmation
    print("\n⚠️  WARNING: This will permanently delete all data in these tables!")
    print("⚠️  This action cannot be undone!")
    
    confirm = input("\nType 'DELETE ALL TABLES' to confirm: ")
    
    if confirm == "DELETE ALL TABLES":
        return drop_specific_tables(tables_to_drop)
    else:
        print("❌ Operation cancelled - confirmation text didn't match")
        return False

def interactive_table_dropper():
    """Interactive mode to select which tables to drop"""
    existing_tables = list_existing_tables()
    
    if not existing_tables:
        print("No tables found to drop")
        return False
    
    print("\n🎯 Interactive Table Selection")
    print("=" * 50)
    print("Enter table numbers to drop (comma-separated), or 'all' for all tables:")
    print("Example: 1,3,5 or all")
    
    selection = input("Your selection: ").strip()
    
    if selection.lower() == 'all':
        tables_to_drop = existing_tables
    else:
        try:
            indices = [int(x.strip()) - 1 for x in selection.split(',')]
            tables_to_drop = [existing_tables[i] for i in indices if 0 <= i < len(existing_tables)]
        except (ValueError, IndexError):
            print("❌ Invalid selection")
            return False
    
    if not tables_to_drop:
        print("No valid tables selected")
        return False
    
    print(f"\n🎯 Selected tables to drop:")
    for table in tables_to_drop:
        print(f"   - {table}")
    
    # Safety confirmation
    print("\n⚠️  WARNING: This will permanently delete all data in these tables!")
    confirm = input("Type 'YES DELETE' to confirm: ")
    
    if confirm == "YES DELETE":
        return drop_specific_tables(tables_to_drop)
    else:
        print("❌ Operation cancelled")
        return False

def main():
    """Main function with options"""
    print("🗑️  Database Table Dropper")
    print("=" * 40)
    print("Choose an option:")
    print("1. Drop your specific tables (cards, goals, etc.)")
    print("2. Interactive table selection")
    print("3. Just list current tables")
    print("4. Exit")
    
    choice = input("\nEnter choice (1-4): ").strip()
    
    if choice == "1":
        success = drop_your_specific_tables()
    elif choice == "2":
        success = interactive_table_dropper()
    elif choice == "3":
        list_existing_tables()
        success = True
    elif choice == "4":
        print("👋 Goodbye!")
        success = True
    else:
        print("❌ Invalid choice")
        success = False
    
    return success

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)