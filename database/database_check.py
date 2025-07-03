#!/usr/bin/env python3
"""
PostgreSQL Database Discovery & Verification Script
Assumes no prior knowledge of database structure
"""

import os
import sys

# Use your existing project structure
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import your existing infrastructure
from .database_factory import create_database_service
from sqlalchemy import text

class PostgreSQLDiscoverer:
    """Discover and verify PostgreSQL database structure and data"""
    
    def __init__(self):
        self.db_service = create_database_service("production")
        self.db_service.initialize(create_tables=False)
    
    def discover_all(self):
        """Run complete database discovery"""
        print("🔍 POSTGRESQL DATABASE DISCOVERY")
        print("=" * 50)
        print("Starting fresh discovery - assuming no prior knowledge...")
        
        # Basic database info
        self.check_database_info()
        
        # Discover schemas
        schemas = self.discover_schemas()
        
        # Discover tables in each schema
        all_tables = self.discover_tables(schemas)
        
        # Analyze table structures
        self.analyze_table_structures(all_tables)
        
        # Check data content
        self.sample_data_from_tables(all_tables)
        
        # Look for relationships
        self.discover_relationships(all_tables)
        
        # Performance overview
        self.basic_performance_check(all_tables)
        
        print("\n✅ Discovery completed!")
        return all_tables
    
    def check_database_info(self):
        """Get basic database information"""
        print("\n📊 DATABASE BASIC INFO")
        print("-" * 30)
        
        with self.db_service.transaction() as session:
            # Database version and basic info
            queries = [
                ("PostgreSQL Version", "SELECT version()"),
                ("Database Name", "SELECT current_database()"),
                ("Current User", "SELECT current_user"),
                ("Database Size", "SELECT pg_size_pretty(pg_database_size(current_database()))"),
                ("Server Time", "SELECT now()"),
            ]
            
            for label, query in queries:
                try:
                    result = session.execute(text(query)).scalar()
                    if label == "PostgreSQL Version":
                        # Extract just the version number
                        version = result.split()[1] if result else "Unknown"
                        print(f"{label}: {version}")
                    else:
                        print(f"{label}: {result}")
                except Exception as e:
                    print(f"{label}: Error - {e}")
    
    def discover_schemas(self):
        """Discover all schemas in the database"""
        print("\n📁 SCHEMAS DISCOVERY")
        print("-" * 30)
        
        with self.db_service.transaction() as session:
            try:
                result = session.execute(text("""
                    SELECT schema_name 
                    FROM information_schema.schemata 
                    WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
                    ORDER BY schema_name
                """))
                
                schemas = [row[0] for row in result.fetchall()]
                
                if schemas:
                    print(f"Found {len(schemas)} user schema(s):")
                    for schema in schemas:
                        print(f"  • {schema}")
                else:
                    print("No user schemas found - using default 'public' schema")
                    schemas = ['public']
                
                return schemas
                
            except Exception as e:
                print(f"Error discovering schemas: {e}")
                return ['public']  # Fallback to public schema
    
    def discover_tables(self, schemas):
        """Discover all tables in the given schemas"""
        print("\n📋 TABLES DISCOVERY")
        print("-" * 30)
        
        all_tables = []
        
        with self.db_service.transaction() as session:
            for schema in schemas:
                try:
                    result = session.execute(text("""
                        SELECT table_name, table_type
                        FROM information_schema.tables 
                        WHERE table_schema = :schema
                        ORDER BY table_name
                    """), {"schema": schema})
                    
                    tables = result.fetchall()
                    
                    if tables:
                        print(f"\nSchema '{schema}' contains {len(tables)} table(s):")
                        for table_name, table_type in tables:
                            full_name = f"{schema}.{table_name}"
                            all_tables.append(full_name)
                            print(f"  • {table_name} ({table_type})")
                    else:
                        print(f"\nSchema '{schema}' contains no tables")
                        
                except Exception as e:
                    print(f"Error discovering tables in schema '{schema}': {e}")
        
        print(f"\nTotal tables found: {len(all_tables)}")
        return all_tables
    
    def analyze_table_structures(self, tables):
        """Analyze the structure of each table"""
        print("\n🏗️  TABLE STRUCTURES")
        print("-" * 30)
        
        with self.db_service.transaction() as session:
            for table in tables:
                schema, table_name = table.split('.')
                print(f"\n📊 Table: {table}")
                
                try:
                    # Get column information
                    result = session.execute(text("""
                        SELECT 
                            column_name,
                            data_type,
                            is_nullable,
                            column_default,
                            character_maximum_length
                        FROM information_schema.columns 
                        WHERE table_schema = :schema AND table_name = :table_name
                        ORDER BY ordinal_position
                    """), {"schema": schema, "table_name": table_name})
                    
                    columns = result.fetchall()
                    
                    if columns:
                        print("  Columns:")
                        for col in columns:
                            nullable = "NULL" if col.is_nullable == "YES" else "NOT NULL"
                            default = f" DEFAULT {col.column_default}" if col.column_default else ""
                            length = f"({col.character_maximum_length})" if col.character_maximum_length else ""
                            print(f"    • {col.column_name}: {col.data_type}{length} {nullable}{default}")
                    
                    # Get row count
                    count_result = session.execute(text(f'SELECT COUNT(*) FROM "{schema}"."{table_name}"'))
                    row_count = count_result.scalar()
                    print(f"  Rows: {row_count:,}")
                    
                    # Get table size
                    size_result = session.execute(text("""
                        SELECT pg_size_pretty(pg_total_relation_size(:full_table))
                    """), {"full_table": table})
                    table_size = size_result.scalar()
                    print(f"  Size: {table_size}")
                    
                except Exception as e:
                    print(f"  Error analyzing table: {e}")
    
    def sample_data_from_tables(self, tables):
        """Sample data from each table to understand content"""
        print("\n🎯 DATA SAMPLING")
        print("-" * 30)
        
        with self.db_service.transaction() as session:
            for table in tables:
                schema, table_name = table.split('.')
                print(f"\n📋 Sample from {table}:")
                
                try:
                    # Get a few sample rows
                    result = session.execute(text(f'SELECT * FROM "{schema}"."{table_name}" LIMIT 3'))
                    rows = result.fetchall()
                    
                    if rows:
                        # Get column names
                        columns = list(result.keys())
                        print(f"  Columns: {', '.join(columns)}")
                        
                        print("  Sample rows:")
                        for i, row in enumerate(rows, 1):
                            print(f"    Row {i}:")
                            for col, val in zip(columns, row):
                                # Truncate long values
                                str_val = str(val)
                                if len(str_val) > 50:
                                    str_val = str_val[:47] + "..."
                                print(f"      {col}: {str_val}")
                    else:
                        print("  No data found")
                        
                except Exception as e:
                    print(f"  Error sampling data: {e}")
    
    def discover_relationships(self, tables):
        """Discover foreign key relationships"""
        print("\n🔗 RELATIONSHIPS DISCOVERY")
        print("-" * 30)
        
        with self.db_service.transaction() as session:
            try:
                result = session.execute(text("""
                    SELECT
                        tc.table_schema,
                        tc.table_name,
                        kcu.column_name,
                        ccu.table_schema AS foreign_table_schema,
                        ccu.table_name AS foreign_table_name,
                        ccu.column_name AS foreign_column_name
                    FROM information_schema.table_constraints AS tc
                    JOIN information_schema.key_column_usage AS kcu
                        ON tc.constraint_name = kcu.constraint_name
                        AND tc.table_schema = kcu.table_schema
                    JOIN information_schema.constraint_column_usage AS ccu
                        ON ccu.constraint_name = tc.constraint_name
                        AND ccu.table_schema = tc.table_schema
                    WHERE tc.constraint_type = 'FOREIGN KEY'
                    ORDER BY tc.table_schema, tc.table_name
                """))
                
                relationships = result.fetchall()
                
                if relationships:
                    print(f"Found {len(relationships)} foreign key relationship(s):")
                    for rel in relationships:
                        print(f"  • {rel.table_schema}.{rel.table_name}.{rel.column_name}")
                        print(f"    → {rel.foreign_table_schema}.{rel.foreign_table_name}.{rel.foreign_column_name}")
                else:
                    print("No foreign key relationships found")
                    
            except Exception as e:
                print(f"Error discovering relationships: {e}")
    
    def basic_performance_check(self, tables):
        """Basic performance and index information"""
        print("\n⚡ PERFORMANCE OVERVIEW")
        print("-" * 30)
        
        with self.db_service.transaction() as session:
            try:
                # Check for indexes
                result = session.execute(text("""
                    SELECT
                        schemaname,
                        tablename,
                        indexname,
                        indexdef
                    FROM pg_indexes
                    WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
                    ORDER BY schemaname, tablename, indexname
                """))
                
                indexes = result.fetchall()
                
                if indexes:
                    print(f"Found {len(indexes)} index(es):")
                    current_table = None
                    for idx in indexes:
                        table_name = f"{idx.schemaname}.{idx.tablename}"
                        if table_name != current_table:
                            print(f"\n  Table: {table_name}")
                            current_table = table_name
                        print(f"    • {idx.indexname}")
                else:
                    print("No indexes found")
                    
            except Exception as e:
                print(f"Error checking indexes: {e}")
    
    def run_custom_query(self, query: str):
        """Run a custom SQL query"""
        print(f"\n🔍 CUSTOM QUERY: {query}")
        print("-" * 50)
        
        with self.db_service.transaction() as session:
            try:
                result = session.execute(text(query))
                
                # Handle different query types
                if query.strip().upper().startswith(('SELECT', 'WITH')):
                    rows = result.fetchall()
                    
                    if not rows:
                        print("No results found.")
                        return
                    
                    # Print column headers
                    if hasattr(result, 'keys'):
                        headers = list(result.keys())
                        print(" | ".join(str(h) for h in headers))
                        print("-" * (len(" | ".join(str(h) for h in headers))))
                    
                    # Print first 20 rows
                    for i, row in enumerate(rows[:20]):
                        formatted_row = []
                        for val in row:
                            str_val = str(val) if val is not None else "NULL"
                            if len(str_val) > 30:
                                str_val = str_val[:27] + "..."
                            formatted_row.append(str_val)
                        print(" | ".join(formatted_row))
                    
                    if len(rows) > 20:
                        print(f"... and {len(rows) - 20} more rows")
                    
                    print(f"\nTotal rows: {len(rows)}")
                else:
                    # For non-SELECT queries
                    print(f"Query executed successfully. Rows affected: {result.rowcount}")
                    
            except Exception as e:
                print(f"❌ Query error: {e}")

def main():
    """Main discovery and verification"""
    print("🚀 Starting PostgreSQL database discovery...")
    print("Assuming no prior knowledge of database structure...")
    
    try:
        discoverer = PostgreSQLDiscoverer()
        tables = discoverer.discover_all()
        
        # Interactive query mode
        print("\n" + "=" * 50)
        print("🔍 INTERACTIVE EXPLORATION MODE")
        print("Now that we've discovered the structure, explore your data!")
        print("Enter SQL queries (or 'quit' to exit)")
        
        if tables:
            print("\nQuick start examples based on discovered tables:")
            for table in tables[:3]:  # Show examples for first 3 tables
                schema, table_name = table.split('.')
                print(f"  SELECT * FROM \"{schema}\".\"{table_name}\" LIMIT 10;")
        
        print("=" * 50)
        
        while True:
            query = input("\nSQL> ").strip()
            
            if query.lower() in ['quit', 'exit', 'q']:
                break
            
            if not query:
                continue
            
            discoverer.run_custom_query(query)
        
        print("\n👋 Thanks for exploring your PostgreSQL database!")
        
    except Exception as e:
        print(f"❌ Discovery failed: {e}")
        print("\nTroubleshooting tips:")
        print("1. Make sure PostgreSQL is running")
        print("2. Check your database connection settings")
        print("3. Verify your database_factory.py configuration")
        print("4. Check if the user has proper permissions")

if __name__ == "__main__":
    main()