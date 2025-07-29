import os
import sys
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import SQLAlchemyError
from database.base import Base

def load_environment():
    """Load environment variables from .env file"""
    load_dotenv()
    print("✓ Environment variables loaded")

def get_postgres_connection_string():
    """Get PostgreSQL connection string from environment variables"""
    host = os.getenv('POSTGRES_HOST', 'localhost')
    port = int(os.getenv('POSTGRES_PORT', 5432))
    user = os.getenv('POSTGRES_USER')
    password = os.getenv('POSTGRES_PASS')
    database = os.getenv('POSTGRES_DB')
    
    # Check for missing variables
    missing = [var for var in ('POSTGRES_USER', 'POSTGRES_PASS', 'POSTGRES_DB') if not os.getenv(var)]
    if missing:
        raise ValueError(f"Missing required PostgreSQL environment variables: {', '.join(missing)}")
    
    conn_str = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    print(f"✓ Connection string built for {user}@{host}:{port}/{database}")
    return conn_str

def test_database_connection(engine):
    """Test if we can connect to the database"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version()"))
            version = result.fetchone()[0]
            print(f"✓ Database connection successful")
            print(f"  PostgreSQL version: {version.split(',')[0]}")
            return True
    except SQLAlchemyError as e:
        print(f"✗ Database connection failed: {e}")
        return False

def import_schema_models():
    """Import all models from the schema to register them with Base"""
    try:
        # Import all models - this registers them with Base.metadata
        from database.match_schema import (
            Competition, Team, Player, Referee, Matchday, MatchdaySummary,
            Match, CommunityPrediction, LeagueTableEntry, TopScorer,
            MatchTeam, Lineup, Substitution, Goal, Card, TeamSide
        )
        print("✓ All schema models imported successfully")
        return True
    except ImportError as e:
        print(f"✗ Failed to import schema models: {e}")
        return False

def analyze_sqlalchemy_metadata():
    """Analyze what tables SQLAlchemy knows about"""
    print("\n" + "="*60)
    print("SQLALCHEMY METADATA ANALYSIS")
    print("="*60)
    
    tables = Base.metadata.tables
    print(f"Total tables registered with SQLAlchemy: {len(tables)}")
    
    if not tables:
        print("⚠️  No tables found in Base.metadata!")
        print("   This usually means the schema models weren't imported properly.")
        return False
    
    print("\nTables that SQLAlchemy will create:")
    for i, (table_name, table_obj) in enumerate(sorted(tables.items()), 1):
        print(f"  {i:2d}. {table_name}")
        
        # Show foreign keys
        fks = [fk.target_fullname for fk in table_obj.foreign_keys]
        if fks:
            print(f"      → Foreign keys: {', '.join(fks)}")
    
    return True

def get_existing_database_tables(engine):
    """Get list of tables that currently exist in the database"""
    try:
        inspector = inspect(engine)
        existing_tables = inspector.get_table_names()
        
        print(f"\n" + "="*60)
        print("EXISTING DATABASE TABLES")
        print("="*60)
        print(f"Total tables in database: {len(existing_tables)}")
        
        if existing_tables:
            print("\nExisting tables in database:")
            for i, table_name in enumerate(sorted(existing_tables), 1):
                print(f"  {i:2d}. {table_name}")
        else:
            print("No tables found in database")
            
        return existing_tables
    except SQLAlchemyError as e:
        print(f"✗ Failed to get existing tables: {e}")
        return []

def get_database_enums(engine):
    """Get custom enum types in the database"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT typname, string_agg(enumlabel, ', ' ORDER BY enumsortorder) as values
                FROM pg_type t 
                JOIN pg_enum e ON t.oid = e.enumtypid 
                WHERE t.typtype = 'e'
                GROUP BY typname
                ORDER BY typname
            """))
            enums = result.fetchall()
            
            print(f"\n" + "="*60)
            print("DATABASE ENUM TYPES")
            print("="*60)
            print(f"Total enum types: {len(enums)}")
            
            if enums:
                for enum_name, values in enums:
                    print(f"  • {enum_name}: {values}")
            else:
                print("No custom enum types found")
                
            return enums
    except SQLAlchemyError as e:
        print(f"✗ Failed to get enum types: {e}")
        return []

def compare_schema_vs_database(sqlalchemy_tables, db_tables):
    """Compare what SQLAlchemy expects vs what's in the database"""
    print(f"\n" + "="*60)
    print("SCHEMA vs DATABASE COMPARISON")
    print("="*60)
    
    sqlalchemy_set = set(sqlalchemy_tables)
    database_set = set(db_tables)
    
    missing_in_db = sqlalchemy_set - database_set
    extra_in_db = database_set - sqlalchemy_set
    common = sqlalchemy_set & database_set
    
    print(f"Tables in schema but missing from database ({len(missing_in_db)}):")
    if missing_in_db:
        for table in sorted(missing_in_db):
            print(f"  ✗ {table}")
    else:
        print("  (none)")
    
    print(f"\nTables in database but not in schema ({len(extra_in_db)}):")
    if extra_in_db:
        for table in sorted(extra_in_db):
            print(f"  ? {table}")
    else:
        print("  (none)")
    
    print(f"\nTables that match ({len(common)}):")
    if common:
        for table in sorted(common):
            print(f"  ✓ {table}")
    else:
        print("  (none)")

def analyze_table_dependencies():
    """Analyze table creation order based on foreign key dependencies"""
    print(f"\n" + "="*60)
    print("TABLE DEPENDENCY ANALYSIS")
    print("="*60)
    
    # Get tables in dependency order (SQLAlchemy figures this out)
    try:
        sorted_tables = Base.metadata.sorted_tables
        print("Table creation order (based on foreign key dependencies):")
        for i, table in enumerate(sorted_tables, 1):
            print(f"  {i:2d}. {table.name}")
            
            # Show what this table depends on
            deps = []
            for fk in table.foreign_keys:
                dep_table = fk.column.table.name
                if dep_table != table.name:  # Exclude self-references
                    deps.append(dep_table)
            
            if deps:
                print(f"      → Depends on: {', '.join(set(deps))}")
                
    except Exception as e:
        print(f"✗ Failed to analyze dependencies: {e}")

def create_tables_with_detailed_logging(engine):
    """Create tables with detailed logging of what happens"""
    print(f"\n" + "="*60)
    print("TABLE CREATION")
    print("="*60)
    
    try:
        print("Starting table creation...")
        Base.metadata.create_all(bind=engine)
        print("✓ Table creation completed successfully")
        return True
    except SQLAlchemyError as e:
        print(f"✗ Table creation failed: {e}")
        return False

def main():
    """Main diagnostic function"""
    print("DATABASE DIAGNOSTICS SCRIPT")
    print("="*60)
    
    # Step 1: Load environment
    try:
        load_environment()
    except Exception as e:
        print(f"✗ Failed to load environment: {e}")
        sys.exit(1)
    
    # Step 2: Build connection string
    try:
        conn_str = get_postgres_connection_string()
    except Exception as e:
        print(f"✗ Failed to build connection string: {e}")
        sys.exit(1)
    
    # Step 3: Create engine
    try:
        engine = create_engine(conn_str, echo=False)  # Set echo=True for SQL logging
        print("✓ SQLAlchemy engine created")
    except Exception as e:
        print(f"✗ Failed to create engine: {e}")
        sys.exit(1)
    
    # Step 4: Test database connection
    if not test_database_connection(engine):
        sys.exit(1)
    
    # Step 5: Import schema models
    if not import_schema_models():
        print("⚠️  Continuing with limited analysis...")
    
    # Step 6: Analyze SQLAlchemy metadata
    has_metadata = analyze_sqlalchemy_metadata()
    
    # Step 7: Get existing database state
    existing_tables = get_existing_database_tables(engine)
    get_database_enums(engine)
    
    # Step 8: Compare schema vs database
    if has_metadata:
        sqlalchemy_tables = list(Base.metadata.tables.keys())
        compare_schema_vs_database(sqlalchemy_tables, existing_tables)
        analyze_table_dependencies()
    
    # Step 9: Offer to create/recreate tables
    print(f"\n" + "="*60)
    print("ACTIONS")
    print("="*60)
    
    if has_metadata:
        user_input = input("Do you want to create/recreate all tables? (y/N): ").strip().lower()
        if user_input in ('y', 'yes'):
            if create_tables_with_detailed_logging(engine):
                print("\n🎉 Table creation successful!")
                # Re-analyze to show final state
                final_tables = get_existing_database_tables(engine)
                print(f"\nFinal result: {len(final_tables)} tables in database")
            else:
                print("\n❌ Table creation failed!")
        else:
            print("Skipping table creation.")
    else:
        print("Cannot create tables - no schema metadata available")
    
    print(f"\n" + "="*60)
    print("DIAGNOSTICS COMPLETE")
    print("="*60)

if __name__ == '__main__':
    main()