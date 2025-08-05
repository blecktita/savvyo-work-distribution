#!/usr/bin/env python3
"""
List PostgreSQL Tables and Columns Script
=========================================

Usage:
  python list_tables.py

This script connects to your PostgreSQL database (configured via environment variables)
and prints the names of all tables in the public schema along with their column names and types.

Environment variables needed:
  POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASS, POSTGRES_DB
"""

import os

from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect

# Load environment variables
load_dotenv()

user = os.getenv("POSTGRES_USER")
password = os.getenv("POSTGRES_PASS")
host = os.getenv("POSTGRES_HOST", "localhost")
port = os.getenv("POSTGRES_PORT", "5432")
db = os.getenv("POSTGRES_DB")

if not all([user, password, db]):
    print(
        "Error: Please set POSTGRES_USER, POSTGRES_PASS, and POSTGRES_DB in your environment."
    )
    exit(1)

# Construct connection URL and engine
url = f"postgresql://{user}:{password}@{host}:{port}/{db}"
engine = create_engine(url)

# Use SQLAlchemy inspector to get table names and column details
inspector = inspect(engine)
tables = inspector.get_table_names(schema="public")

print(f"Tables in database '{db}' (schema='public'):")
for table in tables:
    print(f"\nTable: {table}")
    columns = inspector.get_columns(table, schema="public")
    for col in columns:
        print(f"  - {col['name']}: {col['type']}")
