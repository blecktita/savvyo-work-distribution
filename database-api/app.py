from flask import Flask, request, jsonify
import psycopg2
import psycopg2.extras
import os
from functools import wraps
import logging
from datetime import datetime

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database configuration
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': os.getenv('POSTGRES_PORT', '5432'),
    'database': os.getenv('POSTGRES_DB'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD')
}

# API Security
API_KEY = os.getenv('DUCK_API_KEY', 'super-secret-api-key')

def get_db_connection():
    """Get database connection"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return None

def require_api_key(f):
    """Decorator to require API key authentication"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        api_key = request.headers.get('X-API-Key')
        if not api_key or api_key != API_KEY:
            return jsonify({'error': 'Invalid or missing API key'}), 401
        return f(*args, **kwargs)
    return decorated_function

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    conn = get_db_connection()
    if conn:
        conn.close()
        return jsonify({'status': 'healthy', 'timestamp': datetime.now().isoformat()})
    return jsonify({'status': 'unhealthy', 'timestamp': datetime.now().isoformat()}), 500

@app.route('/api/query', methods=['POST'])
@require_api_key
def execute_query():
    """Execute a SELECT query"""
    try:
        data = request.get_json()
        
        if not data or 'query' not in data:
            return jsonify({'error': 'Query is required'}), 400
        
        query = data['query'].strip()
        params = data.get('params', [])
        
        # Security: Only allow SELECT statements
        if not query.upper().startswith('SELECT'):
            return jsonify({'error': 'Only SELECT queries are allowed'}), 400
        
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
        
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute(query, params)
        results = cursor.fetchall()
        
        # Convert to list of dictionaries
        data = [dict(row) for row in results]
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'data': data,
            'count': len(data)
        })
        
    except Exception as e:
        logger.error(f"Query execution failed: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/execute', methods=['POST'])
@require_api_key
def execute_statement():
    """Execute INSERT, UPDATE, DELETE statements"""
    try:
        data = request.get_json()
        
        if not data or 'query' not in data:
            return jsonify({'error': 'Query is required'}), 400
        
        query = data['query'].strip()
        params = data.get('params', [])
        
        # Security: Block dangerous operations
        dangerous_keywords = ['DROP', 'TRUNCATE', 'DELETE FROM', 'ALTER', 'CREATE', 'GRANT', 'REVOKE']
        query_upper = query.upper()
        
        # Allow only specific operations
        allowed_operations = ['INSERT', 'UPDATE', 'DELETE FROM']
        if not any(query_upper.startswith(op) for op in allowed_operations):
            return jsonify({'error': 'Only INSERT, UPDATE, DELETE operations are allowed'}), 400
        
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
        
        cursor = conn.cursor()
        cursor.execute(query, params)
        
        # Get number of affected rows
        affected_rows = cursor.rowcount
        
        # Commit the transaction
        conn.commit()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'affected_rows': affected_rows,
            'message': 'Statement executed successfully'
        })
        
    except Exception as e:
        logger.error(f"Statement execution failed: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/tables', methods=['GET'])
@require_api_key
def list_tables():
    """List all tables in the database"""
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
        
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute("""
            SELECT table_name, table_schema 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name
        """)
        
        tables = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'tables': [dict(table) for table in tables]
        })
        
    except Exception as e:
        logger.error(f"Failed to list tables: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/table/<table_name>/schema', methods=['GET'])
@require_api_key
def get_table_schema(table_name):
    """Get schema information for a specific table"""
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
        
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute("""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_name = %s AND table_schema = 'public'
            ORDER BY ordinal_position
        """, (table_name,))
        
        columns = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        if not columns:
            return jsonify({'error': 'Table not found'}), 404
        
        return jsonify({
            'success': True,
            'table': table_name,
            'columns': [dict(col) for col in columns]
        })
        
    except Exception as e:
        logger.error(f"Failed to get table schema: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/table/<table_name>/data', methods=['GET'])
@require_api_key
def get_table_data(table_name):
    """Get data from a specific table with optional pagination"""
    try:
        # Get query parameters
        limit = request.args.get('limit', 100, type=int)
        offset = request.args.get('offset', 0, type=int)
        
        # Security: Validate table name (prevent SQL injection)
        if not table_name.replace('_', '').isalnum():
            return jsonify({'error': 'Invalid table name'}), 400
        
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
        
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Check if table exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_name = %s AND table_schema = 'public'
            )
        """, (table_name,))
        
        if not cursor.fetchone()[0]:
            return jsonify({'error': 'Table not found'}), 404
        
        # Get total count
        cursor.execute(f'SELECT COUNT(*) FROM "{table_name}"')
        total_count = cursor.fetchone()[0]
        
        # Get data with pagination
        cursor.execute(f'SELECT * FROM "{table_name}" LIMIT %s OFFSET %s', (limit, offset))
        rows = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'table': table_name,
            'data': [dict(row) for row in rows],
            'pagination': {
                'limit': limit,
                'offset': offset,
                'total': total_count,
                'count': len(rows)
            }
        })
        
    except Exception as e:
        logger.error(f"Failed to get table data: {e}")
        return jsonify({'error': str(e)}), 500

@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Endpoint not found'}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({'error': 'Internal server error'}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)