import requests
import json
from typing import Dict, List, Optional

class DatabaseAPIClient:
    def __init__(self, base_url: str, api_key: str):
        """
        Initialize the database API client
        
        Args:
            base_url: The base URL of your API (e.g., "http://MACHINE_A_IP:5000")
            api_key: Your API key
        """
        self.base_url = base_url.rstrip('/')
        self.headers = {
            'X-API-Key': api_key,
            'Content-Type': 'application/json'
        }
    
    def health_check(self) -> Dict:
        """Check if the API is healthy"""
        try:
            response = requests.get(f"{self.base_url}/health", headers=self.headers)
            return response.json()
        except Exception as e:
            return {"error": str(e)}
    
    def query(self, sql: str, params: Optional[List] = None) -> Dict:
        """
        Execute a SELECT query
        
        Args:
            sql: SELECT SQL query
            params: Optional query parameters
            
        Returns:
            Dictionary with query results
        """
        data = {
            "query": sql,
            "params": params or []
        }
        
        try:
            response = requests.post(
                f"{self.base_url}/api/query",
                headers=self.headers,
                json=data
            )
            return response.json()
        except Exception as e:
            return {"error": str(e)}
    
    def execute(self, sql: str, params: Optional[List] = None) -> Dict:
        """
        Execute INSERT, UPDATE, DELETE statements
        
        Args:
            sql: SQL statement
            params: Optional query parameters
            
        Returns:
            Dictionary with execution results
        """
        data = {
            "query": sql,
            "params": params or []
        }
        
        try:
            response = requests.post(
                f"{self.base_url}/api/execute",
                headers=self.headers,
                json=data
            )
            return response.json()
        except Exception as e:
            return {"error": str(e)}
    
    def list_tables(self) -> Dict:
        """List all tables in the database"""
        try:
            response = requests.get(f"{self.base_url}/api/tables", headers=self.headers)
            return response.json()
        except Exception as e:
            return {"error": str(e)}
    
    def get_table_schema(self, table_name: str) -> Dict:
        """Get schema information for a specific table"""
        try:
            response = requests.get(
                f"{self.base_url}/api/table/{table_name}/schema",
                headers=self.headers
            )
            return response.json()
        except Exception as e:
            return {"error": str(e)}
    
    def get_table_data(self, table_name: str, limit: int = 100, offset: int = 0) -> Dict:
        """
        Get data from a specific table
        
        Args:
            table_name: Name of the table
            limit: Maximum number of rows to return
            offset: Number of rows to skip
            
        Returns:
            Dictionary with table data
        """
        try:
            params = {'limit': limit, 'offset': offset}
            response = requests.get(
                f"{self.base_url}/api/table/{table_name}/data",
                headers=self.headers,
                params=params
            )
            return response.json()
        except Exception as e:
            return {"error": str(e)}

# Example usage
if __name__ == "__main__":
    # Initialize client with your Dynamic DNS hostname and API key
    client = DatabaseAPIClient(
        base_url="http://yourdatabase.duckdns.org:5000",
        api_key="your-super-secret-api-key-change-this-immediately"
    )
    
    # Test connection
    print("Health check:", client.health_check())
    
    # List tables
    print("Tables:", client.list_tables())
    
    # Example query
    result = client.query("SELECT * FROM your_table_name LIMIT 5")
    print("Query result:", result)
    
    # Example insert
    insert_result = client.execute(
        "INSERT INTO your_table_name (column1, column2) VALUES (%s, %s)",
        ["value1", "value2"]
    )
    print("Insert result:", insert_result)
    
    # Get table schema
    schema = client.get_table_schema("your_table_name")
    print("Table schema:", schema)
    
    # Get table data with pagination
    data = client.get_table_data("your_table_name", limit=10, offset=0)
    print("Table data:", data)