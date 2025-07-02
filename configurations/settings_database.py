# configurations/settings_database.py
"""
Database configuration with secure environment integration.
"""

import os
from dataclasses import dataclass
from pathlib import Path
import logging
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from configurations.settings_base import EnvironmentVariables

# Load environment variables
env_path = EnvironmentVariables.env_file_path
if env_path and Path(env_path).exists():
    load_dotenv(env_path)
    logging.info(f"Loaded environment from: {env_path}")
else:
    logging.warning(f"Environment file not found: {env_path}")

@dataclass
class DatabaseConfig:
    """
    Database configuration with secure environment integration
    """
    
    database_url: str
    echo: bool = False
    pool_size: int = 5
    max_overflow: int = 10
    pool_timeout: int = 30
    pool_recycle: int = 3600
    
    @classmethod
    def _build_postgres_url(cls) -> str:
        """
        Build PostgreSQL URL from environment variables with proper URL encoding
        """
        user = os.getenv('POSTGRES_USER', 'aseathletics_datascience')
        password = os.getenv('POSTGRES_PASSWORD')
        host = os.getenv('POSTGRES_HOST', 'localhost')
        port = os.getenv('POSTGRES_PORT', '5432')
        database = os.getenv('POSTGRES_DB', 'production_savvyo_db')
        
        if not password:
            raise ValueError(
                "POSTGRES_PASSWORD not found in environment. "
                "Please set it in .env"
            )
        
        encoded_user = quote_plus(user)
        encoded_password = quote_plus(password)
        
        url = f"postgresql://{encoded_user}:{encoded_password}@{host}:{port}/{database}"
        
        safe_url = f"postgresql://{user}:***@{host}:{port}/{database}"
        logging.info(f"Database URL configured: {safe_url}")
        
        return url
    
    @classmethod
    def development(cls) -> 'DatabaseConfig':
        """
        Development configuration
        """
        return cls(
            database_url=cls._build_postgres_url(),
            echo=True,
            pool_size=3
        )
    
    @classmethod
    def testing(cls) -> 'DatabaseConfig':
        """
        Testing configuration
        """
        return cls(
            database_url="sqlite:///:memory:",
            echo=False,
            pool_size=1
        )
    
    @classmethod
    def production(cls) -> 'DatabaseConfig':
        """
        Production configuration
        """
        return cls(
            database_url=cls._build_postgres_url(),
            echo=False,
            pool_size=10,
            max_overflow=20,
            pool_timeout=60
        )
    
    @classmethod
    def from_url(cls, url: str) -> 'DatabaseConfig':
        """
        Create config from custom URL
        """
        return cls(database_url=url)
    
    @classmethod
    def for_colleague(cls, host_ip: str) -> 'DatabaseConfig':
        """
        Create configuration for colleague connecting to your database.
        
        Args:
            host_ip: Your machine's IP address
        """
        user = os.getenv('POSTGRES_USER', 'aseathletics_datascience')
        password = os.getenv('POSTGRES_PASSWORD')
        port = os.getenv('POSTGRES_PORT', '5432')
        database = os.getenv('POSTGRES_DB', 'production_savvyo_db')
        
        if not password:
            raise ValueError("POSTGRES_PASSWORD required for colleague connection")
        
        encoded_user = quote_plus(user)
        encoded_password = quote_plus(password)
        
        url = f"postgresql://{encoded_user}:{encoded_password}@{host_ip}:{port}/{database}"
        
        return cls(
            database_url=url,
            echo=False,
            pool_size=5
        )
    
    def validate_connection(self) -> bool:
        """
        Test database connection
        """
        try:
            engine = create_engine(self.database_url, pool_pre_ping=True)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            logging.info("Database connection validated successfully")
            return True
            
        except Exception as e:
            logging.error(f"Database connection failed: {e}")
            return False
    
    def get_connection_info(self) -> dict:
        """
        Get safe connection information for logging
        """
        if self.database_url.startswith('postgresql://'):
            parts = self.database_url.replace('postgresql://', '').split('@')
            if len(parts) == 2:
                user_pass = parts[0].split(':')[0]
                host_db = parts[1]
                
                return {
                    'type': 'PostgreSQL',
                    'user': user_pass,
                    'host_db': host_db,
                    'pool_size': self.pool_size,
                    'echo': self.echo
                }
        
        return {
            'type': 'SQLite' if 'sqlite' in self.database_url else 'Unknown',
            'url': self.database_url.split('/')[-1],
            'pool_size': self.pool_size,
            'echo': self.echo
        }


if __name__ == "__main__":
    print("Testing Database Configuration")

    environment = os.getenv('ENVIRONMENT', 'development')
    try:
        if environment == 'production':
            config = DatabaseConfig.production()
        elif environment == 'testing':
            config = DatabaseConfig.testing()
        else:
            config = DatabaseConfig.development()

        print(f"Configuration: {config.get_connection_info()}")

        if config.validate_connection():
            print("Database connection successful!")
        else:
            print("Database connection failed!")

    except Exception as e:
            print(f"Configuration error: {e}")