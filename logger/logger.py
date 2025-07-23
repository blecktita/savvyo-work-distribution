"""
Smart Log Rotation - Keep logs manageable and easy to navigate
"""
import logging
import os
from typing import Union
from pathlib import Path
from logging.handlers import TimedRotatingFileHandler, RotatingFileHandler
from datetime import datetime

# Ensure logs directory exists
os.makedirs("logs", exist_ok=True)


class ColoredFormatter(logging.Formatter):
    """Simple colored formatter"""
    COLORS = {
        'DEBUG': '\033[36m', 'INFO': '\033[32m', 'WARNING': '\033[33m',
        'ERROR': '\033[31m', 'CRITICAL': '\033[35m', 'RESET': '\033[0m'
    }

    def format(self, record):
        if hasattr(record, 'levelname'):
            colored_levelname = (
                f"{self.COLORS.get(record.levelname, '')}"
                f"{record.levelname:<8}"
                f"{self.COLORS['RESET']}"
            )
            record.colored_levelname = colored_levelname
        return super().format(record)


def setup_daily_rotating_logger(
    name: str,
    log_file: Union[str, Path],
    level: int = logging.INFO,
    days_to_keep: int = 30,
    enable_colors: bool = True
) -> logging.Logger:
    """
    Creates a logger that rotates daily - best for production.
    
    Benefits:
    - New file each day (easy to find today's logs)
    - Automatic cleanup of old files
    - Predictable file sizes
    
    Log files will be named like:
    - app.log (current day)
    - app.log.2024-01-15 (previous days)
    """
    
    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Daily rotation handler
    file_handler = TimedRotatingFileHandler(
        filename=log_file,
        when='midnight',      # Rotate at midnight
        interval=1,           # Every 1 day
        backupCount=days_to_keep,  # Keep N days of logs
        encoding='utf-8',
        atTime=None          # Rotate at midnight
    )
    
    # Add date suffix to rotated files
    file_handler.suffix = "%Y-%m-%d"
    
    console_handler = logging.StreamHandler()
    
    # Formatters
    file_formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(name)-20s | %(funcName)-15s:%(lineno)-4d | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    console_formatter = ColoredFormatter(
        '%(asctime)s | %(colored_levelname)s | %(name)-15s | %(message)s',
        datefmt='%H:%M:%S'
    ) if enable_colors else logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(name)-15s | %(message)s',
        datefmt='%H:%M:%S'
    )
    
    file_handler.setFormatter(file_formatter)
    console_handler.setFormatter(console_formatter)
    
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    if logger.hasHandlers():
        logger.handlers.clear()
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    logger.propagate = False
    
    return logger


def setup_size_rotating_logger(
    name: str,
    log_file: Union[str, Path],
    level: int = logging.INFO,
    max_file_size: int = 5 * 1024 * 1024,  # 5MB default
    backup_count: int = 10,
    enable_colors: bool = True
) -> logging.Logger:
    """
    Creates a logger that rotates by file size.
    
    Good for applications with variable logging volume.
    Log files will be named like:
    - app.log (current)
    - app.log.1 (previous)
    - app.log.2 (older)
    """
    
    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    
    file_handler = RotatingFileHandler(
        filename=log_file,
        maxBytes=max_file_size,
        backupCount=backup_count,
        encoding='utf-8'
    )
    
    console_handler = logging.StreamHandler()
    
    # Formatters
    file_formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(name)-20s | %(funcName)-15s:%(lineno)-4d | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    console_formatter = ColoredFormatter(
        '%(asctime)s | %(colored_levelname)s | %(name)-15s | %(message)s',
        datefmt='%H:%M:%S'
    ) if enable_colors else logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(name)-15s | %(message)s',
        datefmt='%H:%M:%S'
    )
    
    file_handler.setFormatter(file_formatter)
    console_handler.setFormatter(console_formatter)
    
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    if logger.hasHandlers():
        logger.handlers.clear()
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    logger.propagate = False
    
    return logger


def setup_session_based_logger(
    name: str,
    log_dir: Union[str, Path] = "logs",
    level: int = logging.INFO,
    enable_colors: bool = True
) -> logging.Logger:
    """
    Creates a new log file for each application run/session.
    
    Perfect for scraping jobs - each run gets its own file.
    Log files will be named like:
    - scraper_2024-01-15_14-30-25.log
    - scraper_2024-01-15_16-45-12.log
    """
    
    log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # Create unique filename with timestamp
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_file = log_dir / f"{name}_{timestamp}.log"
    
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    console_handler = logging.StreamHandler()
    
    # Formatters
    file_formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(name)-20s | %(funcName)-15s:%(lineno)-4d | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    console_formatter = ColoredFormatter(
        '%(asctime)s | %(colored_levelname)s | %(name)-15s | %(message)s',
        datefmt='%H:%M:%S'
    ) if enable_colors else logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(name)-15s | %(message)s',
        datefmt='%H:%M:%S'
    )
    
    file_handler.setFormatter(file_formatter)
    console_handler.setFormatter(console_formatter)
    
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    if logger.hasHandlers():
        logger.handlers.clear()
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    logger.propagate = False
    
    # Log the session start
    logger.info(f"=== NEW SESSION STARTED: {timestamp} ===")
    logger.info(f"Log file: {log_file}")
    
    return logger


def setup_smart_logger(
    name: str,
    log_file: Union[str, Path],
    strategy: str = "daily",
    level: int = logging.INFO,
    **kwargs
) -> logging.Logger:
    """
    Smart logger that chooses the best rotation strategy.
    
    Args:
        name: Logger name
        log_file: Base log file path
        strategy: "daily", "size", or "session"
        level: Logging level
        **kwargs: Additional arguments for specific handlers
    """
    
    if strategy == "daily":
        return setup_daily_rotating_logger(name, log_file, level, **kwargs)
    elif strategy == "size":
        return setup_size_rotating_logger(name, log_file, level, **kwargs)
    elif strategy == "session":
        return setup_session_based_logger(name, Path(log_file).parent, level, **kwargs)
    else:
        raise ValueError(f"Unknown strategy: {strategy}")


# =================================================================
#                         USAGE EXAMPLES                         |
# =================================================================

def demo_rotation_strategies():
    """Demonstrate different rotation strategies"""
    
    print("🔄 DAILY ROTATION - Best for long-running services")
    daily_logger = setup_daily_rotating_logger(
        "DailyApp", 
        "logs/daily_app.log",
        days_to_keep=30
    )
    daily_logger.info("This creates daily rotating logs")
    daily_logger.info("New file each day, automatic cleanup")
    daily_logger.info("Perfect for production services")
    
    print("\n📏 SIZE ROTATION - Good for variable load apps")
    size_logger = setup_size_rotating_logger(
        "SizeApp", 
        "logs/size_app.log",
        max_file_size=1024*1024,  # 1MB for demo
        backup_count=5
    )
    size_logger.info("This rotates when file gets too big")
    size_logger.info("Good when you can't predict log volume")
    
    print("\n📅 SESSION ROTATION - Perfect for scraping jobs")
    session_logger = setup_session_based_logger(
        "scraper",
        "logs"
    )
    session_logger.info("Each run gets its own file")
    session_logger.info("Perfect for batch jobs and scrapers")
    session_logger.info("Easy to find logs for specific runs")
    
    print("\n🎯 SMART LOGGER - One function to rule them all")
    smart_logger = setup_smart_logger(
        "SmartApp",
        "logs/smart_app.log",
        strategy="daily",
        days_to_keep=7
    )
    smart_logger.info("Choose strategy based on your needs")


def show_log_file_structure():
    """Show what the log directory looks like with different strategies"""
    
    print("\n📁 LOG DIRECTORY STRUCTURE:")
    print("""
    logs/
    ├── daily_app.log              # Today's logs
    ├── daily_app.log.2024-01-14   # Yesterday's logs  
    ├── daily_app.log.2024-01-13   # Day before
    │
    ├── size_app.log               # Current log
    ├── size_app.log.1             # Previous rotation
    ├── size_app.log.2             # Older rotation
    │
    ├── scraper_2024-01-15_09-30-15.log  # Morning run
    ├── scraper_2024-01-15_14-22-08.log  # Afternoon run
    └── scraper_2024-01-15_18-45-33.log  # Evening run
    """)


if __name__ == "__main__":
    demo_rotation_strategies()
    show_log_file_structure()