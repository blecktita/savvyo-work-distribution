"""
Logging Configuration Module
"""
# utils/logging/logger.py

import logging
import os
import sys
from typing import Union, Dict, Any, Optional, TextIO
from pathlib import Path
from logging.handlers import RotatingFileHandler

# Ensure logs directory exists
os.makedirs("logs", exist_ok=True)


class ColoredFormatter(logging.Formatter):
    """
    Custom formatter with colors for console output
    """

    # ANSI color codes
    COLORS: Dict[str, str] = {
        'DEBUG': '\033[36m',     # Cyan
        'INFO': '\033[32m',      # Green
        'WARNING': '\033[33m',   # Yellow
        'ERROR': '\033[31m',     # Red
        'CRITICAL': '\033[35m',  # Magenta
        'RESET': '\033[0m'       # Reset
    }

    def format(self, record: logging.LogRecord) -> str:
        # Add color to levelname for console
        if hasattr(record, 'levelname'):
            colored_levelname: str = (
                "%s%s%-8s%s" % (
                    self.COLORS.get(record.levelname, ''),
                    record.levelname,
                    '',
                    self.COLORS['RESET']
                )
            )
            record.colored_levelname = colored_levelname
        return super().format(record)


def setup_logger(
    name: str,
    log_file: Union[str, Path],
    level: int = logging.INFO,
    max_file_size: int = 10 * 1024 * 1024,
    backup_count: int = 3,
    enable_colors: bool = True
) -> logging.Logger:
    """
    Configurations for a structured logger with file rotation and console output.
    This logger supports:

    Args:
        name: Logger name (usually __name__)
        log_file: Path to log file
        level: Logging level
        max_file_size: Max file size before rotation (bytes)
        backup_count: Number of backup files to keep
        enable_colors: Enable colored console output
    """

    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Create rotating file handler to prevent huge log files
    file_handler: RotatingFileHandler = RotatingFileHandler(
        log_file,
        maxBytes=max_file_size,
        backupCount=backup_count
    )

    console_handler: logging.StreamHandler[TextIO] = logging.StreamHandler(sys.stdout)

    # Enhanced file formatter - very clean and structured
    file_formatter: logging.Formatter = logging.Formatter(
        fmt=('%(asctime)s | %(levelname)-8s | %(name)-20s | '
             '%(funcName)-15s:%(lineno)-4d | %(message)s'),
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Console formatter with colors (if enabled)
    console_formatter: logging.Formatter
    if enable_colors:
        console_formatter = ColoredFormatter(
            fmt='%(asctime)s | %(colored_levelname)s | %(name)-15s | %(message)s',
            datefmt='%H:%M:%S'
        )
    else:
        console_formatter = logging.Formatter(
            fmt='%(asctime)s | %(levelname)-8s | %(name)-15s | %(message)s',
            datefmt='%H:%M:%S'
        )

    # Set formatters
    file_handler.setFormatter(file_formatter)
    console_handler.setFormatter(console_formatter)

    # Get logger
    logger: logging.Logger = logging.getLogger(name)
    logger.setLevel(level)

    # Clear existing handlers
    if logger.hasHandlers():
        logger.handlers.clear()

    # Add handlers
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    # Prevent propagation to root logger
    logger.propagate = False

    return logger


def create_section_separator(title: str, logger: logging.Logger,
                           level: int = logging.INFO) -> None:
    """
    Create a visual separator in logs for major sections
    """
    separator: str = "=" * 60
    logger.log(level, "\n%s", separator)
    logger.log(level, "  %s", title.upper().center(56))
    logger.log(level, "%s", separator)


def log_function_entry(logger: logging.Logger, func_name: str, 
                      **kwargs: Any) -> None:
    """
    Log function entry with parameters
    """
    params: str = (
        ", ".join(["%s=%s" % (k, v) for k, v in kwargs.items()]) 
        if kwargs else "no params"
    )
    logger.debug("🔹 ENTERING %s(%s)", func_name, params)


def log_function_exit(logger: logging.Logger, func_name: str, 
                     result: Any = None, duration: Optional[float] = None) -> None:
    """
    Log function exit with optional result and timing
    """
    msg: str = "🔸 EXITING %s" % func_name
    if result is not None:
        msg += " -> %s" % result
    if duration is not None:
        msg += " [%.3fs]" % duration
    logger.debug(msg)


def log_performance_metrics(logger: logging.Logger, operation: str, 
                          **metrics: Any) -> None:
    """
    Log performance metrics in a structured way
    """
    logger.info("📊 PERFORMANCE [%s]:", operation)
    for metric, value in metrics.items():
        logger.info("    %s: %s", metric, value)


# =================================================================
#           EXAMPLE USAGE TO NOT FORGET HOW TO USE LOGGER:        |
# =================================================================
def demo_enhanced_logging() -> None:
    """
    Demonstrate the logging features
    """

    # ===== Setup logger =================
    logger: logging.Logger = setup_logger(
        name="MyApp",
        log_file="logs/enhanced_app.log",
        level=logging.DEBUG
    )

    # ====== Section separator=============================
    create_section_separator("Application Startup", logger)

    logger.info("🚀 Application starting...")
    logger.debug("Loading configuration...")
    logger.info("✅ Configuration loaded successfully")

    # Function tracing example
    log_function_entry(logger, "process_data", batch_size=100, timeout=30)

    # Different log levels with emojis for visual scanning
    logger.debug("🔍 Debug: Detailed processing info")
    logger.info("ℹ️  Info: General application flow")
    logger.warning("⚠️  Warning: Something needs attention")
    logger.error("❌ Error: Something went wrong")

    # Performance metrics
    log_performance_metrics(
        logger,
        "Data Processing",
        records_processed=1500,
        processing_time="2.34s",
        memory_usage="45MB",
        success_rate="99.2%"
    )

    log_function_exit(logger, "process_data", result="Success", duration=2.34)

    create_section_separator("Application Shutdown", logger)
    logger.info("👋 Application shutting down gracefully")


if __name__ == "__main__":
    demo_enhanced_logging()
