# core/logger_manager.py
import logging
import sys
from datetime import datetime
from pathlib import Path


class ColorFormatter(logging.Formatter):
    """
    Adds ANSI color codes to log messages based on log level.
    """
    COLORS = {
        logging.DEBUG: "\033[94m",  # Blue
        # logging.INFO: "\033[92m",      # Green
        logging.WARNING: "\033[93m",  # Yellow
        logging.ERROR: "\033[91m",  # Red
        logging.CRITICAL: "\033[95m",  # Magenta
    }
    RESET = "\033[0m"

    def format(self, record):
        color = self.COLORS.get(record.levelno, self.RESET)
        message = super().format(record)
        return f"{color}{message}{self.RESET}"


class LoggerManager:
    """
    Project-wide logger with pre-configured format, levels, and optional file output.
    """

    def __init__(self, name: str = None, level=logging.INFO, log_to_file: bool = False):
        self.logger = logging.getLogger(name or __name__)
        self.logger.setLevel(level)

        # Avoid duplicate handlers if logger already configured
        if not self.logger.handlers:
            fmt = "%(asctime)s | %(name)s | %(levelname)s | %(message)s",
            datefmt = "%Y-%m-%d %H:%M:%S"
            formatter = logging.Formatter(
                fmt="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
                datefmt=datefmt
            )

            # Console handler
            console_handler = logging.StreamHandler(sys.stdout)
            color_formatter = ColorFormatter("%(asctime)s | %(name)s | %(levelname)s | %(message)s", datefmt=datefmt)
            # console_handler.setFormatter(formatter)
            console_handler.setFormatter(color_formatter)
            self.logger.addHandler(console_handler)

            # Optional file handler
            if log_to_file:
                log_dir = Path("logs")
                log_dir.mkdir(exist_ok=True)
                file_path = log_dir / f"app_{datetime.now().strftime('%Y%m%d')}.log"
                file_handler = logging.FileHandler(file_path)
                file_handler.setFormatter(formatter)
                self.logger.addHandler(file_handler)

    # Predefined helper methods
    def info(self, msg: str):
        self.logger.info(msg)

    def debug(self, msg: str):
        self.logger.debug(msg)

    def warning(self, msg: str):
        self.logger.warning(msg)

    def error(self, msg: str, exc: Exception| None = None, exc_info: bool = True):
        self.logger.error(msg, exc, exc_info=exc_info)

    def _log(self, msg: str):
        self.logger.log(5, msg=msg)

    def critical(self, msg: str):
        self.logger.critical(msg)

    def get_logger(self):
        """Return underlying logger (for compatibility with libraries)."""
        return self.logger
