import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

REPORT_LEVEL = 25
logging.addLevelName(REPORT_LEVEL, "REPORT")


class ColorFormatter(logging.Formatter):
    """ANSI color codes per log level for console output."""
    COLORS = {
        logging.DEBUG:    "\033[94m",  # Blue
        REPORT_LEVEL:     "\033[96m",  # Cyan  — stage-timing reports
        logging.WARNING:  "\033[93m",  # Yellow
        logging.ERROR:    "\033[91m",  # Red
        logging.CRITICAL: "\033[95m",  # Magenta
    }
    RESET = "\033[0m"

    def format(self, record):
        # Fallback: raw-logger callers (no LoggerAdapter) still render cleanly
        if not hasattr(record, "mapper"):
            record.mapper = record.name
        color = self.COLORS.get(record.levelno, self.RESET)
        message = super().format(record)
        return f"{color}{message}{self.RESET}"


def _init_root_console_handler() -> None:
    """Attach a single colored console handler to the root logger on first import."""
    root = logging.getLogger()
    if any(
        isinstance(h, logging.StreamHandler) and getattr(h, "stream", None) is sys.stdout
        for h in root.handlers
    ):
        return
    fmt = "%(asctime)s | %(mapper)s | %(levelname)s | %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(ColorFormatter(fmt=fmt, datefmt=datefmt))
    handler.setLevel(logging.DEBUG)
    root.addHandler(handler)
    # Only raise root level if it is still at Python's default (WARNING)
    if root.level == logging.WARNING:
        root.setLevel(logging.DEBUG)


_init_root_console_handler()


def setup_file_logging(conf: dict) -> None:
    """Attach a rotating file handler to the root logger from config.

    Called once at startup; subsequent calls are no-ops if a RotatingFileHandler
    is already registered.
    """
    if not conf.get("enable", False):
        return
    root = logging.getLogger()
    if any(isinstance(h, RotatingFileHandler) for h in root.handlers):
        return
    log_dir = Path(conf.get("dir", "logs"))
    log_dir.mkdir(parents=True, exist_ok=True)
    rotation = conf.get("rotation", {})
    handler = RotatingFileHandler(
        filename=log_dir / conf.get("filename", "pipeline.log"),
        maxBytes=rotation.get("max_bytes", 10 * 1024 * 1024),
        backupCount=rotation.get("backup_count", 7),
        encoding=rotation.get("encoding", "utf-8"),
    )
    handler.setFormatter(logging.Formatter(
        fmt="%(asctime)s | %(mapper)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))
    level = getattr(logging, str(conf.get("level", "INFO")).upper(), logging.INFO)
    handler.setLevel(level)
    root.addHandler(handler)


class PipelineLogger(logging.LoggerAdapter):
    """Zero-overhead structured logger for pipeline components.

    Uses stdlib LoggerAdapter — no extra method-call hops, no per-instance
    handler setup (a single root handler handles all output).  Injects the
    owning class/component name as 'mapper' into every LogRecord so every
    log line carries the datasource identity.

    Usage:
        self.logger = PipelineLogger(type(self).__name__)
        self.logger.info("message")   # logged as  ElevationMapper | INFO | message
    """

    def __init__(self, name: str, level: int = logging.INFO):
        base = logging.getLogger(name)
        base.setLevel(level)
        super().__init__(base, extra={"mapper": name})

    def process(self, msg, kwargs):
        """Inject mapper name into every log record's extra dict."""
        kwargs.setdefault("extra", {})
        kwargs["extra"]["mapper"] = self.extra["mapper"]
        return msg, kwargs

    # ── Convenience methods matching the legacy LoggerManager API ────────

    def report(self, msg: str, *args, **kwargs):
        """Log at REPORT level (25) — used for stage-timing table output."""
        self.log(REPORT_LEVEL, msg, *args, **kwargs)

    def _log(self, msg: str):
        """Log at trace level 5."""
        self.log(5, msg)

    def get_logger(self) -> logging.Logger:
        """Return the underlying stdlib Logger (for library compatibility)."""
        return self.logger

    def set_name(self, new_name: str):
        """Switch to a differently-named logger and update the mapper tag.

        Used by DbInstance.set_owner() to stamp the responsible mapper name
        onto DB-layer log records after the mapper is known.
        """
        self.logger = logging.getLogger(new_name)
        self.extra["mapper"] = new_name


# Backward-compatibility alias — every existing  LoggerManager(name)  call site
# continues to work without any change.  The only behavioural difference is that
# log records now carry the 'mapper' extra field used by ColorFormatter.
class LoggerManager(PipelineLogger):
    """Deprecated: prefer PipelineLogger directly.  Kept for backwards compatibility."""

    def __init__(self, name: str = None, level: int = logging.INFO, log_to_file: bool = False):
        super().__init__(name or __name__, level)
