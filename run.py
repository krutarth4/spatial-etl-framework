import hashlib
import os
import sys
import threading
import time
from pathlib import Path

import uvicorn
from core.application import Application
from log_manager.logger_manager import LoggerManager
from main_core.core_config import CoreConfig


logger = LoggerManager("RunConfigWatcher").get_logger()


def _file_signature(path: Path) -> tuple[int, str] | None:
    try:
        stat = path.stat()
        content_hash = hashlib.sha256(path.read_bytes()).hexdigest()
        return stat.st_mtime_ns, content_hash
    except FileNotFoundError:
        return None


def _watch_config_and_restart(config_path: Path, poll_seconds: float = 2.0):
    baseline = _file_signature(config_path)
    logger.info(f"Watching config for changes: {config_path.resolve()}")

    while True:
        time.sleep(poll_seconds)
        current = _file_signature(config_path)
        if current != baseline:
            logger.warning(f"Detected config change in {config_path}. Restarting process...")
            os.execv(sys.executable, [sys.executable, *sys.argv])


def _start_config_watcher(runtime_conf: dict | None):
    watch_conf = ((runtime_conf or {}).get("config_watch")) or {}
    enabled = watch_conf.get("enable", True)
    poll_seconds = float(watch_conf.get("poll_seconds", 2))
    if not enabled:
        logger.info("Config watcher disabled via config.yaml runtime.config_watch.enable=false")
        return

    config_path = Path(__file__).resolve().parent / "config.yaml"
    watcher = threading.Thread(
        target=_watch_config_and_restart,
        args=(config_path, poll_seconds),
        daemon=True,
        name="config-watcher",
    )
    watcher.start()


if __name__ == "__main__":
    core_conf = CoreConfig()
    conf = core_conf.get_value("server")
    if conf.get("enable") and conf.get("reload"):
        logger.info("Skipping custom config watcher because Uvicorn reload is enabled")
    elif conf.get("enable"):
        _start_config_watcher(core_conf.get_config().get("runtime"))
    if conf.get("enable"):
        uvicorn.run(conf["app_type"], host=conf["host"], port=conf["port"], reload=conf["reload"])
    else:
        Application().run_standalone()
