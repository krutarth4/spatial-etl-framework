import argparse
import hashlib
import os
import sys
import threading
import time
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env")


from banner import print_banner, log_banner


import uvicorn
from core.application import Application
from utils.logger_manager import LoggerManager, setup_file_logging
from main_core.core_config import CoreConfig


def _parse_args():
    parser = argparse.ArgumentParser(prog="spatial-etl-framework")
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--only",
        help="Comma-separated datasource names to run exclusively (overrides config.yaml enable flags)",
    )
    group.add_argument(
        "--disable",
        help="Comma-separated datasource names to skip (overrides config.yaml enable flags)",
    )
    return parser.parse_args()


def _csv_to_list(value):
    if not value:
        return None
    items = [s.strip() for s in value.split(",") if s.strip()]
    return items or None


logger = LoggerManager("RunConfigWatcher").get_logger()


def _file_signature(path: Path) -> str | None:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except FileNotFoundError:
        return None


def _watch_signatures(config_path: Path) -> dict[str, str]:
    """Return {filepath_str: sha256} for every file that should trigger a restart."""
    sigs: dict[str, str] = {}
    base_dir = config_path.parent
    paths: list[Path] = [config_path]
    for sub, pattern in [("data_source_configs", "*.yaml"), ("data_mappers", "*.py")]:
        d = base_dir / sub
        if d.is_dir():
            paths.extend(sorted(d.glob(pattern)))
    for p in paths:
        try:
            sigs[str(p)] = hashlib.sha256(p.read_bytes()).hexdigest()
        except OSError:
            pass
    return sigs


def _combined_signature(config_path: Path) -> str:
    """Single hash across all watched files — used by the standalone keep-alive."""
    h = hashlib.sha256()
    for key, sig in sorted(_watch_signatures(config_path).items()):
        h.update(key.encode())
        h.update(sig.encode())
    return h.hexdigest()


def _restart_argv(changed_ds_names: list[str]) -> list[str]:
    """Build the argv for a targeted restart, stripping any previous --only flag."""
    base = [a for i, a in enumerate(sys.argv)
            if not (a == "--only" or a.startswith("--only=")
                    or (i > 0 and sys.argv[i - 1] == "--only"))]
    if changed_ds_names:
        base += ["--only", ",".join(changed_ds_names)]
    return base


def _watch_config_and_restart(config_path: Path, poll_seconds: float = 2.0):
    baseline = _watch_signatures(config_path)
    ds_config_dir = str(config_path.parent / "data_source_configs")
    logger.info(
        f"Watching config for changes: {config_path.resolve()} "
        f"(+ data_source_configs/, data_mappers/)"
    )

    while True:
        time.sleep(poll_seconds)
        current = _watch_signatures(config_path)
        if current == baseline:
            continue

        changed = {p for p, sig in current.items() if sig != baseline.get(p)}
        changed |= set(current) - set(baseline)   # newly added files

        main_cfg_changed = str(config_path) in changed
        mapper_changed = any(p.endswith(".py") for p in changed)
        changed_ds_yamls = [
            p for p in changed
            if p.startswith(ds_config_dir) and p.endswith(".yaml")
        ]

        if main_cfg_changed or mapper_changed or not changed_ds_yamls:
            logger.warning(f"Config change detected {changed} — full restart")
            os.execv(sys.executable, [sys.executable, *sys.argv])
        else:
            ds_names = [Path(p).stem for p in changed_ds_yamls]
            logger.warning(
                f"Datasource config(s) changed {ds_names} — targeted restart with --only"
            )
            argv = _restart_argv(ds_names)
            os.execv(sys.executable, [sys.executable, *argv])


def _start_config_watcher(runtime_conf: dict | None):
    watch_conf = ((runtime_conf or {}).get("config_watch")) or {}
    enabled = watch_conf.get("enable", True)
    poll_seconds = float(watch_conf.get("poll_seconds", 2))
    if not enabled:
        logger.info("Config watcher disabled via config.yaml runtime.config_watch.enable=false")
        return

    config_path = Path(CoreConfig.filepath)
    watcher = threading.Thread(
        target=_watch_config_and_restart,
        args=(config_path, poll_seconds),
        daemon=True,
        name="config-watcher",
    )
    watcher.start()


if __name__ == "__main__":
    print_banner()
    args = _parse_args()
    only = _csv_to_list(args.only) or _csv_to_list(os.getenv("ETL_ONLY"))
    disable = _csv_to_list(args.disable) or _csv_to_list(os.getenv("ETL_DISABLE"))
    CoreConfig.set_datasource_override(only=only, disable=disable)
    core_conf = CoreConfig()
    setup_file_logging(core_conf.get_config().get("logging") or {})
    log_banner()  # plain-text banner → pipeline.log (file handler now attached)

    conf = core_conf.get_value("server")
    if conf.get("enable") and conf.get("reload"):
        logger.info("Skipping custom config watcher because Uvicorn reload is enabled")
    elif conf.get("enable"):
        _start_config_watcher(core_conf.get_config().get("runtime"))
    if conf.get("enable"):
        uvicorn.run(conf["app_type"], host=conf["host"], port=conf["port"], reload=conf["reload"])
    else:
        Application().run_standalone()
