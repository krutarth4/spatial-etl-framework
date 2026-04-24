import json
import os
import threading
from datetime import datetime
from pathlib import Path

from log_manager.logger_manager import LoggerManager
from main_core.core_config import CoreConfig


_DEFAULT_STATE_FILE = "tmp/comm_state.json"

# Module-level stores shared across CommRepository instances pointing at the
# same file. Router webhook endpoints, graphMapper, and InitGraph all talk to
# one logical comm state this way.
_STORES: dict[str, dict] = {}
_STORE_LOCKS: dict[str, threading.Lock] = {}
_REGISTRY_LOCK = threading.Lock()


def _resolve_state_file() -> str:
    try:
        config = CoreConfig().get_config() or {}
    except Exception:
        config = {}
    graph_conf = config.get("graph") or {}
    comm_conf = graph_conf.get("communication") or {} if isinstance(graph_conf, dict) else {}
    configured = comm_conf.get("state_file") if isinstance(comm_conf, dict) else None
    path = configured or _DEFAULT_STATE_FILE
    abs_path = str(Path(path).expanduser().resolve())
    return abs_path


def _get_store_and_lock(state_file: str) -> tuple[dict, threading.Lock]:
    with _REGISTRY_LOCK:
        if state_file not in _STORES:
            _STORES[state_file] = _load_from_disk(state_file)
            _STORE_LOCKS[state_file] = threading.Lock()
        return _STORES[state_file], _STORE_LOCKS[state_file]


def _load_from_disk(state_file: str) -> dict:
    path = Path(state_file)
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _persist_to_disk(state_file: str, store: dict) -> None:
    path = Path(state_file)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as f:
        json.dump(store, f, indent=2, default=_json_default)
    os.replace(tmp_path, path)


def _json_default(value):
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _normalize_value(value):
    if isinstance(value, datetime):
        return value.isoformat()
    return value


class CommRepository:
    """In-memory task store persisted to a JSON file on every write.

    Keeps the same public API as the former SQL-backed repository so
    CommService (and its callers) need no changes. `db` and `schema` are
    retained in the signature for backwards compatibility but are not used.
    """

    def __init__(self, db=None, schema: str | None = None):
        self.logger = LoggerManager(type(self).__name__)
        self.state_file = _resolve_state_file()
        self._store, self._lock = _get_store_and_lock(self.state_file)

    def is_present(self) -> bool:
        return Path(self.state_file).exists()

    def create_table(self) -> None:
        # Ensure the snapshot file exists so router/pipeline restarts find state.
        if not Path(self.state_file).exists():
            with self._lock:
                _persist_to_disk(self.state_file, self._store)

    def _snapshot(self, task_key: str) -> dict | None:
        task = self._store.get(task_key)
        return dict(task) if task else None

    def get_task(self, task_key: str) -> dict | None:
        with self._lock:
            return self._snapshot(task_key)

    def get_task_status(self, task_key: str) -> dict | None:
        with self._lock:
            return self._snapshot(task_key)

    def upsert_task(self, task_key: str, defaults: dict) -> dict:
        with self._lock:
            now_iso = datetime.utcnow().isoformat()
            task = self._store.get(task_key)
            if task is None:
                task = {
                    "task_key": task_key,
                    "owner": None,
                    "current_status": "idle",
                    "is_completed": False,
                    "last_run_status": None,
                    "last_run_message": None,
                    "last_checked_at": None,
                    "last_successful_run_at": None,
                    "created_at": now_iso,
                    "updated_at": now_iso,
                }
                self._store[task_key] = task
            for field, value in (defaults or {}).items():
                task[field] = _normalize_value(value)
            task["updated_at"] = now_iso
            _persist_to_disk(self.state_file, self._store)
            return dict(task)

    def update_task(self, task_key: str, **updates) -> dict | None:
        with self._lock:
            task = self._store.get(task_key)
            if task is None:
                return None
            for field, value in (updates or {}).items():
                task[field] = _normalize_value(value)
            task["updated_at"] = datetime.utcnow().isoformat()
            _persist_to_disk(self.state_file, self._store)
            return dict(task)

    def reset_all_task_completion_flags(self) -> int:
        with self._lock:
            count = 0
            for task in self._store.values():
                if task.get("is_completed"):
                    count += 1
                task["is_completed"] = False
            if count:
                _persist_to_disk(self.state_file, self._store)
            return count

    def list_tasks(self) -> list[dict]:
        with self._lock:
            return [dict(task) for task in self._store.values()]
