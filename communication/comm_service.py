import time
from datetime import datetime

import requests

from log_manager.logger_manager import LoggerManager
from main_core.core_config import CoreConfig

from communication.comm_repository import CommRepository


class CommService:
    def __init__(self, db=None, table_schema: str | None = None):
        self.logger = LoggerManager(type(self).__name__)
        self.repository = CommRepository(db, table_schema)
        self._router_base_url, self._router_timeout = self._load_router_config()

    @staticmethod
    def _load_router_config() -> tuple[str | None, float]:
        try:
            config = CoreConfig().get_config() or {}
        except Exception:
            return None, 5.0
        comm_conf = ((config.get("graph") or {}).get("communication") or {})
        router_conf = comm_conf.get("router") or {}
        if not isinstance(router_conf, dict):
            return None, 5.0
        base_url = router_conf.get("base_url")
        timeout = router_conf.get("timeout_seconds", 5.0)
        try:
            timeout_float = float(timeout)
        except (TypeError, ValueError):
            timeout_float = 5.0
        if isinstance(base_url, str) and base_url.strip():
            return base_url.rstrip("/"), timeout_float
        return None, timeout_float

    def create_table(self):
        if self.repository is None:
            return
        if self.repository.is_present():
            return
        self.repository.create_table()

    def ensure_task(
        self,
        task_key: str,
        owner: str | None = None,
        current_status: str = "idle",
        is_completed: bool = False,
        overwrite_existing: bool = False,
    ):
        if self.repository is None or not task_key:
            return None
        self.create_table()
        if not overwrite_existing:
            existing = self.repository.get_task_status(task_key)
            if existing is not None:
                return existing
        return self.repository.upsert_task(
            task_key,
            {
                "owner": owner,
                "current_status": current_status,
                "is_completed": is_completed,
            },
        )

    def update_status(
        self,
        task_key: str,
        *,
        current_status: str | None = None,
        last_run_status: str | None = None,
        last_run_message: str | None = None,
        success: bool = False,
        owner: str | None = None,
        is_completed: bool | None = None,
    ):
        if self.repository is None or not task_key:
            return None
        self.create_table()
        existing = self.repository.get_task_status(task_key)
        if existing is None:
            self.ensure_task(task_key, owner=owner)
        updates = {"last_checked_at": datetime.utcnow()}
        if owner is not None:
            updates["owner"] = owner
        if current_status is not None:
            updates["current_status"] = current_status
        if is_completed is not None:
            updates["is_completed"] = bool(is_completed)
        if last_run_status is not None:
            updates["last_run_status"] = last_run_status
        if last_run_message is not None:
            updates["last_run_message"] = last_run_message
        if success:
            updates["last_successful_run_at"] = datetime.utcnow()
        updated = self.repository.update_task(task_key, **updates)
        if updated is None:
            updated = self.repository.upsert_task(task_key, updates)
        self._notify_router(task_key, updated)
        return updated

    def get_task_status(self, task_key: str) -> dict | None:
        if self.repository is None or not task_key:
            return None
        self.create_table()
        return self.repository.get_task_status(task_key)

    def list_tasks(self) -> list[dict]:
        if self.repository is None:
            return []
        self.create_table()
        return self.repository.list_tasks()

    def reset_all_task_completion_flags(self) -> int:
        if self.repository is None:
            return 0
        self.create_table()
        count = self.repository.reset_all_task_completion_flags()
        self.logger.info(f"Reset is_completed=false for {count} comm task(s)")
        return count

    def _notify_router(self, task_key: str, payload) -> None:
        if not self._router_base_url or payload is None:
            return
        url = f"{self._router_base_url}/comm/tasks/{task_key}"
        try:
            requests.post(url, json=payload, timeout=self._router_timeout)
        except Exception as exc:
            # Router being unreachable must never break the pipeline; state is
            # already persisted locally and router can poll on reconnect.
            self.logger.warning(f"Router notify failed for '{task_key}' at {url}: {exc}")

    def wait_for_task(
        self,
        task_key: str,
        *,
        success_statuses: set[str] | None = None,
        fail_statuses: set[str] | None = None,
        running_statuses: set[str] | None = None,
        poll_seconds: float = 5.0,
        timeout_seconds: float | None = None,
        require_is_completed: bool = False,
    ) -> bool:
        """
        Wait for an external task (e.g. router) to finish. Returns True on success terminal state.
        Returns False on failure terminal state or timeout.
        """
        if self.repository is None or not task_key:
            return False

        success_statuses = {s.lower() for s in (success_statuses or {"success", "completed", "done"})}
        fail_statuses = {s.lower() for s in (fail_statuses or {"failed", "error"})}
        running_statuses = {s.lower() for s in (running_statuses or {"running", "queued", "pending"})}

        started = time.monotonic()
        self.logger.info(f"Waiting for comm task '{task_key}'")
        while True:
            task = self.get_task_status(task_key)
            if task:
                is_completed = bool(task.get("is_completed"))
                if is_completed:
                    self.logger.info(f"Comm task '{task_key}' marked completed (is_completed=true)")
                    return True
                current_status = str(task.get("current_status") or "").lower()
                last_run_status = str(task.get("last_run_status") or "").lower()
                if require_is_completed:
                    if current_status in fail_statuses or last_run_status in fail_statuses:
                        self.logger.warning(f"Comm task '{task_key}' reached failure state")
                        return False
                    self.logger.info(
                        f"Comm task '{task_key}' waiting for is_completed=true "
                        f"(current={current_status}, last={last_run_status}, is_completed={is_completed})"
                    )
                else:
                    if current_status in success_statuses or last_run_status in success_statuses:
                        self.logger.info(f"Comm task '{task_key}' reached success state")
                        return True
                    if current_status in fail_statuses or last_run_status in fail_statuses:
                        self.logger.warning(f"Comm task '{task_key}' reached failure state")
                        return False
                    if current_status in running_statuses:
                        self.logger.info(
                            f"Comm task '{task_key}' still running "
                            f"(current={current_status}, last={last_run_status})"
                        )
            else:
                self.logger.info(f"Comm task '{task_key}' not found yet")

            if timeout_seconds is not None and (time.monotonic() - started) >= timeout_seconds:
                self.logger.warning(f"Timeout while waiting for comm task '{task_key}'")
                return False

            time.sleep(poll_seconds)
