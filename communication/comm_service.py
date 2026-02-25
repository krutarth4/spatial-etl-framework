import time
from datetime import datetime

from database.db_instancce import DbInstance
from log_manager.logger_manager import LoggerManager

from communication.comm_repository import CommRepository


class CommService:
    def __init__(self, db: DbInstance, table_schema: str | None):
        self.logger = LoggerManager(type(self).__name__)
        self.repository = None
        if db is None:
            return
        self.repository = CommRepository(db, table_schema)

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
    ):
        if self.repository is None or not task_key:
            return None
        self.create_table()
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
            return self.repository.upsert_task(task_key, updates)
        return updated

    def get_task_status(self, task_key: str) -> dict | None:
        if self.repository is None or not task_key:
            return None
        self.create_table()
        return self.repository.get_task_status(task_key)

    def wait_for_task(
        self,
        task_key: str,
        *,
        success_statuses: set[str] | None = None,
        fail_statuses: set[str] | None = None,
        running_statuses: set[str] | None = None,
        poll_seconds: float = 5.0,
        timeout_seconds: float | None = None,
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
                current_status = str(task.get("current_status") or "").lower()
                last_run_status = str(task.get("last_run_status") or "").lower()
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
