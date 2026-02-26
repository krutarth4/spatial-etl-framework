from database.base import Base
from database.db_instancce import DbInstance
from log_manager.logger_manager import LoggerManager
from sqlalchemy import Boolean, Column, DateTime, Integer, String, Text, func, select, update


class CommTask(Base):
    __tablename__ = "comm"

    id = Column(Integer, primary_key=True)
    task_key = Column(String, nullable=False, unique=True)
    owner = Column(String, nullable=True)  # e.g. router / pipeline
    current_status = Column(String, default="idle")
    is_completed = Column(Boolean, default=False)
    last_run_status = Column(String)
    last_run_message = Column(Text)
    last_checked_at = Column(DateTime)
    last_successful_run_at = Column(DateTime)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, onupdate=func.now())


class CommRepository:
    table_name = CommTask.__tablename__

    def __init__(self, db: DbInstance, schema: str):
        if db is None:
            return
        self.db = db
        self.schema = schema
        self.logger = LoggerManager(type(self).__name__)
        if self.schema:
            CommTask.__table__.schema = self.schema

    def is_present(self) -> bool:
        return self.db.table_exists(self.table_name, self.schema)

    def create_table(self) -> None:
        try:
            self.db.create_schema_if_not_exists()
        except Exception:
            pass
        self.db.create_table_if_not_exist(self.table_name, self.schema)

    def get_task(self, task_key: str) -> CommTask | None:
        with self.db.session_scope() as session:
            stmt = select(CommTask).where(CommTask.task_key == task_key)
            return session.execute(stmt).scalar_one_or_none()

    def get_task_status(self, task_key: str) -> dict | None:
        with self.db.session_scope() as session:
            stmt = (
                select(
                    CommTask.task_key,
                    CommTask.owner,
                    CommTask.current_status,
                    CommTask.is_completed,
                    CommTask.last_run_status,
                    CommTask.last_run_message,
                    CommTask.last_checked_at,
                    CommTask.last_successful_run_at,
                )
                .where(CommTask.task_key == task_key)
            )
            row = session.execute(stmt).mappings().one_or_none()
            return dict(row) if row else None

    def upsert_task(self, task_key: str, defaults: dict) -> CommTask:
        with self.db.session_scope() as session:
            task = session.execute(
                select(CommTask).where(CommTask.task_key == task_key)
            ).scalar_one_or_none()
            if task is None:
                task = CommTask(task_key=task_key, **defaults)
                session.add(task)
            else:
                for field, value in (defaults or {}).items():
                    if hasattr(task, field):
                        setattr(task, field, value)
            session.flush()
            return task

    def update_task(self, task_key: str, **updates) -> CommTask | None:
        with self.db.session_scope() as session:
            task = session.execute(
                select(CommTask).where(CommTask.task_key == task_key)
            ).scalar_one_or_none()
            if task is None:
                return None
            for field, value in (updates or {}).items():
                if hasattr(task, field):
                    setattr(task, field, value)
            session.flush()
            return task

    def reset_all_task_completion_flags(self) -> int:
        with self.db.session_scope() as session:
            result = session.execute(update(CommTask).values(is_completed=False))
            session.flush()
            return int(result.rowcount or 0)
