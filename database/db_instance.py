from dataclasses import dataclass

from dacite import from_dict
from sqlalchemy import String, Column, Integer, Index


from database.base import Base
from database.db_configuration import DbConfiguration, DBConfigDTO

import threading

from database.db_repository import DBRepository
from utils.logger_manager import PipelineLogger

_thread_owner: threading.local = threading.local()


class _ThreadAwarePipelineLogger(PipelineLogger):
    """PipelineLogger variant that reads the mapper name from thread-local storage.

    When DbInstance is shared across parallel mapper threads, each thread stamps
    its own name via set_owner(). This logger picks up the per-thread name so DB
    log lines correctly identify which mapper triggered the query.
    """

    def process(self, msg, kwargs):
        kwargs.setdefault("extra", {})
        kwargs["extra"]["mapper"] = getattr(_thread_owner, "name", self.extra["mapper"])
        return msg, kwargs

# import all the tables from the mappers and already created static tables
import custom_graph_base_tables
from main_core.core_config import CoreConfig


class DbInstance(DBRepository):

    def __init__(self, db_conf, base, graph):
        self.db_conf = from_dict(DBConfigDTO, db_conf)
        self.logger = _ThreadAwarePipelineLogger(self.__class__.__name__)
        if not self.db_conf.enable:
            self.logger.warning("database enable set to False . Continue...")
            return
        super().__init__(db_conf, base, graph)
        self.logger.info("✅ Database instance initialized.")

    def set_owner(self, mapper_name: str) -> None:
        """Stamp the mapper name in thread-local storage so concurrent mappers
        each see their own name in DB-layer log lines."""
        _thread_owner.name = f"DbInstance.{mapper_name}"

    def get_session(self):
        """Get a new, thread-safe SQLAlchemy session."""
        session = self.get_new_session()
        self.logger.debug("🧩 New session created for task.")
        return session


if __name__ == "__main__":
    conf = CoreConfig()
    db_conf = conf.get_value("database")
    base = conf.get_value("base")
    graph = conf.get_value("graph")
    db = DbInstance(db_conf, base, graph)
    print(db.get_all_db_tables())

    print(db.get_all_metdata_tables())

#  compare

