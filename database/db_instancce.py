from dataclasses import dataclass

from dacite import from_dict
from sqlalchemy import String, Column

from NotUsed import db_conf
from database.db_configuration import DbConfiguration, DBConfigDTO

import threading

from database.db_repository import DBRepository
from log_manager.logger_manager import LoggerManager

# import all the tables from the mappers and already created static tables
import data_tables
from main_core.core_config import CoreConfig


# from data_mappers import * # TODO maybe don't need it anymore as fixed through base import from db


class DbInstance(DBRepository):
    _instance_lock = threading.Lock()
    _instance = None

    def __new__(cls, db_conf, base, graph):
        conf = DBConfigDTO(**db_conf)
        if not conf.enable:
            LoggerManager("DbInstance").warning("DbInstance disabled — not creating scheduler instance")
            return None
        if not cls._instance:
            with cls._instance_lock:
                if not cls._instance:
                    cls._instance = super(DbInstance, cls).__new__(cls)
        return cls._instance

    def __init__(self, db_conf, base, graph):
        super().__init__(db_conf, base, graph)
        self.db_conf = from_dict(DBConfigDTO, db_conf)
        if not self.db_conf.enable:
            self.logger.warning("database enable set to False . Continue...")
            return
        self.logger = LoggerManager(self.__class__.__name__)
        self.logger.info("✅ Database instance initialized.")

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

    #
    # db.add_column("ways_base", "test_column",
    #               "String", "test_runner")

    db.drop_indexes_for_table()