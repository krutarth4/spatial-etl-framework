from dataclasses import dataclass

from dacite import from_dict
from sqlalchemy import String, Column, Integer, Index

from NotUsed import db_conf

from database.base import Base
from database.db_configuration import DbConfiguration, DBConfigDTO

import threading

from database.db_repository import DBRepository
from log_manager.logger_manager import LoggerManager

# import all the tables from the mappers and already created static tables
import custom_graph_base_tables
from main_core.core_config import CoreConfig


# from data_mappers import * # TODO maybe don't need it anymore as fixed through base import from db


class DbInstance(DBRepository):

    def __init__(self, db_conf, base, graph):
        self.db_conf = from_dict(DBConfigDTO, db_conf)
        self.logger = LoggerManager(self.__class__.__name__).get_logger()
        if not self.db_conf.enable:
            self.logger.warning("database enable set to False . Continue...")
            return
        super().__init__(db_conf, base, graph)
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
    print(db.get_all_db_tables())

    print(db.get_all_metdata_tables())

#  compare

