import subprocess
from dataclasses import dataclass
from os.path import exists
from typing import List, Mapping, Any, Optional

from dacite import from_dict

from core.command_runner import CommandRunner
from core.custom_graph_loader import CustomGraphLoader
from core.init_scheduler import InitScheduler
from data_config_dtos.data_source_config_dto import GraphConfDTO
from database.db_instancce import DbInstance
from log_manager.logger_manager import LoggerManager
from main_core.data_source_mapper import DataSourceMapper
from main_core.safe_class import safe_class

# Do not remove this import

import custom_graph_base_tables

@safe_class
class InitGraph:

    BASE_TABLES = ["barrier_nodes", "links"]

    def __init__(self, graph_conf, db: DbInstance | None, scheduler_core: InitScheduler | None):
        self.graph_loader = None
        self.graph_configuration = from_dict(GraphConfDTO, graph_conf)
        self.logger = LoggerManager(type(self).__name__)
        self.is_new_graph_ready = False
        self.scheduler_core = scheduler_core
        self.db = db
        if not self.graph_configuration.enable:
            self.logger.warning("Base graph DISABLED")
            return
        if db is None:
            self.logger.warning("Base graph can not be checked with database as disabled")

    def create_base_table_clone(self):
        self.db.create_base_table_clone(self.graph_configuration.schema, self.graph_configuration.table_name)

    def check_if_base_graph_present(self) -> bool:

        return self.db.has_base_tables()

    def initialize_base_graph(self):
        if self.graph_configuration.enable:
            self.logger.info("Initializing Base Graph")
            DataSourceMapper(self.graph_configuration.datasource, self.db,
                             self.scheduler_core)
        else:
            self.logger.info("Skipping Initializing Graph as enable set to False......")
            self.logger.info("Checking for the base graph tables")

    def load_graph(self):
        if not self.graph_configuration.enable:
            self.create_base_table_if_not_exist()
            self.is_new_graph_ready = True
            return
        tool = self.graph_configuration.tool
        if tool == "terminal":
            self.logger.info("Loading Graph through command runner pgrouting")
            # download latest data for berlin

            cmd_runner = CommandRunner()
            result = cmd_runner.run(self.graph_configuration.cmd, self.graph_configuration.env, False)
            if result.returncode != 0:
                # if table in tables
                self.is_new_graph_ready = False
            else:
                self.is_new_graph_ready = True

        elif tool == "custom":
            self.logger.info("Loading Graph through custom osm handler class")
            self.graph_loader = CustomGraphLoader(self.graph_configuration)
            self.create_base_table_if_not_exist()
            graph_links = self.graph_loader.initialize()
            self.db.bulk_insert(self.graph_configuration.table_name,self.graph_configuration.schema,graph_links)
            self.is_new_graph_ready = True

        else:
            self.is_new_graph_ready = False
            self.logger.error("Graph tool {} not supported".format(tool))
            raise Exception("Graph tool {} not supported".format(tool))
    def create_custom_tables(self):
        if self.db is not None:
            self.db.create_table_if_not_exist(self.graph_configuration.table_name, self.graph_configuration.schema)
        else:
            self.logger.warning("custom tables can't be created as db connection is missing")
    def create_base_table_if_not_exist(self):
        base_present = self.check_if_base_graph_present()
        if not base_present:
            self.logger.info(f"Creating Base graph for the {self.graph_configuration.table_name} table")
            self.create_custom_tables()
            self.is_new_graph_ready = True
        else:
            self.logger.warning("checking FORCED new base graph table, if needs to be created ")
            # self.db.create_base_table_force()

    def get_is_base_graph_ready(self) -> bool:
        return self.is_new_graph_ready

    def reflect_base_tables(self):
        if self.db is not None:
            self.db.reflect_base_tables(self.graph_configuration.schema,self.graph_configuration.table_name)
