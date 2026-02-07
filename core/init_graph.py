import subprocess
from dataclasses import dataclass
from os.path import exists
from typing import List, Mapping, Any, Optional

from dacite import from_dict

from core.base_graph import BaseGraph
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

    def __init__(self, graph_conf, base_graph_conf, metadata_service, db: DbInstance | None,
                 scheduler_core: InitScheduler | None):
        self.graph_loader = None
        self.graph_configuration = from_dict(GraphConfDTO, graph_conf)
        self.logger = LoggerManager(type(self).__name__)
        self.is_raw_graph_ready = False
        self.metadata_service = metadata_service
        self.scheduler_core = scheduler_core
        self.db = db
        self.base_graph_conf = base_graph_conf
        self.base_graph = BaseGraph(db, base_graph_conf)
        self.base_graph.create_base_graph_tables()
        if not self.graph_configuration.enable:
            self.logger.warning("Base graph DISABLED")
            return
        if db is None:
            self.logger.warning("Base graph can not be checked with database as disabled")

    def check_if_raw_graph_present(self) -> bool:
        return self.db.table_exists(self.graph_configuration.table_name, self.graph_configuration.schema)

    def update_graph_source(self):
        if self.graph_configuration.enable:
            self.logger.info("Initializing Base Graph")
            graph_mapper = DataSourceMapper(self.graph_configuration.datasource, self.db,
                                            self.scheduler_core, self.base_graph_conf, self.metadata_service)
            graph_mapper.start_execution()
        else:
            self.logger.info("Skipping Initializing Graph as enable set to False......")

    def ingest_graph_data(self):
        if not self.graph_configuration.enable:
            # self.create_ingested_graph_table_if_not_exist()
            # self.is_new_graph_ready = True
            return
        tool = self.graph_configuration.tool
        if tool == "terminal":
            self.logger.info("Loading Graph through command runner pgrouting")
            # download latest data for berlin

            cmd_runner = CommandRunner()
            result = cmd_runner.run(self.graph_configuration.cmd, self.graph_configuration.env, False)
            if result.returncode != 0:
                # if table in tables
                self.is_raw_graph_ready = False
            else:
                self.is_raw_graph_ready = True

        elif tool == "custom":
            self.logger.info("Loading Graph through custom osm handler class")
            self.execute_custom_strategy()

        elif tool == "external_ingest":
            self.logger.info(f"Loading Graph through {tool} which means the base graph "
                             "data will be parsed at router level")
            self.execute_external_ingest()


        else:
            self.is_raw_graph_ready = False
            self.logger.error("Graph tool {} not supported".format(tool))
            raise Exception("Graph tool {} not supported".format(tool))
    def execute_external_ingest(self):


        #     check if the table is present
        table_conf = self.graph_configuration

        if self.check_if_raw_graph_present():
            self.is_raw_graph_ready = True
            self.logger.info(f"Table {table_conf.table_name} exists")
            # TODO: check if the base graph is ready as it will created new

            base_data_count = self.base_graph.get_base_graph_row_counts()
            if base_data_count == 0 and base_data_count != self.db.get_table_count(self.graph_configuration.table_name,
                                                                                   self.graph_configuration.schema):
                self.logger.info(f"populating data into base_graph")
                self.base_graph.populate_base_graph_table(self.graph_configuration.table_name,
                                                          self.graph_configuration.schema)
            else:
                if not self.is_base_graph_ready():
                    self.base_graph.drop_base_graph_table()
                    self.base_graph.create_base_graph_tables()
                    self.base_graph.populate_base_graph_table(self.graph_configuration.table_name,
                                                              self.graph_configuration.schema)

            return




        else:
            self.logger.warning(f"Table {table_conf.table_name} does not exist")
            self.logger.warning(f"Make sure the schema and tablename are correct in config file"
                                f". If the issue still presist check if external ingestion of table in database was successful")


    def execute_custom_strategy(self):
        # TODO:check with metadata table if the files imported is same
        raw_graph_present = self.check_if_raw_graph_present()
        if raw_graph_present:
            self.logger.warning("Raw graph already present")
            self.is_raw_graph_ready = True
            base_data_count = self.base_graph.get_base_graph_row_counts()
            if base_data_count == 0 and base_data_count != self.db.get_table_count(self.graph_configuration.table_name,
                                                                                   self.graph_configuration.schema):
                self.logger.info(f"populating data into base_graph")
                self.base_graph.populate_base_graph_table(self.graph_configuration.table_name,
                                                          self.graph_configuration.schema)
            return
        self.graph_loader = CustomGraphLoader(self.graph_configuration)
        self.create_ingested_graph_table_if_not_exist()
        graph_links = self.graph_loader.initialize()
        self.db.bulk_insert(self.graph_configuration.table_name, self.graph_configuration.schema, graph_links)
        if not self.is_base_graph_ready():
            self.base_graph.drop_base_graph_table()
            self.base_graph.populate_base_graph_table(self.graph_configuration.table_name,
                                                      self.graph_configuration.schema)
        self.is_raw_graph_ready = True

    def create_custom_tables(self):
        if self.db is not None:
            self.db.create_table_if_not_exist(self.graph_configuration.table_name, self.graph_configuration.schema)
        else:
            self.logger.warning("custom tables can't be created as db connection is missing")

    def create_ingested_graph_table_if_not_exist(self):
        base_present = self.check_if_raw_graph_present()
        if not base_present:
            self.logger.info(f"Creating graph for the {self.graph_configuration.table_name} table")
            self.create_custom_tables()
            self.is_raw_graph_ready = True
        else:
            self.logger.warning("checking FORCED new base graph table, if needs to be created ")
            # self.db.create_base_table_force()

    def reflect_ingested_graph_tables(self):
        if self.db is not None:
            self.db.reflect_base_tables(self.graph_configuration.schema, self.graph_configuration.table_name)

    def is_base_graph_ready(self):
        base_data_count = self.base_graph.get_base_graph_row_counts()
        return base_data_count == self.db.get_table_count(self.graph_configuration.table_name,
                                                          self.graph_configuration.schema)
