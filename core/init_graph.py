import subprocess
from dataclasses import dataclass
from typing import List, Mapping, Any

from dacite import from_dict

from core.command_runner import CommandRunner
from core.init_scheduler import InitScheduler
from data_config_dtos.data_source_config_dto import DataSourceDTO
from database.db_instancce import DbInstance
from log_manager.logger_manager import LoggerManager
from main_core.data_source_mapper import DataSourceMapper


@dataclass
class GraphConfDTO:
    tool: str
    schema: str
    table_name: str
    enable: bool
    check_before_update: bool
    cmd: List[str | Any]
    env: Mapping[str, str]
    datasource: List[DataSourceDTO]


class InitGraph:

    def __init__(self, graph_conf, db: DbInstance | None, scheduler_core: InitScheduler | None):
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

        # base_present = self.check_if_base_graph_present()
        # if not base_present:
        #     self.create_base_table()
        # else:
        #     self.logger.warning("checking FORCED new base graph table, if needs to be created ")
        #     self.db.create_base_table_force()

    def create_base_table(self):
        self.db.create_base_table(self.graph_configuration.schema, self.graph_configuration.table_name)

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

        elif tool == "class":
            self.logger.info("Loading Graph through command runner")
        else:
            self.logger.error("Graph tool {} not supported".format(tool))
            raise Exception("Graph tool {} not supported".format(tool))

    def get_is_base_graph_ready(self) -> bool:
        return self.is_new_graph_ready
