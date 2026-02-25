import subprocess
import time
from dataclasses import dataclass
from os.path import exists
from typing import List, Mapping, Any, Optional

from dacite import from_dict

from core.base_graph import BaseGraph
from core.command_runner import CommandRunner
from core.custom_graph_loader import CustomGraphLoader
from core.init_scheduler import InitScheduler
from communication.comm_service import CommService
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
                 comm_service: CommService | None,
                 scheduler_core: InitScheduler | None):
        self.graph_loader = None
        self.graph_configuration = from_dict(GraphConfDTO, graph_conf)
        self.logger = LoggerManager(type(self).__name__)
        self.is_raw_graph_ready = False
        self.metadata_service = metadata_service
        self.comm_service = comm_service
        self.scheduler_core = scheduler_core
        self.db = db
        self.base_graph_conf = base_graph_conf
        self.base_graph = BaseGraph(db, base_graph_conf)
        self.base_graph.create_base_graph_tables()
        self._ensure_default_comm_tasks()
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
        self._wait_for_coupled_router_if_enabled()
        self._wait_for_main_ways_table_before_base_checks()


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

    def _wait_for_main_ways_table_before_base_checks(self):
        if self.comm_service is None:
            return

        task_key = "main_ways_table"
        try:
            self.comm_service.ensure_task("ways_base_table", owner="mdp", current_status="idle", is_completed=False)
            self.comm_service.update_status(
                "ways_base_table",
                owner="mdp",
                current_status="waiting",
                last_run_status="waiting",
                last_run_message=f"Waiting for '{task_key}' before ways_base_table checks",
                is_completed=False,
            )
        except Exception as e:
            self.logger.warning(f"Unable to update ways_base_table wait state in comm: {e}")

        ok = self.comm_service.wait_for_task(
            task_key,
            success_statuses={"success", "completed", "done"},
            fail_statuses={"failed", "error"},
            running_statuses={"running", "queued", "pending", "waiting", "idle"},
            poll_seconds=5.0,
            timeout_seconds=None,
        )

        try:
            self.comm_service.update_status(
                "ways_base_table",
                owner="mdp",
                current_status="idle" if ok else "failed",
                last_run_status="waiting_done" if ok else "failed",
                last_run_message=(
                    f"'{task_key}' finished. Continuing ways_base_table checks"
                    if ok else f"'{task_key}' failed while waiting"
                ),
                is_completed=False,
            )
        except Exception as e:
            self.logger.warning(f"Unable to update ways_base_table post-wait state in comm: {e}")

        if not ok:
            raise RuntimeError(f"Required comm task '{task_key}' did not finish successfully")

    def _wait_for_coupled_router_if_enabled(self):
        coupled = getattr(self.graph_configuration, "coupled", None)
        if coupled is None or str(coupled).lower() not in {"router", "true", "enabled"}:
            return
        if self.comm_service is None:
            self.logger.warning("graph.coupled is enabled but CommService is not initialized")
            return

        task_key = self._resolve_coupled_task_key()
        poll_seconds = float(getattr(self.graph_configuration, "coupled_poll_seconds", 5.0) or 5.0)
        timeout_seconds = getattr(self.graph_configuration, "coupled_timeout_seconds", None)
        timeout_seconds = float(timeout_seconds) if timeout_seconds is not None else None

        # Register/mark that pipeline is waiting for router task visibility.
        try:
            self.comm_service.update_status(
                f"pipeline_wait_{task_key}",
                owner="pipeline",
                current_status="waiting",
                last_run_status="waiting",
                last_run_message=f"Waiting for router task '{task_key}'",
            )
        except Exception as e:
            self.logger.warning(f"Unable to update comm wait status for coupled router task: {e}")

        ok = self.comm_service.wait_for_task(
            task_key,
            success_statuses={"success", "completed", "done"},
            fail_statuses={"failed", "error"},
            running_statuses={"running", "queued", "pending", "waiting"},
            poll_seconds=poll_seconds,
            timeout_seconds=timeout_seconds,
        )

        try:
            self.comm_service.update_status(
                f"pipeline_wait_{task_key}",
                owner="pipeline",
                current_status="idle" if ok else "failed",
                last_run_status="success" if ok else "failed",
                last_run_message=(
                    f"Router task '{task_key}' completed"
                    if ok else f"Router task '{task_key}' failed or timed out"
                ),
                success=ok,
            )
        except Exception as e:
            self.logger.warning(f"Unable to update pipeline wait comm status: {e}")

        if not ok:
            raise RuntimeError(f"Coupled router task '{task_key}' did not finish successfully")

    def _resolve_coupled_task_key(self) -> str:
        explicit = getattr(self.graph_configuration, "coupled_task_key", None)
        if explicit:
            return explicit
        datasources = getattr(self.graph_configuration, "datasource", None) or []
        if datasources:
            first = datasources[0]
            name = getattr(first, "name", None) if not isinstance(first, dict) else first.get("name")
            if name:
                return str(name)
        return "osm_graph"

    def _ensure_default_comm_tasks(self):
        if self.comm_service is None:
            return
        task_defaults = [
            ("read_osm_file", "router"),
            ("main_ways_table", "router"),
            ("ways_base_table", "mdp"),
            ("osm_file_update", "mdp"),
            ("osm_file_download", "mdp"),
        ]
        try:
            for task_key, owner in task_defaults:
                self.comm_service.ensure_task(task_key, owner=owner, current_status="idle", is_completed=False)
        except Exception as e:
            self.logger.warning(f"Failed to ensure default comm tasks: {e}")


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
