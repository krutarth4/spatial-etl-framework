import hashlib
import os
import sys
import time
import threading
from pathlib import Path

from core.globalconstants import GlobalConstants
from core.init_graph import InitGraph
from core.init_scheduler import InitScheduler
from core.init_server import InitServer
from communication.comm_service import CommService
from database.db_instancce import DbInstance
from log_manager.logger_manager import LoggerManager
from main_core.core_config import CoreConfig
from main_core.data_source_mapper import DataSourceMapper
from metadata.data_source_metadata_service import DataSourceMetadataService


def _file_signature(path: Path) -> tuple[int, str] | None:
    try:
        stat = path.stat()
        content_hash = hashlib.sha256(path.read_bytes()).hexdigest()
        return stat.st_mtime_ns, content_hash
    except FileNotFoundError:
        return None


class Application:
    logger = None
    _server = "server"
    _scheduler = "scheduler"
    _database = "database"
    _env_variables = "env_variables"
    _metadata = "metadata-datasource"
    _datasources = "datasources"
    _graph = "graph"
    _base_graph = "base"

    def __init__(self):
        self._pipeline_lock = threading.Lock()
        self._pipeline_executed = False
        self._config_watch_signature = None
        self.base_graph_conf = None
        self.metadata_service: DataSourceMetadataService | None = None
        self.comm_service: CommService | None = None
        self.graph: InitGraph | None = None
        self.graph_conf = None
        self.sources_conf = None
        self.server_core = None
        self.scheduler_core = None
        self.db_instance: DbInstance | None = None
        self.db_url = None
        self.logger = LoggerManager(type(self).__name__).get_logger()
        self.core_conf = CoreConfig()
        try:
            self._config_watch_signature = _file_signature(Path(self.core_conf.filepath))
        except Exception:
            self._config_watch_signature = None

    def initialize_fast_api_uvicorn_server(self, server_conf):
        self.logger.info("Initializing debug FastAPI server ....")
        self.server_core = InitServer(server_conf)

    def initialize_scheduler(self, scheduler_conf, url: str):
        self.logger.info("Initializing scheduler ....")
        self.scheduler_core = InitScheduler(scheduler_conf, url)

    def initialize_database(self, database_conf):
        self.logger.info("Initializing database ......")
        self.db_instance = DbInstance(database_conf, self.core_conf.get_value(self._base_graph)
                                      , self.core_conf.get_value(self._graph))

    def start_application(self):
        self.logger.info("Starting Application based on the configuration")
        # TODO: make the none check for core conf top level

        # initialize the environment constants to be used by the mapper
        env_variables = self.core_conf.get_value(self._env_variables)
        metadata = self.core_conf.get_value(self._metadata)
        if env_variables is not None:
            GlobalConstants.load(env_variables)

        # setup Db Intance connection
        if self.core_conf is None or self.core_conf.get_value(self._database) is None:
            self.logger.error("Database configuration not set properly")
        else:
            self.initialize_database(self.core_conf.get_value(self._database))

        db_url = self.db_instance.get_db_url() if self.db_instance is not None else None
        # create metadata table if not exist

        self.metadata_service = DataSourceMetadataService(self.db_instance,metadata)
        comm_schema = None
        if isinstance(metadata, dict):
            comm_schema = metadata.get("table_schema")
        self.comm_service = CommService(self.db_instance, comm_schema)

        if self.metadata_service is not None:
            self.metadata_service.create_table()
        if self.comm_service is not None:
            self.comm_service.create_table()
            # Reset completion flags on every app startup so comm-based checks re-run.
            self.comm_service.reset_all_task_completion_flags()

        # start scheduler and server
        server = self.core_conf.get_value(self._server)
        scheduler = self.core_conf.get_value(self._scheduler)
        if self.core_conf is None or self.core_conf.get_value(self._server) is None or self.core_conf.get_value(
                self._server) is None:
            self.logger.error("configuration not set properly. Either the server or scheduler configuration error")
        else:
            self.initialize_scheduler(scheduler, db_url)

            self.initialize_fast_api_uvicorn_server(server)

        # start with the mapper class
        self.sources_conf = self.core_conf.get_value(self._datasources)

        # core graph logic for the base table
        self.graph_conf = self.core_conf.get_value(self._graph)
        self.base_graph_conf = self.core_conf.get_value(self._base_graph)
        self.graph = InitGraph(self.graph_conf, self.base_graph_conf, self.metadata_service, self.db_instance,
                               self.comm_service, self.scheduler_core)

        if not server["enable"] and scheduler["enable"]:
            self.logger.warning("Fallback mechanism activated for keeping thread alive.")
            # self.end_execution()

    def run_pipeline(self):
        with self._pipeline_lock:
            if self._pipeline_executed:
                self.logger.warning("Pipeline logic already executed. Skipping duplicate run.")
                return
            self._pipeline_executed = True

        sources = self.get_all_datasources()

        # check if the base graph is ready or not
        if self.graph is not None:
            self.graph.update_graph_source()
            self.graph.ingest_graph_data()
            # Wait till the new ways_base_graph has been created
            while not self.graph.is_base_graph_ready():
                self.logger.warning("Base graph is not ready")
                time.sleep(10)

        if sources is not None and self.graph is not None and self.graph.is_base_graph_ready():
            mappers = DataSourceMapper(sources, self.db_instance, self.scheduler_core, self.base_graph_conf,
                                       self.metadata_service)
            mappers.start_execution()
        else:
            self.logger.warning("No data sources available or the base graph is not ready and have problems")

    def run_standalone(self, keep_alive_when_idle: bool = True):
        self.start_application()
        self.run_pipeline()
        self.end_execution()
        if keep_alive_when_idle and self.scheduler_core is None:
            self.keep_alive_forever()

    def end_execution(self):
        if self.scheduler_core is not None:
            self.scheduler_core.run_forever()

    def keep_alive_forever(self):
        runtime_conf = (self.core_conf.get_config() or {}).get("runtime", {}) or {}
        watch_conf = (runtime_conf.get("config_watch", {}) or {})
        watch_enabled = watch_conf.get("enable", True)
        poll_seconds = float(watch_conf.get("poll_seconds", 2))
        config_path = Path(self.core_conf.filepath)
        if watch_enabled and self._config_watch_signature is None:
            self._config_watch_signature = _file_signature(config_path)

        self.logger.info("Keep-alive active (no scheduler). Waiting for config changes or manual stop.")
        if watch_enabled:
            self.logger.info(
                f"Config watch enabled: path={config_path.resolve()} poll_seconds={poll_seconds} "
                f"signature={self._config_watch_signature}"
            )
        else:
            self.logger.info("Config watch disabled in keep-alive")
        try:
            while True:
                time.sleep(poll_seconds if watch_enabled else 10)
                if not watch_enabled:
                    continue
                current_sig = _file_signature(config_path)
                if current_sig != self._config_watch_signature:
                    self.logger.warning(f"Detected config change in {config_path}. Restarting process...")
                    os.execv(sys.executable, [sys.executable, *sys.argv])
        except (KeyboardInterrupt, SystemExit):
            self.logger.info("Keep-alive stopped")

    def get_all_datasources(self):
        return self.sources_conf or None

    @staticmethod
    def alarm(time):
        # Application.logger.info("alarm triggered for " + str(time))
        print(f"Alarm! This alarm was scheduled at {time}. Scheduler still awake and running")


if __name__ == "__main__":
    app = Application()
    app.run_standalone()
