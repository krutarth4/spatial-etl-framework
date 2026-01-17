import time

from core.init_graph import InitGraph
from core.init_scheduler import InitScheduler
from core.init_server import InitServer
from database.db_instancce import DbInstance
from log_manager.logger_manager import LoggerManager
from main_core.core_config import CoreConfig
from main_core.data_source_mapper import DataSourceMapper


class Application:
    logger = None
    _server = "server"
    _scheduler = "scheduler"
    _database = "database"
    _datasources = "datasources"
    _graph = "graph"
    _base = "base"

    def __init__(self):
        self.graph: InitGraph | None = None
        self.graph_conf = None
        self.sources_conf = None
        self.server_core = None
        self.scheduler_core = None
        self.db_instance: DbInstance | None = None
        self.db_url = None
        self.logger = LoggerManager(type(self).__name__).get_logger()
        self.core_conf = CoreConfig()

    def initialize_fast_api_uvicorn_server(self, server_conf):
        self.logger.info("Initializing debug FastAPI server ....")
        self.server_core = InitServer(server_conf)

    def initialize_scheduler(self, scheduler_conf, url: str):
        self.logger.info("Initializing scheduler ....")
        self.scheduler_core = InitScheduler(scheduler_conf, url)

    def initialize_database(self, database_conf):
        self.logger.info("Initializing database ......")
        self.db_instance = DbInstance(database_conf, self.core_conf.get_value(self._base)
                                      , self.core_conf.get_value(self._graph))

    def start_application(self):
        self.logger.info("Starting Application based on the configuration")
        # TODO: make the none check for core conf top level

        # setup Db Intance connection
        if self.core_conf is None or self.core_conf.get_value(self._database) is None:
            self.logger.error("Database configuation not set properly")
        else:
            self.initialize_database(self.core_conf.get_value(self._database))

        db_url = self.db_instance.get_db_url() if self.db_instance is not None else None

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
        self.graph = InitGraph(self.graph_conf, self.db_instance, self.scheduler_core)

        if not server["enable"] and scheduler["enable"]:
            self.logger.warning("Fallback mechanism activated for keeping thread alive.")
            # self.end_execution()

    def end_execution(self):
        if self.scheduler_core is not None:
            self.scheduler_core.run_forever()

    def get_all_datasources(self):
        return self.sources_conf or None

    @staticmethod
    def alarm(time):
        # Application.logger.info("alarm triggered for " + str(time))
        print(f"Alarm! This alarm was scheduled at {time}. Scheduler still awake and running")


if __name__ == "__main__":
    app = Application()
    app.start_application()

    sources = app.get_all_datasources()

    # check if the base graph is ready or not
    if app.graph is not None:
        app.graph.initialize_base_graph()
        app.graph.load_graph()
    #             Wait till the new ways_base_graph has been created
        while not app.graph.get_is_base_graph_ready():
            app.logger.warning("Base graph is not ready  ")
            time.sleep(10)

    # breakpoint for base graph as it will be ready


    if sources is not None:
        # TODO:  app.graph is not None and app.graph.get_is_base_graph_ready()
        DataSourceMapper(sources, app.db_instance, app.scheduler_core)
    else:
        print("No data sources available or the base graph is not ready and have problems ")

    app.end_execution()
