from abc import ABC, abstractmethod
from pathlib import Path

from data_config_dtos.data_source_config_dto import SourceDTO


class DataSourceABC(ABC):
    # -------------SOURCE METHODS ------------------------

    @abstractmethod
    def source(self, source: SourceDTO):
        pass

    def source_filter(self, data, filter_function=None):
        return data

    @abstractmethod
    def fetch(self):
        pass

    @abstractmethod
    def read_file_content(self, path):
        pass

    def map_to_links(self):
        pass

    def mapping_db_query(self):
        return None

    def map_to_base(self):
        pass

    def execute_on_staging(self):
        pass

    def staging_db_query(self) -> str | None:
        return None

    def execute_on_enrichment(self):
        pass

    def enrichment_db_query(self) -> str | None:
        return None

    def check_before_update(self):
        return True

    @abstractmethod
    def load(self, data):
        pass

    @abstractmethod
    def transform(self, path: Path | str):
        pass

    @abstractmethod
    def extract(self):
        """
        Extract method extracts the data eiother through the http call or local call and find the paths
        , this is the top class containing source , fetch , and check_metadata functions
        """
        pass

    def pre_filter_processing(self, data):
        pass

    def post_filter_processing(self, data):
        pass

    """
    Execute any function which needs to be implemented before inserting data into the db 
    """

    def pre_database_processing(self):
        """
        Execute any function which needs to be implemented before inserting data into the db
        """
        pass

    def post_database_processing(self):
        """
        Execute any functionality here after the db upload of data is done
        """
        pass

    # -------------------------------------

    @abstractmethod
    def create_job(self):
        """
        Create job for the scheduler
        """
        pass

    def execute(self):
        """
        Trigger datasource execution after initialization.
        """
        pass

    def run(self):
        """
        The main function which executes for all the datasources and contains the main logis of execution steps
        """
        pass

    # Optional lifecycle hooks for extensibility in DataSourceABCImpl subclasses
    def before_process_file(self, path: Path | str):
        pass

    def after_process_file(self, path: Path | str, transformed_data):
        pass

    def on_process_file_error(self, path: Path | str, error: Exception):
        pass

    def run_end_cleanup(self, succeeded: bool, error: Exception | None = None):
        pass
