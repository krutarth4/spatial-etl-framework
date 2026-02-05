from abc import ABC, abstractmethod
from pathlib import Path

from data_config_dtos.data_source_config_dto import SourceDTO


class DataSourceABC(ABC):
    # -------------SOURCE METHODS ------------------------

    @abstractmethod
    def source(self, source: SourceDTO):
        pass

    @staticmethod
    @abstractmethod
    def source_filter(data, filter_function):
        pass

    @abstractmethod
    def fetch(self):
        pass

    @abstractmethod
    def read_file_content(self, path):
        pass

    @abstractmethod
    def map_to_links(self):
        pass

    @abstractmethod
    def mapping_db_query(self):
        pass

    @abstractmethod
    def map_to_base(self):
        pass

    @abstractmethod
    def execute_on_staging(self):
        pass

    @abstractmethod
    def staging_db_query(self) -> str | None:
        pass

    @abstractmethod
    def execute_on_enrichment(self):
        pass

    @abstractmethod
    def enrichment_db_query(self) -> str | None:
        pass

    @abstractmethod
    def check_before_update(self):
        pass

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

    @abstractmethod
    def pre_filter_processing(self, data):
        pass

    @abstractmethod
    def post_filter_processing(self, data):
        pass

    """
    Execute any function which needs to be implemented before inserting data into the db 
    """

    @abstractmethod
    def pre_database_processing(self):
        """
        Execute any function which needs to be implemented before inserting data into the db
        """
        pass

    @abstractmethod
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

    # TODO: Not the best idea for the run as it can be different for each class
    def run(self):
        """
        The main function which executes for all the datasources and contains the main logis of execution steps
        """
        pass
