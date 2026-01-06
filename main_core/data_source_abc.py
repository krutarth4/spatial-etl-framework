from abc import ABC, abstractmethod

from main_core.data_source_mapper import SourceDTO


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
    def map_to_link_db_query(self):
        pass

    @abstractmethod
    def map_to_base(self):
        pass

    @abstractmethod
    def check_before_update(self, old_data, new_data):
        pass

    @abstractmethod
    def load(self):
        pass

    @abstractmethod
    def transform(self):
        pass

    @abstractmethod
    def extract(self):
        pass

    # -------------------------------------

    @abstractmethod
    def create_job(self):
        pass

    # TODO: Not the best idea for the run as it can be different for each class
    def run(self):
        pass
