from abc import ABC, abstractmethod


class DataSourceABC(ABC):
    # -------------SOURCE METHODS ------------------------

    @abstractmethod
    def source(self):
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
    def source_database_handler(self):
        pass

    # ------------------


    @abstractmethod
    def map_to_links(self):
        pass

    @abstractmethod
    def map_to_link_db_query(self):
        pass

    @abstractmethod
    def check_before_update(self, old_data, new_data):
        pass

    @abstractmethod
    def enabled_for(self):
        pass



    # -------------------------------------

    @abstractmethod
    def create_job(self):
        pass

    # TODO: Not the best idea for the run as it can be different for each class
    def run(self):
        pass
