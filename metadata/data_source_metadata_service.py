from dacite import from_dict

from data_config_dtos.data_source_config_dto import MetadataConfDTO
from database.db_instancce import DbInstance
from log_manager.logger_manager import LoggerManager
from metadata.data_source_metadata_repository import DataSourceMetadataRepository


class DataSourceMetadataService:
    def __init__(self, db: DbInstance, metadata_conf):
        if db is None:
            return
        self.metadata_conf = from_dict(MetadataConfDTO, metadata_conf)
        self.metadata_repository = DataSourceMetadataRepository(db, self.metadata_conf.table_schema)
        self.logger = LoggerManager(type(self).__name__)

    def create_table(self):
        if self.metadata_exist():
            self.logger.info("Metadata table already exists")
            return
        self.metadata_repository.create_metadata_table()

    def metadata_exist(self) -> bool:
        return self.metadata_repository.is_metadata_table_present()

    def update(self,key:str, value:dict):
        pass

