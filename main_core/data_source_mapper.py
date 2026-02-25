import importlib

from data_config_dtos.data_source_config_dto import DataSourceDTO
from dacite import from_dict, Config

from log_manager.logger_manager import LoggerManager
from main_core.safe_class import safe_class
from metadata.data_source_metadata_service import DataSourceMetadataService


@safe_class
class DataSourceMapper:
    _prefix_path = "data_mappers"

    def __init__(self, sources, db_instance, scheduler_core, base_graph_conf, metadata_service: DataSourceMetadataService | None):
        self.logger = LoggerManager(type(self).__name__).get_logger()
        self.db_instance = db_instance
        self.scheduler_core = scheduler_core
        self.data_sources = sources
        self.metadata_service = metadata_service
        self.base_graph_conf = base_graph_conf
        self.logger.info(f"Found {len(self.data_sources)} data sources")
        self._register_all_datasource_metadata()
        self.data_sources = self.check_enable_data_sources()
        self.logger.info(f"Enable Found {len(self.data_sources)} data sources")

    @staticmethod
    def _to_datasource_dto(source) -> DataSourceDTO | None:
        if isinstance(source, DataSourceDTO):
            return source
        if isinstance(source, dict):
            return from_dict(DataSourceDTO, data=source, config=Config(cast=[dict]))
        return None

    def _register_all_datasource_metadata(self):
        if self.metadata_service is None:
            return
        for source in self.data_sources or []:
            try:
                dto = self._to_datasource_dto(source)
                if dto is None:
                    continue
                self.metadata_service.register_data_source(dto)
                if not dto.enable:
                    self.metadata_service.update(
                        dto.name,
                        {
                            "is_active": False,
                            "last_run_status": "disabled",
                            "last_run_message": "Datasource disabled in config",
                        },
                    )
            except Exception as e:
                source_name = source.get("name") if isinstance(source, dict) else getattr(source, "name", "unknown")
                self.logger.error(f"Metadata registration failed for datasource {source_name}: {e}")

    def check_enable_data_sources(self):
        try:
            result = []
            for source in self.data_sources:
                data = self._to_datasource_dto(source)
                if data is not None and data.enable:
                    result.append(data)
            return result
        except Exception as e:
            self.logger.error(f"Error loading data sources for {source.get('name')} {e}")

    def run_data_source_mapper(self):
        for source in self.data_sources:
            data = source
            class_name = (data.class_name or "").strip()
            if class_name.endswith("Mapper"):
                class_name = class_name[:-6]

            try:
                module_path = f"{self._prefix_path}.{class_name}Mapper"
                module = importlib.import_module(module_path)
                mapper_class = getattr(module, f"{class_name[0].upper() + class_name[1:]}Mapper")
                instance_data_source = mapper_class(data, self.db_instance, self.scheduler_core, self.base_graph_conf, self.metadata_service)
                self.logger.info(f"execution finished for the {mapper_class.__name__}")
                # instance_data_source.run()
            except Exception as e:
                self.logger.error(f"Error running data source {class_name} :{e}")

    def start_execution(self):
        self.run_data_source_mapper()


if __name__ == "__main__":
    dsm = DataSourceMapper("weather", None, None)
