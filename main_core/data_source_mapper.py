import importlib

from data_config_dtos.data_source_config_dto import DataSourceDTO
from dacite import from_dict, Config

from log_manager.logger_manager import LoggerManager
from main_core.safe_class import safe_class


@safe_class
class DataSourceMapper:
    _prefix_path = "data_mappers"

    def __init__(self, sources, db_instance, scheduler_core):
        self.logger = LoggerManager(type(self).__name__).get_logger()
        self.db_instance = db_instance
        self.scheduler_core = scheduler_core
        self.data_sources = sources
        self.logger.info(f"Found {len(self.data_sources)} data sources")
        self.data_sources = self.check_enable_data_sources()
        self.logger.info(f"Enable Found {len(self.data_sources)} data sources")
        self.run_data_source_mapper()

    def check_enable_data_sources(self):
        try:
            result = []
            for source in self.data_sources:
                if isinstance(source, dict):
                    if source["enable"]:
                        data = from_dict(DataSourceDTO, data=source, config=Config(cast=[dict]))
                        result.append(data)
                elif isinstance(source, DataSourceDTO):
                    if source.enable:
                        result.append(source)
            return result
        except Exception as e:
            self.logger.error(f"Error loading data sources for {source.get("name")} {e}")

    def run_data_source_mapper(self):
        for source in self.data_sources:
            data = source
            class_name = data.class_name

            try:
                module_path = f"{self._prefix_path}.{class_name}Mapper"
                module = importlib.import_module(module_path)
                mapper_class = getattr(module, f"{class_name[0].upper() + class_name[1:]}Mapper")
                instance_data_source = mapper_class(data, self.db_instance, self.scheduler_core)
                self.logger.info(f"execution finished for the {mapper_class.__name__}")
                # instance_data_source.run()
            except Exception as e:
                self.logger.error(f"Error running data source {class_name} :{e}")


if __name__ == "__main__":
    dsm = DataSourceMapper("weather", None, None)
