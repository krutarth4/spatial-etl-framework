from log_manager.logger_manager import LoggerManager
from readers.yaml_reader import YamlReader

class CoreConfig(YamlReader):

    filepath = "../config.yaml"

    def __init__(self,filepath= filepath):
        super().__init__(filepath)
        self.logger = LoggerManager(self.__class__.__name__).get_logger()
        self.config = YamlReader.read(self)
        self.logger.info(f"Config file loaded successfully!")
        

    def get_value(self, param):
        value = self.config.get(param, None)
        if value is None:
            self.logger.warning(f" key {param} doesn't exist in {self.filepath}")
        return self.config[param]

    def get_config(self):
        return self.config

    def get_source_mapper_keys(self):
        return list(self.get_value("datasource").keys())


if __name__ == "__main__":
    config = CoreConfig()

