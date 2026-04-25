import os
from pathlib import Path

from log_manager.logger_manager import LoggerManager
from readers.yaml_reader import YamlReader

def _split_csv_env(name):
    raw = os.getenv(name, "")
    return [s.strip() for s in raw.split(",") if s.strip()] or None


class CoreConfig(YamlReader):

    filepath = str(Path(__file__).resolve().parents[1] / "config.yaml")
    _instance = None
    _override_only: list[str] | None = None
    _override_disable: list[str] | None = None

    def __new__(cls, filepath=filepath):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self, filepath=filepath):
        if self._initialized:
            return
        super().__init__(filepath)
        self.logger = LoggerManager(type(self).__name__)
        self.config = YamlReader.read(self)
        self._apply_env_overrides()
        self._apply_datasource_overrides()
        self._initialized = True
        self.logger.info(f"Config file loaded successfully!")

    @classmethod
    def set_datasource_override(cls, only=None, disable=None):
        cls._override_only = only
        cls._override_disable = disable
        

    def get_value(self, param):
        value = self.config.get(param, None)
        if value is None:
            self.logger.warning(f" key {param} doesn't exist in {self.filepath}")
        return self.config[param]

    def get_config(self):
        return self.config

    def get_source_mapper_keys(self):
        return list(self.get_value("datasource").keys())

    def _apply_env_overrides(self):
        db_conf = self.config.get("database")
        if not isinstance(db_conf, dict):
            return

        host = os.getenv("DB_HOST")
        port = os.getenv("DB_PORT")
        name = os.getenv("DB_NAME")
        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASSWORD")

        if host:
            db_conf["url"] = host
        if port:
            try:
                db_conf["port"] = int(port)
            except ValueError:
                self.logger.warning(f"Invalid DB_PORT '{port}', keeping config value")
        if name:
            db_conf["database_name"] = name

        cred = db_conf.get("credential")
        if isinstance(cred, dict):
            if user:
                cred["username"] = user
            if password:
                cred["password"] = password

    def _apply_datasource_overrides(self):
        only = self._override_only or _split_csv_env("ENABLE_DATASOURCES")
        disable = self._override_disable or _split_csv_env("DISABLE_DATASOURCES")
        if not only and not disable:
            return

        datasources = self.config.get("datasources")
        if not isinstance(datasources, list):
            return

        known = {ds.get("name") for ds in datasources if isinstance(ds, dict)}
        for name in (only or []) + (disable or []):
            if name not in known:
                self.logger.warning(f"Datasource override references unknown name: {name}")

        for ds in datasources:
            if not isinstance(ds, dict):
                continue
            if only is not None:
                ds["enable"] = ds.get("name") in only
            elif disable and ds.get("name") in disable:
                ds["enable"] = False

        if only is not None:
            self.logger.info(f"Datasource override (only): {only}")
        elif disable:
            self.logger.info(f"Datasource override (disable): {disable}")


if __name__ == "__main__":
    config = CoreConfig()
