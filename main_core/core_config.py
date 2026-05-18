import os
from pathlib import Path

from config_features.registry import DatasourceFeatureRegistry
from log_manager.logger_manager import LoggerManager
from readers.yaml_reader import YamlReader
from validators.job_trigger_validator import validate_all_job_triggers

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
        self._merge_mapping_defaults()
        self._resolve_mapping_sql_files()
        self._merge_mv_configs()
        self._validate_job_triggers()
        self._validate_datasource_features()
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

    def _validate_job_triggers(self):
        datasources = self.config.get("datasources")
        if not isinstance(datasources, list):
            return

        errors, warnings = validate_all_job_triggers(datasources)

        for w in warnings:
            self.logger.warning(str(w))

        if errors:
            lines = "\n".join(f"  {e}" for e in errors)
            raise ValueError(
                f"Job trigger configuration errors in {self.filepath}:\n{lines}"
            )

    def _validate_datasource_features(self):
        datasources = self.config.get("datasources")
        if not isinstance(datasources, list):
            return

        DatasourceFeatureRegistry.load_all()
        errors, warnings = DatasourceFeatureRegistry.validate_all(datasources)

        for w in warnings:
            self.logger.warning(str(w))

        if errors:
            lines = "\n".join(f"  {e}" for e in errors)
            raise ValueError(
                f"Datasource feature configuration errors in {self.filepath}:\n{lines}"
            )

    def _merge_mapping_defaults(self):
        """Fill missing top-level mapping keys from mapping_defaults for every datasource."""
        defaults = self.config.get("mapping_defaults")
        if not isinstance(defaults, dict) or not defaults:
            return
        datasources = self.config.get("datasources")
        if not isinstance(datasources, list):
            return
        for ds in datasources:
            if not isinstance(ds, dict):
                continue
            mapping = ds.get("mapping")
            if not isinstance(mapping, dict):
                continue
            for key, value in defaults.items():
                if key not in mapping:
                    mapping[key] = value

    def _resolve_mapping_sql_files(self):
        """Replace mapping.config.sql_file references with the file's SQL content."""
        base_dir = Path(self.filepath).resolve().parent
        datasources = self.config.get("datasources")
        if not isinstance(datasources, list):
            return
        for ds in datasources:
            if not isinstance(ds, dict):
                continue
            mapping = ds.get("mapping")
            if not isinstance(mapping, dict):
                continue
            config = mapping.get("config")
            if not isinstance(config, dict):
                continue
            sql_file = config.get("sql_file")
            if not sql_file:
                continue
            sql_path = (base_dir / str(sql_file)).resolve()
            try:
                config["sql"] = sql_path.read_text(encoding="utf-8")
                config.pop("sql_file")
                self.logger.info(f"Datasource '{ds.get('name')}': loaded mapping SQL from {sql_path.name}")
            except FileNotFoundError:
                raise FileNotFoundError(
                    f"Datasource '{ds.get('name')}' mapping.config.sql_file "
                    f"'{sql_file}' not found: {sql_path}"
                )

    def _merge_mv_configs(self):
        mv_conf = self.config.get("materialized_views")
        if not isinstance(mv_conf, dict):
            return
        folder = mv_conf.get("mv_folder")
        if not folder:
            return

        base_dir = Path(self.filepath).resolve().parent
        mv_dir = (base_dir / folder).resolve()
        if not mv_dir.is_dir():
            self.logger.warning(f"mv_folder '{mv_dir}' does not exist, skipping MV config merge")
            return

        views = []
        for mv_file in sorted(mv_dir.glob("*.yaml")):
            try:
                view_conf = YamlReader.get_yaml_content(str(mv_file))
                if isinstance(view_conf, dict):
                    views.append(view_conf)
                    self.logger.info(f"Loaded MV config: {mv_file.name}")
            except Exception as e:
                self.logger.warning(f"Failed to load MV config {mv_file.name}: {e}")

        mv_conf["views"] = views


if __name__ == "__main__":
    config = CoreConfig()
