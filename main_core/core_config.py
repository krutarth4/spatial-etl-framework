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
        self._load_datasource_configs()
        self._apply_datasource_overrides()
        self._apply_datasource_defaults()
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

    def _load_datasource_configs(self):
        """Load per-datasource YAML files from `data_folder` and append them to
        `datasources`.

        Each file holds a single datasource mapping (or a small list of them).
        Files are read through YamlReader so per-file `${{ ... }}` Python blocks
        and `tmp/...` path resolution behave exactly like the main config. The
        inline `datasources:` list in config.yaml (if any) is preserved and
        files are appended after it; duplicate names are ignored with a warning.
        """
        folder = self.config.get("data_folder")
        if not folder:
            return

        base_dir = Path(self.filepath).resolve().parent
        ds_dir = (base_dir / folder).resolve()
        if not ds_dir.is_dir():
            self.logger.warning(
                f"data_folder '{ds_dir}' does not exist, skipping per-file datasource load"
            )
            return

        datasources = self.config.get("datasources")
        if datasources is None:
            datasources = []
            self.config["datasources"] = datasources
        if not isinstance(datasources, list):
            self.logger.warning("'datasources' is not a list; skipping per-file datasource load")
            return

        existing = {ds.get("name") for ds in datasources if isinstance(ds, dict)}
        for ds_file in sorted(ds_dir.glob("*.yaml")):
            try:
                content = YamlReader.get_yaml_content(str(ds_file))
            except Exception as e:
                self.logger.warning(f"Failed to load datasource config {ds_file.name}: {e}")
                continue
            items = content if isinstance(content, list) else [content]
            for ds in items:
                if not isinstance(ds, dict) or not ds.get("name"):
                    self.logger.warning(f"Skipping invalid datasource in {ds_file.name}")
                    continue
                if ds["name"] in existing:
                    self.logger.warning(
                        f"Duplicate datasource '{ds['name']}' from {ds_file.name} ignored"
                    )
                    continue
                datasources.append(ds)
                existing.add(ds["name"])
                self.logger.info(f"Loaded datasource config: {ds_file.name} ({ds['name']})")

    def _apply_datasource_defaults(self):
        """Fill omitted schema / base_table / job values from global defaults.

        Per-datasource config files only need to declare what is distinctive;
        the uniform boilerplate (table schemas, the mapping base table, common
        job flags) is filled here from `env_variables.base_schema` and `base`.
        Table names fall back to the `<name>_<stage>` convention only when
        omitted, so existing explicit names are never overwritten.
        """
        datasources = self.config.get("datasources")
        if not isinstance(datasources, list):
            return

        env_vars = self.config.get("env_variables") or {}
        default_schema = env_vars.get("base_schema")
        base_conf = self.config.get("base") or {}
        base_table_name = base_conf.get("table_name")
        base_table_schema = base_conf.get("table_schema") or default_schema

        for ds in datasources:
            if not isinstance(ds, dict):
                continue
            name = ds.get("name")

            storage = ds.get("storage")
            if isinstance(storage, dict):
                for stage in ("staging", "enrichment"):
                    tbl = storage.get(stage)
                    if isinstance(tbl, dict):
                        if default_schema is not None:
                            tbl.setdefault("table_schema", default_schema)
                        if not tbl.get("table_name") and name:
                            tbl["table_name"] = f"{name}_{stage}"

            mapping = ds.get("mapping")
            if isinstance(mapping, dict):
                base_table = mapping.get("base_table")
                if isinstance(base_table, dict):
                    if base_table_name is not None:
                        base_table.setdefault("table_name", base_table_name)
                    if base_table_schema is not None:
                        base_table.setdefault("table_schema", base_table_schema)

            job = ds.get("job")
            if isinstance(job, dict):
                if name:
                    job.setdefault("name", f"{name}Job")
                job.setdefault("id", job.get("name"))
                job.setdefault("replace_existing", True)
                job.setdefault("coalesce", True)
                job.setdefault("max_instances", 1)
                job.setdefault("next_run_time", "none")

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
