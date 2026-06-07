import os
from copy import deepcopy
from pathlib import Path

from config_features.registry import DatasourceFeatureRegistry
from log_manager.logger_manager import LoggerManager
from readers.yaml_reader import YamlReader
from validators.job_trigger_validator import validate_all_job_triggers

def _split_csv_env(name):
    raw = os.getenv(name, "")
    return [s.strip() for s in raw.split(",") if s.strip()] or None


def _default_config_path() -> str:
    # Config root sentinel. Mirrors the TMP_DIR mechanism (see
    # readers/yaml_reader.py::_tmp_base): the config file location is relocatable
    # via the CONFIG_FILE env var (e.g. set in .env), falling back to the
    # repo-root config.yaml when unset.
    return os.getenv("CONFIG_FILE") or str(Path(__file__).resolve().parents[1] / "config.yaml")


class CoreConfig(YamlReader):

    filepath = _default_config_path()
    _instance = None
    _override_only: list[str] | None = None
    _override_disable: list[str] | None = None

    def __new__(cls, filepath=None):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self, filepath=None):
        if self._initialized:
            return
        # Resolve the class attribute dynamically (not via a default arg bound at
        # class-definition time) so a runtime override — e.g. experimentation's
        # `CoreConfig.filepath = ...` — and the CONFIG_FILE env var both take effect.
        filepath = filepath or type(self).filepath
        super().__init__(filepath)
        self.logger = LoggerManager(type(self).__name__)
        self.config = YamlReader.read(self)
        self._apply_env_overrides()
        self._load_datasource_configs()
        self._apply_datasource_overrides()
        self._apply_datasource_defaults()
        self._normalize_mapping_strategy()
        self._merge_mapping_defaults()
        self._resolve_mapping_sql_files()
        self._validate_mapping_strategies()
        self._merge_mv_configs()
        self._merge_mv_defaults()
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

    @staticmethod
    def _deep_fill_defaults(target: dict, defaults: dict) -> None:
        """Recursively fill keys absent from `target` using `defaults`.

        Existing values in `target` always win (fill-missing semantics). Nested
        dicts are merged key-by-key; defaults are deep-copied so distinct targets
        never share mutable sub-dicts.
        """
        for key, value in defaults.items():
            if isinstance(value, dict):
                node = target.get(key)
                if node is None:
                    target[key] = deepcopy(value)
                elif isinstance(node, dict):
                    CoreConfig._deep_fill_defaults(node, value)
            else:
                target.setdefault(key, value)

    def _normalize_mapping_strategy(self):
        """Collapse the unified `mapping.strategy` block into the internal
        `strategy` (type/description/link_on) + `config` (everything else) split
        that the strategy builders consume.

        Datasources may declare one `strategy:` block holding the type, the link
        keys (mapping_column / base_column / basis) and all strategy params side
        by side. This rewrites that into the legacy two-block shape, so the YAML
        is authored in one place while downstream code is unchanged. An explicit
        `config:` key and `strategy.link_on:` sub-block (legacy form) still work
        and take precedence over flattened keys.

        Also relocates top-level `mapping.match` / `mapping.reduce` / `mapping.project`
        (the composed-engine authoring shape) into `mapping.config`, since the
        MappingDTO only carries the free-form `config` dict; downstream the engine
        reads the axes from there via resolve_axes.
        """
        datasources = self.config.get("datasources")
        if not isinstance(datasources, list):
            return

        link_keys = ("mapping_column", "base_column", "basis")
        reserved = {"type", "description", "link_on", *link_keys}
        axis_keys = ("match", "reduce", "project")

        for ds in datasources:
            if not isinstance(ds, dict):
                continue
            mapping = ds.get("mapping")
            if not isinstance(mapping, dict):
                continue

            # Relocate top-level composed-engine axes into config (DTO-safe).
            if any(key in mapping for key in axis_keys):
                axis_config = mapping.get("config")
                if not isinstance(axis_config, dict):
                    axis_config = {}
                for key in axis_keys:
                    if key in mapping:
                        axis_config.setdefault(key, mapping.pop(key))
                mapping["config"] = axis_config

            strategy = mapping.get("strategy")
            if not isinstance(strategy, dict):
                continue  # string strategy or absent — nothing to split

            config = mapping.get("config")
            if not isinstance(config, dict):
                config = {}

            link_on = strategy.get("link_on")
            if not isinstance(link_on, dict):
                link_on = {}
            for key in link_keys:
                if strategy.get(key) is not None:
                    link_on.setdefault(key, strategy[key])

            for key, value in strategy.items():
                if key in reserved:
                    continue
                config.setdefault(key, value)

            slim = {"type": strategy.get("type")}
            if strategy.get("description") is not None:
                slim["description"] = strategy.get("description")
            if link_on:
                slim["link_on"] = link_on
            mapping["strategy"] = slim
            if config:
                mapping["config"] = config

    def _merge_mapping_defaults(self):
        """Fill missing mapping keys from mapping_defaults for every datasource.

        Top-level keys (table_schema, incremental, ...) are filled when absent.
        `mapping_defaults.config` is deep-merged per-key into the resolved mapping
        config, but only for enabled, geometry-consuming strategies — so the
        shared geometry-column defaults reach spatial datasources without
        littering non-spatial ones (sql_template / custom / attribute_join). The
        geometry gate keys off the resolved MATCH axis (nearest/within/intersects).
        """
        from main_core.mapping_sql_builder import match_uses_geometry, resolve_axes

        defaults = self.config.get("mapping_defaults")
        if not isinstance(defaults, dict) or not defaults:
            return
        datasources = self.config.get("datasources")
        if not isinstance(datasources, list):
            return

        config_defaults = defaults.get("config")
        config_defaults = config_defaults if isinstance(config_defaults, dict) else None

        for ds in datasources:
            if not isinstance(ds, dict):
                continue
            mapping = ds.get("mapping")
            if not isinstance(mapping, dict):
                continue

            for key, value in defaults.items():
                if key == "config":
                    continue
                if key not in mapping:
                    mapping[key] = value

            if not config_defaults or not mapping.get("enable", False):
                continue
            strategy = mapping.get("strategy")
            type_str = strategy.get("type") if isinstance(strategy, dict) else strategy
            cfg = mapping.get("config")
            if not isinstance(cfg, dict):
                cfg = {}
                mapping["config"] = cfg
            match, _ = resolve_axes(type_str if isinstance(type_str, str) else None, cfg)
            if not match_uses_geometry(match):
                continue
            for key, value in config_defaults.items():
                cfg.setdefault(key, value)

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

    def _validate_mapping_strategies(self):
        """Fail fast on mapping-strategy misconfiguration before any ETL runs.

        For every enabled datasource with mapping enabled: resolves the strategy
        (preset alias or explicit `composed`) into its MATCH / REDUCE axes and
        validates each axis against MATCH_SPECS / REDUCE_SPECS, plus a few
        cross-axis rules (key-match needs join columns; idw-reduce needs
        nearest-match). Warns on unrecognized config keys (typo catcher).
        custom / mapper_sql / none are skipped; sql_template only needs config.sql.
        """
        from main_core.mapping_sql_builder import (
            COMMON_MAPPING_CONFIG_KEYS,
            KNOWN_STRATEGY_NAMES,
            MATCH_SPECS,
            REDUCE_SPECS,
            canonical_strategy_name,
            resolve_axes,
        )

        datasources = self.config.get("datasources")
        if not isinstance(datasources, list):
            return

        special = {"custom", "mapper_sql", "none", "sql_template"}
        errors: list[str] = []
        warnings: list[str] = []

        for ds in datasources:
            if not isinstance(ds, dict) or not ds.get("enable", False):
                continue
            mapping = ds.get("mapping")
            if not isinstance(mapping, dict) or not mapping.get("enable", False):
                continue
            name = ds.get("name")
            strategy = mapping.get("strategy")
            if isinstance(strategy, str):
                type_str = strategy
            elif isinstance(strategy, dict):
                type_str = strategy.get("type") or strategy.get("name")
            else:
                type_str = None

            if not type_str:
                errors.append(f"[{name}] mapping.enable is true but mapping.strategy.type is missing")
                continue

            t = str(type_str).lower()
            if t in special:
                if t == "sql_template":
                    cfg = mapping.get("config") or {}
                    if not cfg.get("sql") and not cfg.get("sql_file"):
                        errors.append(
                            f"[{name}] strategy 'sql_template' requires mapping.strategy.sql_file "
                            f"(or config.sql)"
                        )
                continue

            if canonical_strategy_name(t) is None:
                errors.append(
                    f"[{name}] unknown mapping strategy type '{type_str}'. "
                    f"Known: {sorted(KNOWN_STRATEGY_NAMES)} or one of {sorted(special)}"
                )
                continue

            config = mapping.get("config") if isinstance(mapping.get("config"), dict) else {}
            match, reduce = resolve_axes(t, config)
            match_type = str((match or {}).get("type") or "").lower()
            reduce_type = str((reduce or {}).get("type") or "none").lower()

            mspec = MATCH_SPECS.get(match_type)
            rspec = REDUCE_SPECS.get(reduce_type)
            if mspec is None:
                errors.append(f"[{name}] '{t}' resolves to unknown match type '{match_type}'")
            if rspec is None:
                errors.append(f"[{name}] '{t}' resolves to unknown reduce type '{reduce_type}'")
            if mspec is None or rspec is None:
                continue

            # required_any on the match axis (e.g. within needs max_distance|join_condition_sql)
            for group in mspec.get("required_any", []):
                if not any((match or {}).get(key) not in (None, "") for key in group):
                    errors.append(f"[{name}] match '{match_type}' requires at least one of {group}")
            # required keys on the reduce axis (e.g. idw needs value_columns)
            for key in rspec.get("required", []):
                if (reduce or {}).get(key) in (None, ""):
                    errors.append(f"[{name}] reduce '{reduce_type}' requires '{key}'")

            # cross-axis rules
            link_on = strategy.get("link_on") if isinstance(strategy, dict) else None
            link_on = link_on if isinstance(link_on, dict) else {}
            if match_type == "key":
                base_col = link_on.get("base_column") or config.get("base_join_column")
                map_col = link_on.get("mapping_column") or config.get("enrichment_join_column")
                if not base_col or not map_col:
                    errors.append(
                        f"[{name}] match 'key' requires base+enrichment join columns "
                        f"(mapping.strategy.link_on or config.base_join_column/enrichment_join_column)"
                    )
            if reduce_type == "idw" and match_type != "nearest":
                errors.append(f"[{name}] reduce 'idw' requires match 'nearest' (got '{match_type}')")

            resolved = dict(config)
            for key, value in link_on.items():
                if value is not None:
                    resolved.setdefault(key, value)
            known_keys = COMMON_MAPPING_CONFIG_KEYS | mspec.get("known", set()) | rspec.get("known", set())
            unknown = sorted(k for k in resolved if k not in known_keys)
            if unknown:
                warnings.append(
                    f"[{name}] strategy '{t}' has unrecognized mapping config keys "
                    f"{unknown} (typo? they will be ignored)"
                )

        for w in warnings:
            self.logger.warning(str(w))
        if errors:
            lines = "\n".join(f"  {e}" for e in errors)
            raise ValueError(
                f"Mapping strategy configuration errors in {self.filepath}:\n{lines}"
            )

    def _merge_mv_defaults(self):
        """Deep-fill each loaded MV config from `mv_defaults` (fill-missing).

        Runs after `_merge_mv_configs` has populated materialized_views.views.
        An MV file only declares its distinctive parts; handler/schema/build/
        refresh/triggers boilerplate is filled here unless the file overrides it.
        """
        defaults = self.config.get("mv_defaults")
        if not isinstance(defaults, dict) or not defaults:
            return
        mv_conf = self.config.get("materialized_views")
        if not isinstance(mv_conf, dict):
            return
        views = mv_conf.get("views")
        if not isinstance(views, list):
            return
        for view in views:
            if isinstance(view, dict):
                self._deep_fill_defaults(view, defaults)

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
