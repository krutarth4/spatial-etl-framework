from dataclasses import dataclass
from typing import Any


@dataclass
class MaterializedViewConfig:
    """
    Wrapper around a single MV YAML document.

    Reads the v2 nested schema (`definition.*`, `build.*`, `triggers.*`) and falls
    back to the legacy flat keys (`select_sql`, `custom_sql`, `mapping_table`, ...)
    so old configs keep working.
    """
    raw: dict[str, Any]

    # ---- identity ----------------------------------------------------------
    @property
    def schema(self) -> str:
        return self.raw.get("schema")

    @property
    def name(self) -> str:
        return self.raw.get("name")

    @property
    def identifier(self) -> str:
        return self.raw.get("id") or f"{self.schema}.{self.name}"

    # ---- definition (select / custom_sql / source) -------------------------
    @property
    def _definition(self) -> dict[str, Any]:
        return self.raw.get("definition") or {}

    @property
    def select_sql(self) -> str | None:
        return self._definition.get("select_sql") or self.raw.get("select_sql")

    @property
    def _custom_sql(self) -> dict[str, Any]:
        return self._definition.get("custom_sql") or self.raw.get("custom_sql") or {}

    @property
    def create_sql(self) -> str | None:
        return self._custom_sql.get("create")

    @property
    def refresh_sql(self) -> str | None:
        return self._custom_sql.get("refresh")

    @property
    def source(self) -> dict[str, Any]:
        """Domain-specific source config (used by specialized handlers)."""
        return self._definition.get("source") or {}

    # ---- build options -----------------------------------------------------
    @property
    def _build(self) -> dict[str, Any]:
        return self.raw.get("build") or {}

    @property
    def with_data(self) -> bool:
        # New: build.with_data; legacy: refresh.with_data
        if "with_data" in self._build:
            return bool(self._build.get("with_data", True))
        refresh = self.raw.get("refresh") or {}
        return bool(refresh.get("with_data", True))

    @property
    def tablespace(self) -> str | None:
        return self._build.get("tablespace")

    # ---- refresh -----------------------------------------------------------
    @property
    def _refresh(self) -> dict[str, Any]:
        return self.raw.get("refresh") or {}

    @property
    def refresh_enabled(self) -> bool:
        return bool(self._refresh.get("enabled", True))

    @property
    def refresh_mode(self) -> str:
        return (self._refresh.get("mode") or "normal").lower()

    @property
    def only_on_data_change(self) -> bool:
        # New: triggers.only_on_data_change; legacy: refresh.only_on_data_change
        triggers = self.raw.get("triggers") or {}
        if "only_on_data_change" in triggers:
            return bool(triggers.get("only_on_data_change"))
        return bool(self._refresh.get("only_on_data_change", False))

    # ---- triggers ----------------------------------------------------------
    @property
    def trigger_datasources(self) -> list[str]:
        # New: triggers.on_datasource_success; legacy: depends_on.datasources
        triggers = self.raw.get("triggers") or {}
        names = triggers.get("on_datasource_success")
        if names:
            return list(names)
        deps = self.raw.get("depends_on") or {}
        return list(deps.get("datasources") or [])

    @property
    def depends_on_tables(self) -> list[dict[str, str]]:
        """Return list of {schema, name} dicts from depends_on.tables."""
        deps = self.raw.get("depends_on") or {}
        return list(deps.get("tables") or [])


class BaseMaterializedViewHandler:
    def __init__(self, db, conf: dict[str, Any]):
        self.db = db
        self.conf = MaterializedViewConfig(conf)
        self.logger = getattr(db, "logger", None)

    def check_dependency_tables(self) -> bool:
        """
        Warn and return False if any table listed in depends_on.tables is missing.
        Returns True when all tables exist (or no dependencies are declared).
        """
        if self.db is None:
            return True
        all_present = True
        for entry in self.conf.depends_on_tables:
            schema = entry.get("schema") or self.conf.schema
            name = entry.get("name")
            if not name:
                continue
            if not self.db.table_exists(name, schema):
                if self.logger:
                    self.logger.warning(
                        f"Materialized view '{self.conf.identifier}' depends on table "
                        f"'{schema}.{name}' which does not exist — skipping view creation."
                    )
                all_present = False
        return all_present

    def ensure(self):
        raise NotImplementedError

    def refresh(self):
        raise NotImplementedError

    def _exec(self, sql: str):
        if self.logger is not None:
            self.logger.info(f"MV SQL ({self.conf.identifier}): {sql}")
        self.db.call_sql(sql, raise_on_error=True)

    # ---- shared helpers ----------------------------------------------------
    def _qualified(self, schema: str, table: str) -> str:
        return f'"{schema}"."{table}"'

    def _wrap_create(self, body_sql: str) -> str:
        tablespace = self.conf.tablespace
        ts_clause = f' TABLESPACE "{tablespace}"' if tablespace else ""
        sql = (
            f'CREATE MATERIALIZED VIEW "{self.conf.schema}"."{self.conf.name}"'
            f"{ts_clause} AS {body_sql}"
        )
        if not self.conf.with_data:
            sql = f"{sql} WITH NO DATA"
        return sql

    def _ensure_indexes(self):
        for idx in (self.conf.raw.get("indexes") or []):
            if isinstance(idx, str):
                self._exec(idx.format(schema=self.conf.schema))
                continue
            if idx.get("sql"):
                self._exec(idx["sql"].format(schema=self.conf.schema))
                continue

            index_name = idx.get("name")
            columns = idx.get("columns")
            # legacy single-column form
            if not columns and idx.get("column"):
                columns = [idx["column"]]
            if not index_name or not columns:
                continue

            unique = "UNIQUE " if idx.get("unique", False) else ""
            method = idx.get("method")
            using = f" USING {method}" if method else ""
            cols_sql = ", ".join(columns)
            where = idx.get("where")
            where_sql = f" WHERE {where}" if where else ""
            sql = (
                f'CREATE {unique}INDEX IF NOT EXISTS "{index_name}" '
                f'ON "{self.conf.schema}"."{self.conf.name}"{using} ({cols_sql}){where_sql}'
            )
            self._exec(sql)


class GenericMaterializedViewHandler(BaseMaterializedViewHandler):
    """
    Generic handler. Builds CREATE/REFRESH SQL from `definition.select_sql`
    or uses `definition.custom_sql.{create,refresh}` verbatim when provided.
    """

    def ensure(self):
        if self.db is None:
            return
        if self.db.materialized_view_exists(self.conf.name, self.conf.schema):
            self._ensure_indexes()
            return

        sql = self.conf.create_sql
        if sql is None:
            if not self.conf.select_sql:
                raise ValueError(
                    f"Materialized view '{self.conf.identifier}' missing "
                    f"`definition.select_sql` and `definition.custom_sql.create`"
                )
            sql = self._wrap_create(self.conf.select_sql.format(schema=self.conf.schema))

        self._exec(sql)
        self._ensure_indexes()

    def refresh(self):
        if self.db is None:
            return

        if self.conf.refresh_sql:
            self._exec(self.conf.refresh_sql)
            self._ensure_indexes()
            return

        concurrently = "CONCURRENTLY " if self.conf.refresh_mode == "concurrently" else ""
        with_data = "" if self.conf.with_data else " WITH NO DATA"
        sql = (
            f'REFRESH MATERIALIZED VIEW {concurrently}'
            f'"{self.conf.schema}"."{self.conf.name}"{with_data}'
        )
        self._exec(sql)
        self._ensure_indexes()


class WeatherMaterializedViewHandler(BaseMaterializedViewHandler):
    """
    Domain-specific MV: per-way weather snapshot built from a station-mapping
    table, the weather enrichment table and the base ways table.

    Reads `definition.source.{mapping_table,enrichment_table,base_table,filters}`.
    Falls back to legacy flat keys (`mapping_table`, `weather_table`, `ways_table`,
    `timestamp_filter`) for backward compatibility.
    """

    # ---- config helpers ----------------------------------------------------
    def _table_ref(self, key: str, legacy_key: str, default_name: str) -> tuple[str, str]:
        src = self.conf.source.get(key)
        if isinstance(src, dict) and src.get("name"):
            return src.get("schema") or self.conf.schema, src["name"]
        legacy = self.conf.raw.get(legacy_key, default_name)
        return self.conf.schema, legacy

    def _filters(self) -> dict[str, Any]:
        filters = (self.conf.source.get("filters") or {}) if self.conf.source else {}
        ts = filters.get("timestamp_eq") or self.conf.raw.get("timestamp_filter")
        return {"timestamp_eq": ts}

    # ---- SQL building ------------------------------------------------------
    def _build_select_sql(self) -> str:
        m_schema, m_name = self._table_ref("mapping_table", "mapping_table", "dwd_station_locations_mapping")
        e_schema, e_name = self._table_ref("enrichment_table", "weather_table", "weather_enrichment")
        w_schema, w_name = self._table_ref("base_table", "ways_table", "ways_base")

        ts = self._filters().get("timestamp_eq")
        where_clause = f"\nWHERE e.\"timestamp\" = TIMESTAMPTZ '{ts}'" if ts else ""

        return f"""
SELECT
    m.dwd_station_id,
    w.way_id,
    w.way_link_index,
    m.bearing_degree AS bearing_deg,
    e."timestamp",
    e.visibility,
    e.wind_direction,
    e.wind_speed
FROM {self._qualified(m_schema, m_name)} m
JOIN {self._qualified(e_schema, e_name)} e
    ON e.dwd_station_id::INTEGER = m.dwd_station_id::INTEGER
JOIN {self._qualified(w_schema, w_name)} w
    ON w.id = m.way_id{where_clause}
""".strip()

    # ---- lifecycle ---------------------------------------------------------
    def ensure(self):
        if self.db is None:
            return
        if self.db.materialized_view_exists(self.conf.name, self.conf.schema):
            self._ensure_indexes()
            return

        sql = self.conf.create_sql or self._wrap_create(self._build_select_sql())
        self._exec(sql)
        self._ensure_indexes()

    def refresh(self):
        if self.db is None:
            return

        if self.conf.refresh_sql:
            self._exec(self.conf.refresh_sql)
            self._ensure_indexes()
            return

        concurrently = "CONCURRENTLY " if self.conf.refresh_mode == "concurrently" else ""
        sql = (
            f'REFRESH MATERIALIZED VIEW {concurrently}'
            f'"{self.conf.schema}"."{self.conf.name}"'
        )
        self._exec(sql)
        self._ensure_indexes()
