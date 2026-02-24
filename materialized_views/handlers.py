from dataclasses import dataclass
from typing import Any


@dataclass
class MaterializedViewConfig:
    raw: dict[str, Any]

    @property
    def schema(self) -> str:
        return self.raw.get("schema")

    @property
    def name(self) -> str:
        return self.raw.get("name")

    @property
    def identifier(self) -> str:
        return self.raw.get("id") or f"{self.schema}.{self.name}"

    @property
    def create_sql(self) -> str | None:
        custom = self.raw.get("custom_sql", {}) or {}
        return custom.get("create")

    @property
    def refresh_sql(self) -> str | None:
        custom = self.raw.get("custom_sql", {}) or {}
        return custom.get("refresh")

    @property
    def select_sql(self) -> str | None:
        return self.raw.get("select_sql")

    @property
    def refresh_mode(self) -> str:
        refresh = self.raw.get("refresh", {}) or {}
        return (refresh.get("mode") or "normal").lower()

    @property
    def with_data(self) -> bool:
        refresh = self.raw.get("refresh", {}) or {}
        return bool(refresh.get("with_data", True))


class BaseMaterializedViewHandler:
    def __init__(self, db, conf: dict[str, Any]):
        self.db = db
        self.conf = MaterializedViewConfig(conf)
        self.logger = getattr(db, "logger", None)

    def ensure(self):
        raise NotImplementedError

    def refresh(self):
        raise NotImplementedError

    def _exec(self, sql: str):
        if self.logger is not None:
            self.logger.info(f"MV SQL ({self.conf.identifier}): {sql}")
        self.db.call_sql(sql, raise_on_error=True)


class GenericMaterializedViewHandler(BaseMaterializedViewHandler):
    """
    Generic MV handler.
    Uses `custom_sql` if provided, otherwise builds SQL from config keys.
    """

    def ensure(self):
        if self.db is None:
            return
        if self.db.materialized_view_exists(self.conf.name, self.conf.schema):
            return

        sql = self.conf.create_sql
        if sql is None:
            if not self.conf.select_sql:
                raise ValueError(
                    f"Materialized view '{self.conf.identifier}' missing `select_sql` and `custom_sql.create`"
                )
            sql = (
                f'CREATE MATERIALIZED VIEW "{self.conf.schema}"."{self.conf.name}" AS '
                f"{self.conf.select_sql}"
            )
            if not self.conf.with_data:
                sql = f"{sql} WITH NO DATA"

        self._exec(sql)

    def refresh(self):
        if self.db is None:
            return

        if self.conf.refresh_sql:
            self._exec(self.conf.refresh_sql)
            return

        concurrently = "CONCURRENTLY " if self.conf.refresh_mode == "concurrently" else ""
        with_data = "" if self.conf.with_data else " WITH NO DATA"
        sql = (
            f'REFRESH MATERIALIZED VIEW {concurrently}"{self.conf.schema}"."{self.conf.name}"{with_data}'
        )
        self._exec(sql)


class WeatherMaterializedViewHandler(BaseMaterializedViewHandler):
    """
    Minimal weather MV handler based on config tables and optional timestamp filter.
    Supports optional custom SQL override.
    """

    def _cfg(self, key: str, default=None):
        return self.conf.raw.get(key, default)

    def _qualified(self, schema: str, table: str) -> str:
        return f'"{schema}"."{table}"'

    def _build_select_sql(self) -> str:
        schema = self.conf.schema
        mapping_table = self._cfg("mapping_table", "dwd_station_locations_mapping")
        weather_table = self._cfg("weather_table", "weather_enrichment")
        ways_table = self._cfg("ways_table", "ways_base")
        timestamp_filter = self._cfg("timestamp_filter")

        where_clause = ""
        if timestamp_filter:
            where_clause = f"\nWHERE e.\"timestamp\" = TIMESTAMPTZ '{timestamp_filter}'"

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
FROM {self._qualified(schema, mapping_table)} m
JOIN {self._qualified(schema, weather_table)} e
    ON e.dwd_station_id::INTEGER = m.dwd_station_id::INTEGER
JOIN {self._qualified(schema, ways_table)} w
    ON w.id = m.way_id{where_clause}
""".strip()

    def _ensure_indexes(self):
        indexes = self._cfg("indexes", []) or []
        for idx in indexes:
            if isinstance(idx, str):
                self._exec(idx)
                continue
            if idx.get("sql"):
                self._exec(idx["sql"])
                continue

            index_name = idx.get("name")
            column = idx.get("column")
            unique = "UNIQUE " if idx.get("unique", False) else ""
            if not index_name or not column:
                continue
            sql = (
                f'CREATE {unique}INDEX IF NOT EXISTS "{index_name}" '
                f'ON "{self.conf.schema}"."{self.conf.name}" ({column})'
            )
            self._exec(sql)

    def ensure(self):
        if self.db is None:
            return
        if self.db.materialized_view_exists(self.conf.name, self.conf.schema):
            self._ensure_indexes()
            return

        sql = self.conf.create_sql
        if sql is None:
            sql = (
                f'CREATE MATERIALIZED VIEW "{self.conf.schema}"."{self.conf.name}" AS\n'
                f"{self._build_select_sql()}"
            )

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
        sql = f'REFRESH MATERIALIZED VIEW {concurrently}"{self.conf.schema}"."{self.conf.name}"'
        self._exec(sql)
        self._ensure_indexes()
