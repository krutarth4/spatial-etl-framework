"""__init__, endpoint/dashboard listing, metadata + table-status, shared helpers.

Mixin composed by DebugMapperService (core/debug_mapper_service.py).
Methods unchanged; they run on the composed instance and call peers via self.*
"""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from uuid import UUID
from types import SimpleNamespace

from sqlalchemy import select
from sqlalchemy.sql import text

from database.db_instance import DbInstance
from metadata.data_source_metadata_repository import DataSourceMetadataRepository

from core.debug.adapters import _StorageRef, _StorageConf, _MappingConf, _DataSourceConfigAdapter, TableTarget


class DebugCoreMixin:
    def __init__(self, datasources: list[dict] | None, db: DbInstance | None, metadata_schema: str | None = None):
        self.datasources = datasources or []
        self.db = db
        self.metadata_schema = metadata_schema
        self._endpoint_index = self._build_endpoint_index(self.datasources)
    def list_endpoints(self) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        for ds in self.datasources:
            endpoint_keys = sorted(self._extract_endpoint_keys(ds))
            items.append(
                {
                    "name": ds.get("name"),
                    "class_name": ds.get("class_name"),
                    "enabled": ds.get("enable", True),
                    "primary_endpoint": self._primary_endpoint_key(ds),
                    "endpoint_keys": endpoint_keys,
                }
            )
        return items
    def list_datasources(self) -> list[dict[str, Any]]:
        source_keys = [ds.get("name") for ds in self.datasources if ds.get("name")]
        metadata_map = self._fetch_all_metadata(source_keys)

        items: list[dict[str, Any]] = []
        for ds in self.datasources:
            source_name = ds.get("name")
            items.append(
                {
                    "name": source_name,
                    "description": ds.get("description"),
                    "class_name": ds.get("class_name"),
                    "enabled": ds.get("enable", True),
                    "data_type": ds.get("data_type"),
                    "primary_endpoint": self._primary_endpoint_key(ds),
                    "endpoint_keys": sorted(self._extract_endpoint_keys(ds)),
                    "source": {
                        "fetch": (ds.get("source") or {}).get("fetch"),
                        "url": (ds.get("source") or {}).get("url"),
                    },
                    "metadata": metadata_map.get(source_name),
                    "tables": self._build_table_overview(ds),
                }
            )
        return items
    def fetch_datasource_dashboard(self, mapper_endpoint: str) -> dict[str, Any]:
        ds = self._resolve_datasource(mapper_endpoint)
        source_name = ds.get("name")
        metadata = self._fetch_metadata_row(source_name)

        return {
            "mapper_endpoint": mapper_endpoint,
            "datasource": {
                "name": source_name,
                "description": ds.get("description"),
                "class_name": ds.get("class_name"),
                "enabled": ds.get("enable", True),
                "data_type": ds.get("data_type"),
                "primary_endpoint": self._primary_endpoint_key(ds),
                "endpoint_keys": sorted(self._extract_endpoint_keys(ds)),
                "source": ds.get("source") or {},
                "storage": ds.get("storage") or {},
                "mapping": ds.get("mapping") or {},
                "job": ds.get("job") or {},
            },
            "metadata": metadata,
            "tables": self._build_table_overview(ds),
            "mapping_overview": self._mapping_overview(ds),
            "coverage": self._mapping_coverage(ds),
        }
    def fetch(
        self,
        mapper_endpoint: str,
        target: str,
        limit: int = 100,
    ) -> dict[str, Any]:
        if self.db is None:
            raise ValueError("Database is not initialized.")
        if target not in TableTarget.values():
            raise ValueError(f"Invalid target '{target}'. Use one of: {sorted(TableTarget.values())}")
        if limit <= 0:
            raise ValueError("limit must be > 0")

        ds = self._resolve_datasource(mapper_endpoint)
        table_ref = self._resolve_table_ref(ds, target)
        table_name = table_ref.get("table_name")
        table_schema = table_ref.get("table_schema")
        if not table_name:
            raise ValueError(f"No table is configured for target '{target}' in datasource '{ds.get('name')}'.")

        table = self.db.get_table(table_name, table_schema)
        if table is None:
            raise ValueError(f"Table '{table_schema}.{table_name}' does not exist.")

        with self.db.session_scope() as session:
            rows = session.execute(select(table).limit(limit)).mappings().all()

        return {
            "mapper_endpoint": mapper_endpoint,
            "datasource": {
                "name": ds.get("name"),
                "class_name": ds.get("class_name"),
                "source_url": (ds.get("source") or {}).get("url"),
                "resolved_endpoint_keys": sorted(self._extract_endpoint_keys(ds)),
            },
            "target": target,
            "table": {
                "schema": table_schema,
                "name": table_name,
                "columns": [c.name for c in table.columns],
            },
            "mapping_overview": self._mapping_overview(ds),
            "count": len(rows),
            "rows": [self._to_jsonable(dict(r)) for r in rows],
        }
    def _resolve_datasource(self, mapper_endpoint: str) -> dict[str, Any]:
        key = self._normalize_endpoint_key(mapper_endpoint)
        ds = self._endpoint_index.get(key)
        if ds is None:
            raise ValueError(f"No datasource mapped for endpoint '{mapper_endpoint}'.")
        return ds
    def _resolve_table_ref(self, ds: dict[str, Any], target: str) -> dict[str, Any]:
        storage = ds.get("storage") or {}
        mapping = ds.get("mapping") or {}
        if target == TableTarget.STAGING:
            return storage.get("staging") or {}
        if target == TableTarget.ENRICHMENT:
            return storage.get("enrichment") or {}
        if target == TableTarget.MAPPING:
            return mapping if mapping.get("enable", False) else {}
        return {}
    def _build_table_overview(self, ds: dict[str, Any]) -> dict[str, Any]:
        storage = ds.get("storage") or {}
        return {
            TableTarget.STAGING: self._table_status(storage.get("staging")),
            TableTarget.ENRICHMENT: self._table_status(storage.get("enrichment")),
            TableTarget.MAPPING: self._table_status((ds.get("mapping") or {})),
        }
    def _table_status(self, table_ref: dict[str, Any] | None) -> dict[str, Any]:
        table_ref = table_ref or {}
        table_name = table_ref.get("table_name")
        table_schema = table_ref.get("table_schema")
        enabled = table_ref.get("enable", True)
        exists = False
        row_count = None

        if self.db is not None and table_name and table_schema:
            exists = self.db.table_exists(table_name, table_schema)
            if exists:
                try:
                    # Use the fast pg_class estimate, not exact COUNT(*). This
                    # overview is read on every dashboard/datasource listing; an
                    # exact scan of multi-million-row tables is too slow here and
                    # competes with the ETL for the database.
                    row_count = self.db.get_table_count(table_name, table_schema, estimate=True)
                except Exception:
                    row_count = None

        return {
            "enabled": enabled,
            "schema": table_schema,
            "name": table_name,
            "exists": exists,
            "row_count": row_count,
        }
    def _fetch_all_metadata(self, source_keys: list[str]) -> dict[str, dict[str, Any]]:
        if self.db is None or not source_keys:
            return {}

        table = self.db.get_table(DataSourceMetadataRepository.table_name, self.metadata_schema)
        if table is None or "source_key" not in table.c:
            return {}

        with self.db.session_scope() as session:
            rows = session.execute(
                select(table).where(table.c.source_key.in_(source_keys))
            ).mappings().all()

        return {row["source_key"]: self._to_jsonable(dict(row)) for row in rows}
    def _fetch_metadata_row(self, source_key: str | None) -> dict[str, Any] | None:
        if self.db is None or not source_key:
            return None

        table = self.db.get_table(DataSourceMetadataRepository.table_name, self.metadata_schema)
        if table is None or "source_key" not in table.c:
            return None

        with self.db.session_scope() as session:
            row = session.execute(
                select(table).where(table.c.source_key == source_key).limit(1)
            ).mappings().first()

        return self._to_jsonable(dict(row)) if row is not None else None
    def _primary_endpoint_key(self, ds: dict[str, Any]) -> str | None:
        debug = ds.get("debug") or {}
        debug_endpoint = debug.get("endpoint") if isinstance(debug, dict) else None
        if isinstance(debug_endpoint, str) and debug_endpoint.strip():
            return debug_endpoint.strip()

        name = ds.get("name")
        if isinstance(name, str) and name.strip():
            return name.strip()

        source_url = (ds.get("source") or {}).get("url")
        if isinstance(source_url, str) and source_url.strip():
            path = urlparse(source_url).path.strip("/")
            if path:
                return path.split("/")[-1]
            return source_url.strip()

        return None
    def _build_endpoint_index(self, datasources: list[dict]) -> dict[str, dict]:
        index: dict[str, dict] = {}
        for ds in datasources:
            for raw_key in self._extract_endpoint_keys(ds):
                key = self._normalize_endpoint_key(raw_key)
                if key:
                    index[key] = ds
        return index
    def _extract_endpoint_keys(self, ds: dict[str, Any]) -> set[str]:
        keys: set[str] = set()
        if not isinstance(ds, dict):
            return keys

        debug = ds.get("debug") or {}
        debug_endpoint = debug.get("endpoint") if isinstance(debug, dict) else None
        name = ds.get("name")
        source_url = (ds.get("source") or {}).get("url")

        for candidate in (debug_endpoint, name):
            if isinstance(candidate, str) and candidate.strip():
                keys.add(candidate.strip())

        if isinstance(source_url, str) and source_url.strip():
            path = urlparse(source_url).path.strip("/")
            if path:
                keys.add(path)
                # last segment alias is convenient for frontend routing
                keys.add(path.split("/")[-1])
            keys.add(source_url.strip())

        return keys
    @staticmethod
    def _normalize_endpoint_key(value: str) -> str:
        cleaned = (value or "").strip().strip("/")
        return cleaned
    @staticmethod
    def _quote_ident(value: str) -> str:
        escaped = str(value).replace('"', '""')
        return f'"{escaped}"'
    @staticmethod
    def _pick_primary_value_col(mapping_table, exclude: set[str] | None = None) -> str | None:
        skip = (exclude or set()) | {"way_id", "id", "geom", "geometry", "line_geometry"}
        for col in mapping_table.columns:
            if col.name.lower() not in skip and "geom" not in col.name.lower():
                return col.name
        return None
    @staticmethod
    def _guess_geom_col(table, candidates: list[str]) -> str | None:
        if table is None:
            return None
        col_names = {c.name.lower(): c.name for c in table.columns}
        for c in candidates:
            if c.lower() in col_names:
                return col_names[c.lower()]
        return None
    @staticmethod
    def _try_json_load(value: Any) -> Any:
        if not isinstance(value, str):
            return value
        try:
            import json

            return json.loads(value)
        except Exception:
            return value
    def _to_jsonable(self, value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, (str, int, float, bool)):
            return value
        if isinstance(value, (datetime, date)):
            return value.isoformat()
        if isinstance(value, Decimal):
            return float(value)
        if isinstance(value, UUID):
            return str(value)
        if isinstance(value, Path):
            return str(value)
        if isinstance(value, dict):
            return {str(k): self._to_jsonable(v) for k, v in value.items()}
        if isinstance(value, (list, tuple, set)):
            return [self._to_jsonable(v) for v in value]
        return str(value)
