from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from uuid import UUID

from sqlalchemy import select

from database.db_instancce import DbInstance


class TableTarget:
    STAGING = "staging"
    ENRICHMENT = "enrichment"
    MAPPING = "mapping"

    @classmethod
    def values(cls) -> set[str]:
        return {cls.STAGING, cls.ENRICHMENT, cls.MAPPING}


class DebugMapperService:
    """
    Resolve datasource by a debug endpoint key and fetch table data dynamically.

    Endpoint key resolution priority:
    1. datasource.debug.endpoint (if provided in config)
    2. datasource.name
    3. path fragment from datasource.source.url (e.g. "/weather")
    """

    def __init__(self, datasources: list[dict] | None, db: DbInstance | None):
        self.datasources = datasources or []
        self.db = db
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
                    "endpoint_keys": endpoint_keys,
                }
            )
        return items

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

    def _mapping_overview(self, ds: dict[str, Any]) -> dict[str, Any]:
        mapping = ds.get("mapping") or {}
        strategy = mapping.get("strategy")
        strategy_name = strategy.get("name") if isinstance(strategy, dict) else strategy
        strategy_type = strategy.get("type") if isinstance(strategy, dict) else None
        link_on = strategy.get("link_on") if isinstance(strategy, dict) else None

        return {
            "enabled": mapping.get("enable", False),
            "table_schema": mapping.get("table_schema"),
            "table_name": mapping.get("table_name"),
            "joins_on": mapping.get("joins_on"),
            "strategy_name": strategy_name,
            "strategy_type": strategy_type,
            "link_on": link_on,
            "base_table": mapping.get("base_table"),
            "visualization_hint": {
                "primary_key_for_frontend": "way_id",
                "note": "Use way_id to connect mapping rows to base table geometry for map visualization.",
            },
        }

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
