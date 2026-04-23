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

from database.db_instancce import DbInstance
from metadata.data_source_metadata_repository import DataSourceMetadataRepository


class _StorageRef:
    """Duck-typed storage reference for mapping SQL builder."""
    def __init__(self, d: dict | None):
        self.table_name = (d or {}).get("table_name")
        self.table_schema = (d or {}).get("table_schema")


class _StorageConf:
    """Duck-typed storage configuration."""
    def __init__(self, storage: dict):
        enr = storage.get("enrichment")
        stg = storage.get("staging")
        self.enrichment = _StorageRef(enr) if enr else None
        self.staging = _StorageRef(stg) if stg else None


class _MappingConf:
    """Duck-typed mapping configuration."""
    def __init__(self, mapping: dict):
        base = mapping.get("base_table") or {}
        self.table_name = mapping.get("table_name")
        self.table_schema = mapping.get("table_schema")
        self.joins_on = mapping.get("joins_on")
        self.strategy = mapping.get("strategy")
        self.config = mapping.get("config") or {}
        self.base_table = _StorageRef(base)


class _DataSourceConfigAdapter:
    """Wraps raw datasource dict to provide interface expected by mapping SQL strategies."""
    def __init__(self, ds: dict):
        self.data_source_name = ds.get("name")
        self.data_source_config = SimpleNamespace(
            mapping=_MappingConf(ds.get("mapping") or {}),
            storage=_StorageConf(ds.get("storage") or {}),
        )

    def get_mapping_strategy_type(self) -> str | None:
        """Extract strategy type from mapping config."""
        strategy = self.data_source_config.mapping.strategy
        if strategy is None:
            return None
        if isinstance(strategy, str):
            return strategy
        if isinstance(strategy, dict):
            return str(strategy.get("type") or strategy.get("name") or "")
        return None

    def get_mapping_strategy_link_fields(self) -> dict:
        """Extract link field configuration."""
        mapping_conf = self.data_source_config.mapping
        joins_on = mapping_conf.joins_on
        strategy = mapping_conf.strategy
        link_on = strategy.get("link_on") if isinstance(strategy, dict) else None
        mapping_column = (link_on or {}).get("mapping_column") if isinstance(link_on, dict) else None
        base_column = (link_on or {}).get("base_column") if isinstance(link_on, dict) else None
        basis = (link_on or {}).get("basis") if isinstance(link_on, dict) else None
        return {
            "mapping_column": str(mapping_column) if mapping_column else (str(joins_on) if joins_on else None),
            "base_column": str(base_column) if base_column else None,
            "basis": str(basis) if basis else None,
        }

    def get_mapping_config(self) -> dict:
        """Get mapping config dict."""
        config = self.data_source_config.mapping.config
        return config if isinstance(config, dict) else {}

    def _get_insert_spec(self):
        """Extract insert specification from mapping config."""
        from main_core.mapping_sql_builder import MappingInsertSpec

        insert_conf = self.get_mapping_config().get("insert")
        if not isinstance(insert_conf, dict):
            return None

        columns = insert_conf.get("columns") or []
        conflict_columns = insert_conf.get("conflict_columns")
        update_columns = insert_conf.get("update_columns")

        return MappingInsertSpec(
            columns=[str(column) for column in columns],
            conflict_columns=[str(column) for column in conflict_columns] if conflict_columns else None,
            update_columns=[str(column) for column in update_columns] if update_columns else None,
        )


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
                    "metadata": self._fetch_metadata_row(source_name),
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
            },
            "metadata": metadata,
            "tables": self._build_table_overview(ds),
            "mapping_overview": self._mapping_overview(ds),
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

    def fetch_mapping_visualization(
        self,
        mapper_endpoint: str,
        limit: int = 100,
        way_id: int | None = None,
    ) -> dict[str, Any]:
        if self.db is None:
            raise ValueError("Database is not initialized.")
        if limit <= 0:
            raise ValueError("limit must be > 0")

        ds = self._resolve_datasource(mapper_endpoint)
        mapping = ds.get("mapping") or {}
        if not mapping.get("enable", False):
            raise ValueError(f"Mapping is disabled for datasource '{ds.get('name')}'.")

        mapping_table_name = mapping.get("table_name")
        mapping_schema = mapping.get("table_schema")
        if not mapping_table_name:
            raise ValueError(f"No mapping table configured for datasource '{ds.get('name')}'.")

        mapping_table = self.db.get_table(mapping_table_name, mapping_schema)
        if mapping_table is None:
            raise ValueError(f"Mapping table '{mapping_schema}.{mapping_table_name}' does not exist.")

        base_table_conf = mapping.get("base_table") or {}
        base_table_name = base_table_conf.get("table_name")
        base_table_schema = base_table_conf.get("table_schema")
        enrichment = (ds.get("storage") or {}).get("enrichment") or {}
        enrichment_table_name = enrichment.get("table_name")
        enrichment_table_schema = enrichment.get("table_schema")

        base_table = self.db.get_table(base_table_name, base_table_schema) if base_table_name else None
        enrichment_table = (
            self.db.get_table(enrichment_table_name, enrichment_table_schema) if enrichment_table_name else None
        )

        strategy = mapping.get("strategy")
        link_on = (strategy.get("link_on") if isinstance(strategy, dict) else None) or {}
        mapping_column = link_on.get("mapping_column") or mapping.get("joins_on")
        basis = link_on.get("basis")
        strategy_name = strategy.get("name") if isinstance(strategy, dict) else strategy
        strategy_type = strategy.get("type") if isinstance(strategy, dict) else None

        features: list[dict[str, Any]] = []
        rows: list[dict[str, Any]] = []
        visualization_mode = "table_only"

        base_geom_col = self._guess_geom_col(base_table, ["geometry", "geom", "line_geometry"])
        enrich_geom_col = self._guess_geom_col(enrichment_table, ["point", "geometry", "geom"])
        can_spatial_join = (
            base_table is not None
            and enrichment_table is not None
            and base_geom_col is not None
            and enrich_geom_col is not None
            and mapping_column in mapping_table.c
            and mapping_column in enrichment_table.c
            and "way_id" in mapping_table.c
        )

        can_base_join = (
            not can_spatial_join
            and base_table is not None
            and base_geom_col is not None
            and "way_id" in mapping_table.c
        )

        if can_spatial_join:
            sql = f"""
                SELECT
                    m.way_id,
                    m.{self._quote_ident(mapping_column)} AS mapped_value,
                    COALESCE(
                        m.distance,
                        ST_Distance(
                            b.{self._quote_ident(base_geom_col)}::geography,
                            e.{self._quote_ident(enrich_geom_col)}::geography
                        )
                    ) AS distance_meters,
                    ST_AsGeoJSON(b.{self._quote_ident(base_geom_col)}) AS base_geometry,
                    ST_AsGeoJSON(e.{self._quote_ident(enrich_geom_col)}) AS mapped_geometry,
                    ST_AsGeoJSON(
                        ST_ShortestLine(
                            b.{self._quote_ident(base_geom_col)},
                            e.{self._quote_ident(enrich_geom_col)}
                        )
                    ) AS link_geometry
                FROM "{mapping_schema}"."{mapping_table_name}" m
                JOIN "{base_table_schema}"."{base_table_name}" b
                    ON b.id = m.way_id
                LEFT JOIN "{enrichment_table_schema}"."{enrichment_table_name}" e
                    ON e.{self._quote_ident(mapping_column)} = m.{self._quote_ident(mapping_column)}
                {f"WHERE m.way_id = :way_id" if way_id is not None else ""}
                LIMIT :limit
            """
            params: dict[str, Any] = {"limit": limit}
            if way_id is not None:
                params["way_id"] = way_id
            with self.db.session_scope() as session:
                query_result = session.execute(text(sql), params).mappings().all()

            rows = [self._to_jsonable(dict(r)) for r in query_result]
            visualization_mode = "spatial_line_to_point"
            for row in rows:
                reason = (
                    f"Mapped by strategy={strategy_name or 'none'}"
                    f", type={strategy_type or 'default'}"
                    f", basis={basis or 'config_not_provided'}"
                    f", distance_meters={row.get('distance_meters')}"
                )
                base_geom = self._try_json_load(row.get("base_geometry"))
                feature = {
                    "type": "Feature",
                    "geometry": base_geom or self._try_json_load(row.get("link_geometry")),
                    "properties": {
                        "way_id": row.get("way_id"),
                        "mapped_value": row.get("mapped_value"),
                        "distance_meters": row.get("distance_meters"),
                        "strategy_name": strategy_name,
                        "strategy_type": strategy_type,
                        "basis": basis,
                        "reason": reason,
                        "link_geometry": self._try_json_load(row.get("link_geometry")),
                        "mapped_geometry": self._try_json_load(row.get("mapped_geometry")),
                    },
                }
                features.append(feature)
        elif can_base_join:
            primary_col = self._pick_primary_value_col(mapping_table)
            sql = f"""
                SELECT
                    m.*,
                    ST_AsGeoJSON(b.{self._quote_ident(base_geom_col)}) AS base_geometry
                FROM "{mapping_schema}"."{mapping_table_name}" m
                JOIN "{base_table_schema}"."{base_table_name}" b ON b.id = m.way_id
                {f"WHERE m.way_id = :way_id" if way_id is not None else ""}
                LIMIT :limit
            """
            params2: dict[str, Any] = {"limit": limit}
            if way_id is not None:
                params2["way_id"] = way_id
            with self.db.session_scope() as session:
                query_result = session.execute(text(sql), params2).mappings().all()

            rows = [self._to_jsonable(dict(r)) for r in query_result]
            visualization_mode = "base_geometry_only"
            for row in rows:
                base_geom = self._try_json_load(row.get("base_geometry"))
                props = {k: v for k, v in row.items() if k != "base_geometry"}
                if primary_col:
                    props["mapped_value"] = row.get(primary_col)
                feature = {
                    "type": "Feature",
                    "geometry": base_geom,
                    "properties": props,
                }
                features.append(feature)
        else:
            with self.db.session_scope() as session:
                query_result = session.execute(select(mapping_table).limit(limit)).mappings().all()
            rows = [self._to_jsonable(dict(r)) for r in query_result]

        return {
            "mapper_endpoint": mapper_endpoint,
            "datasource": ds.get("name"),
            "visualization_mode": visualization_mode,
            "strategy": {
                "name": strategy_name,
                "type": strategy_type,
                "basis": basis,
                "link_on": link_on,
            },
            "count": len(rows),
            "rows": rows,
            "geojson": {
                "type": "FeatureCollection",
                "features": features,
            },
            "notes": {
                "why_mapping_explanation": "Use each feature.properties.reason directly in the frontend tooltip.",
                "fallback": "If spatial fields are missing, API returns mapping rows only (table_only mode).",
            },
        }

    def fetch_way_inspector(
        self,
        mapper_endpoint: str,
        way_id: int | None = None,
    ) -> dict[str, Any]:
        if self.db is None:
            raise ValueError("Database is not initialized.")

        ds = self._resolve_datasource(mapper_endpoint)
        mapping = ds.get("mapping") or {}
        if not mapping.get("enable", False):
            raise ValueError(f"Mapping is disabled for datasource '{ds.get('name')}'.")

        mapping_table_name = mapping.get("table_name")
        mapping_schema = mapping.get("table_schema")
        if not mapping_table_name:
            raise ValueError(f"No mapping table configured for datasource '{ds.get('name')}'.")

        mapping_table = self.db.get_table(mapping_table_name, mapping_schema)
        if mapping_table is None:
            raise ValueError(f"Mapping table '{mapping_schema}.{mapping_table_name}' does not exist.")

        base_table_conf = mapping.get("base_table") or {}
        base_table_name = base_table_conf.get("table_name")
        base_table_schema = base_table_conf.get("table_schema")
        enrichment = (ds.get("storage") or {}).get("enrichment") or {}
        enrichment_table_name = enrichment.get("table_name")
        enrichment_table_schema = enrichment.get("table_schema")

        base_table = self.db.get_table(base_table_name, base_table_schema) if base_table_name else None
        enrichment_table = (
            self.db.get_table(enrichment_table_name, enrichment_table_schema) if enrichment_table_name else None
        )

        strategy = mapping.get("strategy")
        link_on = (strategy.get("link_on") if isinstance(strategy, dict) else None) or {}
        mapping_column = link_on.get("mapping_column") or mapping.get("joins_on")
        basis = link_on.get("basis")
        strategy_name = strategy.get("name") if isinstance(strategy, dict) else strategy
        strategy_type = strategy.get("type") if isinstance(strategy, dict) else None

        is_random = False
        if way_id is None:
            random_sql = f'SELECT way_id FROM "{mapping_schema}"."{mapping_table_name}" ORDER BY RANDOM() LIMIT 1'
            with self.db.session_scope() as session:
                rand_row = session.execute(text(random_sql)).mappings().first()
            if rand_row is None:
                raise ValueError(f"Mapping table '{mapping_schema}.{mapping_table_name}' is empty.")
            way_id = rand_row["way_id"]
            is_random = True

        base_geom_col = self._guess_geom_col(base_table, ["geometry", "geom", "line_geometry"])
        enrich_geom_col = self._guess_geom_col(enrichment_table, ["point", "geometry", "geom"])

        can_spatial_join = (
            base_table is not None
            and enrichment_table is not None
            and base_geom_col is not None
            and enrich_geom_col is not None
            and mapping_column is not None
            and mapping_column in mapping_table.c
            and mapping_column in enrichment_table.c
            and "way_id" in mapping_table.c
        )

        mapping_record: dict[str, Any] | None = None
        base_record: dict[str, Any] | None = None
        link_geometry = None
        base_geometry = None
        mapped_geometry = None
        mapped_value = None

        can_base_join = (
            not can_spatial_join
            and base_table is not None
            and base_geom_col is not None
            and "way_id" in mapping_table.c
        )

        if can_spatial_join:
            sql = f"""
                SELECT
                    m.*,
                    b.id AS base_id,
                    b.length_m AS base_length_m,
                    b.from_node_id AS base_from_node_id,
                    b.to_node_id AS base_to_node_id,
                    ST_AsGeoJSON(b.{self._quote_ident(base_geom_col)}) AS base_geometry,
                    ST_AsGeoJSON(e.{self._quote_ident(enrich_geom_col)}) AS mapped_geometry,
                    ST_AsGeoJSON(
                        ST_ShortestLine(
                            b.{self._quote_ident(base_geom_col)},
                            e.{self._quote_ident(enrich_geom_col)}
                        )
                    ) AS link_geometry
                FROM "{mapping_schema}"."{mapping_table_name}" m
                JOIN "{base_table_schema}"."{base_table_name}" b ON b.id = m.way_id
                LEFT JOIN "{enrichment_table_schema}"."{enrichment_table_name}" e
                    ON e.{self._quote_ident(mapping_column)} = m.{self._quote_ident(mapping_column)}
                WHERE m.way_id = :way_id
                LIMIT 1
            """
            with self.db.session_scope() as session:
                row = session.execute(text(sql), {"way_id": way_id}).mappings().first()

            if row is None:
                raise ValueError(f"No mapping row found for way_id={way_id}.")

            row_dict = self._to_jsonable(dict(row))
            base_geometry = self._try_json_load(row_dict.pop("base_geometry", None))
            mapped_geometry = self._try_json_load(row_dict.pop("mapped_geometry", None))
            link_geometry = self._try_json_load(row_dict.pop("link_geometry", None))

            base_record = {
                "id": row_dict.pop("base_id", None),
                "length_m": row_dict.pop("base_length_m", None),
                "from_node_id": row_dict.pop("base_from_node_id", None),
                "to_node_id": row_dict.pop("base_to_node_id", None),
            }
            mapping_record = row_dict
            mapped_value = mapping_record.get(mapping_column)
        elif can_base_join:
            sql = f"""
                SELECT
                    m.*,
                    ST_AsGeoJSON(b.{self._quote_ident(base_geom_col)}) AS base_geometry
                FROM "{mapping_schema}"."{mapping_table_name}" m
                JOIN "{base_table_schema}"."{base_table_name}" b ON b.id = m.way_id
                WHERE m.way_id = :way_id
                LIMIT 1
            """
            with self.db.session_scope() as session:
                row = session.execute(text(sql), {"way_id": way_id}).mappings().first()

            if row is None:
                raise ValueError(f"No mapping row found for way_id={way_id}.")

            row_dict = self._to_jsonable(dict(row))
            base_geometry = self._try_json_load(row_dict.pop("base_geometry", None))
            mapping_record = row_dict
            primary_col = self._pick_primary_value_col(mapping_table)
            mapped_value = mapping_record.get(primary_col) if primary_col else None
            if primary_col:
                mapping_record["mapped_value"] = mapped_value
        else:
            with self.db.session_scope() as session:
                row = session.execute(
                    select(mapping_table).where(mapping_table.c.way_id == way_id).limit(1)
                ).mappings().first()
            if row is None:
                raise ValueError(f"No mapping row found for way_id={way_id}.")
            mapping_record = self._to_jsonable(dict(row))
            mapped_value = mapping_record.get(mapping_column) if mapping_column else None

        enrichment_record: dict[str, Any] | None = None
        if (
            enrichment_table is not None
            and mapping_column
            and mapping_column in enrichment_table.c
            and mapped_value is not None
        ):
            with self.db.session_scope() as session:
                enr_row = session.execute(
                    select(enrichment_table).where(
                        enrichment_table.c[mapping_column] == mapped_value
                    ).limit(1)
                ).mappings().first()
            if enr_row is not None:
                enrichment_record = self._to_jsonable(dict(enr_row))

        distance_meters = None
        if mapping_record:
            distance_meters = mapping_record.get("distance") or mapping_record.get("distance_meters")

        return {
            "way_id": way_id,
            "is_random": is_random,
            "strategy": {
                "name": strategy_name,
                "type": strategy_type,
                "basis": basis,
            },
            "mapping_record": mapping_record,
            "enrichment_record": enrichment_record,
            "base_record": base_record,
            "geojson_feature": {
                "type": "Feature",
                "geometry": base_geometry or link_geometry,
                "properties": {
                    "way_id": way_id,
                    "mapped_value": mapped_value,
                    "distance_meters": distance_meters,
                    "link_geometry": link_geometry,
                    "mapped_geometry": mapped_geometry,
                },
            },
        }

    def fetch_nearest_way(
        self,
        mapper_endpoint: str,
        lat: float,
        lng: float,
    ) -> dict[str, Any]:
        if self.db is None:
            raise ValueError("Database is not initialized.")

        ds = self._resolve_datasource(mapper_endpoint)
        mapping = ds.get("mapping") or {}
        if not mapping.get("enable", False):
            raise ValueError(f"Mapping is disabled for datasource '{ds.get('name')}'.")

        mapping_table_name = mapping.get("table_name")
        mapping_schema = mapping.get("table_schema")
        if not mapping_table_name:
            raise ValueError(f"No mapping table configured for datasource '{ds.get('name')}'.")

        mapping_table = self.db.get_table(mapping_table_name, mapping_schema)
        if mapping_table is None:
            raise ValueError(f"Mapping table '{mapping_schema}.{mapping_table_name}' does not exist.")

        if "way_id" not in mapping_table.c:
            raise ValueError(f"Mapping table '{mapping_schema}.{mapping_table_name}' has no way_id column.")

        base_table_conf = mapping.get("base_table") or {}
        base_table_name = base_table_conf.get("table_name")
        base_table_schema = base_table_conf.get("table_schema")
        base_table = self.db.get_table(base_table_name, base_table_schema) if base_table_name else None

        if base_table is None:
            raise ValueError(f"No base table configured or found for datasource '{ds.get('name')}'.")

        base_geom_col = self._guess_geom_col(base_table, ["geometry", "geom", "line_geometry"])
        if base_geom_col is None:
            raise ValueError(f"No geometry column found in base table '{base_table_schema}.{base_table_name}'.")

        sql = f"""
            SELECT m.way_id
            FROM "{mapping_schema}"."{mapping_table_name}" m
            JOIN "{base_table_schema}"."{base_table_name}" b ON b.id = m.way_id
            ORDER BY b.{self._quote_ident(base_geom_col)}::geography
                <-> ST_SetSRID(ST_MakePoint(:lng, :lat), 4326)::geography
            LIMIT 1
        """
        with self.db.session_scope() as session:
            row = session.execute(text(sql), {"lat": lat, "lng": lng}).mappings().first()

        if row is None:
            raise ValueError(f"No ways found in mapping table '{mapping_schema}.{mapping_table_name}'.")

        return {"way_id": int(row["way_id"])}

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
            "resolved_mapping_sql": self._resolve_mapping_sql(ds),
        }

    def _resolve_mapping_sql(self, ds: dict[str, Any]) -> str | None:
        """Reconstruct and return the actual SQL that will be executed for mapping."""
        mapping = ds.get("mapping") or {}
        if not mapping.get("enable", False):
            return None

        adapter = _DataSourceConfigAdapter(ds)
        strategy_type = (adapter.get_mapping_strategy_type() or "custom").lower()

        # Path 1: none — no mapping
        if strategy_type == "none":
            return None

        # Path 2: sql_template — resolve template variables into final SQL
        if strategy_type == "sql_template":
            config = adapter.get_mapping_config()
            sql = config.get("sql")
            if not sql:
                return None
            context = self._build_template_context(adapter)
            try:
                return str(sql).format(**context)
            except Exception:
                return str(sql)  # return raw if template substitution fails

        # Path 3: custom / mapper_sql — cannot resolve without mapper instance
        if strategy_type in {"custom", "mapper_sql"}:
            return None

        # Path 4: registry strategy (knn, nearest_neighbour, within_distance, etc.)
        try:
            from main_core.mapping_sql_builder import mapping_select_sql_strategy_registry, MappingInsertBuilder

            strategy = mapping_select_sql_strategy_registry.get(strategy_type)
            if strategy is None:
                return None

            select_sql = strategy.build_select(adapter)
            insert_spec = adapter._get_insert_spec()
            if insert_spec is None:
                return select_sql

            builder = MappingInsertBuilder()
            mapping_conf = adapter.data_source_config.mapping
            return builder.build_insert(mapping_conf, select_sql, insert_spec)
        except Exception as exc:
            return f"-- SQL generation failed: {exc}"

    def _build_template_context(self, adapter: _DataSourceConfigAdapter) -> dict[str, str | None]:
        """Build template variable context for sql_template mapping strategy."""
        mapping = adapter.data_source_config.mapping
        storage = adapter.data_source_config.storage
        base = mapping.base_table
        link_fields = adapter.get_mapping_strategy_link_fields()
        strategy_type = adapter.get_mapping_strategy_type()

        # Handle cases where staging or enrichment might not be defined
        staging_table = None
        staging_schema = None
        if storage.staging:
            staging_table = storage.staging.table_name
            staging_schema = storage.staging.table_schema

        enrichment_table = None
        enrichment_schema = None
        if storage.enrichment:
            enrichment_table = storage.enrichment.table_name
            enrichment_schema = storage.enrichment.table_schema
        elif storage.staging:
            # Fallback: if no enrichment, use staging as enrichment
            enrichment_table = storage.staging.table_name
            enrichment_schema = storage.staging.table_schema

        return {
            "datasource_name": adapter.data_source_name,
            "mapping_table": mapping.table_name,
            "mapping_schema": mapping.table_schema,
            "staging_table": staging_table,
            "staging_schema": staging_schema,
            "enrichment_table": enrichment_table,
            "enrichment_schema": enrichment_schema,
            "base_table": base.table_name,
            "base_schema": base.table_schema,
            "joins_on": mapping.joins_on,
            "strategy_type": strategy_type,
            "link_mapping_column": link_fields.get("mapping_column"),
            "link_base_column": link_fields.get("base_column"),
            "link_basis": link_fields.get("basis"),
        }

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
                    row_count = self.db.get_table_count(table_name, table_schema)
                except Exception:
                    row_count = None

        return {
            "enabled": enabled,
            "schema": table_schema,
            "name": table_name,
            "exists": exists,
            "row_count": row_count,
        }

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
