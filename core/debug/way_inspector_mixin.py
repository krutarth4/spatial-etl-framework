"""Per-way inspection and nearest-way lookup for the debug API.

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


class WayInspectorMixin:
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
