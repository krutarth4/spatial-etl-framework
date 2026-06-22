"""Mapping coverage / visualization / SQL resolution for the debug API.

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


class MappingInspectorMixin:
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

        base_geom_col = self._guess_geom_col(
            base_table, ["geometry", "geom", "line_geometry", "geometry_25833", "geom_25833"]
        )
        # Detect point OR line enrichment geometry, in 4326 or 25833.
        enrich_geom_col = self._guess_geom_col(
            enrichment_table,
            ["point", "geom_4326", "geometry_4326", "geometry", "geom", "geometry_25833", "geom_25833"],
        )

        # Resolve the join key linking mapping rows to enrichment rows.
        # Configured strategies provide it via link_on/joins_on; custom strategies
        # (e.g. pleasant_bicycling) declare neither, so fall back to a column shared
        # by both the mapping and enrichment tables (e.g. connection_id).
        if enrichment_table is not None and (
            not mapping_column
            or mapping_column not in mapping_table.c
            or mapping_column not in enrichment_table.c
        ):
            mapping_column = self._shared_join_col(mapping_table, enrichment_table) or mapping_column

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

        can_base_join = (
            not can_spatial_join
            and base_table is not None
            and base_geom_col is not None
            and "way_id" in mapping_table.c
        )

        if can_spatial_join:
            bgeom = f"b.{self._quote_ident(base_geom_col)}"
            egeom = f"e.{self._quote_ident(enrich_geom_col)}"
            # Distance + connector are computed in metric CRS (25833) so they work
            # for point- AND line-enrichment regardless of the source SRID;
            # geometries are emitted as WGS84 for Leaflet. ST_Transform on an
            # already-4326 column is a no-op, so this is safe for every mapper.
            b25 = f"ST_Transform({bgeom}, 25833)"
            e25 = f"ST_Transform({egeom}, 25833)"
            dist_col = next(
                (c for c in ("distance", "distance_m", "distance_meters") if c in mapping_table.c), None
            )
            dist_expr = (
                f"COALESCE(m.{self._quote_ident(dist_col)}, ST_Distance({b25}, {e25}))"
                if dist_col
                else f"ST_Distance({b25}, {e25})"
            )
            where_clause = "WHERE m.way_id = :way_id" if way_id is not None else ""
            # Line enrichment tables hold many rows per join key (e.g. 24 hourly
            # rows per connection_id); DISTINCT ON keeps the nearest one per way.
            sql = f"""
                SELECT DISTINCT ON (m.way_id)
                    m.way_id,
                    m.{self._quote_ident(mapping_column)} AS mapped_value,
                    {dist_expr} AS distance_meters,
                    ST_AsGeoJSON(ST_Transform({bgeom}, 4326)) AS base_geometry,
                    ST_AsGeoJSON(ST_Transform({egeom}, 4326)) AS mapped_geometry,
                    ST_AsGeoJSON(ST_Transform(ST_ShortestLine({b25}, {e25}), 4326)) AS link_geometry
                FROM "{mapping_schema}"."{mapping_table_name}" m
                JOIN "{base_table_schema}"."{base_table_name}" b
                    ON b.id = m.way_id
                LEFT JOIN "{enrichment_table_schema}"."{enrichment_table_name}" e
                    ON e.{self._quote_ident(mapping_column)} = m.{self._quote_ident(mapping_column)}
                {where_clause}
                ORDER BY m.way_id, {dist_expr} NULLS LAST
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
    def fetch_coverage_visualization(
        self,
        mapper_endpoint: str,
        bbox: str | None = None,
        limit: int = 5000,
    ) -> dict[str, Any]:
        """Classify base road segments as covered vs uncovered for the map.

        A segment is *covered* when the mapping table holds a non-null real
        value for it; otherwise it is *uncovered* (it has no row, or a null
        value, and would only ever receive a default/sentinel in the
        materialized view). The geometry comes from the base network so the
        map shows exactly where real data is missing.
        """
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
        if "way_id" not in mapping_table.c:
            raise ValueError("Mapping table has no 'way_id' column to join on.")

        primary_col = self._coverage_value_col(ds, mapping_table)
        if not primary_col:
            raise ValueError("Could not determine a mapped value column on the mapping table.")

        base_conf = mapping.get("base_table") or {}
        base_name = base_conf.get("table_name")
        base_schema = base_conf.get("table_schema")
        base_table = self.db.get_table(base_name, base_schema) if base_name else None
        if base_table is None:
            raise ValueError("Base road network table is not configured or does not exist.")
        base_geom_col = self._guess_geom_col(
            base_table, ["geometry", "geom", "line_geometry", "geometry_25833", "geom_25833"]
        )
        if base_geom_col is None:
            raise ValueError("No geometry column found on the base road network table.")

        envelope = self._parse_bbox(bbox)
        bgeom = f"b.{self._quote_ident(base_geom_col)}"
        where = ""
        params: dict[str, Any] = {"limit": limit}
        if envelope is not None:
            where = (
                f"WHERE ST_Intersects(ST_Transform({bgeom}, 4326), "
                f"ST_MakeEnvelope(:minx, :miny, :maxx, :maxy, 4326))"
            )
            params.update(envelope)

        sql = f"""
            SELECT
                b.id AS way_id,
                (cov.way_id IS NOT NULL) AS covered,
                ST_AsGeoJSON(ST_Transform({bgeom}, 4326)) AS __geojson
            FROM "{base_schema}"."{base_name}" b
            LEFT JOIN (
                SELECT DISTINCT way_id
                FROM "{mapping_schema}"."{mapping_table_name}"
                WHERE "{primary_col}" IS NOT NULL
            ) cov ON cov.way_id = b.id
            {where}
            LIMIT :limit
        """
        with self.db.session_scope() as session:
            result = session.execute(text(sql), params).mappings().all()

        features: list[dict[str, Any]] = []
        shown_covered = 0
        for r in result:
            geom = self._try_json_load(r.get("__geojson"))
            if not geom:
                continue
            is_covered = bool(r.get("covered"))
            if is_covered:
                shown_covered += 1
            features.append({
                "type": "Feature",
                "geometry": geom,
                "properties": {"way_id": r.get("way_id"), "covered": is_covered},
            })

        return {
            "mapper_endpoint": mapper_endpoint,
            "datasource": ds.get("name"),
            "value_column": primary_col,
            "base_table": base_name,
            "bbox": envelope,
            "shown": len(features),
            "shown_covered": shown_covered,
            "shown_uncovered": len(features) - shown_covered,
            # Network-wide totals (independent of the limit/bbox above).
            "coverage": self._mapping_coverage(ds),
            "geojson": {"type": "FeatureCollection", "features": features},
        }

    def _coverage_value_col(self, ds: dict[str, Any], mapping_table) -> str | None:
        """Pick the mapping-table column that holds the real mapped value.

        Prefers what the datasource declares in its strategy, so coverage tests
        the actual value instead of an incidental column (e.g. a distance
        alias). Resolution order, each guarded by existence in the mapping
        table:
          1. strategy.value_columns (e.g. air quality: no2/pm10/pm25)
          2. strategy.aggregation_alias (e.g. tree: trees)
          3. base_table.column_name (semantic label, used only if it maps to a
             real column)
          4. fall back to the first non-id/non-geom column, excluding the
             strategy's distance alias.
        """
        cols = {c.name.lower(): c.name for c in mapping_table.columns}
        mapping = ds.get("mapping") or {}
        strategy = mapping.get("strategy")

        candidates: list[str] = []
        distance_alias: str | None = None
        if isinstance(strategy, dict):
            for vc in strategy.get("value_columns") or []:
                if isinstance(vc, dict) and vc.get("name"):
                    candidates.append(str(vc["name"]))
                elif isinstance(vc, str):
                    candidates.append(vc)
            if strategy.get("aggregation_alias"):
                candidates.append(str(strategy["aggregation_alias"]))
            if strategy.get("distance_alias"):
                distance_alias = str(strategy["distance_alias"]).lower()

        base_col = (mapping.get("base_table") or {}).get("column_name")
        if isinstance(base_col, str) and base_col:
            candidates.append(base_col)

        for cand in candidates:
            if cand.lower() in cols:
                return cols[cand.lower()]

        exclude = {distance_alias} if distance_alias else None
        return self._pick_primary_value_col(mapping_table, exclude=exclude)

    def _mapping_coverage(self, ds: dict[str, Any]) -> dict[str, Any] | None:
        """Coverage of the road network by *real* mapped data.

        Covered roads are road segments with a non-null mapped value in the
        mapping table. The denominator is the full base road network
        (``ways_base``), not the mapping table's own row count, so that roads
        which only receive a default/sentinel value in the materialized view
        (and therefore never get a row in the mapping table) are correctly
        reported as uncovered. Defaults live only in the MV, so the mapping
        table itself already excludes them — counting its real rows against the
        base network gives an honest coverage figure.
        """
        mapping = ds.get("mapping") or {}
        if not mapping.get("enable", False):
            return None
        table_name = mapping.get("table_name")
        table_schema = mapping.get("table_schema")
        if not table_name or not table_schema or self.db is None:
            return None
        mapping_table = self.db.get_table(table_name, table_schema)
        if mapping_table is None:
            return None
        primary_col = self._coverage_value_col(ds, mapping_table)
        if not primary_col:
            return None

        base_conf = mapping.get("base_table") or {}
        base_name = base_conf.get("table_name")
        base_schema = base_conf.get("table_schema")
        # Without a resolvable base table we cannot size the network; bail rather
        # than report a misleading mapping-table-relative percentage.
        if not base_name or not base_schema:
            return None
        join_col = "way_id" if "way_id" in mapping_table.c else None
        if join_col is None:
            return None

        try:
            with self.db.session_scope() as session:
                # Total road segments in the network.
                total = int(
                    session.execute(
                        text(f'SELECT COUNT(*) FROM "{base_schema}"."{base_name}"')
                    ).scalar()
                    or 0
                )
                # Distinct segments that carry a real (non-null) mapped value.
                # DISTINCT guards against strategies that emit several rows per
                # segment (reduce: none).
                covered = int(
                    session.execute(
                        text(
                            f'SELECT COUNT(DISTINCT "{join_col}") '
                            f'FROM "{table_schema}"."{table_name}" '
                            f'WHERE "{primary_col}" IS NOT NULL'
                        )
                    ).scalar()
                    or 0
                )
            covered = min(covered, total)
            uncovered = max(total - covered, 0)
            return {
                "total": total,
                "covered": covered,
                "uncovered": uncovered,
                "covered_pct": round(covered / total * 100, 1) if total > 0 else 0.0,
                "value_column": primary_col,
                "base_table": base_name,
            }
        except Exception:
            return None
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
        # Delegate to the same builder the production ETL path uses so the preview
        # matches the SQL that will actually run (including inferred insert specs).
        try:
            from main_core.mapping_sql_builder import (
                build_mapping_query,
                mapping_select_sql_strategy_registry,
            )

            strategy = mapping_select_sql_strategy_registry.get(strategy_type)
            if strategy is None:
                return None
            return build_mapping_query(adapter, strategy)
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
