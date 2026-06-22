"""Enrichment-table geometry visualization for the debug API.

Mixin composed by DebugMapperService (core/debug_mapper_service.py).

Unlike MappingInspectorMixin (which shows the final values mapped onto
ways_base), this renders the *raw* enrichment-table geometry directly —
points, linestrings, polygons or raster — clipped to a user-drawn bounding
box.  Everything is transformed to EPSG:4326 for the frontend (Leaflet).
"""
from __future__ import annotations

from typing import Any

from sqlalchemy.sql import text


# Geometry columns we know how to read across the enrichment tables.
# Order matters: first hit wins (see _guess_geom_col).
_VECTOR_GEOM_CANDIDATES = [
    "geom_4326",
    "geometry_4326",
    "footprint_4326",
    "point",
    "geometry_25833",
    "geom_25833",
    "geometry",
    "geom",
]
_RASTER_GEOM_CANDIDATES = ["rast", "raster"]

# Target raster-cell budget for a single bbox. A larger box is downsampled
# (coarser cells) to fit this, rather than erroring — so it bounds the number
# of polygons sent to the browser without limiting the area you can inspect.
_DEFAULT_RASTER_MAX_CELLS = 5000


class EnrichmentInspectorMixin:
    def fetch_enrichment_visualization(
        self,
        mapper_endpoint: str,
        bbox: str | None = None,
        limit: int = 500,
        raster_max_cells: int = _DEFAULT_RASTER_MAX_CELLS,
    ) -> dict[str, Any]:
        if self.db is None:
            raise ValueError("Database is not initialized.")
        if limit <= 0:
            raise ValueError("limit must be > 0")

        ds = self._resolve_datasource(mapper_endpoint)
        storage = ds.get("storage") or {}
        enrichment = storage.get("enrichment") or {}
        table_name = enrichment.get("table_name")
        table_schema = enrichment.get("table_schema")

        source = "enrichment"
        warning: str | None = None

        # Try the enrichment table first; only load it if a name is configured.
        table = self.db.get_table(table_name, table_schema) if table_name else None

        # Fall back to the staging table when enrichment is either not
        # configured or its table is missing. This lets datasources without an
        # enrichment step still be inspected, using their raw staging geometry.
        if table is None:
            staging = storage.get("staging") or {}
            staging_name = staging.get("table_name")
            staging_schema = staging.get("table_schema")
            staging_table = (
                self.db.get_table(staging_name, staging_schema) if staging_name else None
            )
            if staging_table is not None:
                if not table_name:
                    warning = (
                        f"Datasource '{ds.get('name')}' has no enrichment table. "
                        f"Showing raw staging data from '{staging_schema}.{staging_name}' instead."
                    )
                else:
                    warning = (
                        f"Enrichment table '{table_schema}.{table_name}' does not exist. "
                        f"Showing raw staging data from '{staging_schema}.{staging_name}' instead."
                    )
                table = staging_table
                table_name, table_schema = staging_name, staging_schema
                source = "staging"

        if table is None:
            if not table_name:
                raise ValueError(
                    f"Datasource '{ds.get('name')}' has neither an enrichment nor a staging table configured."
                )
            raise ValueError(
                f"Neither the enrichment table nor the staging table for "
                f"datasource '{ds.get('name')}' exists in the database."
            )

        envelope = self._parse_bbox(bbox)

        raster_col = self._guess_geom_col(table, _RASTER_GEOM_CANDIDATES)
        vector_col = self._guess_geom_col(table, _VECTOR_GEOM_CANDIDATES)

        # Prefer vector geometry when both exist (e.g. elevation has both a
        # raster `rast` and a `footprint_4326` polygon — but raster carries the
        # actual values, so only fall back to footprint when no raster).
        if raster_col is not None:
            return self._fetch_raster(
                ds, mapper_endpoint, table_schema, table_name, raster_col,
                envelope, raster_max_cells, source=source, warning=warning,
            )
        if vector_col is not None:
            return self._fetch_vector(
                ds, mapper_endpoint, table, table_schema, table_name, vector_col,
                envelope, limit, source=source, warning=warning,
            )
        raise ValueError(
            f"No supported geometry/raster column found on '{table_schema}.{table_name}'."
        )

    # ------------------------------------------------------------------ vector
    def _fetch_vector(
        self, ds, mapper_endpoint, table, table_schema, table_name, geom_col,
        envelope, limit, source="enrichment", warning=None,
    ) -> dict[str, Any]:
        prop_cols = [
            c.name for c in table.columns
            if c.name != geom_col and "geom" not in c.name.lower()
            and c.name.lower() not in ("rast", "raster")
        ]
        select_props = ", ".join(self._quote_ident(c) for c in prop_cols)
        select_props = (select_props + ",") if select_props else ""

        where = ""
        params: dict[str, Any] = {"limit": limit}
        if envelope is not None:
            where = (
                f"WHERE ST_Intersects("
                f"ST_Transform(g.{self._quote_ident(geom_col)}, 4326), "
                f"ST_MakeEnvelope(:minx, :miny, :maxx, :maxy, 4326))"
            )
            params.update(envelope)

        sql = f"""
            SELECT
                {select_props}
                ST_GeometryType(g.{self._quote_ident(geom_col)}) AS __geom_type,
                ST_AsGeoJSON(ST_Transform(g.{self._quote_ident(geom_col)}, 4326)) AS __geojson
            FROM "{table_schema}"."{table_name}" g
            {where}
            LIMIT :limit
        """
        with self.db.session_scope() as session:
            result = session.execute(text(sql), params).mappings().all()
        rows = [self._to_jsonable(dict(r)) for r in result]

        features: list[dict[str, Any]] = []
        numeric_vals: list[float] = []
        geom_type_label = "vector"
        for row in rows:
            geom = self._try_json_load(row.get("__geojson"))
            if not geom:
                continue
            gt = (row.get("__geom_type") or "").replace("ST_", "")
            geom_type_label = gt or geom_type_label
            props = {k: v for k, v in row.items() if not k.startswith("__")}
            for v in props.values():
                if isinstance(v, (int, float)) and not isinstance(v, bool):
                    numeric_vals.append(float(v))
            features.append({"type": "Feature", "geometry": geom, "properties": props})

        return self._build_response(
            mapper_endpoint, ds, geom_type_label, geom_col, envelope,
            features, self._value_stats(numeric_vals),
            extra={"source": source, "warning": warning},
        )

    # ------------------------------------------------------------------ raster
    def _fetch_raster(
        self, ds, mapper_endpoint, table_schema, table_name, raster_col,
        envelope, raster_max_cells, source="enrichment", warning=None,
    ) -> dict[str, Any]:
        if envelope is None:
            raise ValueError(
                "A bounding box is required to visualize raster enrichment. "
                "Draw a box on the map first."
            )

        qcol = self._quote_ident(raster_col)
        # Clip each intersecting tile to the bbox and union into one raster,
        # then downsample (ST_Rescale, bilinear) so the cell count fits the
        # budget before exploding to per-pixel polygons. This keeps a large
        # bbox usable — cells just get coarser — instead of erroring out.
        # ST_PixelAsPolygons returns (geom, val, x, y).
        sql = f"""
            WITH env AS (
                SELECT ST_MakeEnvelope(:minx, :miny, :maxx, :maxy, 4326) AS g
            ),
            clipped AS (
                SELECT ST_Union(ST_Clip(
                    g.{qcol},
                    ST_Transform((SELECT g FROM env), ST_SRID(g.{qcol}))
                )) AS rast
                FROM "{table_schema}"."{table_name}" g, env
                WHERE ST_Intersects(
                    g.{qcol},
                    ST_Transform(env.g, ST_SRID(g.{qcol}))
                )
            ),
            factor AS (
                SELECT rast,
                    GREATEST(
                        sqrt((ST_Width(rast)::float * ST_Height(rast)) / :target_cells),
                        1.0
                    ) AS f
                FROM clipped
                WHERE rast IS NOT NULL
            ),
            resampled AS (
                SELECT CASE
                    WHEN f > 1.0 THEN ST_Rescale(
                        rast, ST_ScaleX(rast) * f, ST_ScaleY(rast) * f, 'Bilinear'
                    )
                    ELSE rast
                END AS rast
                FROM factor
            ),
            pix AS (
                SELECT (ST_PixelAsPolygons(r.rast)).*,
                       ABS(ST_ScaleX(r.rast)) AS px_size
                FROM resampled r
            )
            SELECT
                val AS value,
                px_size,
                ST_AsGeoJSON(ST_Transform(geom, 4326)) AS __geojson
            FROM pix
            WHERE val IS NOT NULL
            LIMIT :limit
        """
        # Hard ceiling well above the target (resample rounding can slightly
        # overshoot); the resample is what actually bounds the size.
        params = dict(envelope, target_cells=raster_max_cells, limit=raster_max_cells * 4)
        with self.db.session_scope() as session:
            result = session.execute(text(sql), params).mappings().all()

        features: list[dict[str, Any]] = []
        numeric_vals: list[float] = []
        pixel_size = None
        for r in result:
            geom = self._try_json_load(r.get("__geojson"))
            if not geom:
                continue
            val = r.get("value")
            if val is not None:
                numeric_vals.append(float(val))
            if pixel_size is None and r.get("px_size") is not None:
                pixel_size = float(r["px_size"])
            features.append({
                "type": "Feature",
                "geometry": geom,
                "properties": {"value": self._to_jsonable(val)},
            })

        return self._build_response(
            mapper_endpoint, ds, "raster", raster_col, envelope,
            features, self._value_stats(numeric_vals),
            extra={
                "pixel_size": pixel_size,
                "downsampled": pixel_size is not None and pixel_size > 1.0,
                "source": source,
                "warning": warning,
            },
        )

    # ------------------------------------------------------------------ shared
    def _build_response(
        self, mapper_endpoint, ds, geometry_type, geom_col, envelope,
        features, value_stats, extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        resp = {
            "mapper_endpoint": mapper_endpoint,
            "datasource": ds.get("name"),
            "geometry_type": geometry_type,
            "geometry_column": geom_col,
            "bbox": envelope,
            "count": len(features),
            "value_stats": value_stats,
            "geojson": {"type": "FeatureCollection", "features": features},
        }
        if extra:
            resp.update(extra)
        return resp

    @staticmethod
    def _value_stats(values: list[float]) -> dict[str, Any]:
        if not values:
            return {"min": None, "max": None, "avg": None, "numeric": False}
        return {
            "min": min(values),
            "max": max(values),
            "avg": sum(values) / len(values),
            "numeric": True,
        }

    @staticmethod
    def _parse_bbox(bbox: str | None) -> dict[str, float] | None:
        if not bbox:
            return None
        parts = [p.strip() for p in bbox.split(",")]
        if len(parts) != 4:
            raise ValueError("bbox must be 'minLng,minLat,maxLng,maxLat'.")
        try:
            minx, miny, maxx, maxy = (float(p) for p in parts)
        except ValueError:
            raise ValueError("bbox values must be numeric.")
        if minx >= maxx or miny >= maxy:
            raise ValueError("bbox must have minLng<maxLng and minLat<maxLat.")
        return {"minx": minx, "miny": miny, "maxx": maxx, "maxy": maxy}
