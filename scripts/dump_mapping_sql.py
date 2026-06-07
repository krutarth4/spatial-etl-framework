"""Golden-SQL oracle for the mapping strategy refactor.

Dumps the fully-resolved mapping INSERT…SELECT for (a) every live datasource
config and (b) a set of synthetic fixtures that exercise every registry strategy
and its option branches. Run before and after each refactor phase and diff the
output — it must stay byte-identical for all *preset* strategies.

This is a black-box oracle: it replicates the production assembly
(build_select -> explicit-or-inferred insert spec -> build_insert) using only the
public registry/builder surface, so it stays valid across Phase A (insert spec
moves to the base class) and Phase B (classes replaced by a composed engine).

Usage (from repo root, with .venv):
    .venv/bin/python scripts/dump_mapping_sql.py > /tmp/mapping_sql_baseline.txt
"""
from __future__ import annotations

import contextlib
import io
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from core.debug.adapters import _DataSourceConfigAdapter  # noqa: E402


def build_sql(adapter) -> str | None:
    """Resolve mapping SQL via the production builder (build_mapping_query)."""
    from main_core.mapping_sql_builder import (
        build_mapping_query,
        mapping_select_sql_strategy_registry,
    )

    stype = (adapter.get_mapping_strategy_type() or "").lower()
    if stype in {"", "none", "custom", "mapper_sql", "sql_template"}:
        return f"-- skipped (strategy={stype!r}: not registry-built)"

    strat = mapping_select_sql_strategy_registry.get(stype)
    if strat is None:
        return f"-- skipped (unknown strategy {stype!r})"

    return build_mapping_query(adapter, strat)


def _emit(name: str, adapter) -> None:
    try:
        stype = adapter.get_mapping_strategy_type()
        # Suppress any stray prints inside the builder so the oracle output is
        # pure SQL (and stays diffable once the stray print is removed in A5).
        with contextlib.redirect_stdout(io.StringIO()):
            sql = build_sql(adapter)
    except Exception as exc:  # noqa: BLE001
        stype, sql = "?", f"-- ERROR: {type(exc).__name__}: {exc}"
    print(f"===== {name} [strategy={stype}] =====")
    print((sql or "").strip())
    print()


# --------------------------------------------------------------------------- #
# (a) Live configs
# --------------------------------------------------------------------------- #
def dump_live() -> None:
    from main_core.core_config import CoreConfig

    cfg = CoreConfig()
    rows = []
    for ds in cfg.config.get("datasources") or []:
        if not isinstance(ds, dict) or not ds.get("enable"):
            continue
        mapping = ds.get("mapping") or {}
        if not mapping.get("enable"):
            continue
        rows.append((ds.get("name"), ds))
    print("########## LIVE CONFIGS ##########\n")
    for name, ds in sorted(rows, key=lambda r: str(r[0])):
        _emit(f"live:{name}", _DataSourceConfigAdapter(ds))


# --------------------------------------------------------------------------- #
# (b) Synthetic fixtures — normalized form (strategy={type,link_on} + config)
# --------------------------------------------------------------------------- #
def _ds(strategy: dict, config: dict, *, enrichment=True, insert=None,
        base_col="geometry_25833", enr_col="geometry_25833") -> dict:
    mapping = {
        "enable": True,
        "table_name": "fx_mapping",
        "table_schema": "trial",
        "strategy": strategy,
        "config": {"base_geometry_column": base_col,
                   "enrichment_geometry_column": enr_col,
                   "base_id_column": "id", **config},
        "base_table": {"table_name": "ways_base", "table_schema": "trial"},
    }
    if insert is not None:
        mapping["config"]["insert"] = insert
    storage = {"staging": {"table_name": "fx_staging", "table_schema": "trial"}}
    if enrichment:
        storage["enrichment"] = {"table_name": "fx_enrichment", "table_schema": "trial"}
    return {"name": "fx", "enable": True, "mapping": mapping, "storage": storage}


FIXTURES: dict[str, dict] = {
    "knn_basic": _ds({"type": "knn", "link_on": {"mapping_column": "station_id"}}, {}),
    "knn_geog_bearing": _ds(
        {"type": "knn", "link_on": {"mapping_column": "dwd_station_id"}},
        {
            "base_geometry_column": "geometry",
            "enrichment_geometry_column": "point",
            "distance_sql": "ST_Distance({base_geometry}::geography, {enrichment_geometry}::geography)",
            "order_by_sql": "ST_Distance({base_geometry}::geography, {enrichment_geometry}::geography)",
            "select_columns": [
                {"expression": "MOD((DEGREES(ST_Azimuth(ST_StartPoint({base_geometry}), ST_EndPoint({base_geometry}))) + 360)::NUMERIC, 360)",
                 "alias": "bearing_degree"},
            ],
        },
    ),
    "knn_explicit_insert": _ds(
        {"type": "knn", "link_on": {"mapping_column": "station_id"}}, {},
        insert={"columns": ["way_id", "station_id", "distance"],
                "conflict_columns": ["way_id"],
                "update_columns": ["station_id", "distance"]},
    ),
    "knn_no_mapping_column": _ds({"type": "knn"}, {}),
    "nearest_k": _ds(
        {"type": "nearest_k", "link_on": {"mapping_column": "parking_id"}},
        {"k": 5,
         "order_by_sql": "ST_Distance({base_geometry}::geography, {enrichment_geometry}::geography)",
         "select_columns": [{"expression": "{enrichment_alias}.capacity", "alias": "capacity"}]},
    ),
    "within_distance_default": _ds(
        {"type": "within_distance", "link_on": {"mapping_column": "sensor_id"}},
        {"max_distance": 500},
    ),
    "within_distance_custom_join": _ds(
        {"type": "within_distance", "link_on": {"mapping_column": "sensor_id"}},
        {"max_distance": 500,
         "join_condition_sql": "ST_DWithin({base_geometry}, {enrichment_geometry}, {max_distance})",
         "enrichment_filter_sql": "status = 'active'"},
    ),
    "intersection_default": _ds(
        {"type": "intersection", "link_on": {"mapping_column": "boundary_id"}}, {},
    ),
    "intersection_custom": _ds(
        {"type": "intersection", "link_on": {"mapping_column": "boundary_id"}},
        {"join_condition_sql": "ST_Intersects({base_geometry}, {enrichment_geometry})",
         "base_filter_sql": "highway IN ('primary','secondary')"},
    ),
    "aggregate_jsonb_agg": _ds(
        {"type": "aggregate_within_distance"},
        {"max_distance": 50, "aggregation_type": "jsonb_agg",
         "aggregation_column": "tree_id", "aggregation_alias": "nearby_trees",
         "select_columns": [{"expression": "COUNT({enrichment_alias}.tree_id)", "alias": "tree_count"}]},
    ),
    "aggregate_count": _ds(
        {"type": "aggregate_within_distance"},
        {"max_distance": 50, "aggregation_type": "count",
         "aggregation_column": "tree_id", "aggregation_alias": "tree_count"},
    ),
    "aggregate_jsonb_build_object": _ds(
        {"type": "aggregate_within_distance"},
        {"max_distance": 50, "aggregation_type": "jsonb_build_object",
         "aggregation_alias": "trees",
         "aggregation_expression": (
             "COALESCE(jsonb_agg(jsonb_build_object('tree_id', {enrichment_alias}.id, "
             "'distance_m', ST_Distance({enrichment_alias}.geometry_25833, {base_geometry})) "
             "ORDER BY ST_Distance({enrichment_alias}.geometry_25833, {base_geometry})) "
             "FILTER (WHERE {enrichment_alias}.id IS NOT NULL), '[]'::jsonb)"
         )},
    ),
    "idw_array": _ds(
        {"type": "idw"},
        {"k": 4, "power": 2,
         "enrichment_geometry_column": "geom_25833",
         "distance_alias": "nearest_distance_m",
         "value_columns": [{"name": "no2", "type": "array"},
                           {"name": "pm10", "type": "array"},
                           {"name": "pm25", "type": "array"}],
         "enrichment_filter_sql": "e.no2 IS NOT NULL AND e.forecast_time = (SELECT MAX(ee.forecast_time) FROM {enrichment_table} ee)"},
    ),
    "idw_scalar": _ds(
        {"type": "idw"},
        {"k": 3, "power": 2,
         "value_columns": [{"name": "temp", "type": "scalar"}]},
    ),
    "attribute_join_inner": _ds(
        {"type": "attribute_join",
         "link_on": {"base_column": "osm_id", "mapping_column": "external_osm_id"}},
        {"join_type": "INNER", "select_all_enrichment": True}, enrichment=True,
    ),
    "attribute_join_left_cols": _ds(
        {"type": "attribute_join",
         "link_on": {"base_column": "osm_id", "mapping_column": "external_osm_id"}},
        {"join_type": "LEFT",
         "select_columns": ["traffic_volume",
                            {"expression": "{enrichment_alias}.speed_limit * 1.60934", "alias": "speed_limit_kmh"}],
         "enrichment_filter_sql": "status = 'active'"},
    ),
}


def dump_fixtures() -> None:
    print("########## SYNTHETIC FIXTURES ##########\n")
    for name in sorted(FIXTURES):
        _emit(f"fx:{name}", _DataSourceConfigAdapter(FIXTURES[name]))


if __name__ == "__main__":
    dump_fixtures()
    dump_live()
