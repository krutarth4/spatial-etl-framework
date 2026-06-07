"""
Declarative enrichment operators — generate PostGIS SQL from YAML config.

Operator types
--------------
In-place (UPDATE, no row count change):
  make_point        — ST_SetSRID(ST_MakePoint(x, y), srid)
  reproject         — ST_Transform(source_col, target_srid)
  snap_to_grid      — ST_SnapToGrid(source_col, cell_size)
  derive            — target_col = <expression> per row (e.g. a score over other
                      enrichment columns) so the value is debug-visible and the MV
                      stays a thin LEFT JOIN
  normalize         — scale source_col into target_col table-wide
                      (minmax → [0,1], or zscore)

Reshape (bypass default staging→enrichment sync, TRUNCATE + INSERT SELECT):
  aggregate         — temporal/attribute GROUP BY with aggregation functions
  spatial_aggregate — snap geometry to grid + GROUP BY in one pass
  raster_aggregate  — downsample a raster to a coarser grid via ST_Resample
                      (e.g. 1 m DEM → 10 m cells averaging each block)
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from data_config_dtos.data_source_config_dto import (
        EnrichmentOperatorDTO,
        EnrichmentOperatorsConfigDTO,
    )

_RESHAPE_TYPES = {"aggregate", "spatial_aggregate", "raster_aggregate"}
_VALID_AGG_FUNCTIONS = {"avg", "sum", "count", "max", "min"}


@dataclass
class EnrichmentOperatorContext:
    staging_schema: str
    staging_table: str
    enrichment_schema: str
    enrichment_table: str


@runtime_checkable
class EnrichmentOperatorStrategy(Protocol):
    name: str
    is_reshape: bool

    def build_sql(
        self,
        operator: "EnrichmentOperatorDTO",
        context: EnrichmentOperatorContext,
    ) -> str:
        ...


# ---------------------------------------------------------------------------
# In-place operators
# ---------------------------------------------------------------------------

class MakePointOperatorStrategy:
    name = "make_point"
    is_reshape = False

    def build_sql(self, operator: "EnrichmentOperatorDTO", ctx: EnrichmentOperatorContext) -> str:
        if not all([operator.x_col, operator.y_col, operator.target_col, operator.join_col]):
            raise ValueError(
                "make_point requires x_col, y_col, target_col, join_col"
            )
        srid = operator.srid or 4326
        condition_clause = f"\n  AND {operator.condition}" if operator.condition else ""
        return (
            f"UPDATE {ctx.enrichment_schema}.{ctx.enrichment_table} e\n"
            f"SET {operator.target_col} = ST_SetSRID(\n"
            f"    ST_MakePoint(s.{operator.x_col}, s.{operator.y_col}),\n"
            f"    {srid}\n"
            f")\n"
            f"FROM {ctx.staging_schema}.{ctx.staging_table} s\n"
            f"WHERE e.{operator.join_col} = s.{operator.join_col}"
            f"{condition_clause};"
        )


class ReprojectOperatorStrategy:
    name = "reproject"
    is_reshape = False

    def build_sql(self, operator: "EnrichmentOperatorDTO", ctx: EnrichmentOperatorContext) -> str:
        if not all([operator.source_col, operator.target_col, operator.target_srid]):
            raise ValueError(
                "reproject requires source_col, target_col, target_srid"
            )
        null_guard = f"{operator.source_col} IS NOT NULL"
        condition_clause = (
            f"\n  AND {operator.condition}"
            if operator.condition
            else ""
        )
        return (
            f"UPDATE {ctx.enrichment_schema}.{ctx.enrichment_table}\n"
            f"SET {operator.target_col} = ST_Transform({operator.source_col}, {operator.target_srid})\n"
            f"WHERE {null_guard}"
            f"{condition_clause};"
        )


class SnapToGridOperatorStrategy:
    name = "snap_to_grid"
    is_reshape = False

    def build_sql(self, operator: "EnrichmentOperatorDTO", ctx: EnrichmentOperatorContext) -> str:
        if not all([operator.source_col, operator.target_col, operator.cell_size]):
            raise ValueError(
                "snap_to_grid requires source_col, target_col, cell_size"
            )
        condition_clause = (
            f"\n  AND {operator.condition}"
            if operator.condition
            else ""
        )
        return (
            f"UPDATE {ctx.enrichment_schema}.{ctx.enrichment_table}\n"
            f"SET {operator.target_col} = ST_SnapToGrid({operator.source_col}, {operator.cell_size})\n"
            f"WHERE {operator.source_col} IS NOT NULL"
            f"{condition_clause};"
        )


class DeriveOperatorStrategy:
    """Compute a derived column from an expression over each enrichment row.

    The expression is raw SQL evaluated per row against the enrichment table
    (reference any of its columns), e.g. a normalized score::

        - type: derive
          target_col: pleasant_score
          expression: "LEAST(1.0, GREATEST(0.0, avg_speed_performance_index / 30.0))"

    Keeping the score in enrichment makes it debug-visible and lets the MV read it
    with a plain LEFT JOIN. `target_col` must already exist on the enrichment table.
    """
    name = "derive"
    is_reshape = False

    def build_sql(self, operator: "EnrichmentOperatorDTO", ctx: EnrichmentOperatorContext) -> str:
        if not operator.target_col or not operator.expression:
            raise ValueError("derive requires target_col and expression")
        where_clause = f"\nWHERE {operator.condition}" if operator.condition else ""
        return (
            f"UPDATE {ctx.enrichment_schema}.{ctx.enrichment_table}\n"
            f"SET {operator.target_col} = {operator.expression}"
            f"{where_clause};"
        )


class NormalizeOperatorStrategy:
    """Scale a column into a normalized target column across the whole table.

    method 'minmax' → (x - min) / (max - min) in [0, 1];
    method 'zscore' → (x - avg) / stddev_pop. Rows where source is NULL are left
    untouched. `target_col` must already exist on the enrichment table.
    """
    name = "normalize"
    is_reshape = False

    def build_sql(self, operator: "EnrichmentOperatorDTO", ctx: EnrichmentOperatorContext) -> str:
        if not operator.source_col or not operator.target_col:
            raise ValueError("normalize requires source_col and target_col")
        table = f"{ctx.enrichment_schema}.{ctx.enrichment_table}"
        src = operator.source_col
        method = (operator.method or "minmax").lower()
        if method == "minmax":
            stats_sql = f"SELECT MIN({src}) AS min_v, MAX({src}) AS max_v FROM {table}"
            expr = f"(e.{src} - stats.min_v) / NULLIF(stats.max_v - stats.min_v, 0)"
        elif method == "zscore":
            stats_sql = f"SELECT AVG({src}) AS avg_v, STDDEV_POP({src}) AS std_v FROM {table}"
            expr = f"(e.{src} - stats.avg_v) / NULLIF(stats.std_v, 0)"
        else:
            raise ValueError(
                f"normalize: unsupported method '{operator.method}' (expected 'minmax' or 'zscore')"
            )
        condition_clause = f"\n  AND {operator.condition}" if operator.condition else ""
        return (
            f"UPDATE {table} e\n"
            f"SET {operator.target_col} = {expr}\n"
            f"FROM ({stats_sql}) stats\n"
            f"WHERE e.{src} IS NOT NULL"
            f"{condition_clause};"
        )


# ---------------------------------------------------------------------------
# Reshape operators
# ---------------------------------------------------------------------------

def _resolve_agg_function(function: str, column: str, alias: str) -> str:
    fn = function.lower()
    if fn not in _VALID_AGG_FUNCTIONS:
        raise ValueError(
            f"Unsupported aggregation function '{function}'. "
            f"Supported: {sorted(_VALID_AGG_FUNCTIONS)}"
        )
    return f"{fn.upper()}({column}) AS {alias}"


def _build_conflict_clause(conflict_columns: list[str], all_select_aliases: list[str]) -> str:
    update_cols = [c for c in all_select_aliases if c not in conflict_columns]
    if not update_cols:
        return f"ON CONFLICT ({', '.join(conflict_columns)}) DO NOTHING"
    updates = ",\n    ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
    return (
        f"ON CONFLICT ({', '.join(conflict_columns)})\n"
        f"DO UPDATE SET\n"
        f"    {updates}"
    )


class AggregateOperatorStrategy:
    name = "aggregate"
    is_reshape = True

    def build_sql(self, operator: "EnrichmentOperatorDTO", ctx: EnrichmentOperatorContext) -> str:
        if not operator.aggregations:
            raise ValueError("aggregate operator requires at least one aggregation")

        # Resolve source table
        source_table = operator.source_table or "staging"
        if source_table == "staging":
            src = f"{ctx.staging_schema}.{ctx.staging_table}"
        else:
            src = f"{ctx.enrichment_schema}.{ctx.enrichment_table}"

        # Build GROUP BY expressions and their aliases
        group_by_exprs = []
        group_by_aliases = []
        for item in (operator.group_by or []):
            if isinstance(item, dict):
                col = item.get("column")
                expr = item.get("expression")
                alias = item.get("alias")
                if expr:
                    a = alias or f"_grp_{len(group_by_aliases)}"
                    group_by_exprs.append((expr, a))
                    group_by_aliases.append(a)
                elif col:
                    a = alias or col
                    group_by_exprs.append((col, a))
                    group_by_aliases.append(a)
            else:
                # GroupByExpressionDTO dataclass
                if getattr(item, "expression", None):
                    a = item.alias or f"_grp_{len(group_by_aliases)}"
                    group_by_exprs.append((item.expression, a))
                    group_by_aliases.append(a)
                elif getattr(item, "column", None):
                    a = item.alias or item.column
                    group_by_exprs.append((item.column, a))
                    group_by_aliases.append(a)

        # Build SELECT columns (group-by first, then aggregations)
        select_parts = [f"{expr} AS {alias}" for expr, alias in group_by_exprs]
        all_aliases = list(group_by_aliases)

        for agg in operator.aggregations:
            alias = agg.alias or agg.column
            select_parts.append(_resolve_agg_function(agg.function, agg.column, alias))
            all_aliases.append(alias)

        select_sql = ",\n    ".join(select_parts)
        group_by_sql = ", ".join(expr for expr, _ in group_by_exprs)
        insert_cols = ", ".join(all_aliases)
        filter_clause = f"\nWHERE {operator.filter}" if operator.filter else ""
        conflict_clause = (
            _build_conflict_clause(operator.conflict_columns, all_aliases)
            if operator.conflict_columns
            else ""
        )

        return (
            f"TRUNCATE TABLE {ctx.enrichment_schema}.{ctx.enrichment_table};\n\n"
            f"INSERT INTO {ctx.enrichment_schema}.{ctx.enrichment_table} ({insert_cols})\n"
            f"SELECT\n"
            f"    {select_sql}\n"
            f"FROM {src}"
            f"{filter_clause}\n"
            f"GROUP BY {group_by_sql}\n"
            f"{conflict_clause};"
        )


class SpatialAggregateOperatorStrategy:
    name = "spatial_aggregate"
    is_reshape = True

    def build_sql(self, operator: "EnrichmentOperatorDTO", ctx: EnrichmentOperatorContext) -> str:
        if not all([operator.geometry_col, operator.snapped_col, operator.cell_size]):
            raise ValueError(
                "spatial_aggregate requires geometry_col, snapped_col, cell_size"
            )
        if not operator.aggregations:
            raise ValueError("spatial_aggregate requires at least one aggregation")

        source_table = operator.source_table or "staging"
        if source_table == "staging":
            src = f"{ctx.staging_schema}.{ctx.staging_table}"
        else:
            src = f"{ctx.enrichment_schema}.{ctx.enrichment_table}"

        snap_expr = f"ST_SnapToGrid({operator.geometry_col}, {operator.cell_size})"
        snapped_alias = operator.snapped_col
        all_aliases = [snapped_alias]

        agg_parts = []
        for agg in operator.aggregations:
            alias = agg.alias or agg.column
            agg_parts.append(_resolve_agg_function(agg.function, agg.column, alias))
            all_aliases.append(alias)

        select_sql = f"{snap_expr} AS {snapped_alias}"
        if agg_parts:
            select_sql += ",\n    " + ",\n    ".join(agg_parts)

        insert_cols = ", ".join(all_aliases)
        filter_clause = f"\nWHERE {operator.filter}" if operator.filter else ""
        conflict_clause = (
            _build_conflict_clause(operator.conflict_columns, all_aliases)
            if operator.conflict_columns
            else ""
        )

        return (
            f"TRUNCATE TABLE {ctx.enrichment_schema}.{ctx.enrichment_table};\n\n"
            f"INSERT INTO {ctx.enrichment_schema}.{ctx.enrichment_table} ({insert_cols})\n"
            f"SELECT\n"
            f"    {select_sql}\n"
            f"FROM {src}"
            f"{filter_clause}\n"
            f"GROUP BY {snap_expr}\n"
            f"{conflict_clause};"
        )


class RasterAggregateOperatorStrategy:
    """Downsample a raster column into a coarser raster via ST_Resample.

    Reshape operator: TRUNCATE + INSERT the resampled raster. Each output pixel
    aggregates the source pixels beneath it using the GDAL resampling `algorithm`
    ('Average' by default → block-mean; also 'Min'/'Max'/'Bilinear'/'Mode'/...).
    """
    name = "raster_aggregate"
    is_reshape = True

    def build_sql(self, operator: "EnrichmentOperatorDTO", ctx: EnrichmentOperatorContext) -> str:
        if not operator.cell_size:
            raise ValueError("raster_aggregate requires cell_size")

        raster_col = operator.raster_col or "rast"
        target_col = operator.target_col or "rast"
        algorithm = operator.algorithm or "Average"
        cell = operator.cell_size

        source_table = operator.source_table or "staging"
        if source_table == "staging":
            src = f"{ctx.staging_schema}.{ctx.staging_table}"
        else:
            src = f"{ctx.enrichment_schema}.{ctx.enrichment_table}"

        where_parts = [f"{raster_col} IS NOT NULL"]
        if operator.filter:
            where_parts.append(operator.filter)
        where_sql = " AND ".join(where_parts)

        return (
            f"TRUNCATE TABLE {ctx.enrichment_schema}.{ctx.enrichment_table};\n\n"
            f"INSERT INTO {ctx.enrichment_schema}.{ctx.enrichment_table} ({target_col})\n"
            # Cast scale to double precision so PostGIS picks the (scalex, scaley)
            # overload. Bare integer literals resolve to ST_Resample(rast, width,
            # height, ...) instead, where -cell wraps to a huge unsigned dimension
            # and trips "Dimensions requested exceed the maximum (65535 x 65535)".
            f"SELECT ST_Resample({raster_col}, {cell}::double precision, (-{cell})::double precision, 0, 0, 0, 0, '{algorithm}')\n"
            f"FROM {src}\n"
            f"WHERE {where_sql};"
        )


# ---------------------------------------------------------------------------
# Registry and Builder
# ---------------------------------------------------------------------------

class EnrichmentOperatorRegistry:
    def __init__(self) -> None:
        self._strategies: dict[str, EnrichmentOperatorStrategy] = {}

    def register(self, strategy: EnrichmentOperatorStrategy) -> None:
        self._strategies[strategy.name] = strategy

    def get(self, type_name: str) -> EnrichmentOperatorStrategy:
        strategy = self._strategies.get(type_name)
        if strategy is None:
            raise ValueError(
                f"Unknown enrichment operator type '{type_name}'. "
                f"Known: {sorted(self._strategies)}"
            )
        return strategy


class EnrichmentOperatorBuilder:
    def __init__(self, registry: EnrichmentOperatorRegistry) -> None:
        self._registry = registry

    def has_reshape_operators(self, config: "EnrichmentOperatorsConfigDTO") -> bool:
        return any(op.type in _RESHAPE_TYPES for op in config.operators)

    def build_sql_sequence(
        self,
        config: "EnrichmentOperatorsConfigDTO",
        context: EnrichmentOperatorContext,
    ) -> list[tuple[str, str]]:
        """Return [(operator_type, sql), ...] in declaration order."""
        results = []
        for op in config.operators:
            strategy = self._registry.get(op.type)
            sql = strategy.build_sql(op, context)
            results.append((op.type, sql))
        return results


# ---------------------------------------------------------------------------
# Module-level singleton registry — import and use directly
# ---------------------------------------------------------------------------

enrichment_operator_registry = EnrichmentOperatorRegistry()
enrichment_operator_registry.register(MakePointOperatorStrategy())
enrichment_operator_registry.register(ReprojectOperatorStrategy())
enrichment_operator_registry.register(SnapToGridOperatorStrategy())
enrichment_operator_registry.register(DeriveOperatorStrategy())
enrichment_operator_registry.register(NormalizeOperatorStrategy())
enrichment_operator_registry.register(AggregateOperatorStrategy())
enrichment_operator_registry.register(SpatialAggregateOperatorStrategy())
enrichment_operator_registry.register(RasterAggregateOperatorStrategy())
