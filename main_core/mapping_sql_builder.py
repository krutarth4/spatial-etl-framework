from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from data_config_dtos.data_source_config_dto import MappingDTO
    from main_core.data_source_abc_impl import DataSourceABCImpl


class MappingSelectSqlStrategy(Protocol):
    name: str

    def build_select(self, datasource: "DataSourceABCImpl") -> str:
        ...


@dataclass
class MappingInsertSpec:
    columns: list[str]
    conflict_columns: list[str] | None = None
    update_columns: list[str] | None = None


class MappingInsertBuilder:
    def build_insert(self, mapping: "MappingDTO", select_sql: str, spec: MappingInsertSpec) -> str:
        if not spec.columns:
            raise ValueError("Mapping insert spec requires at least one column")

        cleaned_select_sql = select_sql.strip().rstrip(";")
        columns_sql = ", ".join(spec.columns)

        print(columns_sql)
        sql = (
            f"INSERT INTO {mapping.table_schema}.{mapping.table_name} ({columns_sql})\n"
            f"{cleaned_select_sql}"
        )

        if not spec.conflict_columns:
            return f"{sql};"

        update_columns = spec.update_columns or [
            column for column in spec.columns if column not in spec.conflict_columns
        ]
        if not update_columns:
            return f"{sql}\nON CONFLICT ({', '.join(spec.conflict_columns)}) DO NOTHING;"

        update_sql = ",\n                ".join(
            f"{column} = EXCLUDED.{column}" for column in update_columns
        )
        return (
            f"{sql}\n"
            f"ON CONFLICT ({', '.join(spec.conflict_columns)})\n"
            f"DO UPDATE SET\n"
            f"                {update_sql};"
        )


class SpatialRelationshipMappingSelectStrategy:
    name = ""
    aliases: tuple[str, ...] = ()

    def build_select(self, datasource: "DataSourceABCImpl") -> str:
        base = datasource.data_source_config.mapping.base_table
        enrichment = datasource.data_source_config.storage.enrichment
        link_fields = datasource.get_mapping_strategy_link_fields()
        config = datasource.get_mapping_config()

        base_alias = "b"
        enrichment_alias = "e"
        base_id_column = str(config.get("base_id_column") or "id")
        base_geometry_column = str(config.get("base_geometry_column") or "geometry")
        enrichment_geometry_column = str(config.get("enrichment_geometry_column") or "geometry")
        mapping_column = link_fields.get("mapping_column") or config.get("mapping_column")

        base_geometry_sql = f"{base_alias}.{base_geometry_column}"
        enrichment_geometry_sql = f"{enrichment_alias}.{enrichment_geometry_column}"
        distance_alias = str(config.get("distance_alias") or "distance")
        distance_sql = self._resolve_distance_sql(
            config,
            base_geometry_sql=base_geometry_sql,
            enrichment_geometry_sql=enrichment_geometry_sql,
        )

        select_columns = [f"{base_alias}.{base_id_column} AS way_id"]
        if mapping_column:
            select_columns.append(f"{enrichment_alias}.{mapping_column} AS {mapping_column}")
        if self.includes_distance:
            select_columns.append(f"{distance_sql} AS {distance_alias}")
        select_columns.extend(
            self._render_extra_selects(
                config.get("select_columns"),
                base_geometry=base_geometry_sql,
                enrichment_geometry=enrichment_geometry_sql,
                base_alias=base_alias,
                enrichment_alias=enrichment_alias,
                base_geometry_column=base_geometry_column,
                enrichment_geometry_column=enrichment_geometry_column,
                distance_sql=distance_sql,
            )
        )

        base_filter_sql = self._normalize_where_clause(config.get("base_filter_sql"))
        join_where_sql = self._normalize_where_clause(config.get("enrichment_filter_sql"))

        return f"""
                    SELECT
                        {",\n    ".join(select_columns)}
                    FROM {base.table_schema}.{base.table_name} {base_alias}
                    {self.build_join_sql(
                        config,
                        base_alias=base_alias,
                        enrichment_alias=enrichment_alias,
                        enrichment_table=f"{enrichment.table_schema}.{enrichment.table_name}",
                        base_geometry_sql=base_geometry_sql,
                        enrichment_geometry_sql=enrichment_geometry_sql,
                        join_where_sql=join_where_sql,
                    )}
                    {base_filter_sql}
                """

    @property
    def includes_distance(self) -> bool:
        return False

    def build_join_sql(
        self,
        config: dict[str, Any],
        *,
        base_alias: str,
        enrichment_alias: str,
        enrichment_table: str,
        base_geometry_sql: str | None,
        enrichment_geometry_sql: str | None,
        join_where_sql: str,
    ) -> str:
        raise NotImplementedError

    def _resolve_distance_sql(
        self,
        config: dict[str, Any],
        *,
        base_geometry_sql: str,
        enrichment_geometry_sql: str,
    ) -> str:
        distance_sql_template = config.get("distance_sql")
        if distance_sql_template:
            return str(distance_sql_template).format(
                base_geometry=base_geometry_sql,
                enrichment_geometry=enrichment_geometry_sql,
            )
        return f"ST_Distance({base_geometry_sql}, {enrichment_geometry_sql})"

    def _normalize_where_clause(self, sql: Any) -> str:
        if not sql:
            return ""
        normalized = str(sql).strip().rstrip(";")
        if not normalized:
            return ""
        if normalized.lower().startswith("where "):
            return normalized
        return f"WHERE {normalized}"

    def _render_extra_selects(
        self,
        raw_columns: Any,
        **context: str,
    ) -> list[str]:
        if not raw_columns:
            return []

        rendered: list[str] = []
        for item in raw_columns:
            if isinstance(item, str):
                rendered.append(item.format(**context).strip())
                continue

            if not isinstance(item, dict):
                raise ValueError(f"Unsupported select_columns item: {item!r}")

            expression = item.get("expression")
            alias = item.get("alias")
            if not expression or not alias:
                raise ValueError(
                    "Each mapping.config.select_columns entry must define 'expression' and 'alias'"
                )
            rendered.append(f"{str(expression).format(**context)} AS {alias}")
        return rendered


class NearestNeighbourMappingSelectStrategy(SpatialRelationshipMappingSelectStrategy):
    name = "nearest_neighbour"
    aliases = ("nearest_neighbor", "knn", "nearest_station")

    @property
    def includes_distance(self) -> bool:
        return True

    def build_join_sql(
        self,
        config: dict[str, Any],
        *,
        base_alias: str,
        enrichment_alias: str,
        enrichment_table: str,
        base_geometry_sql: str,
        enrichment_geometry_sql: str,
        join_where_sql: str,
    ) -> str:
        order_by_template = config.get("order_by_sql")
        if order_by_template:
            order_by = str(order_by_template).format(
                base_geometry=base_geometry_sql,
                enrichment_geometry=enrichment_geometry_sql,
                base_alias=base_alias,
                enrichment_alias=enrichment_alias,
            )
        else:
            order_by = f"{base_geometry_sql} <-> {enrichment_geometry_sql}"
        where_lines = []
        if join_where_sql:
            where_lines.append(join_where_sql[6:] if join_where_sql.lower().startswith("where ") else join_where_sql)

        where_sql = ""
        if where_lines:
            where_sql = "\n    WHERE " + "\n      AND ".join(where_lines)

        return f"""JOIN LATERAL (
                    SELECT *
                    FROM {enrichment_table} {enrichment_alias}{where_sql}
                    ORDER BY {order_by}
                    LIMIT 1
                ) {enrichment_alias} ON TRUE"""


class WithinDistanceMappingSelectStrategy(SpatialRelationshipMappingSelectStrategy):
    name = "within_distance"

    @property
    def includes_distance(self) -> bool:
        return True

    def build_join_sql(
        self,
        config: dict[str, Any],
        *,
        base_alias: str,
        enrichment_alias: str,
        enrichment_table: str,
        base_geometry_sql: str,
        enrichment_geometry_sql: str,
        join_where_sql: str,
    ) -> str:
        max_distance = config.get("max_distance")
        join_condition = config.get("join_condition_sql")
        if join_condition:
            predicate = str(join_condition).format(
                base_geometry=base_geometry_sql,
                enrichment_geometry=enrichment_geometry_sql,
                max_distance=max_distance,
            )
        else:
            if max_distance is None:
                raise ValueError(
                    "Mapping strategy 'within_distance' requires mapping.config.max_distance "
                    "or mapping.config.join_condition_sql"
                )
            predicate = (
                f"ST_DWithin({base_geometry_sql}, {enrichment_geometry_sql}, {max_distance})"
            )

        extra_clause = ""
        if join_where_sql:
            extra_clause = f"\n    AND {join_where_sql[6:]}" if join_where_sql.lower().startswith("where ") else f"\n    AND {join_where_sql}"

        return (
            f"JOIN {enrichment_table} {enrichment_alias}\n"
            f"    ON {predicate}{extra_clause}"
        )


class IntersectionMappingSelectStrategy(SpatialRelationshipMappingSelectStrategy):
    name = "intersection"

    def build_join_sql(
        self,
        config: dict[str, Any],
        *,
        base_alias: str,
        enrichment_alias: str,
        enrichment_table: str,
        base_geometry_sql: str,
        enrichment_geometry_sql: str,
        join_where_sql: str,
    ) -> str:
        predicate = str(
            config.get("join_condition_sql")
            or f"ST_Intersects({base_geometry_sql}, {enrichment_geometry_sql})"
        ).format(
            base_geometry=base_geometry_sql,
            enrichment_geometry=enrichment_geometry_sql,
        )

        extra_clause = ""
        if join_where_sql:
            extra_clause = f"\n    AND {join_where_sql[6:]}" if join_where_sql.lower().startswith("where ") else f"\n    AND {join_where_sql}"

        return (
            f"JOIN {enrichment_table} {enrichment_alias}\n"
            f"    ON {predicate}{extra_clause}"
        )


class NearestKMappingSelectStrategy(SpatialRelationshipMappingSelectStrategy):
    """
    Maps each base geometry to K nearest enrichment features.
    Supports aggregation (array, jsonb, first, etc.)
    """
    name = "nearest_k"
    aliases = ("k_nearest", "knn_multiple")

    @property
    def includes_distance(self) -> bool:
        return True

    def build_join_sql(
        self,
        config: dict[str, Any],
        *,
        base_alias: str,
        enrichment_alias: str,
        enrichment_table: str,
        base_geometry_sql: str,
        enrichment_geometry_sql: str,
        join_where_sql: str,
    ) -> str:
        k = config.get("k", 1)
        order_by_template = config.get("order_by_sql")
        if order_by_template:
            order_by = str(order_by_template).format(
                base_geometry=base_geometry_sql,
                enrichment_geometry=enrichment_geometry_sql,
                base_alias=base_alias,
                enrichment_alias=enrichment_alias,
            )
        else:
            order_by = f"{base_geometry_sql} <-> {enrichment_geometry_sql}"

        where_lines = []
        if join_where_sql:
            where_lines.append(join_where_sql[6:] if join_where_sql.lower().startswith("where ") else join_where_sql)

        where_sql = ""
        if where_lines:
            where_sql = "\n    WHERE " + "\n      AND ".join(where_lines)

        return f"""JOIN LATERAL (
                    SELECT *
                    FROM {enrichment_table} {enrichment_alias}{where_sql}
                    ORDER BY {order_by}
                    LIMIT {k}
                ) {enrichment_alias} ON TRUE"""


class AggregateWithinDistanceMappingSelectStrategy(SpatialRelationshipMappingSelectStrategy):
    """
    Aggregates all enrichment features within a distance of each base geometry.
    Supports jsonb_agg, array_agg, count, avg, sum, etc.
    """
    name = "aggregate_within_distance"
    aliases = ("buffer_aggregate", "aggregate_buffer")

    @property
    def includes_distance(self) -> bool:
        return True

    def build_select(self, datasource: "DataSourceABCImpl") -> str:
        base = datasource.data_source_config.mapping.base_table
        enrichment = datasource.data_source_config.storage.enrichment
        link_fields = datasource.get_mapping_strategy_link_fields()
        config = datasource.get_mapping_config()

        base_alias = "b"
        enrichment_alias = "e"
        base_id_column = str(config.get("base_id_column") or "id")
        base_geometry_column = str(config.get("base_geometry_column") or "geometry")
        enrichment_geometry_column = str(config.get("enrichment_geometry_column") or "geometry")
        mapping_column = link_fields.get("mapping_column") or config.get("mapping_column")

        base_geometry_sql = f"{base_alias}.{base_geometry_column}"
        enrichment_geometry_sql = f"{enrichment_alias}.{enrichment_geometry_column}"

        # Aggregation configuration
        agg_type = str(config.get("aggregation_type") or "jsonb_agg")
        agg_column = str(config.get("aggregation_column") or mapping_column or "id")
        agg_alias = str(config.get("aggregation_alias") or agg_column + "_agg")

        # Build aggregation expression
        if agg_type == "jsonb_agg":
            agg_expr = f"COALESCE(jsonb_agg({enrichment_alias}.{agg_column}), '[]'::jsonb)"
        elif agg_type == "array_agg":
            agg_expr = f"array_agg({enrichment_alias}.{agg_column})"
        elif agg_type == "count":
            agg_expr = f"COUNT({enrichment_alias}.{agg_column})"
        elif agg_type == "avg":
            agg_expr = f"AVG({enrichment_alias}.{agg_column})"
        elif agg_type == "sum":
            agg_expr = f"SUM({enrichment_alias}.{agg_column})"
        elif agg_type == "min":
            agg_expr = f"MIN({enrichment_alias}.{agg_column})"
        elif agg_type == "max":
            agg_expr = f"MAX({enrichment_alias}.{agg_column})"
        elif agg_type.startswith("jsonb_build_object"):
            # Custom jsonb object builder
            agg_expr = config.get("aggregation_expression", agg_type)
        else:
            agg_expr = agg_type

        select_columns = [
            f"{base_alias}.{base_id_column} AS way_id",
            f"{agg_expr} AS {agg_alias}"
        ]

        # Add extra columns if specified
        select_columns.extend(
            self._render_extra_selects(
                config.get("select_columns"),
                base_geometry=base_geometry_sql,
                enrichment_geometry=enrichment_geometry_sql,
                base_alias=base_alias,
                enrichment_alias=enrichment_alias,
                base_geometry_column=base_geometry_column,
                enrichment_geometry_column=enrichment_geometry_column,
            )
        )

        base_filter_sql = self._normalize_where_clause(config.get("base_filter_sql"))
        enrichment_filter_sql = self._normalize_where_clause(config.get("enrichment_filter_sql"))

        max_distance = config.get("max_distance")
        if max_distance is None:
            raise ValueError(
                "Mapping strategy 'aggregate_within_distance' requires mapping.config.max_distance"
            )

        join_condition = f"ST_DWithin({base_geometry_sql}, {enrichment_geometry_sql}, {max_distance})"

        extra_clause = ""
        if enrichment_filter_sql:
            extra_clause = f"\n    AND {enrichment_filter_sql[6:]}" if enrichment_filter_sql.lower().startswith("where ") else f"\n    AND {enrichment_filter_sql}"

        return f"""
                    SELECT
                        {",\n    ".join(select_columns)}
                    FROM {base.table_schema}.{base.table_name} {base_alias}
                    LEFT JOIN {enrichment.table_schema}.{enrichment.table_name} {enrichment_alias}
                        ON {join_condition}{extra_clause}
                    {base_filter_sql}
                    GROUP BY {base_alias}.{base_id_column}
                """

    def build_join_sql(
        self,
        config: dict[str, Any],
        *,
        base_alias: str,
        enrichment_alias: str,
        enrichment_table: str,
        base_geometry_sql: str | None,
        enrichment_geometry_sql: str | None,
        join_where_sql: str,
    ) -> str:
        # Not used in this strategy - overridden in build_select
        raise NotImplementedError("This strategy overrides build_select directly")


class AttributeJoinMappingSelectStrategy:
    """
    Non-spatial join based on attribute columns (e.g., station ID, road name).
    Useful for linking datasets by shared identifiers.
    """
    name = "attribute_join"
    aliases = ("id_join", "key_join")

    def build_select(self, datasource: "DataSourceABCImpl") -> str:
        base = datasource.data_source_config.mapping.base_table
        enrichment = datasource.data_source_config.storage.enrichment
        link_fields = datasource.get_mapping_strategy_link_fields()
        config = datasource.get_mapping_config()

        base_alias = "b"
        enrichment_alias = "e"
        base_id_column = str(config.get("base_id_column") or "id")

        # Join columns
        base_join_column = link_fields.get("base_column") or config.get("base_join_column")
        enrichment_join_column = link_fields.get("mapping_column") or config.get("enrichment_join_column")

        if not base_join_column or not enrichment_join_column:
            raise ValueError(
                "Mapping strategy 'attribute_join' requires base_join_column and enrichment_join_column "
                "in mapping.config or mapping.strategy.link_on"
            )

        select_columns = [f"{base_alias}.{base_id_column} AS way_id"]

        # Add all enrichment columns or specific ones
        if config.get("select_all_enrichment"):
            select_columns.append(f"{enrichment_alias}.*")
        elif config.get("select_columns"):
            for col_def in config.get("select_columns"):
                if isinstance(col_def, str):
                    select_columns.append(f"{enrichment_alias}.{col_def}")
                elif isinstance(col_def, dict):
                    expr = col_def.get("expression", "")
                    alias = col_def.get("alias", "")
                    if expr and alias:
                        select_columns.append(f"{expr} AS {alias}")
        else:
            select_columns.append(f"{enrichment_alias}.{enrichment_join_column}")

        base_filter_sql = self._normalize_where_clause(config.get("base_filter_sql"))
        enrichment_filter_sql = self._normalize_where_clause(config.get("enrichment_filter_sql"))

        extra_clause = ""
        if enrichment_filter_sql:
            extra_clause = f"\n    AND {enrichment_filter_sql[6:]}" if enrichment_filter_sql.lower().startswith("where ") else f"\n    AND {enrichment_filter_sql}"

        join_type = config.get("join_type", "INNER").upper()

        return f"""
                    SELECT
                        {",\n    ".join(select_columns)}
                    FROM {base.table_schema}.{base.table_name} {base_alias}
                    {join_type} JOIN {enrichment.table_schema}.{enrichment.table_name} {enrichment_alias}
                        ON {base_alias}.{base_join_column} = {enrichment_alias}.{enrichment_join_column}{extra_clause}
                    {base_filter_sql}
                """

    def _normalize_where_clause(self, sql: Any) -> str:
        if not sql:
            return ""
        normalized = str(sql).strip().rstrip(";")
        if not normalized:
            return ""
        if normalized.lower().startswith("where "):
            return normalized
        return f"WHERE {normalized}"


class MappingSelectSqlStrategyRegistry:
    def __init__(self):
        self._strategies: dict[str, MappingSelectSqlStrategy] = {}
        self.register(NearestNeighbourMappingSelectStrategy())
        self.register(WithinDistanceMappingSelectStrategy())
        self.register(IntersectionMappingSelectStrategy())
        self.register(NearestKMappingSelectStrategy())
        self.register(AggregateWithinDistanceMappingSelectStrategy())
        self.register(AttributeJoinMappingSelectStrategy())

    def register(self, strategy: MappingSelectSqlStrategy) -> None:
        names = [str(strategy.name).lower()]
        aliases = getattr(strategy, "aliases", ())
        names.extend(str(alias).lower() for alias in aliases)
        for name in names:
            self._strategies[name] = strategy

    def get(self, name: str | None) -> MappingSelectSqlStrategy | None:
        if not name:
            return None
        return self._strategies.get(str(name).lower())


mapping_select_sql_strategy_registry = MappingSelectSqlStrategyRegistry()
