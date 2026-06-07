from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace
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


@dataclass
class ProjCol:
    """One output column of a mapping SELECT.

    `expr` is the SQL expression; `alias` is the output column name (or None for
    a bare expression with no `AS`, e.g. `e.*` or an un-aliased column ref). The
    projection is the single source of truth for both the rendered SELECT columns
    and the inferred INSERT column list — see `_render_proj_cols` and
    `ComposedMappingStrategy.infer_insert_spec`.
    """
    expr: str
    alias: str | None = None


class MappingInsertBuilder:
    def build_insert(self, mapping: "MappingDTO", select_sql: str, spec: MappingInsertSpec) -> str:
        if not spec.columns:
            raise ValueError("Mapping insert spec requires at least one column")

        cleaned_select_sql = select_sql.strip().rstrip(";")
        columns_sql = ", ".join(spec.columns)

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


def _explicit_insert_spec(ds_like: "DataSourceABCImpl") -> MappingInsertSpec | None:
    """Read an explicit `mapping.config.insert` block into a MappingInsertSpec.

    Single source for what used to be duplicated as EtlMappingMixin.
    get_mapping_insert_spec and _DataSourceConfigAdapter._get_insert_spec.
    """
    insert_conf = ds_like.get_mapping_config().get("insert")
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


def build_mapping_query(ds_like: "DataSourceABCImpl", select_strategy) -> str | None:
    """Assemble the full mapping query for a datasource-like object.

    Shared by the production ETL path (EtlMappingMixin) and the debug preview
    (MappingInspectorMixin) so both emit identical SQL. Returns the bare SELECT
    when neither an explicit nor an inferred insert spec is available (one-to-many
    strategies), otherwise the wrapped INSERT … SELECT.

    `ds_like` must expose `data_source_config.mapping` and `get_mapping_config()`;
    `select_strategy` is the resolved strategy (or None → returns None).
    """
    if select_strategy is None:
        return None
    select_sql = select_strategy.build_select(ds_like)
    insert_spec = _explicit_insert_spec(ds_like)
    if insert_spec is None and hasattr(select_strategy, "infer_insert_spec"):
        insert_spec = select_strategy.infer_insert_spec(ds_like)
    if insert_spec is None:
        return select_sql
    return MappingInsertBuilder().build_insert(
        ds_like.data_source_config.mapping, select_sql, insert_spec
    )


def _build_incremental_filter_sql(datasource: "DataSourceABCImpl", base_alias: str, base_id_column: str) -> str | None:
    """Returns the SQL fragment that scopes the SELECT to changed ways, or
    None when the datasource isn't opted into incremental mapping. The fragment
    is bare (no WHERE prefix) so callers can AND-merge it with user filters."""
    mapping = datasource.data_source_config.mapping
    if not getattr(mapping, "incremental", False):
        return None
    changes_fqn = datasource.base_graph.get_changes_table_fqn()
    return (
        f"{base_alias}.{base_id_column} IN ("
        f"SELECT base_id FROM {changes_fqn} "
        f"WHERE op IN ('added','modified'))"
    )


def _merge_where_clauses(*clauses: str) -> str:
    """AND-merge any number of normalized WHERE clauses (each either '' or
    'WHERE ...'). Returns either '' or a single 'WHERE a AND b ...'."""
    parts: list[str] = []
    for c in clauses:
        if not c:
            continue
        c = c.strip()
        if c.lower().startswith("where "):
            c = c[6:].strip()
        if c:
            parts.append(f"({c})")
    if not parts:
        return ""
    return "WHERE " + " AND ".join(parts)


def _strip_where(clause: str) -> str:
    """Drop a leading 'WHERE ' from a normalized clause, leaving a bare predicate."""
    if not clause:
        return ""
    return clause[6:] if clause.lower().startswith("where ") else clause


def _as_and_fragment(clause: str) -> str:
    """Render a normalized WHERE clause as an ' AND <pred>' tail to splice onto
    an existing JOIN/ON condition (or '' when the clause is empty)."""
    fragment = _strip_where(clause)
    return f"\n    AND {fragment}" if fragment else ""


def _render_proj_cols(cols: list[ProjCol], sep: str) -> str:
    """Render projection columns into a SELECT list. A column with no alias (or
    whose alias equals its expression) renders bare; otherwise `expr AS alias`."""
    parts: list[str] = []
    for c in cols:
        if c.alias is None or c.expr == c.alias:
            parts.append(c.expr)
        else:
            parts.append(f"{c.expr} AS {c.alias}")
    return sep.join(parts)


# ===========================================================================
# Composable mapping engine — MATCH × REDUCE × PROJECT
# ===========================================================================
#
# Every mapping compiles to the same skeleton:
#
#   INSERT INTO mapping (way_id, <cols>)
#   SELECT b.id AS way_id, <projection>
#   FROM base b <JOIN> other e ON <predicate>
#   [WHERE filters] [GROUP BY b.id]
#
# Rather than enumerate the popular combinations as classes, a mapping is a point
# in four orthogonal axes resolved from config:
#
#   MATCH   how a base row finds candidate enrichment rows
#           nearest (k-NN LATERAL) | within (ST_DWithin) | intersects | key (=)
#   CARD.   how many candidates / keep unmatched  (k, keep_unmatched → LEFT JOIN)
#   REDUCE  how candidates collapse  none | agg (GROUP BY) | idw (interpolation)
#   PROJECT extra emitted columns
#
# The named strategies (knn, idw, within_distance, …) are *aliases* that expand
# to these axes via `resolve_axes`, so existing configs run unchanged. New combos
# (e.g. nearest-3 + jsonb_agg) are expressible purely from config with no new code.

# alias → canonical preset name
_PRESET_ALIASES: dict[str, str] = {
    "nearest_neighbour": "nearest_neighbour",
    "nearest_neighbor": "nearest_neighbour",
    "knn": "nearest_neighbour",
    "nearest_station": "nearest_neighbour",
    "nearest_k": "nearest_k",
    "k_nearest": "nearest_k",
    "knn_multiple": "nearest_k",
    "within_distance": "within_distance",
    "intersection": "intersection",
    "aggregate_within_distance": "aggregate_within_distance",
    "buffer_aggregate": "aggregate_within_distance",
    "aggregate_buffer": "aggregate_within_distance",
    "idw": "idw",
    "inverse_distance": "idw",
    "inverse_distance_weighting": "idw",
    "attribute_join": "attribute_join",
    "id_join": "attribute_join",
    "key_join": "attribute_join",
    "composed": "composed",
}

# Every strategy type the engine understands (presets + the explicit `composed`).
KNOWN_STRATEGY_NAMES: set[str] = set(_PRESET_ALIASES)


def canonical_strategy_name(type_str: str | None) -> str | None:
    if not type_str:
        return None
    return _PRESET_ALIASES.get(str(type_str).lower())


def resolve_axes(type_str: str | None, config: dict[str, Any]) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Resolve a strategy type + config into (match, reduce) axis dicts.

    Single source of truth for preset → axes expansion. Returns (None, None) for
    unknown/special types (none / custom / sql_template). For `type: composed`
    the axes are read straight from `config.match` / `config.reduce`.
    """
    canonical = canonical_strategy_name(type_str)
    if canonical is None:
        return None, None
    cfg = config or {}

    if canonical == "composed":
        return dict(cfg.get("match") or {}), dict(cfg.get("reduce") or {})

    if canonical == "nearest_neighbour":
        return {"type": "nearest", "k": 1, "order_by_sql": cfg.get("order_by_sql")}, {"type": "none"}

    if canonical == "nearest_k":
        return {"type": "nearest", "k": cfg.get("k", 1), "order_by_sql": cfg.get("order_by_sql")}, {"type": "none"}

    if canonical == "within_distance":
        return (
            {"type": "within", "max_distance": cfg.get("max_distance"),
             "join_condition_sql": cfg.get("join_condition_sql")},
            {"type": "none"},
        )

    if canonical == "intersection":
        return {"type": "intersects", "join_condition_sql": cfg.get("join_condition_sql")}, {"type": "none"}

    if canonical == "aggregate_within_distance":
        reduce: dict[str, Any] = {"type": "agg"}
        for key in ("aggregation_type", "aggregation_column", "aggregation_alias", "aggregation_expression"):
            if cfg.get(key) is not None:
                reduce[key] = cfg[key]
        return {"type": "within", "max_distance": cfg.get("max_distance"), "keep_unmatched": True}, reduce

    if canonical == "idw":
        reduce = {"type": "idw"}
        for key in ("power", "epsilon", "value_columns"):
            if cfg.get(key) is not None:
                reduce[key] = cfg[key]
        return {"type": "nearest", "k": cfg.get("k", 4)}, reduce

    if canonical == "attribute_join":
        return {"type": "key", "join_type": cfg.get("join_type", "INNER")}, {"type": "none"}

    return None, None


def match_uses_geometry(match: dict[str, Any] | None) -> bool:
    """Whether a match consumes geometry columns (gates the geometry defaults merge)."""
    if not isinstance(match, dict):
        return False
    return str(match.get("type") or "").lower() in {"nearest", "within", "intersects"}


class ComposedMappingStrategy:
    """The one mapping strategy: composes a MATCH and a REDUCE resolved from config.

    Registered under `composed` and under every legacy preset name/alias, so the
    registry returns this single instance for any of them. It is stateless — all
    behaviour derives from the datasource's resolved axes at build time.
    """

    name = "composed"
    aliases = tuple(name for name in _PRESET_ALIASES if name != "composed")

    # ── axis / context resolution ─────────────────────────────────────────

    def _axes(self, datasource: "DataSourceABCImpl") -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
        config = datasource.get_mapping_config()
        match, reduce = resolve_axes(datasource.get_mapping_strategy_type(), config)
        if match is None:
            raise ValueError(
                f"Datasource {datasource.data_source_name}: cannot resolve mapping axes for "
                f"strategy type {datasource.get_mapping_strategy_type()!r}"
            )
        return match, reduce, config

    @staticmethod
    def _match_type(match: dict[str, Any]) -> str:
        return str(match.get("type") or "").lower()

    @staticmethod
    def _reduce_type(reduce: dict[str, Any]) -> str:
        return str(reduce.get("type") or "none").lower()

    def _tables(self, datasource: "DataSourceABCImpl"):
        storage = datasource.data_source_config.storage
        enrichment = storage.enrichment if storage.enrichment else storage.staging
        if not enrichment:
            raise ValueError(
                f"Datasource {datasource.data_source_name} must have either enrichment or staging "
                f"storage configured for the mapping strategy"
            )
        return datasource.data_source_config.mapping.base_table, enrichment

    def _geom_ctx(self, datasource: "DataSourceABCImpl", config: dict[str, Any]) -> SimpleNamespace:
        link_fields = datasource.get_mapping_strategy_link_fields()
        base_alias = "b"
        enrichment_alias = "e"
        base_id_column = str(config.get("base_id_column") or "id")
        base_geometry_column = str(config.get("base_geometry_column") or "geometry")
        enrichment_geometry_column = str(config.get("enrichment_geometry_column") or "geometry")
        mapping_column = link_fields.get("mapping_column") or config.get("mapping_column")
        base_geometry_sql = f"{base_alias}.{base_geometry_column}"
        enrichment_geometry_sql = f"{enrichment_alias}.{enrichment_geometry_column}"
        distance_alias = str(config.get("distance_alias") or "distance")
        distance_sql = self._resolve_distance_sql(config, base_geometry_sql, enrichment_geometry_sql)
        return SimpleNamespace(
            link_fields=link_fields,
            base_alias=base_alias,
            enrichment_alias=enrichment_alias,
            base_id_column=base_id_column,
            base_geometry_column=base_geometry_column,
            enrichment_geometry_column=enrichment_geometry_column,
            mapping_column=mapping_column,
            base_geometry_sql=base_geometry_sql,
            enrichment_geometry_sql=enrichment_geometry_sql,
            distance_alias=distance_alias,
            distance_sql=distance_sql,
        )

    @staticmethod
    def _includes_distance(match_type: str) -> bool:
        return match_type in {"nearest", "within"}

    # ── shared helpers ────────────────────────────────────────────────────

    def _resolve_distance_sql(self, config: dict[str, Any], base_geometry_sql: str, enrichment_geometry_sql: str) -> str:
        template = config.get("distance_sql")
        if template:
            return str(template).format(
                base_geometry=base_geometry_sql,
                enrichment_geometry=enrichment_geometry_sql,
            )
        return f"ST_Distance({base_geometry_sql}, {enrichment_geometry_sql})"

    def _normalize_where(self, sql: Any) -> str:
        if not sql:
            return ""
        normalized = str(sql).strip().rstrip(";")
        if not normalized:
            return ""
        if normalized.lower().startswith("where "):
            return normalized
        return f"WHERE {normalized}"

    @staticmethod
    def _extra_cols_config(config: dict[str, Any]) -> Any:
        """The PROJECT axis: `project` (preferred) falling back to `select_columns`."""
        project = config.get("project")
        return project if project is not None else config.get("select_columns")

    def _extra_proj_cols(self, raw_columns: Any, **context: str) -> list[ProjCol]:
        """Turn PROJECT entries (config `project` / `select_columns`) into ProjCols.

        String entries are formatted and rendered bare (no alias, so they do not
        contribute to the inferred INSERT columns); dict entries require an
        expression + alias, given as either `{expression, alias}` or `{expr, as}`.
        """
        if not raw_columns:
            return []
        rendered: list[ProjCol] = []
        for item in raw_columns:
            if isinstance(item, str):
                rendered.append(ProjCol(item.format(**context).strip(), None))
                continue
            if not isinstance(item, dict):
                raise ValueError(f"Unsupported project/select_columns item: {item!r}")
            expression = item.get("expression", item.get("expr"))
            alias = item.get("alias", item.get("as"))
            if not expression or not alias:
                raise ValueError(
                    "Each project/select_columns entry must define an expression and alias "
                    "('expression'+'alias' or 'expr'+'as')"
                )
            rendered.append(ProjCol(str(expression).format(**context), str(alias)))
        return rendered

    # ── projection (single source for SELECT columns + insert spec) ───────

    def build_projection(self, datasource: "DataSourceABCImpl") -> list[ProjCol]:
        match, reduce, config = self._axes(datasource)
        reduce_type = self._reduce_type(reduce)
        if reduce_type == "idw":
            return self._idw_projection(reduce, config)
        if reduce_type == "agg":
            return self._agg_projection(datasource, reduce, config)
        if self._match_type(match) == "key":
            return self._key_projection(datasource, config)
        return self._spatial_none_projection(datasource, match, config)

    def _spatial_none_projection(self, datasource, match, config) -> list[ProjCol]:
        ctx = self._geom_ctx(datasource, config)
        cols = [ProjCol(f"{ctx.base_alias}.{ctx.base_id_column}", "way_id")]
        if ctx.mapping_column:
            cols.append(ProjCol(f"{ctx.enrichment_alias}.{ctx.mapping_column}", str(ctx.mapping_column)))
        if self._includes_distance(self._match_type(match)):
            cols.append(ProjCol(ctx.distance_sql, ctx.distance_alias))
        cols.extend(
            self._extra_proj_cols(
                self._extra_cols_config(config),
                base_geometry=ctx.base_geometry_sql,
                enrichment_geometry=ctx.enrichment_geometry_sql,
                base_alias=ctx.base_alias,
                enrichment_alias=ctx.enrichment_alias,
                base_geometry_column=ctx.base_geometry_column,
                enrichment_geometry_column=ctx.enrichment_geometry_column,
                distance_sql=ctx.distance_sql,
            )
        )
        return cols

    def _key_projection(self, datasource, config) -> list[ProjCol]:
        link_fields = datasource.get_mapping_strategy_link_fields()
        base_alias = "b"
        enrichment_alias = "e"
        base_id_column = str(config.get("base_id_column") or "id")
        enrichment_join_column = link_fields.get("mapping_column") or config.get("enrichment_join_column")

        extra_cols = self._extra_cols_config(config)
        cols = [ProjCol(f"{base_alias}.{base_id_column}", "way_id")]
        if config.get("select_all_enrichment"):
            cols.append(ProjCol(f"{enrichment_alias}.*", None))
        elif extra_cols:
            for col_def in extra_cols:
                if isinstance(col_def, str):
                    cols.append(ProjCol(f"{enrichment_alias}.{col_def}", None))
                elif isinstance(col_def, dict):
                    expr = col_def.get("expression", col_def.get("expr", ""))
                    alias = col_def.get("alias", col_def.get("as", ""))
                    if expr and alias:
                        cols.append(ProjCol(expr, alias))
        else:
            cols.append(ProjCol(f"{enrichment_alias}.{enrichment_join_column}", None))
        return cols

    def _agg_projection(self, datasource, reduce, config) -> list[ProjCol]:
        base_alias = "b"
        base_id_column = str(config.get("base_id_column") or "id")
        base_geometry_column = str(config.get("base_geometry_column") or "geometry")
        enrichment_geometry_column = str(config.get("enrichment_geometry_column") or "geometry")
        base_geometry_sql = f"{base_alias}.{base_geometry_column}"
        enrichment_geometry_sql = f"e.{enrichment_geometry_column}"

        agg_expr, agg_alias = self._aggregation_expr(datasource, reduce, config)
        cols = [
            ProjCol(f"{base_alias}.{base_id_column}", "way_id"),
            ProjCol(agg_expr, agg_alias),
        ]
        cols.extend(
            self._extra_proj_cols(
                self._extra_cols_config(config),
                base_geometry=base_geometry_sql,
                enrichment_geometry=enrichment_geometry_sql,
                base_alias=base_alias,
                enrichment_alias="e",
                base_geometry_column=base_geometry_column,
                enrichment_geometry_column=enrichment_geometry_column,
            )
        )
        return cols

    def _idw_projection(self, reduce, config) -> list[ProjCol]:
        value_columns = self._value_columns(reduce)
        scalar_cols = [c["name"] for c in value_columns if c["type"] == "scalar"]
        array_cols = [c["name"] for c in value_columns if c["type"] == "array"]
        distance_alias = str(config.get("distance_alias") or "nearest_distance_m")

        cols = [ProjCol("way_id", "way_id")]
        if array_cols:
            for c in array_cols:
                cols.append(ProjCol(f"array_agg({c}_v ORDER BY ord)", c))
            cols.append(ProjCol(f"MIN({distance_alias})", distance_alias))
        else:
            for c in scalar_cols:
                cols.append(ProjCol(f"SUM(knn.{c} * knn.wgt) / NULLIF(SUM(knn.wgt), 0)", c))
            cols.append(ProjCol("MIN(knn.dist)", distance_alias))
        return cols

    # ── insert spec ───────────────────────────────────────────────────────

    def emits_one_row_per_way(self, datasource: "DataSourceABCImpl") -> bool:
        match, reduce, _ = self._axes(datasource)
        reduce_type = self._reduce_type(reduce)
        if reduce_type in {"agg", "idw"}:
            return True
        if reduce_type == "none" and self._match_type(match) == "nearest" and int(match.get("k", 1)) == 1:
            return True
        return False

    def infer_insert_spec(self, datasource: "DataSourceABCImpl") -> MappingInsertSpec | None:
        """Derive the INSERT spec from build_projection (single source of truth).

        Returns None for one-to-many results (e.g. within/intersects/key/nearest-k
        with reduce=none), which then emit a bare SELECT."""
        if not self.emits_one_row_per_way(datasource):
            return None
        columns = [c.alias for c in self.build_projection(datasource) if c.alias is not None]
        update_cols = [c for c in columns if c != "way_id"]
        return MappingInsertSpec(columns=columns, conflict_columns=["way_id"], update_columns=update_cols)

    # ── SELECT assembly ───────────────────────────────────────────────────

    def build_select(self, datasource: "DataSourceABCImpl") -> str:
        match, reduce, config = self._axes(datasource)
        reduce_type = self._reduce_type(reduce)
        if reduce_type == "idw":
            return self._build_idw(datasource, match, reduce, config)
        if reduce_type == "agg":
            return self._build_aggregate(datasource, match, reduce, config)
        return self._build_flat(datasource, match, config)

    def _render_match_join(self, datasource, match, ctx, enrichment, enrichment_filter_sql, *, left_join: bool) -> str:
        """Render the FROM-side JOIN block for the given match (candidate source)."""
        match_type = self._match_type(match)
        enrichment_table = f"{enrichment.table_schema}.{enrichment.table_name}"
        ea = ctx.enrichment_alias

        if match_type == "nearest":
            k = int(match.get("k", 1))
            order_template = match.get("order_by_sql")
            if order_template:
                order_by = str(order_template).format(
                    base_geometry=ctx.base_geometry_sql,
                    enrichment_geometry=ctx.enrichment_geometry_sql,
                    base_alias=ctx.base_alias,
                    enrichment_alias=ea,
                )
            else:
                order_by = f"{ctx.base_geometry_sql} <-> {ctx.enrichment_geometry_sql}"
            fragment = _strip_where(enrichment_filter_sql)
            where_sql = f"\n    WHERE {fragment}" if fragment else ""
            return f"""JOIN LATERAL (
                    SELECT *
                    FROM {enrichment_table} {ea}{where_sql}
                    ORDER BY {order_by}
                    LIMIT {k}
                ) {ea} ON TRUE"""

        if match_type == "within":
            max_distance = match.get("max_distance")
            join_condition = match.get("join_condition_sql")
            if join_condition:
                predicate = str(join_condition).format(
                    base_geometry=ctx.base_geometry_sql,
                    enrichment_geometry=ctx.enrichment_geometry_sql,
                    max_distance=max_distance,
                )
            else:
                if max_distance is None:
                    raise ValueError(
                        "match 'within' requires match.max_distance or match.join_condition_sql"
                    )
                predicate = f"ST_DWithin({ctx.base_geometry_sql}, {ctx.enrichment_geometry_sql}, {max_distance})"
            keyword = "LEFT JOIN" if left_join else "JOIN"
            return f"{keyword} {enrichment_table} {ea}\n    ON {predicate}{_as_and_fragment(enrichment_filter_sql)}"

        if match_type == "intersects":
            predicate = str(
                match.get("join_condition_sql")
                or f"ST_Intersects({ctx.base_geometry_sql}, {ctx.enrichment_geometry_sql})"
            ).format(
                base_geometry=ctx.base_geometry_sql,
                enrichment_geometry=ctx.enrichment_geometry_sql,
            )
            keyword = "LEFT JOIN" if left_join else "JOIN"
            return f"{keyword} {enrichment_table} {ea}\n    ON {predicate}{_as_and_fragment(enrichment_filter_sql)}"

        if match_type == "key":
            base_join_column = ctx.link_fields.get("base_column") or datasource.get_mapping_config().get("base_join_column")
            enrichment_join_column = ctx.link_fields.get("mapping_column") or datasource.get_mapping_config().get("enrichment_join_column")
            if not base_join_column or not enrichment_join_column:
                raise ValueError(
                    "match 'key' requires base/enrichment join columns "
                    "(mapping.strategy.link_on or config.base_join_column/enrichment_join_column)"
                )
            join_type = str(match.get("join_type") or "INNER").upper()
            return (
                f"{join_type} JOIN {enrichment_table} {ea}\n"
                f"    ON {ctx.base_alias}.{base_join_column} = {ea}.{enrichment_join_column}{_as_and_fragment(enrichment_filter_sql)}"
            )

        raise ValueError(f"Unknown match type {match_type!r}")

    def _build_flat(self, datasource, match, config) -> str:
        base, enrichment = self._tables(datasource)
        ctx = self._geom_ctx(datasource, config)
        select_columns_sql = _render_proj_cols(self.build_projection(datasource), ",\n    ")

        base_filter_sql = self._normalize_where(config.get("base_filter_sql"))
        enrichment_filter_sql = self._normalize_where(config.get("enrichment_filter_sql"))
        incremental_pred = _build_incremental_filter_sql(datasource, ctx.base_alias, ctx.base_id_column)
        if incremental_pred:
            base_filter_sql = _merge_where_clauses(base_filter_sql, incremental_pred)

        join_sql = self._render_match_join(
            datasource, match, ctx, enrichment, enrichment_filter_sql,
            left_join=bool(match.get("keep_unmatched")),
        )

        return f"""
                    SELECT
                        {select_columns_sql}
                    FROM {base.table_schema}.{base.table_name} {ctx.base_alias}
                    {join_sql}
                    {base_filter_sql}
                """

    def _build_aggregate(self, datasource, match, reduce, config) -> str:
        if getattr(datasource.data_source_config.mapping, "incremental", False):
            raise ValueError(
                f"Datasource {datasource.data_source_name} uses reduce='agg' with "
                f"mapping.incremental=true. This combination is unsafe: changes to one way "
                f"require recomputing aggregates for all neighbors within the match, which the "
                f"per-way diff filter does not cover. Disable incremental for this datasource."
            )

        base, enrichment = self._tables(datasource)
        ctx = self._geom_ctx(datasource, config)
        select_columns_sql = _render_proj_cols(self.build_projection(datasource), ",\n    ")

        base_filter_sql = self._normalize_where(config.get("base_filter_sql"))
        enrichment_filter_sql = self._normalize_where(config.get("enrichment_filter_sql"))

        # Aggregates keep every base way (LEFT) unless explicitly disabled.
        left_join = match.get("keep_unmatched", True)
        join_sql = self._render_match_join(
            datasource, match, ctx, enrichment, enrichment_filter_sql, left_join=left_join,
        )

        return f"""
                    SELECT
                        {select_columns_sql}
                    FROM {base.table_schema}.{base.table_name} {ctx.base_alias}
                    {join_sql}
                    {base_filter_sql}
                    GROUP BY {ctx.base_alias}.{ctx.base_id_column}
                """

    def _aggregation_expr(self, datasource, reduce, config) -> tuple[str, str]:
        link_fields = datasource.get_mapping_strategy_link_fields()
        mapping_column = link_fields.get("mapping_column") or config.get("mapping_column")

        enrichment_alias = "e"
        base_alias = "b"
        base_geometry_column = str(config.get("base_geometry_column") or "geometry")
        enrichment_geometry_column = str(config.get("enrichment_geometry_column") or "geometry")
        base_geometry_sql = f"{base_alias}.{base_geometry_column}"
        enrichment_geometry_sql = f"{enrichment_alias}.{enrichment_geometry_column}"

        def _param(key, default=None):
            value = reduce.get(key)
            if value is None:
                value = config.get(key)
            return value if value is not None else default

        agg_type = str(_param("aggregation_type") or "jsonb_agg")
        agg_column = str(_param("aggregation_column") or mapping_column or "id")
        agg_alias = str(_param("aggregation_alias") or agg_column + "_agg")

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
            raw_expr = _param("aggregation_expression", agg_type)
            agg_expr = str(raw_expr).format(
                enrichment_alias=enrichment_alias,
                base_geometry=base_geometry_sql,
                enrichment_geometry=enrichment_geometry_sql,
                base_alias=base_alias,
                base_geometry_column=base_geometry_column,
                enrichment_geometry_column=enrichment_geometry_column,
            )
        else:
            agg_expr = agg_type

        return agg_expr, agg_alias

    # ── IDW (inverse-distance-weighted interpolation) ─────────────────────

    def _value_columns(self, reduce: dict[str, Any]) -> list[dict[str, str]]:
        raw = reduce.get("value_columns")
        if not raw:
            raise ValueError(
                "reduce 'idw' requires reduce.value_columns (a list of {name, type: scalar|array})"
            )
        cols: list[dict[str, str]] = []
        for item in raw:
            if isinstance(item, str):
                cols.append({"name": item, "type": "scalar"})
            elif isinstance(item, dict) and item.get("name"):
                cols.append({"name": str(item["name"]), "type": str(item.get("type") or "scalar").lower()})
            else:
                raise ValueError(f"Unsupported value_columns entry: {item!r}")
        return cols

    def _format_idw_filter(self, sql: Any, *, enrichment_table: str, enrichment_alias: str) -> str:
        if not sql:
            return ""
        normalized = str(sql).strip().rstrip(";")
        if normalized.lower().startswith("where "):
            normalized = normalized[6:].strip()
        if not normalized:
            return ""
        return normalized.format(enrichment_table=enrichment_table, enrichment_alias=enrichment_alias)

    def _build_idw(self, datasource, match, reduce, config) -> str:
        if self._match_type(match) != "nearest":
            raise ValueError(
                f"Datasource {datasource.data_source_name}: reduce='idw' requires match='nearest' "
                f"(got {self._match_type(match)!r})"
            )

        base, enrichment = self._tables(datasource)

        value_columns = self._value_columns(reduce)
        scalar_cols = [c["name"] for c in value_columns if c["type"] == "scalar"]
        array_cols = [c["name"] for c in value_columns if c["type"] == "array"]
        if scalar_cols and array_cols:
            raise ValueError(
                "reduce 'idw' does not support mixing scalar and array value_columns in one "
                "mapping (unnest would distort the scalar weighting). Split them into separate mappings."
            )

        k = int(match.get("k", 4))
        power = reduce.get("power", 2)
        epsilon = reduce.get("epsilon", 0.001)
        base_id_column = str(config.get("base_id_column") or "id")
        base_geometry_column = str(config.get("base_geometry_column") or "geometry")
        enrichment_geometry_column = str(config.get("enrichment_geometry_column") or "geometry")
        distance_alias = str(config.get("distance_alias") or "nearest_distance_m")

        base_alias = "b"
        enrichment_table = f"{enrichment.table_schema}.{enrichment.table_name}"
        base_geometry_sql = str(
            config.get("base_geometry_sql") or f"{base_alias}.{base_geometry_column}"
        ).format(base_alias=base_alias)
        enrichment_geometry_sql = f"e.{enrichment_geometry_column}"
        distance_sql = f"{enrichment_geometry_sql} <-> {base_geometry_sql}"

        enrichment_filter = self._format_idw_filter(
            config.get("enrichment_filter_sql"),
            enrichment_table=enrichment_table,
            enrichment_alias="e",
        )
        knn_where = f"\n                    WHERE {enrichment_filter}" if enrichment_filter else ""

        base_filter_sql = self._normalize_where(config.get("base_filter_sql"))
        incremental_pred = _build_incremental_filter_sql(datasource, base_alias, base_id_column)
        if incremental_pred:
            base_filter_sql = _merge_where_clauses(base_filter_sql, incremental_pred)

        value_col_list = ", ".join(c["name"] for c in value_columns)
        weight_sql = f"1.0 / power(GREATEST(e.dist, {epsilon}), {power})"

        knn_cte = f"""knn AS (
                SELECT
                    {base_alias}.{base_id_column} AS way_id,
                    {value_col_list},
                    e.dist,
                    {weight_sql} AS wgt
                FROM {base.table_schema}.{base.table_name} {base_alias}
                JOIN LATERAL (
                    SELECT {value_col_list}, {distance_sql} AS dist
                    FROM {enrichment_table} e{knn_where}
                    ORDER BY {distance_sql}
                    LIMIT {k}
                ) e ON TRUE
                {base_filter_sql}
            )"""

        outer_proj = _render_proj_cols(self.build_projection(datasource), ",\n                ")

        if array_cols:
            unnest_args = ", ".join(f"knn.{c}" for c in array_cols)
            unnest_cols = ", ".join(f"{c}_e" for c in array_cols)
            per_index_selects = ",\n                    ".join(
                f"SUM({c}_e * knn.wgt) / NULLIF(SUM(knn.wgt), 0) AS {c}_v" for c in array_cols
            )
            return f"""
            WITH {knn_cte},
            expanded AS (
                SELECT
                    knn.way_id,
                    ord,
                    {per_index_selects},
                    MIN(knn.dist) AS {distance_alias}
                FROM knn,
                LATERAL unnest({unnest_args}) WITH ORDINALITY AS u({unnest_cols}, ord)
                GROUP BY knn.way_id, ord
            )
            SELECT
                {outer_proj}
            FROM expanded
            GROUP BY way_id
            """

        return f"""
            WITH {knn_cte}
            SELECT
                {outer_proj}
            FROM knn
            GROUP BY way_id
            """


class MappingSelectSqlStrategyRegistry:
    def __init__(self):
        self._strategies: dict[str, MappingSelectSqlStrategy] = {}
        # One composed engine, registered under `composed` + every preset alias.
        self.register(ComposedMappingStrategy())

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


# ---------------------------------------------------------------------------
# Axis config specs — single source of truth for startup validation. Keyed by
# the canonical MATCH / REDUCE type. `known` lists axis-specific keys; the
# validator pairs these with COMMON keys to flag typos.
# ---------------------------------------------------------------------------

COMMON_MAPPING_CONFIG_KEYS: set[str] = {
    "base_id_column",
    "base_geometry_column",
    "enrichment_geometry_column",
    "base_filter_sql",
    "enrichment_filter_sql",
    "select_columns",
    "project",
    "match",
    "reduce",
    "mapping_column",
    "base_column",
    "basis",
    "description",
    "distance_alias",
    "distance_sql",
    "insert",
    # legacy flattened preset params still accepted (expanded by resolve_axes)
    "k",
    "max_distance",
    "order_by_sql",
    "join_condition_sql",
    "join_type",
    "select_all_enrichment",
    "base_join_column",
    "enrichment_join_column",
    "aggregation_type",
    "aggregation_column",
    "aggregation_alias",
    "aggregation_expression",
    "power",
    "epsilon",
    "value_columns",
    "base_geometry_sql",
}

MATCH_SPECS: dict[str, dict[str, Any]] = {
    "nearest": {"uses_geometry": True, "required": [], "required_any": [], "known": {"k", "order_by_sql"}},
    "within": {"uses_geometry": True, "required": [], "required_any": [["max_distance", "join_condition_sql"]],
               "known": {"max_distance", "join_condition_sql", "keep_unmatched"}},
    "intersects": {"uses_geometry": True, "required": [], "required_any": [], "known": {"join_condition_sql", "keep_unmatched"}},
    "key": {"uses_geometry": False, "required": [], "required_any": [], "known": {"join_type", "keep_unmatched"}},
}

REDUCE_SPECS: dict[str, dict[str, Any]] = {
    "none": {"required": [], "known": set()},
    "agg": {"required": [], "known": {"aggregation_type", "aggregation_column", "aggregation_alias", "aggregation_expression", "keep_unmatched"}},
    "idw": {"required": ["value_columns"], "known": {"k", "power", "epsilon", "value_columns", "base_geometry_sql"}},
}
