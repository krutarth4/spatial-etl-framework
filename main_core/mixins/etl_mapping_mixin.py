"""Mapping ETL operations: enrichment → base graph (ways_base) mapping.

Covers full-rescan and incremental mapping, the strategy dispatch, and the
strategy-builder / sql_template query construction.

Reads from self:  self.db, self.data_source_config, self.data_source_name,
                  self.logger, self.base_graph
Calls into:       EtlQueryMixin (execute_query)
"""
from typing import Any

from main_core.mapping_sql_builder import (
    MappingInsertBuilder,
    MappingInsertSpec,
    mapping_select_sql_strategy_registry,
)
from main_core.safe_class import safe_class


@safe_class
class EtlMappingMixin:
    """Implements the enrichment → ways_base mapping step."""

    # ── Mapping ───────────────────────────────────────────────────────────

    # Overridable: mappers may override this method for custom mapping SQL
    def mapping_db_query(self) -> None | str:
        """Return the INSERT/SELECT mapping query, or None.

        Default: delegates to build_mapping_db_query() (strategy-builder path).
        Override to provide hand-written SQL when the auto-generated query is
        insufficient (e.g. complex KNN joins with custom LATERAL sub-queries).
        """
        return self.build_mapping_db_query()

    def map_to_links(self):
        """Execute the query returned by mapping_db_query()."""
        query = self.mapping_db_query()
        self.execute_query("Mapping", query)

    # Overridable: mappers may override this method for custom mapping logic
    def map_to_base(self):
        """Run the mapping step (full-rescan or incremental depending on config)."""
        if not self.data_source_config.mapping.enable:
            return
        if self.db is None:
            return
        try:
            self.logger.info("Mapping started on Mapping Table.....")
            if self.data_source_config.mapping.incremental:
                self._map_to_base_incremental()
            else:
                self._map_to_base_full_rescan()
        except Exception as e:
            self.logger.error(f"Error occurred during base table update {e}")
            raise

    def _map_to_base_full_rescan(self):
        """Run the mapping strategy only when the mapping table has fewer rows than ways_base.

        Using '<' (not '!=') is intentional: some datasources map multiple rows per
        way (e.g. air quality grids), so their table will legitimately exceed ways_base.
        """
        total_ways_count = self.base_graph.get_base_graph_row_counts()
        mapped_ways_count = self.db.get_table_count(
            self.data_source_config.mapping.table_name,
            self.data_source_config.mapping.table_schema,
        )
        if mapped_ways_count < total_ways_count:
            self.logger.info(
                f"Mapping table has {mapped_ways_count} rows but ways_base has "
                f"{total_ways_count} — running mapping strategy."
            )
            self.execute_mapping_strategy()
        else:
            self.logger.info(
                f"Skipping mapping — table already has {mapped_ways_count} rows "
                f">= {total_ways_count} ways_base rows."
            )

    def _map_to_base_incremental(self):
        """Diff-driven mapping: runs only when unprocessed changes exist.

        Deletes stale mapping rows for changed/removed ways, then executes the
        strategy SQL scoped to the changed-ways filter.  Marks the generation
        consumed afterwards to short-circuit subsequent runs.
        """
        if not self.base_graph.has_pending_changes_for(self.data_source_name):
            self.logger.info(
                f"Skipping incremental mapping for {self.data_source_name}: "
                f"already consumed current ways_base change-set."
            )
            return
        # Snapshot generation BEFORE mapping so a concurrent populate doesn't
        # cause us to mark consumed > what we actually processed.
        generation = self.base_graph.current_generation()
        self._delete_mapping_for_changed_ways()
        self.execute_mapping_strategy()
        self.base_graph.mark_consumed(self.data_source_name, generation)
        self.logger.info(
            f"[incremental] Marked {self.data_source_name} consumed at "
            f"ways_base_changes generation {generation}"
        )

    def _delete_mapping_for_changed_ways(self):
        """Delete mapping rows for ways that have been removed or modified."""
        mapping = self.data_source_config.mapping
        changes_fqn = self.base_graph.get_changes_table_fqn()
        sql = (
            f"DELETE FROM {mapping.table_schema}.{mapping.table_name} "
            f"WHERE way_id IN ("
            f"  SELECT base_id FROM {changes_fqn} "
            f"  WHERE op IN ('removed','modified')"
            f");"
        )
        self.logger.info(
            f"[incremental] Cleaning stale mapping rows for "
            f"{mapping.table_schema}.{mapping.table_name}"
        )
        self.db.call_sql(sql)

    # ── Mapping strategy builder ──────────────────────────────────────────

    def execute_mapping_strategy(self):
        """Dispatch to the correct mapping implementation based on strategy type."""
        strategy_type = (self.get_mapping_strategy_type() or "custom").lower()
        self.logger.info(
            f"Executing mapping strategy type '{strategy_type}' for datasource "
            f"{self.data_source_name}"
        )

        if strategy_type == "none":
            self.logger.info("Mapping strategy type 'none': skipping mapping step")
            return
        if strategy_type == "sql_template":
            self.execute_mapping_sql_template()
            return
        if strategy_type in {"custom", "mapper_sql"}:
            self.map_to_links()
            return

        select_strategy = self.get_mapping_select_strategy()
        if select_strategy is None:
            self.logger.warning(
                f"Unknown mapping strategy type '{strategy_type}' for datasource "
                f"{self.data_source_name}. Falling back to mapper SQL."
            )
            self.map_to_links()
            return

        query = self.build_mapping_db_query()
        self.execute_query("Mapping", query)

    def build_mapping_db_query(self) -> str | None:
        """Build the full INSERT … SELECT mapping query via the strategy builder."""
        select_strategy = self.get_mapping_select_strategy()
        if select_strategy is None:
            return None

        select_sql = select_strategy.build_select(self)
        insert_spec = self.get_mapping_insert_spec()
        if insert_spec is None and hasattr(select_strategy, "infer_insert_spec"):
            insert_spec = select_strategy.infer_insert_spec(self)
        if insert_spec is None:
            return select_sql

        builder = MappingInsertBuilder()
        return builder.build_insert(self.data_source_config.mapping, select_sql, insert_spec)

    def execute_mapping_sql_template(self):
        """Execute the sql_template mapping strategy by formatting config.sql."""
        mapping_conf = getattr(self.data_source_config, "mapping", None)
        config = getattr(mapping_conf, "config", None) or {}
        sql = config.get("sql")
        if not sql:
            raise ValueError(
                f"Mapping strategy 'sql_template' requires mapping.config.sql "
                f"for datasource {self.data_source_name}"
            )
        try:
            sql = sql.format(**self.get_mapping_template_context())
        except Exception:
            pass
        self.execute_query("Mapping", sql)

    def get_mapping_template_context(self) -> dict[str, str | None]:
        """Build the format-string context for sql_template mapping queries."""
        mapping = self.data_source_config.mapping
        storage = self.data_source_config.storage
        base = mapping.base_table
        link_fields = self.get_mapping_strategy_link_fields()
        strategy_type = self.get_mapping_strategy_type()

        staging_table = staging_schema = None
        if storage.staging:
            staging_table = storage.staging.table_name
            staging_schema = storage.staging.table_schema

        enrichment_table = enrichment_schema = None
        if storage.enrichment:
            enrichment_table = storage.enrichment.table_name
            enrichment_schema = storage.enrichment.table_schema
        elif storage.staging:
            enrichment_table = storage.staging.table_name
            enrichment_schema = storage.staging.table_schema

        if mapping.incremental:
            changes_fqn = self.base_graph.get_changes_table_fqn()
            changed_ways_subquery = (
                f"SELECT base_id FROM {changes_fqn} WHERE op IN ('added','modified')"
            )
            changed_ways_filter = f"w.id IN ({changed_ways_subquery})"
        else:
            changed_ways_subquery = (
                f"SELECT id FROM {base.table_schema}.{base.table_name}"
            )
            changed_ways_filter = "TRUE"

        return {
            "datasource_name": self.data_source_name,
            "mapping_table": mapping.table_name,
            "mapping_schema": mapping.table_schema,
            "staging_table": staging_table,
            "staging_schema": staging_schema,
            "enrichment_table": enrichment_table,
            "enrichment_schema": enrichment_schema,
            "base_table": base.table_name,
            "base_schema": base.table_schema,
            "strategy_type": strategy_type,
            "link_mapping_column": link_fields.get("mapping_column"),
            "link_base_column": link_fields.get("base_column"),
            "link_basis": link_fields.get("basis"),
            "changed_ways_filter": changed_ways_filter,
            "changed_ways_subquery": changed_ways_subquery,
        }

    def get_mapping_strategy_type(self) -> str | None:
        mapping_conf = getattr(self.data_source_config, "mapping", None)
        strategy = getattr(mapping_conf, "strategy", None)
        if strategy is None:
            return None
        if isinstance(strategy, str):
            return strategy
        if isinstance(strategy, dict):
            value = strategy.get("type")
            if value:
                return str(value)
            legacy_value = strategy.get("name")
            return str(legacy_value) if legacy_value else None
        value = getattr(strategy, "type", None)
        if value is not None:
            return str(value)
        legacy_value = getattr(strategy, "name", None)
        return str(legacy_value) if legacy_value else None

    def get_mapping_strategy_link_fields(self) -> dict[str, str | None]:
        mapping_conf = getattr(self.data_source_config, "mapping", None)
        strategy = getattr(mapping_conf, "strategy", None) if mapping_conf else None
        link_on = None
        if isinstance(strategy, dict):
            link_on = strategy.get("link_on")
        elif strategy is not None:
            link_on = getattr(strategy, "link_on", None)

        if isinstance(link_on, dict):
            mapping_column = link_on.get("mapping_column")
            base_column = link_on.get("base_column")
            basis = link_on.get("basis")
        else:
            mapping_column = getattr(link_on, "mapping_column", None) if link_on is not None else None
            base_column = getattr(link_on, "base_column", None) if link_on is not None else None
            basis = getattr(link_on, "basis", None) if link_on is not None else None

        return {
            "mapping_column": str(mapping_column) if mapping_column else None,
            "base_column": str(base_column) if base_column else None,
            "basis": str(basis) if basis else None,
        }

    def get_mapping_config(self) -> dict[str, Any]:
        mapping_conf = getattr(self.data_source_config, "mapping", None)
        config = getattr(mapping_conf, "config", None) if mapping_conf else None
        if isinstance(config, dict):
            return config
        return {}

    def get_custom_mapping_select_strategy(self):
        """Override in mapper classes to return a custom SQL select strategy object.

        The object must implement: `name` and `build_select(datasource)`.
        Default returns None (falls back to registry lookup).
        """
        return None

    def get_mapping_select_strategy(self):
        custom_strategy = self.get_custom_mapping_select_strategy()
        if custom_strategy is not None:
            return custom_strategy
        return mapping_select_sql_strategy_registry.get(self.get_mapping_strategy_type())

    def get_mapping_insert_spec(self) -> MappingInsertSpec | None:
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
