"""Staging, enrichment, and mapping ETL operations.

Reads from self:  self.db, self.data_source_config, self.data_source_name,
                  self.logger, self.base_graph, self._stage_timings
Calls into:       TableManagementMixin  (_note_stage_warning)
                  TimingReportingMixin  (_should_use_batching, _get_batch_size via self)
                  MetadataTrackingMixin (_note_stage_warning)
"""
from typing import Any

from main_core.mapping_sql_builder import (
    MappingInsertBuilder,
    MappingInsertSpec,
    mapping_select_sql_strategy_registry,
)
from main_core.safe_class import safe_class


@safe_class
class EtlOperationsMixin:
    """Implements the staging → enrichment → mapping ETL operations."""

    # ── Staging ───────────────────────────────────────────────────────────

    # Overridable: mappers may override this method for custom staging SQL
    def staging_db_query(self) -> None | str:
        """Return a custom SQL query to run against the staging table, or None.

        Called by execute_on_staging().  Override when you need a post-load
        transformation on the staging table (e.g. geometry normalisation).
        """
        return None

    def execute_on_staging(self):
        """Execute the staging query (if any) returned by staging_db_query()."""
        query = self.staging_db_query()
        self.execute_query("Staging", query)

    def sync_raw_to_staging(self) -> dict:
        """Copy rows from the raw-staging table into the permanent staging table."""
        batch_size = self._get_batch_size() if self._should_use_batching() else None
        return self.db.sync_source_to_target_table(
            self.raw_staging_schema,
            self.raw_staging_table,
            self.data_source_config.storage.staging.table_schema,
            self.data_source_config.storage.staging.table_name,
            batch_size=batch_size,
        )

    # ── Enrichment ────────────────────────────────────────────────────────

    # Overridable: mappers may override this method for custom enrichment SQL
    def enrichment_db_query(self) -> None | str:
        """Return a custom SQL query to run against the enrichment table, or None.

        Called by execute_on_enrichment() legacy path.  Override when the
        enrichment step needs bespoke SQL (e.g. CRS conversion, aggregation).
        """
        return None

    # Overridable: mappers may override this method for custom enrichment logic
    def execute_on_enrichment(self):
        """Run enrichment operators (config-driven) or the legacy enrichment_db_query().

        If enrichment_operators are configured they take precedence.  Otherwise
        falls back to the Python override enrichment_db_query().
        """
        operators_config = getattr(self.data_source_config, "enrichment_operators", None)
        if operators_config is not None:
            self._execute_enrichment_operators(operators_config)
            return
        query = self.enrichment_db_query()
        self.execute_query("Enrichment", query)

    def _execute_enrichment_operators(self, operators_config):
        """Execute a sequence of declarative enrichment operators from config."""
        from main_core.enrichment_operator_builder import (
            EnrichmentOperatorBuilder,
            EnrichmentOperatorContext,
            enrichment_operator_registry,
        )
        ctx = EnrichmentOperatorContext(
            staging_schema=self.data_source_config.storage.staging.table_schema,
            staging_table=self.data_source_config.storage.staging.table_name,
            enrichment_schema=self.data_source_config.storage.enrichment.table_schema,
            enrichment_table=self.data_source_config.storage.enrichment.table_name,
        )
        builder = EnrichmentOperatorBuilder(enrichment_operator_registry)
        for op_type, sql in builder.build_sql_sequence(operators_config, ctx):
            self.logger.info(f"[enrichment_operator:{op_type}] Executing")
            self.db.call_sql(sql)

    # Overridable: mappers may override this method for custom staging→enrichment sync
    def sync_staging_to_enrichment(self):
        """Copy rows from staging to enrichment.

        Skips the default verbatim copy when a reshape enrichment operator is
        configured (it inserts directly).  Override in mappers that need a custom
        aggregation step (e.g. hourly rollup).
        """
        operators_config = getattr(self.data_source_config, "enrichment_operators", None)
        if operators_config is not None:
            from main_core.enrichment_operator_builder import (
                EnrichmentOperatorBuilder,
                enrichment_operator_registry,
            )
            if EnrichmentOperatorBuilder(enrichment_operator_registry).has_reshape_operators(
                operators_config
            ):
                self.logger.info(
                    "[enrichment_operators] Skipping default staging→enrichment sync: "
                    "reshape operator will INSERT directly."
                )
                return

        if self.data_source_config.storage.enrichment:
            batch_size = self._get_batch_size() if self._should_use_batching() else None
            self.db.sync_staging_to_enrichment(
                self.data_source_config.storage.staging.table_schema,
                self.data_source_config.storage.staging.table_name,
                self.data_source_config.storage.enrichment.table_schema,
                self.data_source_config.storage.enrichment.table_name,
                batch_size=batch_size,
            )

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
            "joins_on": mapping.joins_on,
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
        joins_on = getattr(mapping_conf, "joins_on", None) if mapping_conf else None
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
            "mapping_column": str(mapping_column) if mapping_column else str(joins_on) if joins_on else None,
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

    # ── Query execution ───────────────────────────────────────────────────

    def execute_query(self, table_key: str, query: str | None, params=None):
        """Execute a raw SQL query via self.db, routing mapping queries through batching logic."""
        if query is not None:
            if table_key.lower() == "mapping":
                self._execute_mapping_query(query, params)
            else:
                self.db.call_sql(query, params)
        else:
            if table_key.lower() == "mapping":
                self.logger.info(
                    "No mapping Query given. Please write a postgresql query in the "
                    "respective mapper class. Implement func map_to_link_db_query"
                )

    def _execute_mapping_query(self, query: str, params=None) -> None:
        """Decide between single-shot vs batched execution for a mapping query.

        Incremental mappings already scope to changed ways via a WHERE filter, so
        the full-base LIMIT/OFFSET batching path would scan everything for nothing.
        Only fall through to batched execution when the diff is large enough that
        one transaction would hold locks too long.
        """
        mapping_conf = getattr(self.data_source_config, "mapping", None)
        if getattr(mapping_conf, "incremental", False):
            batch_size = self._get_batch_size()
            change_count = self.base_graph.pending_change_count()
            if change_count == 0:
                self.logger.info(
                    f"[incremental] No changed ways to map for {self.data_source_name}, "
                    f"skipping query"
                )
                return
            if change_count <= batch_size or not self._should_use_batching():
                self.logger.info(
                    f"[incremental] Running mapping in single transaction "
                    f"({change_count} changed ways, batch_size={batch_size})"
                )
                self.db.call_sql(query, params)
                return
            self.logger.info(
                f"[incremental] Large diff ({change_count} changed ways) — "
                f"using batched execution with batch size {batch_size}"
            )
            self.db.call_sql_batched(query, batch_size=batch_size, params=params)
            return

        if self._should_use_batching():
            batch_size = self._get_batch_size()
            self.logger.info(
                f"Using batched execution for Mapping with batch size: {batch_size}"
            )
            self.db.call_sql_batched(query, batch_size=batch_size, params=params)
        else:
            self.db.call_sql(query, params)

    def _should_use_batching(self) -> bool:
        """Return True when database.performance.enable_batching is set in global config."""
        try:
            from main_core.core_config import CoreConfig
            config = CoreConfig().get_config()
            db_config = config.get("database", {})
            perf_config = db_config.get("performance", {})
            return perf_config.get("enable_batching", False)
        except Exception as e:
            self.logger.warning(f"Could not read batching config, defaulting to disabled: {e}")
            return False

    def _get_batch_size(self) -> int:
        """Return the batch size — datasource-specific first, then global default."""
        try:
            if hasattr(self.data_source_config, "mapping") and self.data_source_config.mapping:
                datasource_batch_size = getattr(
                    self.data_source_config.mapping, "batch_size", None
                )
                if datasource_batch_size is not None:
                    self.logger.info(
                        f"Using datasource-specific batch size: {datasource_batch_size}"
                    )
                    return int(datasource_batch_size)

            from main_core.core_config import CoreConfig
            config = CoreConfig().get_config()
            db_config = config.get("database", {})
            perf_config = db_config.get("performance", {})
            return perf_config.get("default_batch_size", 10000)
        except Exception as e:
            self.logger.warning(f"Could not read batch size config, using default: {e}")
            return 10000
