"""Shared SQL execution + batching helpers for the ETL stage mixins.

Reads from self:  self.db, self.data_source_config, self.data_source_name,
                  self.logger, self.base_graph
Used by:          EtlStagingMixin, EtlEnrichmentMixin, EtlMappingMixin (via self).
"""
from main_core.safe_class import safe_class


@safe_class
class EtlQueryMixin:
    """Routes raw SQL through single-shot or batched execution."""

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
