"""Enrichment ETL operations: staging → enrichment table.

Supports both the declarative config-driven `enrichment_operators` path and the
legacy Python `enrichment_db_query()` override.

Reads from self:  self.db, self.data_source_config, self.logger
Calls into:       EtlQueryMixin (execute_query, _get_batch_size, _should_use_batching)
"""
from main_core.safe_class import safe_class


@safe_class
class EtlEnrichmentMixin:
    """Implements the staging → enrichment step."""

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
