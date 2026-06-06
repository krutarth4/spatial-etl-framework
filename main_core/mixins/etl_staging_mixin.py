"""Staging ETL operations: raw-staging → staging table.

Reads from self:  self.db, self.data_source_config, self.raw_staging_schema,
                  self.raw_staging_table
Calls into:       EtlQueryMixin (execute_query, _get_batch_size, _should_use_batching)
"""
from main_core.safe_class import safe_class


@safe_class
class EtlStagingMixin:
    """Implements the raw-staging → staging step."""

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
