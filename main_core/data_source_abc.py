"""Abstract contract for all datasource pipeline components.

This is the PRIMARY reference for mapper authors.  Every method listed here
can be overridden in a concrete mapper class.  Methods marked @abstractmethod
MUST be implemented (DataSourceABCImpl provides a default, so in practice
you only need to override the ones relevant to your datasource).

Pipeline stage overview:
  FETCH      source() → fetch() / multi_fetch()
  FILTER     transform() → pre_filter_processing() → source_filter()
  PERSIST    load()
  ENRICH     execute_on_staging() → sync_staging_to_enrichment() → execute_on_enrichment()
  MAP        map_to_base() → map_to_links() / mapping_db_query()
  SCHEDULE   create_job() / execute() / run()
"""
from abc import ABC, abstractmethod
from pathlib import Path

from data_config_dtos.data_source_config_dto import SourceDTO


class DataSourceABC(ABC):

    # ── FETCH stage ───────────────────────────────────────────────────────

    @abstractmethod
    def source(self, source: SourceDTO):
        """Validate the source config and dispatch to fetch() or multi_fetch().

        Called by: extract()
        Returns:   list[str] — local file paths to process
        """
        pass

    def source_filter(self, data, filter_function=None):
        """Reshape or flatten the fetched record list before storage.

        Called by: transform(), inside the filter pipeline
        Override:  to flatten nested API structures, filter by field values,
                   deduplicate records, etc.
        Returns:   list — the processed records (must return a list)
        """
        return data

    @abstractmethod
    def fetch(self) -> list[str]:
        """Download a single URL or resolve a local file path.

        Called by: source()
        Returns:   list[str] — one-element list containing the local file path
        """
        pass

    @abstractmethod
    def read_file_content(self, path) -> list | dict | object:
        """Parse a single file and return records.

        Called by: read_files(), which is called by transform()
        Override:  to handle custom or proprietary file formats.
                   Return NotImplemented to fall back to FileHandler.
        Returns:   list[dict] — records ready for bulk insert
        """
        pass

    # ── MAPPING helper methods ────────────────────────────────────────────

    def map_to_links(self):
        """Execute the query from mapping_db_query() against the mapping table.

        Called by: execute_mapping_strategy() for 'custom' and 'mapper_sql' types
        """
        pass

    def mapping_db_query(self) -> str | None:
        """Return the INSERT … SELECT mapping SQL, or None.

        Called by: map_to_links()
        Override:  for hand-written mapping SQL that the auto-builder cannot produce
                   (e.g. LEFT JOIN LATERAL, complex CTEs).
        Returns:   str SQL  or  None (causes map_to_links to log a notice and skip)
        """
        return None

    def map_to_base(self):
        """Run the mapping step — routes to full-rescan or incremental based on config.

        Called by: finalize_after_file_processing()
        """
        pass

    # ── ENRICH helper methods ─────────────────────────────────────────────

    def execute_on_staging(self):
        """Execute staging_db_query() against the staging table.

        Called by: finalize_after_file_processing(), after raw→staging sync
        """
        pass

    def staging_db_query(self) -> str | None:
        """Return a custom SQL query to transform the staging table, or None.

        Called by: execute_on_staging()
        Override:  for post-load staging transformations (e.g. geometry normalisation).
        """
        return None

    def execute_on_enrichment(self):
        """Execute enrichment operators (config-driven) or enrichment_db_query().

        Called by: finalize_after_file_processing(), after staging→enrichment sync
        """
        pass

    def enrichment_db_query(self) -> str | None:
        """Return a custom SQL query to transform the enrichment table, or None.

        Called by: execute_on_enrichment() (legacy path, no enrichment_operators config)
        Override:  for enrichment-table transformations (e.g. CRS conversion, joins).
        """
        return None

    def check_before_update(self) -> bool:
        """Return True to allow the run to proceed; False to abort early.

        Called by: run pipeline (optional gate before processing begins)
        Override:  to add pre-run checks (e.g. minimum expected record count).
        """
        return True

    @abstractmethod
    def load(self, data):
        """Persist transformed records to the raw-staging table.

        Called by: process_file(), inside the ETLWorker thread pool
        Override:  to skip loading (e.g. return immediately to accumulate data in
                   memory first) or to use a custom persistence strategy.
        Args:
            data: list[dict] — the records returned by transform()
        """
        pass

    @abstractmethod
    def transform(self, path: Path | str) -> list:
        """Read a file and run the full filter pipeline.

        Called by: process_file()
        Runs:      read_files() → before_filter_pipeline() → pre_filter_processing()
                   → source_filter() → post_filter_processing() → after_filter_pipeline()
        Returns:   list[dict] — filtered, ready-to-load records
        """
        pass

    @abstractmethod
    def extract(self) -> list[str]:
        """Call source() to get paths and log the count.

        Called by: execute_run_pipeline()
        Returns:   list[str] — local file paths to process
        """
        pass

    # ── Filter pipeline hooks ─────────────────────────────────────────────

    def pre_filter_processing(self, data):
        """Hook executed before source_filter() inside transform().

        Called by: transform()
        Override:  for pre-processing work that depends on the raw data but should
                   not change it (e.g. building a KDTree or spatial index).
        Args:
            data: list[dict] — raw records from read_files()
        """
        pass

    def post_filter_processing(self, data):
        """Hook executed after source_filter() inside transform().

        Called by: transform()
        Default:   optionally saves data to a file when after_filter_hook.save is set.
        Override:  for post-filter work (e.g. validation, extra transformations).
        """
        pass

    def pre_database_processing(self):
        """Hook executed inside load() just before the bulk insert.

        Called by: load()
        Override:  to prepare data structures before writing to the DB.
        """
        pass

    def post_database_processing(self):
        """Hook executed after all file threads finish, before the raw→staging sync.

        Called by: finalize_after_file_processing()
        Override:  to flush in-memory results to the DB (e.g. elevation metrics
                   accumulated across tile threads).
        """
        pass

    # ── Scheduler / run entry points ──────────────────────────────────────

    @abstractmethod
    def create_job(self):
        """Register this datasource as an APScheduler job.

        Called by: execute() when a scheduler is configured
        """
        pass

    def execute(self):
        """Trigger datasource execution after initialisation.

        Called once per datasource startup.  Routes to create_job() when a
        scheduler exists, or calls run() directly otherwise.
        """
        pass

    def run(self) -> dict:
        """Main execution function — called by the scheduler or directly.

        Resets run state, runs the full pipeline, records metadata outcome,
        and fires run_end_cleanup() unconditionally in the finally block.
        Returns: dict with 'message', 'duration', and 'level' keys
        """
        pass

    # ── Per-file lifecycle hooks ──────────────────────────────────────────

    def before_process_file(self, path: Path | str):
        """Called before transform() for each individual file in the thread pool.

        Called by: process_file()
        Override:  for per-file setup (e.g. opening a side-channel connection).
        """
        pass

    def after_process_file(self, path: Path | str, transformed_data):
        """Called after load() for each individual file in the thread pool.

        Called by: process_file()
        Override:  for per-file post-processing (e.g. update a progress counter).
        """
        pass

    def on_process_file_error(self, path: Path | str, error: Exception):
        """Called when process_file() raises an exception.

        Called by: process_file() exception handler
        Override:  for per-file error handling (e.g. mark file as failed in a log).
        """
        pass

    def run_end_cleanup(self, succeeded: bool, error: Exception | None = None):
        """Called once at the very end of a run, regardless of outcome.

        Called by: run() finally block
        Override:  for cleanup (e.g. delete temp files, release memory caches).
        Args:
            succeeded: True if the run completed without an unhandled exception
            error:     the Exception if succeeded is False, else None
        """
        pass
