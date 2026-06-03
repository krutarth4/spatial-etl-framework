"""Overridable lifecycle hooks and the default load() implementation.

These are the primary extension points for mapper subclasses.  Every method
here has a working default; mappers override only the ones they need.

Overridable by mappers:
  load()                  — persist transformed records to raw-staging
  source_filter()         — reshape/flatten the fetched data list
  pre_filter_processing() — custom logic before source_filter (e.g. KDTree build)
  post_database_processing() — post-load work (e.g. metrics → staging upsert)
  before_process_file()   — per-file hook before transform
  after_process_file()    — per-file hook after load
  on_process_file_error() — per-file error hook
  run_end_cleanup()       — cleanup at the very end of a run

Reads from self:  self.db, self.logger, self.data_source_config,
                  self.data_source_name, self.raw_staging_table,
                  self.raw_staging_schema
"""
from pathlib import Path
from typing import Any, List

from handlers.file_handler import FileHandler
from main_core.safe_class import safe_class


@safe_class
class LifecycleHooksMixin:
    """Default implementations of all overridable pipeline hooks."""

    # ── Data loading ──────────────────────────────────────────────────────

    # Overridable: mappers may override this method to skip or customise the DB insert
    def load(self, data):
        """Persist transformed records to the raw-staging table.

        Accepts either a plain list[dict] (full-load path) or a generator of
        list[dict] chunks (streaming path).  In streaming mode each chunk is
        inserted and freed before the next is fetched, keeping peak RAM to one
        chunk instead of the full dataset.

        Override in a mapper to skip loading (return immediately) or to use a
        completely different persistence strategy (e.g. accumulate in memory).
        """
        db_storage = self.data_source_config.storage
        try:
            if not db_storage.persistent:
                self.logger.warning(
                    f"data source {self.data_source_name} persistent is set to false. "
                    f"Hence it won't be saved to the database"
                )
            else:
                if self.db is not None:
                    self.logger.warning("found new data hence continuing with db upsert")
                    self.before_load(data)
                    self.pre_database_processing()
                    if isinstance(data, list):
                        self.db.bulk_insert(
                            self.raw_staging_table, self.raw_staging_schema, data, True
                        )
                    else:
                        # Streaming: insert one chunk at a time, each freed after insert
                        for chunk in data:
                            self.db.bulk_insert(
                                self.raw_staging_table, self.raw_staging_schema, chunk, True
                            )
                    self.after_load(data)
        except Exception as e:
            self.logger.error(f"Error occurred while loading the file into Database: {e}")
            raise

    # ── Filter pipeline hooks ─────────────────────────────────────────────

    # Overridable: mappers may override this method to flatten/transform API responses
    def source_filter(self, data: list[Any]) -> list[Any]:
        """Reshape the fetched data list before it is stored.

        Called inside transform() after read_files().  Override to flatten
        nested API structures, filter records, etc.  Default: returns data unchanged.
        """
        return data

    # Overridable: mappers may override this method (e.g. to build a KDTree)
    def pre_filter_processing(self, data):
        """Hook executed before source_filter().

        Override for pre-processing work that depends on the raw record list
        but should not change it (e.g. building a spatial index from the data).
        """
        pass

    def post_filter_processing(self, data):
        """Hook executed after source_filter().

        Default: optionally saves data to a file if after_filter_hook.save is set.
        """
        if (
            self.data_source_config.after_filter_hook is not None
            and self.data_source_config.after_filter_hook.save
        ):
            conf = self.data_source_config.after_filter_hook
            if conf is not None and conf.save:
                self.post_filter_processing_save_data(conf, data)

    def post_filter_processing_save_data(self, conf, data):
        file_handler = FileHandler(conf.destination)
        file_handler.save_data(conf.destination, data, True)

    def before_filter_pipeline(self, data, path):
        """Hook called at the very start of the filter pipeline in transform()."""
        pass

    def after_filter_pipeline(self, data, path):
        """Hook called at the very end of the filter pipeline in transform()."""
        pass

    # ── Database hooks ────────────────────────────────────────────────────

    def before_load(self, data):
        """Hook called just before bulk_insert inside load()."""
        pass

    def after_load(self, data):
        """Hook called immediately after bulk_insert inside load()."""
        pass

    # Overridable: mappers may override this method for pre-DB work
    def pre_database_processing(self):
        """Hook executed before the DB insert inside load().

        Override to prepare data structures (e.g. clear in-memory caches).
        """
        pass

    # Overridable: mappers may override this method (e.g. metrics → staging upsert)
    def post_database_processing(self):
        """Hook executed after all files are loaded, before staging sync.

        Override to flush in-memory results to the DB (e.g. elevation metrics).
        """
        pass

    # ── Per-file hooks ────────────────────────────────────────────────────

    # Overridable: mappers may override this method for per-file setup
    def before_process_file(self, path: Path | str):
        """Called before transform() for each individual file."""
        pass

    # Overridable: mappers may override this method for per-file post-processing
    def after_process_file(self, path: Path | str, transformed_data):
        """Called after load() for each individual file."""
        pass

    # Overridable: mappers may override this method for custom error handling
    def on_process_file_error(self, path: Path | str, error: Exception):
        """Called when process_file() raises an exception."""
        pass

    def should_load_transformed_data(self, transformed_data, path: str) -> bool:
        """Return True if the transformed data should be passed to load().

        Override to add custom skip conditions (e.g. empty geometry check).
        """
        return bool(transformed_data)

    # ── Run-level cleanup ─────────────────────────────────────────────────

    # Overridable: mappers may override this method for cleanup on run end
    def run_end_cleanup(self, succeeded: bool, error: Exception | None = None):
        """Final hook executed once at the very end of a run (success or failure).

        Override in mappers for temp-file cleanup, memory release, etc.
        """
        pass
