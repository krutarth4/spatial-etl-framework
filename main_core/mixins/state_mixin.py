"""Instance initialisation, per-run state reset, and health-check predicates.

This is the only mixin that defines __init__.  All instance attributes
(self.db, self.logger, self._stage_timings, …) are created here so every
other mixin can safely access them via self.*.

Calls into:  MetadataTrackingMixin._register_datasource_metadata()  (at end of __init__)
             MetadataTrackingMixin._note_stage_warning()             (via _downstream_tables_are_populated)
"""
import threading
import time
from typing import Any

from core.base_graph import BaseGraph
from core.init_scheduler import InitScheduler
from database.db_instancce import DbInstance
from log_manager.logger_manager import PipelineLogger
from main_core.safe_class import safe_class
from data_config_dtos.data_source_config_dto import DataSourceDTO


@safe_class
class StateMixin:
    """Owns __init__ and all per-run state that the other mixins share via self.*"""

    # Maximum parallel file-processing workers (capped so we don't overload a small VM)
    _default_max_workers_cap = 3

    def __init__(
        self,
        data_source_conf: DataSourceDTO,
        db_instance: DbInstance | None,
        scheduler_core: InitScheduler,
        base_graph_conf,
        metadata_service,
    ):
        # ── Core services ──────────────────────────────────────────────────
        self.metadata_service = metadata_service
        # Zero-overhead structured logger; injects mapper name into every log record
        self.logger = PipelineLogger(type(self).__name__)
        self.logger.info(f"Initializing {type(self).__name__}")

        self.base_graph = BaseGraph(db_instance, base_graph_conf)
        self.data_source_config = data_source_conf
        self.data_source_name = data_source_conf.name
        self.db = db_instance
        self.job_configuration = data_source_conf.job
        self.scheduler = scheduler_core

        # Stamp the mapper name onto the DB logger so DB errors identify their owner
        if self.db is not None:
            self.db.set_owner(type(self).__name__)

        # ── Per-run mutable state ──────────────────────────────────────────
        self.start_timer = None
        self.end_timer = None
        self.raw_staging_table = None
        self.raw_staging_schema = None
        self._last_fetch_performed_download: bool | None = None
        self._stage_timings: dict[str, float] = {}
        self._stage_lock = threading.Lock()

        # Register datasource in metadata store (calls MetadataTrackingMixin)
        self._register_datasource_metadata()

    # ── Run-state management ───────────────────────────────────────────────

    def _reset_run_state(self):
        """Reset all per-run instance state so consecutive runs are fully independent."""
        self.raw_staging_table = None
        self.raw_staging_schema = None
        self._last_fetch_performed_download = None
        self._run_degraded = False
        self._run_stage_warnings = []
        self._stage_timings = {}
        self._run_started_at = None

    def start_execution(self):
        """Record start timestamp and perf counter for duration calculation."""
        import time
        from datetime import datetime
        self.logger.info(f"Executing starting for datasource {self.data_source_config.name}")
        self.start_timer = time.perf_counter()
        self._run_started_at = datetime.utcnow()

    def on_run_error(self, error: Exception):
        self.logger.error(f"Error occurred in run {error}")

    # ── Input / health-check predicates ────────────────────────────────────

    @staticmethod
    def is_file_available(path: list) -> bool:
        """Return True if the path list is non-empty."""
        if path is None or len(path) == 0:
            return False
        return True

    def is_run_input_available(self, paths: list | None) -> bool:
        return StateMixin.is_file_available(paths)

    def check_before_update(self) -> bool:
        """Return True to proceed with the run (default: always run).

        Override to add custom pre-run checks (e.g. compare checksums).
        """
        return True

    @staticmethod
    def check_before_update_condition(self, old_data: Any, new_data: Any):
        if old_data is None or new_data is None or (len(old_data) != len(new_data)):
            return True
        return False

    def should_load_transformed_data(self, transformed_data, path: str) -> bool:
        return bool(transformed_data)

    def _safe_table_count(self, table_name: str, schema: str) -> int:
        """Return row count of a table, or 0 if it does not exist yet."""
        try:
            if not self.db.table_exists(table_name, schema):
                return 0
            return self.db.get_table_count(table_name, schema)
        except Exception as e:
            self.logger.warning(
                f"[{self.data_source_name}] Could not count {schema}.{table_name}: {e}"
            )
            return 0

    def _downstream_tables_are_populated(self) -> tuple[bool, str]:
        """Check that staging, enrichment, and mapping tables all have data.

        For the mapping table the threshold is >= ways_base row count because some
        datasources map multiple rows per way (e.g. air-quality grids).

        Returns:
            (True,  "")                        — all tables healthy, safe to skip
            (False, "<human-readable reason>") — at least one table is empty/missing
        """
        if self.db is None:
            return False, "no db connection"

        storage = self.data_source_config.storage
        reasons: list[str] = []
        checks_performed = 0

        # ── staging ───────────────────────────────────────────────────────
        if storage and storage.staging:
            checks_performed += 1
            s = storage.staging
            count = self._safe_table_count(s.table_name, s.table_schema)
            if count == 0:
                reasons.append(
                    f"staging table {s.table_schema}.{s.table_name} is empty or missing"
                )

        # ── enrichment ────────────────────────────────────────────────────
        if storage and storage.enrichment:
            checks_performed += 1
            e = storage.enrichment
            count = self._safe_table_count(e.table_name, e.table_schema)
            if count == 0:
                reasons.append(
                    f"enrichment table {e.table_schema}.{e.table_name} is empty or missing"
                )

        # ── mapping: must have >= ways_base row count ─────────────────────
        mapping_conf = getattr(self.data_source_config, "mapping", None)
        if mapping_conf and getattr(mapping_conf, "enable", False):
            checks_performed += 1
            mapped_count = self._safe_table_count(
                mapping_conf.table_name, mapping_conf.table_schema
            )
            ways_count = self.base_graph.get_base_graph_row_counts()
            if mapped_count < ways_count:
                reasons.append(
                    f"mapping table {mapping_conf.table_schema}.{mapping_conf.table_name} "
                    f"has {mapped_count} rows but ways_base has {ways_count}"
                )
            else:
                self.logger.info(
                    f"[{self.data_source_name}] Mapping table healthy: "
                    f"{mapped_count} mapping rows >= {ways_count} ways_base rows"
                )

        # No DB tables configured (persistent: false, mapping disabled) → force re-run
        if checks_performed == 0:
            return False, "no DB tables configured to verify health — re-running pipeline"

        if reasons:
            return False, "; ".join(reasons)
        return True, ""

    def _is_dataset_expired(self) -> bool:
        """Return True if the dataset has passed its configured expiry date."""
        expires_after = getattr(self.data_source_config.storage, "expires_after", None)
        if not expires_after or self.metadata_service is None:
            return False
        return self.metadata_service.is_dataset_expired(self.data_source_name, expires_after)

    def _clear_dataset_tables(self):
        """Drop staging and enrichment tables so they are force-recreated on this run."""
        storage = self.data_source_config.storage
        tables = []
        if storage and storage.staging:
            tables.append((storage.staging.table_schema, storage.staging.table_name))
        if storage and storage.enrichment:
            tables.append(
                (storage.enrichment.table_schema, storage.enrichment.table_name)
            )
        for schema, table in tables:
            try:
                self.db.drop_table(table, schema, backup=False, check_exist=True, cascade=True)
                self.logger.info(f"Expired dataset: dropped {schema}.{table}")
            except Exception as e:
                self.logger.error(f"Failed to drop expired table {schema}.{table}: {e}")

    def _is_run_once_and_completed(self) -> bool:
        """Return True when the datasource uses a run_once trigger, has already
        finished successfully, AND downstream tables are still healthy.

        Used by execute() to skip unnecessary re-runs on restart while still
        re-running when data is missing (e.g. after a DB wipe).
        """
        try:
            trigger_conf = (
                self.job_configuration.trigger.type
                if self.job_configuration and self.job_configuration.trigger
                else None
            )
            if trigger_conf is None or getattr(trigger_conf, "name", None) != "run_once":
                return False
            if self.metadata_service is None:
                return False
            if not self.metadata_service.has_completed_successfully(self.data_source_name):
                return False
            populated, reason = self._downstream_tables_are_populated()
            if not populated:
                self.logger.info(
                    f"[run_once] '{self.data_source_name}' ran before but data is missing "
                    f"({reason}) — re-running."
                )
                return False
            return True
        except Exception as e:
            self.logger.warning(
                f"[run_once] Could not determine prior completion for "
                f"'{self.data_source_name}': {e} — will run."
            )
            return False
