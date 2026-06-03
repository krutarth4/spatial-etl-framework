"""Run lifecycle, thread-pool file processing, and APScheduler job creation.

Reads from self:  ALL other mixins (this is the top-level orchestrator)
Calls into:       StateMixin       (_reset_run_state, _is_dataset_expired, …)
                  MetadataTrackingMixin (_mark_metadata_run_started/finished, …)
                  TableManagementMixin  (create_data_tables, create_indexes_for_table, …)
                  FileReadTransformMixin (process_file via transform)
                  EtlOperationsMixin    (sync_raw_to_staging, execute_on_staging, …)
                  TimingReportingMixin  (_time_stage, _accumulate_stage, run_job_response)
                  LifecycleHooksMixin   (post_database_processing, run_end_cleanup)
"""
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from enum import Enum

from apscheduler.triggers.calendarinterval import CalendarIntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger

from main_core.safe_class import safe_class


class TriggerTypeEnum(Enum):
    CRON = "cron"
    DATE = "date"
    INTERVAL = "interval"

    @classmethod
    def has_value(cls, value):
        return value in cls._value2member_map_


@safe_class
class RunPipelineMixin:
    """Orchestrates the full run lifecycle and manages the APScheduler job."""

    # ── Top-level entry points ────────────────────────────────────────────

    def execute(self):
        """Trigger datasource execution after initialisation.

        If a scheduler is configured, registers the job and returns.
        If run_once semantics apply and the datasource already completed
        successfully with healthy tables, skips the run.
        Otherwise calls run() directly.
        """
        if self.scheduler is not None:
            self.create_job()
            return

        # Honour run_once semantics even without a scheduler
        if self._is_run_once_and_completed():
            self.start_timer = time.perf_counter()
            self._run_started_at = datetime.utcnow()
            self._stage_timings = {}
            self.logger.info(
                f"[run_once] '{self.data_source_name}' already completed successfully "
                f"in a previous run — skipping."
            )
            self.run_job_response("Skipped — already completed (run_once)", level="info")
            return

        self.logger.debug("No scheduler found, executing datasource directly")
        self.run()

    def run(self) -> dict:
        """Main execution function — called directly or by the scheduler.

        Resets run state, runs the pipeline, records metadata, and fires the
        cleanup hook regardless of outcome.
        """
        self._reset_run_state()
        self.start_execution()
        self._mark_metadata_run_started()
        run_started_at = datetime.utcnow()
        run_succeeded = False
        run_error: Exception | None = None
        run_result = None
        try:
            if self._is_dataset_expired():
                self.logger.info(
                    f"Dataset '{self.data_source_name}' has expired — clearing tables for fresh load"
                )
                self._clear_dataset_tables()
            result = self.execute_run_pipeline()
            run_result = result
            run_succeeded = True
            return result
        except Exception as e:
            run_error = e
            self.on_run_error(e)
            return self.run_job_response(f"Job failed: {e}", level="error")
        finally:
            run_duration = int((datetime.utcnow() - run_started_at).total_seconds())
            self._mark_metadata_run_finished(run_succeeded, run_result, run_error, run_duration)
            self.run_end_cleanup(run_succeeded, run_error)

    def execute_run_pipeline(self) -> dict:
        """Orchestrate the full extract → transform → load → map pipeline.

        Short-circuits when no new data is available AND downstream tables
        are already healthy (avoids redundant processing on repeated runs).
        """
        with self._time_stage("extract"):
            paths = self.extract()
        self._update_metadata_runtime_paths(paths)
        if not self.is_run_input_available(paths):
            return self.run_job_response("No files available")

        # _last_fetch_performed_download is False when a metadata check found no
        # new data.  None means no check was performed (e.g. local source).
        if self._last_fetch_performed_download is False:
            populated, reason = self._downstream_tables_are_populated()
            if populated:
                self.logger.info(
                    f"[{self.data_source_name}] No new data and tables are healthy — "
                    f"skipping transform / load / mapping stages."
                )
                self.after_datasource_success(sync_result={"inserted": 0, "updated": 0})
                return self.run_job_response(
                    "Skipped — no new data, tables already populated"
                )
            else:
                self.logger.warning(
                    f"[{self.data_source_name}] No new data downloaded but tables need "
                    f"(re)building: {reason} — forcing full pipeline run."
                )

        with self._time_stage("prepare_tables"):
            self.prepare_run_resources(paths)
        with self._time_stage("process_files_wall"):
            self.process_extracted_paths(paths)
        self.finalize_after_file_processing()
        if self._run_degraded:
            return self.run_job_response(
                f"Job finished with warnings: {self._run_stage_warnings}",
                level="warning",
            )
        return self.run_job_response("Job finished Successfully !!!")

    # ── Pipeline stages ───────────────────────────────────────────────────

    def prepare_run_resources(self, paths: list[str]):
        """Create ETL tables before file processing begins."""
        self.create_data_tables()

    def process_extracted_paths(self, paths: list[str]):
        self.run_file_processing_stage(paths)

    def run_file_processing_stage(self, paths: list[str]):
        backend = self.get_process_file_backend()
        if backend == "threadpool":
            self.run_threadpool_file_processing(paths)
            return
        raise ValueError(f"Unsupported process_file backend: {backend}")

    def get_process_file_backend(self) -> str:
        return "threadpool"

    def get_process_file_worker_count(self) -> int:
        cpu_count = os.cpu_count() or 1
        return min(self._default_max_workers_cap, cpu_count * 2)

    def run_threadpool_file_processing(self, paths: list[str]):
        """Submit all file paths to a thread pool and wait for completion."""
        max_workers = self.get_process_file_worker_count()
        self.logger.critical(f"Starting with {max_workers} workers")
        with ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="ETLWorker"
        ) as executor:
            futures = [executor.submit(self.process_file, path) for path in paths]
            for future in as_completed(futures):
                future.result()

    def process_file(self, path: str):
        """Single-file ETL unit: transform → load, with lifecycle hooks and timing."""
        thread = threading.current_thread()
        thread_id = threading.get_ident()
        start = time.monotonic()
        self.logger.info(
            f"[THREAD START] name={thread.name} id={thread_id} file={path}"
        )
        self.logger.info(f"Processing file {path}")

        try:
            self.before_process_file(path)
            t0 = time.monotonic()
            with self._accumulate_stage("transform"):
                transformed_data = self.transform(path)
            self.logger.info(
                f"[THREAD TRANSFORM DONE] name={thread.name} "
                f"rows={len(transformed_data) if transformed_data else 0} "
                f"time={time.monotonic() - t0:.2f}s"
            )

            if not self.should_load_transformed_data(transformed_data, path):
                self.logger.info(f"[THREAD SKIP] name={thread.name} no data")
                return

            t1 = time.monotonic()
            with self._accumulate_stage("load_raw_staging"):
                self.load(transformed_data)
            self.logger.info(
                f"[THREAD LOAD DONE] name={thread.name} "
                f"time={time.monotonic() - t1:.2f}s"
            )
            self.after_process_file(path, transformed_data)

        except Exception as e:
            self.on_process_file_error(path, e)
            self.logger.error(f"[THREAD ERROR] name={thread.name} file={path}")
            raise
        finally:
            self.logger.info(
                f"[THREAD END] name={thread.name} "
                f"total_time={time.monotonic() - start:.2f}s"
            )

    def finalize_after_file_processing(self):
        """Run all post-load stages: post_database_processing, sync, index, enrich, map."""
        self.post_database_processing()
        sync_result = None
        if self.data_source_config.storage.persistent:
            with self._time_stage("raw->staging"):
                sync_result = self.sync_raw_to_staging()
            with self._time_stage("idx_staging"):
                self.create_indexes_for_table("staging")
            with self._time_stage("exec_on_staging"):
                self.execute_on_staging()
            with self._time_stage("staging->enrichment"):
                self.sync_staging_to_enrichment()
            with self._time_stage("idx_enrichment"):
                self.create_indexes_for_table("enrichment")
            with self._time_stage("exec_on_enrichment"):
                self.execute_on_enrichment()
        with self._time_stage("map_to_base"):
            self.map_to_base()
        with self._time_stage("idx_mapping"):
            self.create_indexes_for_table("mapping")
        self.after_datasource_success(sync_result)
        self.cleanup_after_finalize(sync_result)

    def after_datasource_success(self, sync_result: dict | None = None):
        """Trigger materialized view refresh on successful datasource run."""
        with self._time_stage("mv_refresh"):
            self.trigger_materialized_views(sync_result)

    def cleanup_after_finalize(self, sync_result: dict | None):
        """Drop the raw-staging table (backup if sync was not successful)."""
        backup_raw = not (sync_result or {}).get("success")
        self.clean_raw_staging_table(backup_raw)

    def trigger_materialized_views(self, sync_result: dict | None = None):
        """Refresh materialized views that depend on this datasource."""
        if self.db is None:
            return
        try:
            from main_core.core_config import CoreConfig
            from materialized_views.manager import MaterializedViewManager
            conf = CoreConfig().get_config()
            mv_conf = (conf or {}).get("materialized_views", {})
            MaterializedViewManager(self.db, mv_conf).on_datasource_success(
                self.data_source_name, sync_result
            )
        except Exception as e:
            self.logger.error(
                f"Materialized view trigger failed for datasource {self.data_source_name}: {e}"
            )
            self._note_stage_warning("trigger_materialized_views", e)

    # ── Scheduler job creation ────────────────────────────────────────────

    def create_job(self):
        """Register this datasource as an APScheduler job."""
        self.logger.info(f"Job creation started for {self.job_configuration.name}")

        trigger_conf = self.job_configuration.trigger.type

        REQUIRES_CONFIG = {"interval", "cron", "calendar_interval"}
        FORBIDS_CONFIG = {"run_once", "date"}

        if trigger_conf.name in FORBIDS_CONFIG and trigger_conf.config:
            self.logger.warning(
                "Trigger type '%s' does not use 'config' — remove it from the job "
                "definition for datasource '%s'.",
                trigger_conf.name,
                self.data_source_name,
            )
        if trigger_conf.name in REQUIRES_CONFIG and not trigger_conf.config:
            raise ValueError(
                f"Trigger type '{trigger_conf.name}' requires a 'config' block "
                f"for datasource '{self.data_source_name}'."
            )

        TRIGGER_MAP = {
            "interval": IntervalTrigger,
            "date": DateTrigger,
            "cron": CronTrigger,
            "calendar_interval": CalendarIntervalTrigger,
            "run_once": DateTrigger,
        }

        trigger_cls = TRIGGER_MAP[trigger_conf.name]
        if trigger_conf.name == "run_once":
            trigger = trigger_cls(run_date=datetime.now())
        else:
            if trigger_conf.start_date is not None:
                if trigger_conf.name != "date":
                    trigger = trigger_cls(
                        **trigger_conf.config, start_date=trigger_conf.start_date
                    )
                else:
                    trigger = trigger_cls(run_date=trigger_conf.start_date)
            else:
                trigger = trigger_cls(**trigger_conf.config)

        job_conf = {
            "func": self.run,
            "trigger": trigger,
            "name": self.job_configuration.name,
            "replace_existing": self.job_configuration.replace_existing,
            "executor": (
                "process" if self.job_configuration.executor is not None else "default"
            ),
        }
        self.scheduler.add_job(
            job_conf, self.job_configuration.id or self.data_source_name
        )
