"""Instance initialisation, per-run state reset, and health-check predicates.

This is the only mixin that defines __init__.  All instance attributes
(self.db, self.logger, self._stage_timings, …) are created here so every
other mixin can safely access them via self.*.

Calls into:  MetadataTrackingMixin._register_datasource_metadata()  (at end of __init__)
             MetadataTrackingMixin._note_stage_warning()             (via _downstream_tables_are_populated)
"""
import threading
import time
from pathlib import Path
from typing import Any

from core.base_graph import BaseGraph
from core.init_scheduler import InitScheduler
from database.db_instance import DbInstance
from log_manager.logger_manager import PipelineLogger
from main_core.safe_class import safe_class
from data_config_dtos.data_source_config_dto import DataSourceDTO, SourceInputDTO


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
        # name -> DataSourceDTO for peer datasources; injected by DataSourceMapper
        # before execute(). Empty when run standalone (no dependencies resolvable).
        self.peer_configs: dict = {}
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

    # ── Dependency resolution (depends_on) ─────────────────────────────────

    @staticmethod
    def is_file_present_and_nonempty(path) -> bool:
        """Return True if path points to an existing, non-empty file."""
        if not path:
            return False
        try:
            p = Path(path)
            return p.exists() and p.is_file() and p.stat().st_size > 0
        except OSError:
            return False

    def _normalize_depends_on(self) -> list[str]:
        """Return depends_on as a list of upstream names ([] when unset)."""
        raw = getattr(self.data_source_config, "depends_on", None)
        if not raw:
            return []
        if isinstance(raw, str):
            return [raw]
        if isinstance(raw, (list, tuple)):
            return [str(name) for name in raw if name]
        return []

    def _resolve_source_input_paths(self) -> list[str]:
        """Auto-resolve the file(s) this datasource consumes as its source input.

        Returns [] when there is no pre-existing input file to verify (e.g.
        multi-fetch strategies that download during extract).
        """
        source = getattr(self.data_source_config, "source", None)
        if source is None:
            return []

        multi = getattr(source, "multi_fetch", None)
        if multi is not None and getattr(multi, "enable", False):
            strategy = getattr(multi, "strategy", None)
            if strategy == "explicit_url_list":
                urls = getattr(multi, "urls", None)
                if isinstance(urls, SourceInputDTO):
                    return [urls.input]
                if isinstance(urls, dict) and urls.get("input"):
                    return [urls["input"]]
            # expand_params / url_template fetch during extract — nothing to verify
            return []

        if source.fetch in ("local",):
            return [source.file_path] if source.file_path else []

        # single http: verify the cached download if present
        resolved = self.resolve_latest_saved_path(source.destination)
        candidate = resolved or source.destination
        return [candidate] if candidate else []

    def _upstream_tables_satisfied(self, upstream_dto) -> tuple[bool, list[str]]:
        """Check an upstream's configured output tables exist and are non-empty.

        Tables not configured on the upstream are skipped. An upstream with no
        configured DB tables (file-only producer) is satisfied vacuously.
        """
        reasons: list[str] = []
        if self.db is None:
            return True, []  # cannot verify; do not block on table check
        storage = getattr(upstream_dto, "storage", None)
        if storage and getattr(storage, "staging", None):
            s = storage.staging
            if self._safe_table_count(s.table_name, s.table_schema) == 0:
                reasons.append(f"staging table {s.table_schema}.{s.table_name} empty/missing")
        if storage and getattr(storage, "enrichment", None):
            e = storage.enrichment
            if self._safe_table_count(e.table_name, e.table_schema) == 0:
                reasons.append(f"enrichment table {e.table_schema}.{e.table_name} empty/missing")
        mapping = getattr(upstream_dto, "mapping", None)
        if mapping and getattr(mapping, "enable", False) and getattr(mapping, "table_name", None):
            if self._safe_table_count(mapping.table_name, mapping.table_schema) == 0:
                reasons.append(f"mapping table {mapping.table_schema}.{mapping.table_name} empty/missing")
        return (not reasons), reasons

    def _dependency_satisfied(self) -> tuple[bool, list[str], list[str]]:
        """Evaluate all depends_on conditions.

        Returns (satisfied, unmet_reasons, blocking_disabled_upstreams).
        """
        names = self._normalize_depends_on()
        if not names:
            return True, [], []

        reasons: list[str] = []
        blocking_disabled: list[str] = []

        for name in names:
            upstream = self.peer_configs.get(name)
            if upstream is None:
                # Unknown name can never complete on its own — treat as blocking
                # (skip) rather than waiting forever on a config typo.
                reasons.append(f"unknown upstream datasource '{name}'")
                blocking_disabled.append(name)
                continue

            upstream_unmet: list[str] = []

            # (1) metadata: upstream completed successfully at least once
            completed = bool(
                self.metadata_service
                and self.metadata_service.has_completed_successfully(name)
            )
            if not completed:
                upstream_unmet.append(f"'{name}' not completed in metadata")

            # (2) upstream output tables exist & non-empty (vacuous if none configured)
            tables_ok, table_reasons = self._upstream_tables_satisfied(upstream)
            if not tables_ok:
                upstream_unmet.append(f"'{name}': " + "; ".join(table_reasons))

            if upstream_unmet:
                reasons.extend(upstream_unmet)
                if not getattr(upstream, "enable", False):
                    blocking_disabled.append(name)

        # (3) current datasource's own source input file(s) present & non-empty
        for input_path in self._resolve_source_input_paths():
            if not self.is_file_present_and_nonempty(input_path):
                reasons.append(f"source input file missing/empty: {input_path}")

        return (not reasons), reasons, blocking_disabled

    def _wait_or_skip_for_dependencies(self) -> tuple[str, str]:
        """Gate the run on depends_on. Returns ("proceed", "") or ("skip", reason).

        - Already satisfied        -> proceed immediately.
        - Unmet + disabled upstream -> warn + skip (it can never finish on its own).
        - Unmet, all enabled        -> warn + poll every 5s (no timeout) until satisfied.
        """
        if not self._normalize_depends_on():
            return "proceed", ""

        waited = False
        while True:
            satisfied, reasons, blocking_disabled = self._dependency_satisfied()
            if satisfied:
                if waited:
                    self.logger.info(
                        f"[{self.data_source_name}] Dependencies satisfied — proceeding."
                    )
                return "proceed", ""

            reason_text = "; ".join(reasons)
            if blocking_disabled:
                msg = (
                    f"[{self.data_source_name}] depends_on not met and upstream(s) "
                    f"{blocking_disabled} are DISABLED — skipping this run. "
                    f"Enable and run them first. Details: {reason_text}"
                )
                self.logger.warning(msg)
                return "skip", reason_text

            self.logger.warning(
                f"[{self.data_source_name}] Waiting for dependencies (poll 5s): {reason_text}"
            )
            if self.metadata_service is not None:
                try:
                    self.metadata_service.update_run_status(
                        self.data_source_name,
                        status="waiting",
                        message=f"Waiting for dependencies: {reason_text}",
                    )
                except Exception:
                    pass
            waited = True
            time.sleep(5)

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

    def _source_has_change_detection(self) -> bool:
        """True when the source is configured to detect upstream changes via a
        metadata check (e.g. last_modified), so a completed run_once datasource
        should still poll the source instead of being skipped outright.
        """
        try:
            source = getattr(self.data_source_config, "source", None)
            check = getattr(source, "check_metadata", None) if source else None
            return bool(check and getattr(check, "enable", False))
        except Exception:
            return False

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
            # When the source can detect upstream changes (e.g. last_modified),
            # don't blanket-skip. Let the run proceed so extract() performs the
            # cheap metadata check; execute_run_pipeline() then skips the heavy
            # transform/load/mapping stages itself when nothing new was fetched.
            if self._source_has_change_detection():
                self.logger.info(
                    f"[run_once] '{self.data_source_name}' completed before, but source "
                    f"has change detection — running metadata check instead of skipping."
                )
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
