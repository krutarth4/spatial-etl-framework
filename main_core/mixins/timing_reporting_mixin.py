"""Stage timing accumulation and ASCII / CSV reporting.

Reads from self:  self.logger, self._stage_timings, self._stage_lock,
                  self._run_started_at, self.data_source_config, self.start_timer
"""
import csv
import threading
import time
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

from main_core.safe_class import safe_class
from utils.execution_time import format_duration


@safe_class
class TimingReportingMixin:
    """Accumulates wall-clock stage durations and emits an ASCII table + CSV row."""

    # Fixed order so the table reads top-to-bottom along the pipeline.
    # '(Σ)' marks stages that are summed across parallel workers.
    _STAGE_REPORT_ORDER = [
        ("extract",              "extract"),
        ("download",             "  download (Σ)"),
        ("prepare_tables",       "prepare_tables"),
        ("process_files_wall",   "process_files (wall)"),
        ("transform",            "  transform (Σ)"),
        ("load_raw_staging",     "  load_raw_staging (Σ)"),
        ("raw->staging",         "raw -> staging"),
        ("idx_staging",          "idx_staging"),
        ("exec_on_staging",      "exec_on_staging"),
        ("staging->enrichment",  "staging -> enrichment"),
        ("idx_enrichment",       "idx_enrichment"),
        ("exec_on_enrichment",   "exec_on_enrichment"),
        ("map_to_base",          "map_to_base"),
        ("idx_mapping",          "idx_mapping"),
        ("mv_refresh",           "mv_refresh"),
    ]

    @contextmanager
    def _time_stage(self, name: str):
        """Record wall-clock duration of a single (serial) pipeline stage."""
        t0 = time.perf_counter()
        try:
            yield
        finally:
            self._stage_timings[name] = (
                self._stage_timings.get(name, 0.0) + (time.perf_counter() - t0)
            )

    @contextmanager
    def _accumulate_stage(self, name: str):
        """Thread-safe accumulator for stages that run inside parallel workers."""
        t0 = time.perf_counter()
        try:
            yield
        finally:
            dt = time.perf_counter() - t0
            with self._stage_lock:
                self._stage_timings[name] = self._stage_timings.get(name, 0.0) + dt

    def run_job_response(self, message: str, level: str = "info") -> dict:
        """Build the job-result dict, log the summary line, and emit the timing report."""
        end_timer = time.perf_counter()
        duration = end_timer - self.start_timer
        formatted_duration = format_duration(duration)

        log_line = (
            f"Finished run for {self.data_source_config.name} "
            f"in {formatted_duration} seconds -> message: {message}"
        )
        log_fn = getattr(self.logger, level, self.logger.info)
        log_fn(log_line)

        self._emit_stage_timing_report(duration, level, message)

        return {"message": message, "duration": formatted_duration, "level": level}

    def _emit_stage_timing_report(self, total_seconds: float, level: str, message: str):
        """Print an ASCII timing table and append a CSV row."""
        timings = self._stage_timings or {}
        ds_name = self.data_source_config.name
        rows = []
        for key, label in self._STAGE_REPORT_ORDER:
            if key in timings:
                rows.append((label, format_duration(timings[key]), timings[key]))
            else:
                rows.append((label, "-", None))

        label_w = max(len(r[0]) for r in rows)
        value_w = max(len(r[1]) for r in rows)
        header = f" {ds_name} — total {format_duration(total_seconds)} "
        inner_w = max(label_w + value_w + 4, len(header))
        top = f"┌{header.center(inner_w, '─')}┐"
        bot = "└" + "─" * inner_w + "┘"
        body_lines = [
            f"│ {label.ljust(label_w)}  {value.rjust(value_w)}"
            f"{' ' * (inner_w - label_w - value_w - 4)} │"
            for label, value, _ in rows
        ]
        table = "\n".join(["", top, *body_lines, bot])
        self.logger.report(table)

        self._append_stage_timing_csv(ds_name, total_seconds, level, message, timings)

    def _append_stage_timing_csv(
        self,
        ds_name: str,
        total_seconds: float,
        level: str,
        message: str,
        timings: dict[str, float],
    ):
        """Append one row to logs/stage_timings.csv."""
        try:
            log_dir = Path("logs")
            log_dir.mkdir(parents=True, exist_ok=True)
            csv_path = log_dir / "stage_timings.csv"
            stage_keys = [key for key, _ in self._STAGE_REPORT_ORDER]
            fieldnames = [
                "start_timestamp", "end_timestamp", "datasource",
                "status", "message", "total_s",
                *stage_keys,
            ]
            end_dt = datetime.utcnow()
            start_dt = self._run_started_at or end_dt
            row = {
                "start_timestamp": start_dt.isoformat(timespec="seconds") + "Z",
                "end_timestamp": end_dt.isoformat(timespec="seconds") + "Z",
                "datasource": ds_name,
                "status": level,
                "message": message,
                "total_s": f"{total_seconds:.3f}",
            }
            for key in stage_keys:
                row[key] = f"{timings[key]:.3f}" if key in timings else ""
            write_header = not csv_path.exists()
            with open(csv_path, "a", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                if write_header:
                    writer.writeheader()
                writer.writerow(row)
        except Exception as e:
            self.logger.error(f"Failed to append stage timings CSV: {e}", exc_info=False)
