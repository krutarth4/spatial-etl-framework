"""HTTP / local data acquisition: fetch, multi-fetch, metadata comparison.

Reads from self:  self.data_source_config, self.logger,
                  self._last_fetch_performed_download, self._accumulate_stage
Writes to self:   self._last_fetch_performed_download
"""
import re
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from enum import Enum
from itertools import product
from pathlib import Path
from typing import Any, List

from data_config_dtos.data_source_config_dto import (
    SourceDTO,
    SourceFetchModeEnum,
    SourceInputDTO,
    SourceMultiFetchStrategy,
)
from handlers.file_handler import FileHandler
from handlers.http_handler import HttpHandler
from main_core.safe_class import safe_class


class FetchTypeEnum(Enum):
    HTTP = "http"
    HTTPS = "https"
    LOCAL = "local"

    @classmethod
    def has_value(cls, value):
        return value in cls._value2member_map_


@safe_class
class FetchMixin:
    """Handles all data acquisition: single HTTP fetch, parallel multi-fetch, and local files."""

    # ── Public entry points ───────────────────────────────────────────────

    def source(self, source: SourceDTO) -> List[Any] | None:
        """Validate fetch configuration and dispatch to fetch() or multi_fetch()."""
        source = self.data_source_config.source
        if source is None:
            return None

        if not FetchTypeEnum.has_value(source.fetch):
            self.logger.error(
                f"Invalid fetch type '{source.fetch}'. "
                f"Expected one of: {[e.value for e in FetchTypeEnum]}"
            )
            return None
        if not SourceFetchModeEnum.has_value(source.mode):
            self.logger.error(
                f"Invalid fetch mode '{source.mode}'. "
                f"Expected one of: {[e.value for e in SourceFetchModeEnum]}"
            )
            return None

        if source.mode == SourceFetchModeEnum.SINGLE.value:
            if source.check_metadata.enable:
                return self.fetch()
        elif source.mode == SourceFetchModeEnum.MULTI.value:
            return self.multi_fetch()
        return None

    def extract(self):
        """Call source() and log the number of paths returned."""
        paths = self.source(self.data_source_config.source)
        self.logger.info(f"Total number of paths found {len(paths)}")
        return paths

    # Overridable: mappers may override this method for custom fetch logic
    def fetch(self) -> list[str]:
        """Fetch a single URL (HTTP) or resolve a local file path.

        A metadata check is performed first; if unchanged the cached path is
        returned and _last_fetch_performed_download is set to False.
        """
        source = self.data_source_config.source
        paths: list[str] = []
        self._last_fetch_performed_download = None
        if source.fetch in (FetchTypeEnum.HTTP.value, FetchTypeEnum.HTTPS.value):
            check = self.is_metadata_for_single_fetch_changed()
            if check:
                self._last_fetch_performed_download = True
                http_handler = HttpHandler()
                with self._accumulate_stage("download"):
                    path = http_handler.call(
                        uri=source.url,
                        destination_path=source.destination,
                        stream=source.stream,
                        headers=source.headers,
                        params=source.params,
                        file_extension=source.response_type,
                    )
                paths.append(path)
            else:
                self._last_fetch_performed_download = False
                resolved_path = self.resolve_latest_saved_path(source.destination)
                paths.append(resolved_path or source.destination)

        elif source.fetch in FetchTypeEnum.LOCAL.value:
            path = Path(source.file_path)
            if source.check_metadata.enable:
                self._last_fetch_performed_download = self.is_local_source_changed(path)
            else:
                # No change detection configured: never short-circuit (None means
                # "no metadata check performed" to run_pipeline_mixin).
                self._last_fetch_performed_download = None
            paths.append(path)
        else:
            self.logger.error(f"Invalid fetch type: {source.fetch}")
            return None
        return paths

    def multi_fetch(self) -> list[str]:
        """Fetch multiple URLs in parallel using a configurable strategy.

        Strategies: EXPAND_PARAMS (Cartesian product), URL_TEMPLATE (formatted URLs),
        EXPLICIT_URL_LIST (direct list or file).
        """
        source = self.data_source_config.source
        multi_fetch = source.multi_fetch
        paths: list[str] = []
        any_downloaded = False

        if source.fetch in (FetchTypeEnum.HTTP.value or FetchTypeEnum.HTTPS.value):
            if multi_fetch.enable:
                if not SourceMultiFetchStrategy.has_value(multi_fetch.strategy):
                    self.logger.error(f"Not valid fetch type: {multi_fetch.strategy}")
                    return paths

                if multi_fetch.strategy == SourceMultiFetchStrategy.EXPAND_PARAMS.value:
                    params = multi_fetch.expand or {}
                    constant_param = multi_fetch.params or {}
                    keys = list(params.keys())
                    values = list(params.values())
                    tasks = []
                    for combo in product(*values):
                        call_params = dict(zip(keys, combo))
                        param = {**constant_param, **call_params}
                        path = FetchMixin.create_file_name_for_multi_fetch_expand_params(
                            source, param
                        )
                        tasks.append((source.url, param, path, True))
                    paths, any_downloaded = self._run_parallel_fetch(
                        source, multi_fetch, tasks
                    )

                elif multi_fetch.strategy == SourceMultiFetchStrategy.URL_TEMPLATE.value:
                    template_values = multi_fetch.template_params
                    keys = list(template_values.keys())
                    values = list(template_values.values())
                    length = len(values[0])
                    tasks = []
                    for i in range(length):
                        params_dict = {key: values[j][i] for j, key in enumerate(keys)}
                        try:
                            url = multi_fetch.url_template.format(**params_dict)
                        except Exception as e:
                            self.logger.error(
                                f"URL template and template urls specified not correct {e}"
                            )
                            continue
                        path = FetchMixin.create_file_name_for_multi_fetch_expand_params(
                            source, params_dict
                        )
                        tasks.append((url, source.params, path, True))
                    paths, any_downloaded = self._run_parallel_fetch(
                        source, multi_fetch, tasks
                    )

                elif multi_fetch.strategy == SourceMultiFetchStrategy.EXPLICIT_URL_LIST.value:
                    url_list = None
                    if isinstance(multi_fetch.urls, list):
                        url_list = multi_fetch.urls
                    elif isinstance(multi_fetch.urls, SourceInputDTO):
                        file_handler = FileHandler(multi_fetch.urls.input)
                        url_list = file_handler.read_local_file(
                            f"{multi_fetch.urls.input.split('/')[-1]}"
                        )
                    if url_list:
                        tasks = []
                        for url in url_list:
                            url_name = url.split("/")[-1:]
                            path = FetchMixin.create_file_name_for_multi_fetch_expand_params(
                                source, {"url": "_".join(url_name)}
                            )
                            tasks.append((url, source.params, path, False))
                        paths, any_downloaded = self._run_parallel_fetch(
                            source, multi_fetch, tasks
                        )

        elif source.fetch in FetchTypeEnum.LOCAL.value:
            if multi_fetch.enable:
                if not SourceMultiFetchStrategy.has_value(multi_fetch.strategy):
                    self.logger.error(f"Not valid fetch type: {multi_fetch.strategy}")
                    raise ValueError(f"Invalid fetch type: {multi_fetch.strategy}")

                if multi_fetch.strategy == SourceMultiFetchStrategy.EXPAND_PARAMS.value:
                    paths.append(source.file_path)
                elif multi_fetch.strategy == SourceMultiFetchStrategy.URL_TEMPLATE.value:
                    template_values = multi_fetch.template_params
                    keys = list(template_values.keys())
                    values = list(template_values.values())
                    length = len(values[0])
                    for i in range(length):
                        params_dict = {key: values[j][i] for j, key in enumerate(keys)}
                        path = multi_fetch.url_template.format(**params_dict)
                        paths.append(path)
                elif multi_fetch.strategy == SourceMultiFetchStrategy.EXPLICIT_URL_LIST.value:
                    if isinstance(multi_fetch.urls, list):
                        for url in multi_fetch.urls:
                            paths.append(url)
        else:
            self.logger.error(
                f"Not valid multi fetch type strategy: {multi_fetch.strategy}"
            )

        if paths:
            self._last_fetch_performed_download = any_downloaded
        return paths

    def process_multi_fetch_expand_list(self, source, urls) -> list[str]:
        multi_fetch = source.multi_fetch
        tasks = []
        for url in urls:
            url_name = url.split("/")[-1:]
            path = FetchMixin.create_file_name_for_multi_fetch_expand_params(
                source, {"url": "_".join(url_name)}
            )
            tasks.append((url, source.params, path, False))
        paths, downloaded = self._run_parallel_fetch(source, multi_fetch, tasks)
        if downloaded:
            self._last_fetch_performed_download = True
        return paths

    # ── Metadata comparison ───────────────────────────────────────────────

    def is_local_source_changed(self, path) -> bool:
        """Detect whether a local source file changed since the last successful run.

        Tiered, mirroring HTTP's validator logic:
          1. os.stat → compare size + mtime against the stored signature (no read).
          2. Both unchanged → not changed (fast path, no hashing).
          3. Either differs → sha256 the file and compare to the stored checksum,
             confirming a real content change (absorbs `touch`-only false positives).
          4. First run / unknown signature → changed.

        The signature is persisted in the metadata table's file_checksum /
        file_size_bytes / file_mtime columns. `check_metadata.keys` is HTTP-specific
        and intentionally ignored here.
        """
        p = Path(path)
        if not p.exists():
            self.logger.warning(f"Local source file missing: {p} — nothing to process")
            return False

        svc = getattr(self, "metadata_service", None)
        if svc is None or getattr(svc, "metadata_repository", None) is None:
            self.logger.warning(
                "Metadata service unavailable — cannot compare local signature; "
                "processing local source unconditionally."
            )
            return True

        stat = p.stat()
        size = stat.st_size
        mtime = datetime.utcfromtimestamp(stat.st_mtime)

        row = svc.metadata_repository.get_metadata(self.data_source_name)
        old_checksum = getattr(row, "file_checksum", None) if row else None
        old_size = getattr(row, "file_size_bytes", None) if row else None
        old_mtime = getattr(row, "file_mtime", None) if row else None

        # First run / no recorded signature → treat as changed and record it.
        if old_checksum is None:
            self._persist_local_signature(size, mtime, FileHandler.compute_checksum(p))
            self.logger.info(f"[{self.data_source_name}] No prior signature — processing local source.")
            return True

        # Cheap tier: stat unchanged → assume unchanged, skip hashing.
        if old_size == size and old_mtime == mtime:
            self.logger.info(
                f"[{self.data_source_name}] Local source unchanged (size + mtime match) — skipping."
            )
            return False

        # Stat changed → confirm via content hash before reprocessing.
        new_checksum = FileHandler.compute_checksum(p)
        self._persist_local_signature(size, mtime, new_checksum)
        if new_checksum != old_checksum:
            self.logger.info(f"[{self.data_source_name}] Local source content changed — processing.")
            return True
        self.logger.info(
            f"[{self.data_source_name}] Local source stat changed but content identical "
            f"(checksum match) — skipping."
        )
        return False

    def _persist_local_signature(self, size, mtime, checksum) -> None:
        """Store the latest local-file signature in the metadata table."""
        try:
            self.metadata_service.update(
                self.data_source_name,
                {
                    "file_size_bytes": size,
                    "file_mtime": mtime,
                    "file_checksum": checksum,
                    "last_checked_at": datetime.utcnow(),
                },
            )
        except Exception as e:
            self.logger.warning(f"Failed to persist local source signature: {e}")

    def is_metadata_for_single_fetch_changed(self) -> bool:
        """Compare HTTP response headers with cached metadata to detect new data."""
        source = self.data_source_config.source
        current_metadata = HttpHandler().call_remote_metadata(
            uri=source.url, headers=source.headers, params=source.params
        )
        file_handler = FileHandler(source.destination)
        name = source.destination.split("/")[-1].split(".")
        old_metadata = file_handler.read_metadata(".".join(name[:-1]))
        if self.is_metadata_changed(old_metadata, current_metadata, source.check_metadata.keys):
            self.logger.info(
                "New UPDATES available for Metadata checks. Fetching new DATA ......"
            )
            return True
        self.logger.warning(
            "No new data found for metadata before fetch check. "
            "Hence skipping the rest of processing steps"
        )
        return False

    def check_multi_metadata_before_fetch(self, url, headers, params, path) -> bool:
        """Per-URL metadata check for multi-fetch; returns True if new data is available."""
        source = self.data_source_config.source
        current_metadata = HttpHandler().call_remote_metadata(
            uri=url, headers=headers, params=params
        )
        file_handler = FileHandler(path)
        name = path.split("/")[-1].split(".")
        old_metadata = file_handler.read_metadata(".".join(name[:-1]))
        if self.is_metadata_changed(old_metadata, current_metadata, source.check_metadata.keys):
            self.logger.info(
                "New UPDATES available for MULTI Metadata checks. Need to fetch new data ......"
            )
            return True
        self.logger.warning(
            "No new data found for metadata before fetch check. "
            "Hence skipping the rest of processing steps"
        )
        return False

    def is_metadata_changed(
        self, old_metadata, current_metadata, keys: list[str]
    ) -> bool:
        """Return True if any of the tracked metadata keys differ (or metadata is absent)."""
        if old_metadata is None or current_metadata is None:
            return True
        if not keys:
            return old_metadata != current_metadata
        for key in keys:
            if old_metadata.get(key) != current_metadata.get(key):
                return True
        return False

    def is_new_data_available_in_multi_fetch(
        self, source, url, path, headers, params
    ) -> bool:
        return (not source.check_metadata.enable) and self.check_multi_metadata_before_fetch(
            url=url, headers=headers, params=params, path=path
        )

    # ── Path helpers ──────────────────────────────────────────────────────

    def resolve_latest_saved_path(self, candidate_path: str | Path | None) -> str | None:
        """Return the latest cached file at candidate_path, or None if not found."""
        if not candidate_path:
            return None
        try:
            candidate = Path(candidate_path)
            file_handler = FileHandler(candidate.parent)
            latest = file_handler.get_local_file(candidate.name)
            return str(latest) if latest is not None else None
        except Exception as e:
            self.logger.warning(
                f"Failed to resolve latest saved path for {candidate_path}: {e}"
            )
            return None

    @staticmethod
    def create_file_name_for_multi_fetch_expand_params(source, param) -> str:
        """Build a deterministic file name from request parameters for multi-fetch."""
        base, ext = source.destination.rsplit(".", 1)

        # Volatile request params (e.g. current timestamp/date) should not change
        # the logical file identity, otherwise retention won't group files correctly.
        ignore_file_name_keys = {"date"}

        parts = []
        for k in sorted(param.keys()):
            if k in ignore_file_name_keys:
                continue
            v = re.sub(r"[^\w\-\.]", "_", str(param[k]))
            parts.append(f"{k}-{v}")

        suffix = "_".join(parts) if parts else "request"
        return f"{base}_{suffix}.{ext}"

    # ── Internal parallel-fetch infrastructure ────────────────────────────

    def _multi_fetch_settings(self, multi_fetch):
        workers = max(1, int(getattr(multi_fetch, "fetch_workers", 8) or 8))
        timeout = int(getattr(multi_fetch, "request_timeout", 120) or 120)
        retries = max(1, int(getattr(multi_fetch, "retry_attempts", 3) or 3))
        backoff = float(getattr(multi_fetch, "retry_backoff", 1.0) or 1.0)
        delay = float(getattr(multi_fetch, "inter_request_delay", 0.0) or 0.0)
        fail_fast = bool(getattr(multi_fetch, "fail_fast", False))
        return workers, timeout, retries, backoff, delay, fail_fast

    def _execute_fetch_task(
        self, source, url, params, path, resolve_on_skip, timeout, retries, backoff, delay
    ):
        """Execute a single fetch task: metadata check → optional download → return path."""
        try:
            if self.check_multi_metadata_before_fetch(
                url=url, headers=source.headers, params=params, path=path
            ):
                http_handler = HttpHandler()
                with self._accumulate_stage("download"):
                    final_path = http_handler.call(
                        uri=url,
                        destination_path=path,
                        stream=source.stream,
                        headers=source.headers,
                        params=params,
                        file_extension=source.response_type,
                        timeout=(min(30, timeout), timeout),
                        retry_attempts=retries,
                        retry_backoff=backoff,
                    )
                if delay > 0:
                    time.sleep(delay)
                return final_path, True, None
            else:
                final_path = (
                    self.resolve_latest_saved_path(path) or path
                ) if resolve_on_skip else path
                return final_path, False, None
        except Exception as e:
            return path, False, e

    def _run_parallel_fetch(self, source, multi_fetch, tasks):
        """Run tasks concurrently via ThreadPoolExecutor with retry on failure.

        tasks: list of (url, params, path, resolve_on_skip)
        Returns: (paths_list, any_downloaded)
        """
        workers, timeout, retries, backoff, delay, fail_fast = self._multi_fetch_settings(
            multi_fetch
        )
        n = len(tasks)
        results: list[str] = [t[2] for t in tasks]
        any_downloaded = False
        failures: list[tuple[int, str, Exception]] = []
        if n == 0:
            return results, any_downloaded

        effective_workers = max(1, min(workers, n))
        self.logger.info(
            f"multi_fetch: {n} requests, {effective_workers} workers, "
            f"timeout={timeout}s, retries={retries}, backoff={backoff}s"
        )

        with ThreadPoolExecutor(
            max_workers=effective_workers,
            thread_name_prefix=f"multifetch-{self.data_source_name}",
        ) as ex:
            future_to_idx = {
                ex.submit(
                    self._execute_fetch_task,
                    source, url, params, path, resolve_on_skip,
                    timeout, retries, backoff, delay,
                ): i
                for i, (url, params, path, resolve_on_skip) in enumerate(tasks)
            }
            done_count = 0
            for fut in as_completed(future_to_idx):
                idx = future_to_idx[fut]
                final_path, downloaded, err = fut.result()
                done_count += 1
                if err is not None:
                    failures.append((idx, tasks[idx][0], err))
                    self.logger.error(
                        f"multi_fetch [{done_count}/{n}] FAILED {tasks[idx][0]}: {err}"
                    )
                    if fail_fast:
                        for f in future_to_idx:
                            f.cancel()
                        raise err
                else:
                    results[idx] = final_path
                    any_downloaded = any_downloaded or downloaded
                    self.logger.info(
                        f"multi_fetch [{done_count}/{n}] OK "
                        f"{'downloaded' if downloaded else 'skipped'}: {tasks[idx][0]}"
                    )

        # Sequential final-retry pass for anything that failed in the parallel phase
        permanent_failures: list[tuple[int, str, Exception]] = []
        if failures:
            self.logger.warning(
                f"multi_fetch first pass: {len(failures)}/{n} failed after per-URL retries; "
                f"starting sequential final-retry pass"
            )
            for idx, url, prev_err in failures:
                _, params, path, resolve_on_skip = tasks[idx]
                final_path, downloaded, err = self._execute_fetch_task(
                    source, url, params, path, resolve_on_skip,
                    timeout, retries, backoff, delay,
                )
                if err is None:
                    results[idx] = final_path
                    any_downloaded = any_downloaded or downloaded
                    self.logger.info(f"multi_fetch final-retry OK: {url}")
                else:
                    permanent_failures.append((idx, url, err))
                    self.logger.error(f"multi_fetch final-retry FAILED {url}: {err}")

        ok_count = n - len(permanent_failures)
        if permanent_failures:
            lines = [
                f"  FAILED: {url}  reason: {err}"
                for _, url, err in permanent_failures
            ]
            self.logger.error(
                f"[multi_fetch summary] datasource={self.data_source_name} "
                f"total={n} ok={ok_count} failed={len(permanent_failures)}\n"
                + "\n".join(lines)
            )
            failed_idxs = {i for i, _, _ in permanent_failures}
            filtered = [p for i, p in enumerate(results) if i not in failed_idxs]
            self.logger.error(
                f"[multi_fetch summary] datasource={self.data_source_name}: "
                f"filtered out {len(permanent_failures)} failed entries; "
                f"continuing with {len(filtered)}/{n} successful paths"
            )
            return filtered, any_downloaded
        return results, any_downloaded
