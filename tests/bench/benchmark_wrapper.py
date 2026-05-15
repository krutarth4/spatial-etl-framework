import threading
import time


def make_benchmark_class(mapper_class):
    """
    Return a new class inheriting mapper_class that records perf_counter wall-times
    for each ETL phase into _bench_timings after .run() returns.

    transform and load run inside a ThreadPoolExecutor (one call per file), so their
    timings are accumulated across threads — _bench_timings['transform'] is cumulative
    thread-seconds, not wall-clock wall time.  All other phases run on the calling thread.

    The harness must call _reset_timings() before each run() invocation.
    The harness measures 'total' externally with perf_counter around run().
    """

    class BenchmarkMapper(mapper_class):

        def _reset_timings(self):
            self._bench_timings = {}
            self._transform_acc = 0.0
            self._load_acc = 0.0
            self._transform_lock = threading.Lock()
            self._load_lock = threading.Lock()

        # ── main-thread phases ────────────────────────────────────────────

        def extract(self):
            t0 = time.perf_counter()
            result = super().extract()
            self._bench_timings["extract"] = time.perf_counter() - t0
            return result

        def sync_raw_to_staging(self):
            t0 = time.perf_counter()
            result = super().sync_raw_to_staging()
            self._bench_timings["sync_raw_to_staging"] = time.perf_counter() - t0
            return result

        def sync_staging_to_enrichment(self):
            t0 = time.perf_counter()
            result = super().sync_staging_to_enrichment()
            self._bench_timings["sync_staging_to_enrichment"] = time.perf_counter() - t0
            return result

        def execute_on_staging(self):
            t0 = time.perf_counter()
            super().execute_on_staging()
            self._bench_timings["execute_on_staging"] = time.perf_counter() - t0

        def execute_on_enrichment(self):
            t0 = time.perf_counter()
            super().execute_on_enrichment()
            self._bench_timings["execute_on_enrichment"] = time.perf_counter() - t0

        def map_to_base(self):
            t0 = time.perf_counter()
            super().map_to_base()
            self._bench_timings["map_to_base"] = time.perf_counter() - t0

        # ── threaded phases (accumulate across files) ─────────────────────

        def transform(self, path):
            t0 = time.perf_counter()
            result = super().transform(path)
            elapsed = time.perf_counter() - t0
            with self._transform_lock:
                self._transform_acc += elapsed
                self._bench_timings["transform"] = self._transform_acc
            return result

        def load(self, data):
            t0 = time.perf_counter()
            super().load(data)
            elapsed = time.perf_counter() - t0
            with self._load_lock:
                self._load_acc += elapsed
                self._bench_timings["load"] = self._load_acc

    BenchmarkMapper.__name__ = f"Bench_{mapper_class.__name__}"
    BenchmarkMapper.__qualname__ = BenchmarkMapper.__name__
    return BenchmarkMapper
