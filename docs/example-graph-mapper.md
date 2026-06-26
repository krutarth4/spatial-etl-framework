# Example: Graph Mapper (Python)

**Source file:** [`data_mappers/graphMapper.py`](../data_mappers/graphMapper.py)  
**Config:** Inline in `config.yaml` under `datasources:` (not a separate file in `data_source_configs/`)

The graph mapper is a special-purpose datasource that does not follow the standard staging → enrichment → mapping ETL flow. It downloads the Berlin OSM PBF file from Geofabrik, then signals the Java router (via `CommService`) to reload the road graph. It demonstrates:

- Overriding `execute_run_pipeline()` to replace the entire ETL sequence with custom logic
- Using `CommService` to write a shared state signal that another process (the Java router) watches
- Falling back to a locally cached file if the download fails

---

## What it does (and what it doesn't do)

Unlike every other mapper, `GraphMapper` has **no staging table, no enrichment table, and no mapping table**. Its purpose is to:

1. Download `berlin-latest.osm.pbf` from Geofabrik (or skip if the remote file hasn't changed)
2. Update the `comm_state.json` / database task record to tell the Java router where the new file is
3. Signal the router that a new OSM file is ready

The Java router picks up the signal, re-imports the graph using `osm2pgsql` / `osm2pgrouting`, and then populates `ways_base`.

---

## Mapper class

### `execute_run_pipeline` — full pipeline override

```python
def execute_run_pipeline(self):
    comm_service = self._get_comm_service()

    # Signal: pipeline is checking / downloading
    if comm_service is not None:
        comm_service.update_status(
            self._osm_download_task_key,
            current_status="running",
            last_run_status="running",
            last_run_message="Checking metadata / downloading OSM file",
            is_completed=False,
        )

    try:
        try:
            paths = self.extract()               # download if changed
        except Exception as download_error:
            paths = self._fallback_to_cached_osm(download_error)  # use cache

        self._update_metadata_runtime_paths(paths)
        self._publish_metadata_before_comm_signal(paths)

        downloaded = self._last_fetch_performed_download
        msg = "Downloaded new OSM file" if downloaded else "OSM file already available"

        if comm_service is not None:
            comm_service.update_status(
                self._osm_download_task_key,
                current_status="idle",
                last_run_status="success",
                last_run_message=msg,
                is_completed=True,
            )

        return self.run_job_response("Graph source prepared")

    except Exception as e:
        if comm_service is not None:
            comm_service.update_status(
                self._osm_download_task_key,
                current_status="failed",
                last_run_message=str(e),
                is_completed=False,
            )
        raise
```

**`override execute_run_pipeline()` vs a lifecycle hook**  
All lifecycle hooks (`source_filter`, `enrichment_db_query`, etc.) are called from within the default `execute_run_pipeline()`. Overriding `execute_run_pipeline()` replaces the entire ETL sequence — there is no staging insert, no enrichment sync, no mapping step. Only download + signal.

**`CommService`**  
A thin wrapper around a database task record (or `comm_state.json` in hybrid mode). It tracks the state of the OSM download task so the Java router can read it:

| Field | Meaning |
|-------|---------|
| `current_status` | `"running"` / `"idle"` / `"failed"` |
| `last_run_status` | `"success"` / `"running"` / `"failed"` |
| `is_completed` | `true` when the file is ready for the router |
| `last_run_message` | Human-readable status message |

The router polls this record. When `is_completed = true` and `last_run_status = "success"`, it reads the file path from metadata and starts the graph import.

### `_fallback_to_cached_osm` — graceful degradation

```python
def _fallback_to_cached_osm(self, download_error: Exception) -> list[str]:
    destination = self.data_source_config.source.destination
    cached_path = self.resolve_latest_saved_path(destination)
    if cached_path:
        self.logger.warning(f"OSM download failed; falling back to cached: {cached_path}")
        self._last_fetch_performed_download = False
        return [cached_path]
    raise RuntimeError(f"Download failed and no cache at '{destination}'") from download_error
```

If the download fails (network error, Geofabrik outage), the mapper falls back to the most recent locally cached PBF file. `resolve_latest_saved_path()` looks in the destination directory for a previously saved file. If none exists, the error is re-raised and the router is notified of failure.

---

## Config (from `config.yaml`)

```yaml
name: graph
class_name: graph
data_type: static
source:
  fetch: http
  mode: single
  check_metadata:
    enable: true
    keys: ["last_modified"]
  url: "https://download.geofabrik.de/europe/germany/berlin-latest.osm.pbf"
  save_local: true
  destination: tmp/osm_graph/berlin.osm.pbf
  response_type: pbf
job:
  trigger:
    type:
      name: interval
      config:
        days: 1
```

`response_type: pbf` tells the framework the file is an OSM PBF binary. The mapper does not read the file contents — it only needs the downloaded file path, which is passed to the router via metadata.

`check_metadata.keys: ["last_modified"]` skips the download if Geofabrik reports the same `Last-Modified` header as the cached download. Berlin OSM data updates roughly daily.

---

## How it fits in the pipeline

```
Geofabrik (HTTP)
        │
        ▼  extract() — download if Last-Modified changed
        │  fallback to cache on failure
        ▼
  tmp/osm_graph/berlin.osm.pbf   (local file)
        │
        ▼  _publish_metadata_before_comm_signal()
        │
  CommService / comm_state.json  (is_completed = true)
        │
        ▼  Java router polls this record
        │  → runs osm2pgsql / osm2pgrouting
        │  → populates ways_base
        ▼
  ways_base ready for enrichment datasources
```

---

## Key patterns to reuse

| Pattern | Where | When to use |
|---------|-------|-------------|
| Override `execute_run_pipeline()` | `GraphMapper` | Your datasource doesn't do DB staging at all — it downloads a file and triggers an external process |
| `CommService` inter-process signal | `update_status(is_completed=True)` | A separate process (Java, Python script) needs to know when a file is ready |
| Cache fallback on download failure | `_fallback_to_cached_osm` | Large binary files where a download failure shouldn't stop the pipeline entirely |
| Inline config in `config.yaml` | Top-level `datasources:` list | One-off datasources that don't have enough config to warrant a separate file in `data_source_configs/` |
