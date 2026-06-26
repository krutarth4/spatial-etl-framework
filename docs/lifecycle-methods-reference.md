# Lifecycle Methods — Override Guide

This guide helps you decide **which methods to override** in your mapper class based on what your datasource actually needs. You do not need to understand the whole framework — just find your situation below and follow the pattern.

The base class (`DataSourceABCImpl`) handles everything by default. You only write a method when the default is not enough for your data.

Cross-references:
- [mapper-README.md](mapper-README.md) — full execution order and mermaid diagrams
- [configure-data-source-step-by-step.md](configure-data-source-step-by-step.md) — end-to-end walkthrough with working examples
- [mapping-strategies-reference.md](mapping-strategies-reference.md) — mapping strategies in depth

---

## How to use this guide

Work through the checklist for your datasource. Each question maps to a method. If the answer is "yes, I need that", read the section and copy the pattern.

```
1. How does the file arrive?             → read_file_content
2. Is the payload nested or messy?       → source_filter
3. Do I need to build an in-memory index first?  → pre_filter_processing
4. Do I need to skip some files?         → should_load_transformed_data
5. Do I need per-file setup/teardown?    → before_process_file / after_process_file
6. Do I need to accumulate across files? → after_load + post_database_processing
7. Do I need geometry built in staging?  → staging_db_query
8. Do I need geometry or CRS in enrichment? → enrichment_db_query
9. Do the built-in mapping strategies fit? → if not: mapping_db_query
10. Do I need pre-run checks?            → check_before_update
11. Do I need cleanup when the run ends? → run_end_cleanup
```

---

## Step 1 — Reading the file

**Default:** the framework auto-reads common formats (`csv`, `json`, `gpkg`, `shp`, `geojson`, `parquet`, `xlsx`) based on `response_type` in your YAML. You need no Python for these.

### When do I need to override `read_file_content`?

Override it when:
- Your file is compressed (`gz`, `zip`)
- Your file is `xml`, protobuf, or another binary format
- You need geometry encoded as WKB/WKT before inserting into PostGIS
- You need to merge two related files (geometry + attribute CSV)

**Signature:**
```python
def read_file_content(self, path: str) -> list[dict]:
```
Return a flat list of dicts. Each dict becomes one row in the staging table.

---

**Situation: file is gzip-compressed JSON**
```python
import gzip, orjson

def read_file_content(self, path: str) -> list[dict]:
    with gzip.open(path, "rb") as f:
        payload = orjson.loads(f.read())
    rows = []
    for feature in payload.get("features", []):
        x, y = feature["geometry"]["coordinates"]
        rows.append({
            **feature["properties"],
            "geom_25833": f"SRID=25833;POINT({x} {y})",
        })
    return rows
```

**Situation: file is a Shapefile and you need WKT geometry**
```python
import geopandas as gpd

def read_file_content(self, path: str) -> list[dict]:
    gdf = gpd.read_file(path).to_crs(25833)
    gdf["geom_25833"] = gdf.geometry.apply(lambda g: f"SRID=25833;{g.wkt}")
    return gdf.drop(columns="geometry").to_dict("records")
```

**Situation: two files — geometry shapefile + attribute CSV — must be merged**
```python
import geopandas as gpd, pandas as pd

def read_file_content(self, path: str) -> list[dict]:
    gdf = gpd.read_file(path).to_crs(25833)
    attrs = pd.read_csv(path.replace(".shp", "_attrs.csv"))
    merged = gdf.merge(attrs, on="id")
    merged["geom_25833"] = merged.geometry.apply(lambda g: f"SRID=25833;{g.wkt}")
    return merged.drop(columns="geometry").to_dict("records")
```

> **Tip:** If your format is one of the auto-read types, skip this method entirely. Add `response_type: gpkg` (or the relevant type) to `source:` in your YAML and the base class handles it.

---

## Step 2 — Reshaping the payload

**Default:** `source_filter` returns the data unchanged.

### When do I need to override `source_filter`?

Override it when:
- The API response is a nested dict (e.g. `{"sources": [...]}`) instead of a flat list
- You need to filter out rows that don't apply (old records, wrong observation type)
- You need to inject a field from the file/context into every row (e.g. station id from the filename)
- You need to rename or convert fields to match your staging table columns

**Signature:**
```python
def source_filter(self, data: list | dict) -> list[dict]:
```
Receive the raw parsed payload. Return a flat list of dicts.

---

**Situation: API response is nested — stations are inside `data[0]["sources"]`**
```python
def source_filter(self, data: list[dict]) -> list[dict]:
    stations = data[0]["sources"]
    return [
        row for row in stations
        if row.get("observation_type") == "forecast"
    ]
```

**Situation: multi-fetch where each file has a sources list and you need to inject a station id into every weather row**
```python
def source_filter(self, data: list) -> list[dict]:
    result = []
    for content in data:
        sources = content.get("sources", [])
        if not sources:
            continue
        station_id = int(sources[0]["dwd_station_id"])
        for row in content.get("weather", []):
            result.append({**row, "dwd_station_id": station_id})
    return result
```

**Situation: flat JSON list, just need to drop some rows**
```python
def source_filter(self, data: list[dict]) -> list[dict]:
    return [row for row in data if row.get("active") and row.get("lat") is not None]
```

---

## Step 3 — Building a spatial index before filtering

**Default:** `pre_filter_processing` is a no-op.

### When do I need to override `pre_filter_processing`?

Override it when `source_filter` needs to do spatial lookups against the data itself (e.g. snapping each point to the nearest grid cell in the same dataset). Building the index once here is much faster than rebuilding it per row inside `source_filter`.

**Signature:**
```python
def pre_filter_processing(self, data: list) -> None:
```
Do not return anything. Store the result on `self`.

---

**Situation: need to snap each sensor reading to the nearest grid point**
```python
from scipy.spatial import cKDTree
import numpy as np

def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)
    self._tree = None
    self._grid_ids = []

def pre_filter_processing(self, data: list) -> None:
    coords = np.array([[r["x"], r["y"]] for r in data])
    self._grid_ids = [r["grid_id"] for r in data]
    self._tree = cKDTree(coords)

def source_filter(self, data: list[dict]) -> list[dict]:
    result = []
    for row in data:
        dist, idx = self._tree.query([row["sensor_x"], row["sensor_y"]])
        if dist < 500:   # within 500 m
            result.append({**row, "nearest_grid_id": self._grid_ids[idx]})
    return result
```

---

## Step 4 — Skipping a file conditionally

**Default:** a file is loaded if `source_filter` returned a non-empty list.

### When do I need to override `should_load_transformed_data`?

Override it when the filter passed but you still want to skip the DB insert for that file (e.g. below a minimum row count, missing a required geometry, checksum matches last run).

**Signature:**
```python
def should_load_transformed_data(self, transformed_data: list, path: str) -> bool:
```
Return `False` to skip the insert for this file. The file is counted as processed but nothing goes into the database.

---

**Situation: skip if fewer than 10 rows — probably a malformed response**
```python
def should_load_transformed_data(self, transformed_data: list, path: str) -> bool:
    if len(transformed_data) < 10:
        self.logger.warning(f"Only {len(transformed_data)} rows in {path} — skipping")
        return False
    return True
```

**Situation: skip if any row is missing a geometry**
```python
def should_load_transformed_data(self, transformed_data: list, path: str) -> bool:
    missing = [r for r in transformed_data if not r.get("geom_25833")]
    if missing:
        self.logger.error(f"{len(missing)} rows missing geometry in {path} — skipping file")
        return False
    return True
```

---

## Step 5 — Per-file setup and teardown

**Default:** both `before_process_file` and `after_process_file` are no-ops.

### When do I need these?

| Situation | Method |
|-----------|--------|
| Each file has a different station id you need to track | `before_process_file` |
| You want to delete the downloaded file after processing | `after_process_file` |
| You need to reset per-file counters | `before_process_file` |
| A file failed and you want to quarantine it | `on_process_file_error` |

**Signatures:**
```python
def before_process_file(self, path: str) -> None:
def after_process_file(self, path: str, transformed_data: list) -> None:
def on_process_file_error(self, path: str, error: Exception) -> None:
```

---

**Situation: track which station each file belongs to**
```python
def before_process_file(self, path: str) -> None:
    # e.g. filename is "station_12345.json"
    self._current_station_id = int(path.split("_")[1].split(".")[0])
```

**Situation: delete the temp file after a successful insert**
```python
import os

def after_process_file(self, path: str, transformed_data: list) -> None:
    os.remove(path)
```

**Situation: move bad files to a quarantine folder instead of silently skipping them**
```python
import shutil

def on_process_file_error(self, path: str, error: Exception) -> None:
    dest = path.replace("/tmp/", "/tmp/quarantine/")
    shutil.move(path, dest)
    self.logger.error(f"Quarantined {path}: {error}")
```

---

## Step 6 — Accumulating results across multiple files

**Default:** each file's records are inserted independently into raw staging.

### When do I need this pattern?

Use it when you cannot write final results per file — e.g. you are sampling a raster for every file and need to aggregate all the samples before inserting, or you need a cross-file deduplication.

The pattern: stash results in `__init__`, collect in `after_load`, flush in `post_database_processing`.

**Signatures:**
```python
def after_load(self, data: list[dict]) -> None:      # fires after each file's insert
def post_database_processing(self) -> None:           # fires once, after all files
```

---

**Situation: collect elevation samples across many raster tiles, insert once at the end**
```python
def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)
    self._elevation_results = []

def after_load(self, data: list[dict]) -> None:
    # data is still in memory here — compute per-file metrics
    self._elevation_results.extend([
        {"way_id": r["id"], "elevation_m": r["elevation"]}
        for r in data
    ])

def post_database_processing(self) -> None:
    if self._elevation_results:
        self.db.bulk_insert(
            "elevation_metrics", "exp_null", self._elevation_results, upsert=True
        )
        self._elevation_results.clear()
```

---

## Step 7 — Building geometry in the staging stage

**Default:** `staging_db_query` returns `None` — no SQL is run after the raw → staging sync.

### When do I need to override `staging_db_query`?

Override it when your source has raw coordinate columns (e.g. `lon`, `lat`) that need to be assembled into a PostGIS geometry column after the data is in staging.

**Signature:**
```python
def staging_db_query(self) -> str | None:
```
Return a SQL string. Return `None` to skip. Use `self.data_source_config.storage.staging` to get the table name and schema.

---

**Situation: build a point geometry from lon/lat columns that arrived from the API**
```python
def staging_db_query(self) -> str | None:
    stg = self.data_source_config.storage.staging
    return f"""
        UPDATE {stg.table_schema}.{stg.table_name}
        SET geom_4326 = ST_SetSRID(ST_MakePoint(lon, lat), 4326)
        WHERE lon IS NOT NULL
          AND lat IS NOT NULL
          AND geom_4326 IS NULL
    """
```

**Situation: normalize a text field across the whole table**
```python
def staging_db_query(self) -> str | None:
    stg = self.data_source_config.storage.staging
    return f"""
        UPDATE {stg.table_schema}.{stg.table_name}
        SET station_name = TRIM(LOWER(station_name))
        WHERE station_name IS NOT NULL
    """
```

> **When to use staging vs enrichment:** staging SQL runs on the raw-to-staging copy. Use it for normalisation that must happen before the staging → enrichment sync. If the computation depends on joined data from another table, put it in enrichment instead.

---

## Step 8 — Building geometry or doing CRS transforms in enrichment

**Default:** `enrichment_db_query` returns `None` — no SQL is run after the staging → enrichment sync.

### When do I need to override `enrichment_db_query`?

Override it when:
- You need to copy geometry from staging and reproject it (e.g. from EPSG:25833 to EPSG:4326)
- You need to join staging data to a reference table to build derived columns
- You need to derive enrichment columns that require the full staging table (e.g. distance to centroid)

**Signature:**
```python
def enrichment_db_query(self) -> str | None:
```
Return a SQL string. Return `None` to skip.

---

**Situation: copy lon/lat from staging and build the point geometry in enrichment**
```python
def enrichment_db_query(self) -> str | None:
    stg = self.data_source_config.storage.staging
    enr = self.data_source_config.storage.enrichment
    return f"""
        UPDATE {enr.table_schema}.{enr.table_name} e
        SET point = ST_SetSRID(ST_MakePoint(s.lon, s.lat), 4326)
        FROM {stg.table_schema}.{stg.table_name} s
        WHERE e.station_id = s.station_id
          AND e.point IS NULL
    """
```

**Situation: data arrived in EPSG:25833, mapping strategy expects EPSG:4326**
```python
def enrichment_db_query(self) -> str | None:
    enr = self.data_source_config.storage.enrichment
    return f"""
        UPDATE {enr.table_schema}.{enr.table_name}
        SET geom_4326 = ST_Transform(geom_25833, 4326)
        WHERE geom_25833 IS NOT NULL
          AND geom_4326 IS NULL
    """
```

**No-Python alternative — YAML operators:**
For common geometry operations you can declare them in the datasource YAML instead of writing Python:

```yaml
enrichment_operators:
  operators:
    - { type: make_point,  target_col: geom_4326,  x_col: lon, y_col: lat, srid: 4326 }
    - { type: reproject,   target_col: geom_25833, source_col: geom_4326, target_srid: 25833 }
    - { type: derive,      target_col: height_class,
        expression: "CASE WHEN height_m >= 20 THEN 'large' ELSE 'small' END" }
```

Supported types: `derive`, `make_point`, `reproject`, `snap_to_grid`, `normalize`, `aggregate`, `spatial_aggregate`, `raster_aggregate`.

---

## Step 9 — Mapping to road segments

The framework ships several built-in strategies. Try these first — they require no Python.

| Your data | Strategy to use |
|-----------|----------------|
| Sparse points (weather stations, sensors) — one road per point | `knn` |
| Dense points (trees, stops) — aggregate all within a buffer per road | `aggregate_within_distance` |
| Points — K nearest roads per feature | `nearest_k` |
| Polygons or lines that overlap roads | `intersection` |
| Shared column with base graph (OSM id, external key) | `attribute_join` |
| Complex join (LATERAL, CTE, multi-table) | `custom` → `mapping_db_query()` |

### When do I need to override `mapping_db_query`?

Only when `strategy.type: custom` is set in YAML and none of the built-in strategies cover your spatial relationship.

**Signature:**
```python
def mapping_db_query(self) -> str | None:
```
Return a full `INSERT … SELECT … ON CONFLICT DO UPDATE` statement. The target table is `self.data_source_config.mapping.table_name`.

---

**Situation: LATERAL join — nearest road per station with distance stored**
```python
def mapping_db_query(self) -> str | None:
    enr = self.data_source_config.storage.enrichment
    m   = self.data_source_config.mapping
    return f"""
        INSERT INTO {m.table_schema}.{m.table_name}
            (way_id, station_id, distance_m)
        SELECT DISTINCT ON (e.station_id)
            w.id,
            e.station_id,
            ST_Distance(w.geometry_25833, e.point::geometry) AS distance_m
        FROM {enr.table_schema}.{enr.table_name} e
        CROSS JOIN LATERAL (
            SELECT id, geometry_25833
            FROM {m.table_schema}.ways_base
            ORDER BY geometry_25833 <-> e.point::geometry
            LIMIT 1
        ) w
        ON CONFLICT (way_id) DO UPDATE SET
            station_id = EXCLUDED.station_id,
            distance_m = EXCLUDED.distance_m
    """
```

---

## Step 10 — Pre-run checks and guards

**Default:** `check_before_update` returns `True` and the run proceeds.

### When do I need to override `check_before_update`?

Override it when you want to abort the run before any file is downloaded or processed:
- A dependency datasource has not finished yet
- The source API is known to be in maintenance
- It is too early in the day to fetch fresh data

**Signature:**
```python
def check_before_update(self) -> bool:
```
Return `False` to abort. The run is logged as skipped, not failed.

---

**Situation: do not run until the weather station mapper has completed successfully**
```python
def check_before_update(self) -> bool:
    if not self.metadata_service.has_completed_successfully("weather_station_bright_sky"):
        self.logger.warning("Weather stations not ready — skipping weather forecast run")
        return False
    return True
```

**Situation: check if the dataset has expired before re-fetching**
```python
def check_before_update(self) -> bool:
    if not self.metadata_service.is_dataset_expired("my_datasource", expires_after={"hours": 12}):
        self.logger.info("Data is still fresh — skipping run")
        return False
    return True
```

---

## Step 11 — Cleanup when the run ends

**Default:** `run_end_cleanup` is a no-op.

### When do I need to override `run_end_cleanup`?

Override it when the mapper holds resources that must be released after every run, regardless of success or failure:
- Large in-memory objects (`_kdtree`, accumulated results lists)
- Open file handles or database connections opened in `__init__`
- Temp files that were not already deleted in `after_process_file`

**Signature:**
```python
def run_end_cleanup(self, succeeded: bool, error: Exception | None = None) -> None:
```
Always fires — even when the run failed.

---

**Situation: release a KDTree and an open raster dataset**
```python
def run_end_cleanup(self, succeeded: bool, error: Exception | None = None) -> None:
    self._tree = None
    self._grid_ids = []
    if hasattr(self, "_raster") and self._raster:
        self._raster.close()
        self._raster = None
    if error:
        self.logger.error(f"Run ended with error: {error}")
```

---

## Decision summary

```
My file format is not auto-supported (gz, zip, xml, binary)?
  → override read_file_content

My API response is nested / needs row filtering or field injection?
  → override source_filter

I need a spatial index or lookup table before row-level filtering?
  → override pre_filter_processing  (store on self, use in source_filter)

I want to skip loading a file based on row count or data quality?
  → override should_load_transformed_data

I need to open/close something per file or quarantine bad files?
  → override before_process_file / after_process_file / on_process_file_error

I am collecting results across many files before inserting?
  → override after_load + post_database_processing  (declare list in __init__)

My source has raw lon/lat columns that need a geometry column built?
  → override staging_db_query (build geometry after raw→staging sync)
  OR use enrichment_operators in YAML (no Python needed)

My geometry needs a CRS transform, or I need to join staging to a reference table?
  → override enrichment_db_query (after staging→enrichment sync)

The built-in mapping strategies don't cover my spatial relationship?
  → set strategy.type: custom in YAML + override mapping_db_query

I need to abort the run if a dependency isn't ready?
  → override check_before_update (return False to skip)

I hold large objects in memory or need to close resources after a run?
  → override run_end_cleanup
```

---

## Common combinations by datasource type

| Datasource type | Methods you will typically override |
|-----------------|-------------------------------------|
| Flat JSON / CSV / GeoPackage | nothing (auto-read handles it) |
| Nested JSON API | `source_filter` |
| Compressed file (gz, zip) | `read_file_content` |
| Station + readings (multi-fetch) | `source_filter` (inject station id into each row) |
| Raster / elevation tiles | `read_file_content`, `pre_filter_processing`, `after_load`, `post_database_processing`, `run_end_cleanup` |
| Static reference table (no geometry) | `source_filter` (if nested), `staging_db_query` (if geometry derivable) |
| Points with raw lon/lat | `staging_db_query` or `enrichment_db_query` (build geometry) |
| Points in non-WGS84 CRS | `enrichment_db_query` (CRS transform) |
| Datasource with a dependency | `check_before_update` |
| Datasource with large memory use | `run_end_cleanup` |
