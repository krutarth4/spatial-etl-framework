# Adding a New Data Source — Step-by-Step

This guide walks through adding a new datasource end-to-end: from creating the mapper class and table models, through writing the YAML config, to verifying data flows through every ETL stage.

The example used throughout is the **DWD Weather Stations** mapper — fetches weather station locations from Bright Sky, builds a point geometry in enrichment, and maps each road segment to its nearest station.

---

## Naming Conventions

The pipeline discovers mapper classes by convention from `class_name` in YAML:

| `class_name` in YAML | File | Class |
|----------------------|------|-------|
| `weatherStation` | `data_mappers/weatherStationMapper.py` | `WeatherStationMapper` |
| `airQualityData` | `data_mappers/airQualityDataMapper.py` | `AirQualityDataMapper` |
| `tree` | `data_mappers/treeMapper.py` | `TreeMapper` |

Rule: append `Mapper` to the filename, convert `class_name` to PascalCase and append `Mapper` for the class.

---

## Step 1 — Define Table Models

Both staging and enrichment table classes live **in the same mapper file** as the mapper class. The framework auto-creates and migrates them on startup via SQLAlchemy. Reference them from YAML using `table_class:`.

### StagingTable

The staging table holds the raw records loaded from the source. It maps directly to what your `read_file_content()` / `source_filter()` returns — one column per dict key.

```python
# data_mappers/weatherStationMapper.py
from geoalchemy2 import Geometry
from sqlalchemy import Column, Integer, String, Float, DateTime, UniqueConstraint, Index

from database_tables.staging_table import StagingTable


class DwdStationsStagingTable(StagingTable):
    __tablename__ = "dwd_station_locations_staging"

    uid = Column(Integer, primary_key=True, autoincrement=True)
    dwd_station_id = Column(Integer, unique=True, nullable=False)
    station_name = Column(String)
    observation_type = Column(String)
    lat = Column(Float)
    lon = Column(Float)
    height = Column(Float)
    wmo_station_id = Column(String)
    first_record = Column(DateTime(timezone=True))
    last_record = Column(DateTime(timezone=True))
```

Rules for staging tables:
- Inherit from `StagingTable` (provides `__abstract__ = True` and the SQLAlchemy base)
- Always add a surrogate PK (`uid = Column(Integer, primary_key=True, autoincrement=True)`)
- Add `UniqueConstraint` on the natural key — the framework uses it for upserts
- Keep only what comes from the source; derived columns (geometry, aggregates) go in enrichment

### EnrichmentTable

Enrichment holds cleaned, derived, and geometry-enriched versions of staging records.

```python
class DwdWeatherStationEnrichmentTable(EnrichmentTable):
    __tablename__ = "dwd_station_locations_enrichment"

    uid = Column(Integer, primary_key=True, autoincrement=True)
    dwd_station_id = Column(Integer, unique=True, nullable=False)
    station_name = Column(String)
    point = Column(Geometry(geometry_type="POINT", srid=4326), index=True)
```

Rules for enrichment tables:
- Inherit from `EnrichmentTable`
- Add a GIST index on geometry columns: `Column(Geometry(...), index=True)` or `Index(None, "col", postgresql_using="gist")`
- Only include columns you actually need for mapping — don't mirror all staging columns unless necessary

### MappingTable (optional)

Only needed when using `strategy.type: custom` (or `mapper_sql`) — the framework creates the mapping table automatically for built-in strategies.

```python
from database_tables.mapping_table import MappingTable


class DwdMappingTable(MappingTable):
    __tablename__ = "dwd_station_locations_mapping"

    uid = Column(Integer, primary_key=True, autoincrement=True)
    dwd_station_id = Column(Integer, nullable=False)
    distance = Column(Float, nullable=False)
    bearing_degree = Column(Float, nullable=True)
```

`MappingTable` base provides: `way_id = Column(Integer, ForeignKey(ways_base.id), unique=True)`.

---

## Step 2 — Write the Mapper Class

Subclass `DataSourceABCImpl` and override only what is dataset-specific. The base class handles everything else: download, file reading, bulk insert, staging/enrichment sync, mapping, scheduling, and metadata.

### Minimal (no overrides needed)

For CSV, JSON (flat list), GeoPackage, Shapefile, Parquet — the built-in reader handles the file. Your mapper file just needs to exist and import the base class:

```python
from main_core.data_source_abc_impl import DataSourceABCImpl


class MyDataMapper(DataSourceABCImpl):
    pass
```

### With `source_filter` — reshaping a nested JSON response

`source_filter` receives the raw parsed payload (the whole dict or list from the file) and must return a flat `list[dict]`. This is the most commonly overridden method for JSON APIs.

```python
# Real example from weatherStationMapper.py
class WeatherStationMapper(DataSourceABCImpl):

    def source_filter(self, data: list[dict]) -> list[dict]:
        # Bright Sky wraps stations in data[0]["sources"]
        stations = data[0]["sources"]
        filtered = [
            row for row in stations
            if row.get("observation_type") == "forecast"
            and int(row.get("last_record", "0")[:4]) >= 2024
        ]
        self.logger.info(f"Filtered {len(stations)} → {len(filtered)} stations")
        return filtered
```

```python
# Real example from weatherMapper.py — multi-fetch where each file has a sources list
class WeatherMapper(DataSourceABCImpl):

    def source_filter(self, data: list) -> list[dict]:
        result = []
        for content in data:
            sources = content.get("sources", [])
            if not sources:
                continue
            dwd_station_id = int(sources[0].get("dwd_station_id"))
            for weather in content.get("weather", []):
                enriched = dict(weather)
                enriched["dwd_station_id"] = dwd_station_id
                result.append(enriched)
        return result
```

### With `read_file_content` — custom/binary formats

Override this when the format isn't handled automatically (gz, zip, xml, pbf) or when you need WKB geometry encoding.

```python
# Real example from airQualityDataMapper.py — gzip-compressed GeoJSON
import gzip
import orjson

class AirQualityDataMapper(DataSourceABCImpl):

    def read_file_content(self, path: str) -> list[dict]:
        with gzip.open(path, "rb") as f:
            payload = orjson.loads(f.read())

        rows = []
        skipped = 0
        for feature in payload.get("features", []):
            try:
                props = feature["properties"]
                x, y = feature["geometry"]["coordinates"]
                rows.append({
                    "grid_id": props["id"],
                    "forecast_time": props["date_time_forecast_iso8601"],
                    "no2": props.get("no2"),
                    "pm10": props.get("pm10"),
                    "x_utm": x,
                    "y_utm": y,
                    "geom_25833": f"SRID=25833;POINT({x} {y})",
                })
            except Exception:
                skipped += 1
        if skipped:
            self.logger.warning(f"Skipped {skipped} malformed features in {path}")
        return rows
```

**Geometry encoding:** pass geometry as EWKT strings (`"SRID=25833;POINT(x y)"`) — PostGIS parses them on insert via geoalchemy2.

---

## Step 3 — Write the YAML Config

Each file in `data_source_configs/` is a **single flat dict** — no `datasources:` list wrapper. The framework auto-discovers every `*.yaml` in the folder on startup; no registration in `config.yaml` needed.

```yaml
# data_source_configs/weather_station_bright_sky.yaml

name: weather_station_bright_sky
description: "DWD weather station catalog from Bright Sky"
enable: true
class_name: weatherStation   # → weatherStationMapper.py → WeatherStationMapper
data_type: static
debug:
  endpoint: weather-station  # /debug/mappers/weather-station/...

source:
  fetch: http
  mode: single
  url: "https://api.brightsky.dev/sources"
  destination: "tmp/brightsky/stations/weatherStation.json"
  response_type: json
  check_metadata:
    enable: true
    keys: ["last_modified"]   # skip re-download when server file unchanged
  header:
    Accept: "application/json"

job:
  trigger:
    type:
      name: interval
      config:
        hours: 10

storage:
  persistent: true
  staging:
    table_name: dwd_station_locations_staging
    table_class: DwdStationsStagingTable
  enrichment:
    table_name: dwd_station_locations_enrichment
    table_class: DwdWeatherStationEnrichmentTable

mapping:
  enable: true
  table_name: dwd_station_locations_mapping
  strategy:
    type: knn
    base_geometry_column: geometry
    enrichment_geometry_column: point
    distance_sql: "ST_Distance({base_geometry}::geography, {enrichment_geometry}::geography)"
    order_by_sql: "ST_Distance({base_geometry}::geography, {enrichment_geometry}::geography)"
  base_table:
    column_name: dwd_station_id
    column_type: String
```

`table_schema` defaults to the `DB_SCHEMA` environment variable (`exp_null` in MDP); set it explicitly if you need a different schema.

---

## Stage Reference: Extraction

The framework reads each downloaded file through a filter pipeline. Override only what is dataset-specific.

### Built-in format support (no override needed)

| `response_type` | Library | What you get |
|----------------|---------|--------------|
| `json` | orjson | Raw `dict` or `list` — use `source_filter()` to reshape |
| `csv` / `tsv` | pandas | `list[dict]` |
| `gpkg` / `shp` / `geojson` | geopandas | `list[dict]` (geometry dropped by default) |
| `parquet` | pandas | `list[dict]` |
| `xlsx` / `xls` | pandas | First sheet as `list[dict]` |

For spatial formats, add a `reader:` block to reproject without Python:

```yaml
source:
  response_type: gpkg
  reader:
    engine: pyogrio        # "pyogrio" (default) or "fiona"
    target_crs: 25833      # auto-reprojects if source CRS differs
```

For `gz`, `zip`, `xml`, `pbf` — implement `read_file_content()`.

### Override reference

| Method | Signature | Default | When to override |
|--------|-----------|---------|-----------------|
| `read_file_content(path)` | `(str) → list[dict]` | Auto-detect by `response_type` | Unsupported format, WKB geometry needed, multi-file merge |
| `source_filter(data)` | `(list\|dict) → list[dict]` | Pass through | Flatten nested JSON, filter rows, add computed fields |
| `pre_filter_processing(data)` | `(list) → None` | No-op | Build in-memory spatial index (KDTree) before filtering |
| `post_filter_processing(data)` | `(list) → None` | No-op | Post-filter validation, write debug output |
| `before_filter_pipeline(data, path)` | `(list, str) → None` | No-op | Per-file setup (open side-channel connection) |
| `after_filter_pipeline(data, path)` | `(list, str) → None` | No-op | Per-file metrics or progress counter |
| `should_load_transformed_data(data, path)` | `(list, str) → bool` | `bool(data)` | Return `False` to skip DB insert for this file |
| `before_process_file(path)` | `(str) → None` | No-op | Per-file setup before `transform()` is called |
| `after_process_file(path, data)` | `(str, list) → None` | No-op | Per-file cleanup after `load()` |
| `on_process_file_error(path, error)` | `(str, Exception) → None` | Log error | Custom error handling or quarantine |

---

## Stage Reference: Load (Raw Staging)

After `source_filter`, the framework bulk-inserts records into a raw-staging clone of your table. These hooks fire around that insert.

### Hook order

```
for each file:
    before_load(data)
    pre_database_processing()
    → db.bulk_insert(raw_staging_table, records)
    after_load(data)

after all files:
    post_database_processing()
```

### Hook reference

| Method | Called when | Purpose |
|--------|-------------|---------|
| `before_load(data)` | Before bulk insert | Final validation, add metadata columns |
| `pre_database_processing()` | Before bulk insert | Prepare DB structures, clear in-memory caches |
| `after_load(data)` | After bulk insert | Record stats, reset per-file state |
| `post_database_processing()` | After **all** files are processed | Flush in-memory results accumulated across files |
| `load(data)` | Default bulk insert | Override entirely to skip DB (keep data in memory) |

### Bulk insert helper

```python
# Available inside any hook as self.db
self.db.bulk_insert(
    table_name="my_staging",
    schema="exp_null",
    data=records_list,   # list[dict]
    upsert=True          # INSERT ... ON CONFLICT DO UPDATE
)
self.db.call_sql("UPDATE exp_null.my_staging SET ...")
self.db.get_table_count("my_staging", "exp_null")  # → int
```

### Example — accumulate results across files (elevation mapper pattern)

```python
class MyMapper(DataSourceABCImpl):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._metrics = []

    def after_load(self, data):
        # Compute per-file metrics while data is still in memory
        self._metrics.extend([{"way_id": r["id"], "elev": r["elevation"]} for r in data])

    def post_database_processing(self):
        # Flush once all files are done
        if self._metrics:
            self.db.bulk_insert("elevation_metrics", "exp_null", self._metrics, upsert=True)
            self._metrics.clear()
```

---

## Stage Reference: Staging

After all files are processed, raw-staging is synced to the permanent staging table. Then `staging_db_query()` fires.

### Hook reference

| Method | Signature | Purpose |
|--------|-----------|---------|
| `staging_db_query()` | `() → str \| None` | SQL to run after raw→staging sync. Return `None` to skip. |
| `sync_raw_to_staging()` | `() → dict` | Override for custom aggregation or deduplication on sync |

### Accessing table names inside `staging_db_query`

```python
def staging_db_query(self) -> str | None:
    stg = self.data_source_config.storage.staging
    # stg.table_schema  →  "exp_null"
    # stg.table_name    →  "dwd_station_locations_staging"
    return f"""
        UPDATE {stg.table_schema}.{stg.table_name}
        SET geom_4326 = ST_SetSRID(ST_MakePoint(lon, lat), 4326)
        WHERE lon IS NOT NULL
          AND lat IS NOT NULL
          AND geom_4326 IS NULL
    """
```

Return `None` (the default) to skip this stage entirely.

---

## Stage Reference: Enrichment

After staging→enrichment sync, `enrichment_db_query()` fires.

### Hook reference

| Method | Signature | Purpose |
|--------|-----------|---------|
| `enrichment_db_query()` | `() → str \| None` | SQL to run after staging→enrichment sync. Return `None` to skip. |
| `sync_staging_to_enrichment()` | override | Custom aggregation on sync (e.g. hourly rollup, grid binning) |

### Accessing table names inside `enrichment_db_query`

```python
# Real example from weatherStationMapper.py
def enrichment_db_query(self) -> str | None:
    staging = self.data_source_config.storage.staging
    enrichment = self.data_source_config.storage.enrichment
    return f"""
        UPDATE {enrichment.table_schema}.{enrichment.table_name} e
        SET point = ST_SetSRID(ST_MakePoint(s.lon, s.lat), 4326)
        FROM {staging.table_schema}.{staging.table_name} s
        WHERE e.dwd_station_id = s.dwd_station_id
          AND e.point IS NULL
    """
```

```python
# Real example from airQualityDataMapper.py — CRS transform
def enrichment_db_query(self) -> str | None:
    enrichment = self.data_source_config.storage.enrichment
    return f"""
        UPDATE {enrichment.table_schema}.{enrichment.table_name}
        SET geom_4326 = ST_Transform(geom_25833, 4326)
        WHERE geom_25833 IS NOT NULL
          AND geom_4326 IS NULL
    """
```

### Alternative: `enrichment_operators` (config-driven, no Python needed)

For common transformations, declare operators in YAML instead of writing SQL:

```yaml
# From tree.yaml — derives typed columns from a raw JSONB attributes column
enrichment_operators:
  operators:
    - { type: derive, target_col: species_de, expression: "attributes->>'art_dtsch'" }
    - { type: derive, target_col: height_m,   expression: "NULLIF(attributes->>'baumhoehe','')::numeric" }
    - type: derive
      target_col: size_class
      expression: >
        CASE
          WHEN height_m >= 20 THEN 'large'
          WHEN height_m >= 10 THEN 'medium'
          WHEN height_m >  0  THEN 'small'
          ELSE 'unknown'
        END
```

Supported operator types: `derive`, `make_point`, `reproject`, `snap_to_grid`, `normalize`, `aggregate`, `spatial_aggregate`, `raster_aggregate`.

---

## Stage Reference: Mapping

Mapping links enrichment rows to `ways_base` road segments. The framework either auto-generates the PostGIS SQL (built-in strategies) or calls your `mapping_db_query()` (custom strategy).

### Pick the right strategy

| Data type | Density | Strategy | Key config |
|-----------|---------|----------|-----------|
| Points (stations, sensors) | Sparse | `knn` | `base_geometry_column`, `enrichment_geometry_column` |
| Points (trees, stops) | Dense | `aggregate_within_distance` | `max_distance`, `aggregation_type` |
| Regular grid cells | Medium | `sql_template` or `custom` | `sql` or `mapping_db_query()` |
| Polygons / lines | — | `intersection` | geometry columns |
| K nearest per road segment | — | `nearest_k` | `k` |
| Shared non-spatial key | — | `attribute_join` | `base_column`, `mapping_column` |
| Complex / LATERAL / CTE | — | `custom` | implement `mapping_db_query()` |

### Built-in strategy (YAML-only, no Python)

```yaml
# knn — nearest road per feature (weather stations)
mapping:
  enable: true
  table_name: dwd_station_locations_mapping
  strategy:
    type: knn
    base_geometry_column: geometry
    enrichment_geometry_column: point
    distance_sql: "ST_Distance({base_geometry}::geography, {enrichment_geometry}::geography)"
    order_by_sql: "ST_Distance({base_geometry}::geography, {enrichment_geometry}::geography)"
  base_table:
    column_name: dwd_station_id
    column_type: String
```

```yaml
# aggregate_within_distance — aggregate all trees within 50 m (tree mapper)
mapping:
  enable: true
  table_name: tree_mapping
  strategy:
    type: aggregate_within_distance
    max_distance: 50
    aggregation_type: jsonb_build_object
    aggregation_expression: |
      COALESCE(
        jsonb_agg(
          jsonb_build_object(
            'tree_id', {enrichment_alias}.id,
            'distance_m', ST_Distance({enrichment_alias}.geometry_25833, {base_geometry})
          )
          ORDER BY ST_Distance({enrichment_alias}.geometry_25833, {base_geometry})
        ) FILTER (WHERE {enrichment_alias}.id IS NOT NULL),
        '[]'::jsonb
      )
    aggregation_alias: trees
  base_table:
    column_name: tree_factor
    column_type: Integer
```

### Custom strategy — `mapping_db_query()`

Set `strategy.type: custom` (or `mapper_sql`) in YAML and implement `mapping_db_query()` in your mapper:

```yaml
mapping:
  enable: true
  table_name: my_mapping
  strategy:
    type: custom
```

```python
def mapping_db_query(self) -> str | None:
    enr = self.data_source_config.storage.enrichment
    m = self.data_source_config.mapping
    # m.table_schema, m.table_name  →  where to insert
    # enr.table_schema, enr.table_name  →  source of enrichment data
    return f"""
        INSERT INTO {m.table_schema}.{m.table_name}
            (way_id, dwd_station_id, distance)
        SELECT DISTINCT ON (e.dwd_station_id)
            w.id AS way_id,
            e.dwd_station_id,
            ST_Distance(w.geometry_25833, e.point::geometry) AS distance
        FROM {enr.table_schema}.{enr.table_name} e
        CROSS JOIN LATERAL (
            SELECT id, geometry_25833
            FROM {m.table_schema}.ways_base
            ORDER BY geometry_25833 <-> e.point::geometry
            LIMIT 1
        ) w
        ON CONFLICT (way_id) DO UPDATE SET
            dwd_station_id = EXCLUDED.dwd_station_id,
            distance        = EXCLUDED.distance
    """
```

### Available config attributes in mapping methods

```python
# Storage tables
self.data_source_config.storage.staging.table_name     # "dwd_station_locations_staging"
self.data_source_config.storage.staging.table_schema   # "exp_null"
self.data_source_config.storage.enrichment.table_name
self.data_source_config.storage.enrichment.table_schema
# Mapping table
self.data_source_config.mapping.table_name
self.data_source_config.mapping.table_schema
# Datasource identity
self.data_source_name                                   # "weather_station_bright_sky"
```

---

## Run-Level Hooks

| Method | Signature | When it fires | Use |
|--------|-----------|--------------|-----|
| `run_end_cleanup(succeeded, error)` | `(bool, Exception\|None)` | Always, even on failure | Temp file cleanup, memory release |
| `check_before_update()` | `() → bool` | Before extraction starts | Return `False` to abort the run |
| `after_datasource_success()` | `()` | After a successful run | Notify external system |
| `on_run_error(error)` | `(Exception)` | On unhandled run exception | Custom error reporting |

```python
def run_end_cleanup(self, succeeded: bool, error=None):
    if hasattr(self, "_temp_index"):
        self._temp_index = None   # release memory
    if error:
        self.logger.error(f"Run failed: {error}")
```

---

## Step 4 — Run and Verify

```bash
# Run only this datasource
python3 run.py --only weather_station_bright_sky

# Check row counts at each stage
psql -U postgres -d mydb -c "SELECT COUNT(*) FROM exp_null.dwd_station_locations_staging;"
psql -U postgres -d mydb -c "SELECT COUNT(*) FROM exp_null.dwd_station_locations_enrichment;"
psql -U postgres -d mydb -c "SELECT COUNT(*) FROM exp_null.dwd_station_locations_mapping;"

# Debug API (pipeline must be running)
curl http://localhost:8000/debug/datasources
curl http://localhost:8000/debug/mappers/weather-station/staging
curl http://localhost:8000/debug/mappers/weather-station/enrichment
curl http://localhost:8000/debug/mappers/weather-station/mapping
```

Hot-reload: edit any file in `data_source_configs/` → save → pipeline reloads in ~2 s. No restart needed.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| Staging table empty | `source_filter` returns empty list, or `read_file_content` fails | Check `source_filter()` logic; inspect raw file at `destination` path |
| Geometry column is NULL | `enrichment_db_query()` not running or wrong column names | Verify method returns a non-None SQL string; check `self.data_source_config.storage.*` attribute values |
| Too few mapping rows | Wrong geometry column or CRS mismatch | Check `base_geometry_column` / `enrichment_geometry_column` in mapping config; verify SRID consistency |
| Mapping skipped | `mapping.enable: false` or table count already matches | Set `enable: true` and check if incremental mapping is short-circuiting |
| Job skipped (no download) | Remote file hasn't changed, `check_metadata` cached it | Disable `check_metadata.enable` temporarily to force re-download |
| `table_class not found` | Class name in YAML doesn't match class in Python file | `table_class: DwdStationsStagingTable` must exactly match the Python class name |

---

## Checklist

- [ ] `data_mappers/<name>Mapper.py` created, class name matches `class_name` + `Mapper` suffix
- [ ] Staging and enrichment table classes defined in the mapper file
- [ ] `data_source_configs/<name>.yaml` written as a flat dict (no `datasources:` wrapper)
- [ ] `storage.staging.table_class` and `storage.enrichment.table_class` match your Python class names
- [ ] Strategy type chosen; geometry column names match what's actually in your enrichment table
- [ ] First run completed; staging / enrichment / mapping row counts > 0

---

## Related Docs

- [lifecycle-methods-reference.md](lifecycle-methods-reference.md) — every overridable method: signature, default behaviour, use cases, organized by ETL phase
- [mapper-README.md](mapper-README.md) — full lifecycle reference, all method signatures
- [mapping-strategies-reference.md](mapping-strategies-reference.md) — every strategy with full config examples
- [mapping-quick-reference.md](mapping-quick-reference.md) — one-page strategy cheat sheet
- [config-reference.md](config-reference.md) — every YAML field documented
- [migration-example-tree-mapper.md](migration-example-tree-mapper.md) — real migration from custom SQL to built-in strategy
- [example-weather-station-simplified.md](example-weather-station-simplified.md) — same example, minimal version
