# Example: Weather Station Mapper + Config

**Mapper:** [`data_mappers/weatherStationMapper.py`](../data_mappers/weatherStationMapper.py)  
**Config:** [`data_source_configs/weather_station_bright_sky.yaml`](../data_source_configs/weather_station_bright_sky.yaml)

This datasource fetches DWD weather station locations from the Bright Sky API, builds a PostGIS point geometry in enrichment, and maps each road segment to its nearest station while also computing the road's travel bearing. It is the simplest example of the full ETL pipeline (no custom `read_file_content`, standard JSON source) and a good reference for:

- `source_filter()` to unwrap a nested JSON envelope and filter by field value
- `enrichment_db_query()` to build a point geometry from lat/lon columns
- `knn` strategy overriding the default geometry columns with `::geography` distance for accurate metre-level distances
- `select_columns` to compute a derived column (`bearing_degree`) during mapping

---

## Table models

```python
class DwdStationsTable(StagingTable):
    __tablename__ = "dwd_station_locations_staging"

    uid            = Column(Integer, primary_key=True, autoincrement=True)
    id             = Column(Integer)
    dwd_station_id = Column(Integer, unique=True, nullable=False)
    station_name   = Column(String)
    observation_type = Column(String)
    lat            = Column(Float)
    lon            = Column(Float)
    height         = Column(Float)
    wmo_station_id = Column(String)
    first_record   = Column(DateTime(timezone=True))
    last_record    = Column(DateTime(timezone=True))


class DwdWeatherStationEnrichmentTable(EnrichmentTable):
    __tablename__ = "dwd_station_locations_enrichment"

    uid            = Column(Integer, primary_key=True, autoincrement=True)
    dwd_station_id = Column(Integer, unique=True, nullable=False)
    point          = Column(Geometry(geometry_type="POINT", srid=4326), index=True)


class DwdMappingTable(MappingTable):
    __tablename__ = "dwd_station_locations_mapping"

    uid            = Column(Integer, primary_key=True, autoincrement=True)
    dwd_station_id = Column(Integer, ForeignKey(...), nullable=False)
    distance       = Column(Float, nullable=False)
    bearing_degree = Column(Float, nullable=True)   # road travel direction (0-360°)
```

Staging holds the raw API response fields including `lat` and `lon`. Enrichment holds only the `dwd_station_id` and the `point` geometry derived from those coordinates — keeping enrichment lean for the KNN query. The mapping table adds `bearing_degree` computed during the mapping stage.

---

## Mapper class

### `source_filter` — unwrap and filter

```python
def source_filter(self, data: list[dict]) -> list[dict]:
    data = data[0]["sources"]   # unwrap: {"sources": [...]} → [...]

    filtered = [
        row for row in data
        if row.get("observation_type") == "forecast"
        and int(row.get("last_record")[:4]) >= 2024
    ]

    self.logger.info(f"Filtered {len(data)} → {len(filtered)} rows")
    return filtered
```

The Bright Sky `/sources` endpoint returns `{"sources": [...], "url": "..."}`. `data[0]["sources"]` unwraps the outer dict to get the list of station records.

The filter keeps only stations with `observation_type == "forecast"` that have records from 2024 or later. Historical-only stations are excluded because the weather forecast datasource only fetches forecasts, not historical observations.

### `enrichment_db_query` — build point geometry from lat/lon

```python
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

The enrichment table has `dwd_station_id` (copied from staging by the default sync) but no geometry yet. This UPDATE reads `lon` and `lat` from staging, builds a `POINT` geometry in WGS84, and writes it to `enrichment.point`.

`AND e.point IS NULL` makes the query idempotent — stations that already have a geometry are not recomputed.

---

## Config

### Source — query-string params

```yaml
source:
  fetch: http
  mode: single
  url: https://api.brightsky.dev/sources
  response_type: json
  check_metadata:
    enable: true
    keys: ["last_modified"]
  params:
    dwd_station_id: ["00399", "00403", "00400", "00410", "00420", "00427", "00430", "00433"]
```

`params` appends query-string parameters to the URL. Unlike `multi_fetch.expand` (which fires one request per value), a list value here sends all eight station IDs in a single request. The Bright Sky API accepts multi-value params and returns all matching stations in one response.

---

### Mapping — KNN with geography distance + bearing

```yaml
mapping:
  enable: true
  table_name: dwd_station_locations_mapping
  strategy:
    type: knn
    mapping_column: dwd_station_id
    base_column: dwd_station_id
    basis: nearest_by_distance
    base_geometry_column: geometry           # ways_base uses EPSG:4326 geometry
    enrichment_geometry_column: point        # enrichment.point is EPSG:4326
    distance_sql: >
      ST_Distance({base_geometry}::geography, {enrichment_geometry}::geography)
    order_by_sql: >
      ST_Distance({base_geometry}::geography, {enrichment_geometry}::geography)
    select_columns:
      - expression: |
          MOD(
            (DEGREES(
              ST_Azimuth(
                ST_StartPoint({base_geometry}),
                ST_EndPoint({base_geometry})
              )
            ) + 360)::NUMERIC,
            360
          )
        alias: bearing_degree
```

**`::geography` cast**  
Casting geometry to `geography` makes `ST_Distance` return metres on the WGS84 ellipsoid instead of degrees. Without this, distance comparisons between EPSG:4326 geometries would be in degrees (meaningless for "nearest station within N km").

**`select_columns`**  
These are extra expressions computed during the KNN mapping INSERT — columns beyond what the built-in strategy generates by default. Each entry becomes an additional column in the `INSERT … SELECT`:

- `ST_Azimuth(ST_StartPoint(…), ST_EndPoint(…))` returns the direction of the road segment in radians (0 = north)
- `DEGREES(…)` converts to degrees
- `MOD(… + 360, 360)` normalises to 0–360° (ST_Azimuth can return values < 0°)

The `alias: bearing_degree` maps to the `bearing_degree` column in `DwdMappingTable`. The weather MV reads this column to compute headwind/tailwind.

---

### Storage — staging non-persistent

```yaml
storage:
  persistent: true
  staging:
    table_name: dwd_station_locations_staging
    table_class: DwdStationsTable
    persistent: false    # staging truncated each run
  enrichment:
    table_name: dwd_station_locations_enrichment
    table_class: DwdWeatherStationEnrichmentTable
```

Station data changes rarely — the same ~eight stations are returned on every run. Truncating staging each run avoids accumulating duplicate rows. Enrichment is kept (`persistent: true` from the parent block) and updated via upsert on `dwd_station_id`.

---

## How the stages connect

```
GET /sources?dwd_station_id=00399&00403&…
        │  JSON: {"sources": [...]}
        ▼  source_filter()
        │  → unwrap "sources" key
        │  → filter observation_type + year
        ▼
  dwd_station_locations_staging    (lat, lon, station_name, …)
        │  default sync copies matching columns
        ▼
  dwd_station_locations_enrichment (dwd_station_id → point geometry)
        │  enrichment_db_query() sets point from lat/lon
        ▼  knn strategy + select_columns
  dwd_station_locations_mapping    (way_id → dwd_station_id, distance, bearing_degree)
        │
        ▼
  (consumed by mv_weather via LEFT JOIN on dwd_station_id)
```

---

## Key patterns to reuse

| Pattern | Where | When to use |
|---------|-------|-------------|
| Unwrap nested envelope in `source_filter` | `data[0]["sources"]` | API returns `{"results": [...]}` or similar wrapper |
| Filter records in `source_filter` | `observation_type == "forecast"` | Source returns more records than needed; filter before staging |
| `ST_SetSRID(ST_MakePoint(lon, lat), 4326)` | `enrichment_db_query` | Raw lat/lon in staging; need a geometry column in enrichment |
| `AND column IS NULL` in enrichment SQL | `AND e.point IS NULL` | Idempotent UPDATE — skip rows already processed |
| `::geography` in KNN distance SQL | `distance_sql` + `order_by_sql` | Need accurate metre distances from WGS84 coordinates |
| `select_columns` for derived mapping columns | `bearing_degree` expression | Mapping should compute an extra column (bearing, length, ratio) from the base geometry |
