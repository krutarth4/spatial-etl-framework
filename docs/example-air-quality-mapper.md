# Example: Air Quality Data Mapper (Python)

**Source file:** [`data_mappers/airQualityDataMapper.py`](../data_mappers/airQualityDataMapper.py)  
**Config:** [`data_source_configs/air_quality_data_download.yaml`](../data_source_configs/air_quality_data_download.yaml) — see [example-air-quality-config.md](example-air-quality-config.md)

This mapper ingests air pollution grid data (NO2, PM10, PM2.5) from the DCAITI / TU Berlin FairQ server. Data arrives as gzip-compressed GeoJSON files, paged across multiple URLs. It is a good example of:

- Parsing a gzip-compressed JSON file manually in `read_file_content()`
- Building a PostGIS geometry string (EWKT) inline during parsing — no separate enrichment step for geometry
- Using `enrichment_db_query()` to add a second geometry column in a different CRS
- Storing pollutant values as `ARRAY(Float)` columns — one array per forecast horizon per cell

---

## Table models

### AirPollutionGridStagingTable

```python
class AirPollutionGridStagingTable(StagingTable):
    __tablename__ = "air_pollution_grid"

    uid           = Column(Integer, primary_key=True, autoincrement=True)
    grid_id       = Column(Integer, nullable=False, index=True)
    forecast_time = Column(DateTime, nullable=True)
    forecast_range = Column(String(100), nullable=True)

    # Pollutant forecast arrays — one float per hour in the forecast window
    no2  = Column(ARRAY(Float))
    pm10 = Column(ARRAY(Float))
    pm25 = Column(ARRAY(Float))

    x_utm = Column(Float, nullable=False)
    y_utm = Column(Float, nullable=False)
    geom_25833 = Column(Geometry("POINT", srid=25833), nullable=False)

    __table_args__ = (
        UniqueConstraint("grid_id", "forecast_time"),  # composite natural key
        Index(None, "geom_25833", postgresql_using="gist"),
    )
```

**Why `ARRAY(Float)` for pollutant values?**  
Each grid cell carries a full forecast array — one concentration value per forecast hour. Storing the array as a single column avoids a separate row per hour, which would expand the table ~24× and make the KNN/IDW mapping join expensive.

**Composite unique key `(grid_id, forecast_time)`**  
The same grid cell has a different forecast record for each forecast origin time. Both columns together form the natural key for upserts.

---

### AirPollutionGridEnrichmentTable

Identical to staging except it adds a second geometry column in `EPSG:4326`:

```python
geom_25833 = Column(Geometry("POINT", srid=25833), nullable=False)
geom_4326  = Column(Geometry("POINT", srid=4326))
```

The IDW mapping strategy uses `geom_25833`; the `geom_4326` column is populated by `enrichment_db_query()` and is kept for debug panel visualization.

---

### AirPollutionGridMappingTable

```python
class AirPollutionGridMappingTable(MappingTable):
    __tablename__ = "air_pollution_grid_mapping"

    way_id           = Column(Integer, ForeignKey(...), primary_key=True)
    no2              = Column(ARRAY(Float), nullable=True)
    pm10             = Column(ARRAY(Float), nullable=True)
    pm25             = Column(ARRAY(Float), nullable=True)
    nearest_distance_m = Column(Float, nullable=True)
```

The IDW mapping strategy writes one interpolated array per pollutant per road segment. `nearest_distance_m` records how far the nearest contributing grid cell was — useful for diagnosing coverage gaps.

---

## Mapper class

### `read_file_content` — gzip decompression + EWKT geometry

```python
def read_file_content(self, path):
    return self.load_and_store_gz_json(path)

def load_and_store_gz_json(self, gz_path):
    with gzip.open(gz_path, "rb") as f:
        payload = orjson.loads(f.read())       # fast binary JSON decode

    features = payload.get("features", []) or []
    rows = []
    skipped = 0
    for feature in features:
        try:
            props = feature["properties"]
            x, y = feature["geometry"]["coordinates"]
            rows.append({
                "grid_id":        props["id"],
                "forecast_time":  props["date_time_forecast_iso8601"],
                "forecast_range": props["forecast_range_iso8601"],
                "no2":            props.get("no2"),
                "pm10":           props.get("pm10"),
                "pm25":           props.get("pm2.5"),
                "x_utm":          x,
                "y_utm":          y,
                "geom_25833":     f"SRID=25833;POINT({x} {y})",  # EWKT
            })
        except Exception:
            skipped += 1
    if skipped:
        self.logger.warning(f"Skipped {skipped} malformed features in {gz_path}")
    return rows
```

**EWKT geometry string (`SRID=25833;POINT(x y)`)**  
PostGIS accepts EWKT strings directly via psycopg when inserting into a `Geometry` column. Building the string inline in the parser means no round-trip through a separate enrichment SQL step just to add geometry — the geometry is already in staging.

Compare this with the tree mapper, which uses WKB hex. EWKT is simpler when you have raw coordinates; WKB hex is needed when reading from GeoPandas (which produces Shapely objects).

**`orjson` over `json`**  
orjson is 2–4× faster for large files and handles `datetime` objects natively — which matters here because `forecast_time` is an ISO 8601 timestamp.

**Skipping malformed features**  
The source is a research API that occasionally emits partial records. Using a try/except per feature and logging a warning keeps the run alive even if a few hundred cells are malformed.

---

### `enrichment_db_query` — CRS transform

```python
def enrichment_db_query(self) -> str | None:
    enrichment = self.data_source_config.storage.enrichment
    return f"""
        UPDATE {enrichment.table_schema}.{enrichment.table_name}
        SET geom_4326 = ST_Transform(geom_25833, 4326)
        WHERE geom_25833 IS NOT NULL
          AND geom_4326 IS NULL;
    """
```

`ST_Transform(geom_25833, 4326)` reprojects the stored EPSG:25833 point to WGS84. The `geom_4326 IS NULL` guard makes the query idempotent — re-running it skips cells that were already projected.

---

## How the stages connect

```
gz files (4 pages × ~100k features)
        │
        ▼  read_file_content()
        │  → gzip decompress → orjson parse → EWKT geometry inline
        ▼
  air_pollution_grid (staging)      (grid_id, forecast_time, no2/pm10/pm25 arrays, geom_25833)
        │  default sync copies matching columns
        ▼
  air_pollution_grid_enrichment     (+ geom_4326 added by enrichment_db_query)
        │  IDW strategy (k=4, p=2) interpolates arrays onto road segments
        ▼
  air_pollution_grid_mapping        (way_id → no2/pm10/pm25 arrays)
        │
        ▼
  mv_air_pollution                  (ways_base LEFT JOIN mapping, + forecast window CTE)
```

---

## Key patterns to reuse

| Pattern | Where | When to use |
|---------|-------|-------------|
| gzip decompression in `read_file_content` | `load_and_store_gz_json` | Source is `.gz` compressed JSON |
| EWKT string geometry in parser | `f"SRID=25833;POINT({x} {y})"` | You have raw coordinates; simpler than WKB |
| `ARRAY(Float)` column | `no2 / pm10 / pm25` columns | Source carries a time-series per feature |
| Per-feature try/except with warning | parser loop | Research APIs with occasional malformed rows |
| `geom_4326 IS NULL` guard in enrichment SQL | `enrichment_db_query` | Idempotent column population; safe to re-run |
