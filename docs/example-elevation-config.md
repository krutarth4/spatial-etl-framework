# Example: Elevation Config (YAML)

**Source file:** [`data_source_configs/elevation.yaml`](../data_source_configs/elevation.yaml)  
**Mapper:** [`data_mappers/elevationMapper.py`](../data_mappers/elevationMapper.py) — see [example-elevation-mapper.md](example-elevation-mapper.md)

**Related config:** [`data_source_configs/elevation_grids_links.yaml`](../data_source_configs/elevation_grids_links.yaml) — see [example-elevation-grid-links.md](example-elevation-grid-links.md)

This config drives the 1 m DEM raster pipeline: download tiles listed by another datasource (`elevation_grids_links`), build PostGIS rasters in staging, downsample in enrichment, then compute per-way ascent/descent/slope. It demonstrates:

- `depends_on` inter-datasource dependency
- `run_once` trigger (static dataset; fetch only on first run)
- `explicit_url_list` multi-fetch — URLs come from a file produced by another datasource
- `raster_aggregate` enrichment operator to downsample a raster
- `sql_template` mapping strategy pointing to an external SQL file

---

## Source and fetch

```yaml
source:
  fetch: http
  mode: multi
  response_type: zip
  save_local: true
  destination: tmp/elevation_zips/elevation.zip
  check_metadata:
    enable: true
    keys: ["last_modified"]
  multi_fetch:
    enable: true
    strategy: explicit_url_list
    urls:
      input: data/grid/elevation_grid_links.json   # written by elevation_grids_links
```

**`strategy: explicit_url_list`**  
Instead of generating URLs from a template, this reads a JSON file that already contains the list of download URLs. The file (`elevation_grid_links.json`) is produced by the `elevation_grids_links` datasource's `after_filter_hook`.

This pattern is useful when the URL list is not predictable in advance — it must be discovered by parsing an index page or API response first. The two datasources form a producer-consumer chain: `elevation_grids_links` writes the URL file; `elevation` reads it.

---

## Inter-datasource dependency

```yaml
depends_on: elevation_grids_links
```

This single line tells the scheduler to run `elevation_grids_links` first and wait for it to complete successfully before starting this datasource. If `elevation_grids_links` fails, `elevation` is skipped for that cycle.

---

## Job trigger — run_once

```yaml
job:
  executor: process
  trigger:
    type:
      name: run_once
```

`run_once` fires the datasource exactly once when the pipeline starts, then never again. Appropriate for static datasets (DEM tiles change infrequently). When new tiles are needed, set `enable: false`, remove the locally cached data, and restart with `enable: true`.

`executor: process` runs the raster processing in a separate OS process. The XYZ → GeoTIFF conversion allocates a large numpy array (hundreds of MB per tile); using a subprocess ensures the memory is fully released when the tile is done.

---

## Enrichment operator — raster downsample

```yaml
enrichment_operators:
  operators:
    - type: raster_aggregate
      source_table: staging
      raster_col: rast
      target_col: rast
      cell_size: 10        # 1 m → 10 m
      algorithm: Average   # ST_Resample algorithm
```

`raster_aggregate` calls `ST_Resample(rast, cell_size, algorithm)` on each staging tile and inserts the result into the enrichment table. This is config-driven downsampling — no Python needed.

**Why downsample?**  
The elevation SQL template (`elevation_raster.sql`) samples the raster at 10 m intervals along each road segment. If the source raster is 1 m, each sample hits a single 1 m pixel — trivially fast. But spatial queries against hundreds of millions of 1 m pixels are slow. Downsampling to 10 m averaged cells:
- Reduces the tile count 100× (10² = 100)
- Smooths out noise in raw LiDAR data
- Has no visible effect on ascent/descent calculations at road scale

Available `algorithm` values (from `ST_Resample`): `Average`, `Bilinear`, `Cubic`, `Min`, `Max`, `Mode`.

---

## Mapping — sql_template

```yaml
mapping:
  enable: true
  table_name: elevation_mapping
  strategy:
    type: sql_template
    sql_file: mapping_sql/elevation_raster.sql
  base_table:
    column_name: "elevation_factor"
    column_type: Integer
```

`sql_template` reads the SQL from the file and executes it as-is, substituting the standard placeholders:

| Placeholder | Replaced with |
|-------------|---------------|
| `{base_schema}.{base_table}` | `exp_null.ways_base` |
| `{enrichment_schema}.{enrichment_table}` | `exp_null.elevation_enrichment` |
| `{mapping_schema}.{mapping_table}` | `exp_null.elevation_mapping` |

The SQL file ([`mapping_sql/elevation_raster.sql`](../mapping_sql/elevation_raster.sql)) walks each road segment at 10 m intervals, reads the raster value at each sample point with `ST_Value()`, and computes ascent/descent from consecutive elevation differences. Key parts:

```sql
-- Walk each way at 10 m intervals, generating sample points
samples AS (
    SELECT way_id, ST_LineInterpolatePoint(geom, LEAST(1.0, (i * step_m) / len)) AS pt
    FROM ways CROSS JOIN LATERAL generate_series(0, CEIL(len / step_m)) AS gs(i)
),
-- Read elevation at each point from the raster
elev AS (
    SELECT s.way_id, ST_Value(r.rast, s.pt) AS z
    FROM samples s
    JOIN elevation_enrichment r
      ON ST_ConvexHull(r.rast) && s.pt
     AND ST_Intersects(r.rast, s.pt)
),
-- Compute per-segment statistics from elevation differences
agg AS (
    SELECT way_id,
           SUM(dz) FILTER (WHERE dz > 0)   AS total_ascent,
           -SUM(dz) FILTER (WHERE dz < 0)  AS total_descent,
           MAX(ABS(dz))                    AS max_step_rise
    FROM (SELECT z - LAG(z) OVER (PARTITION BY way_id ORDER BY i) AS dz FROM elev) d
)
```

`ST_ConvexHull(r.rast) && s.pt` is the GiST index hit — it eliminates all tiles whose bounding box doesn't contain the sample point before the more expensive `ST_Intersects` check.

---

## Storage

```yaml
storage:
  persistent: true
  expires_after: 168h   # 7 days — matches the tile update interval
  staging:
    table_name: elevation_staging
    table_class: ElevationStagingTable
  enrichment:
    table_name: elevation_enrichment
    table_class: ElevationEnrichmentTable
```

`expires_after: 168h` (7 days) matches the DEM tile publication cadence. New tiles are available approximately weekly; forcing a re-fetch sooner would download identical data.

---

## Materialized view

```yaml
materialized_view:
  name: mv_ways_with_elevation
  depends_on:
    tables:
      - { name: ways_base }
      - { name: elevation_mapping }
  definition:
    select_sql: |
      SELECT
          w.way_id, w.way_link_index,
          COALESCE(m.total_ascent, 0.0)  AS total_ascent,
          COALESCE(m.total_descent, 0.0) AS total_descent,
          COALESCE(m.max_slope, 0.0)     AS max_slope
      FROM {schema}.ways_base w
      LEFT JOIN {schema}.elevation_mapping m ON w.id = m.way_id
  indexes:
    - { name: idx_mv_elevation_way_id_link, columns: [way_id, way_link_index], unique: true }
```

The unique index on `(way_id, way_link_index)` means this MV can use `CONCURRENT` refresh (the default from `mv_defaults`). Concurrent refresh does not lock the view during refresh, so the router continues serving queries uninterrupted.

---

## Summary of patterns used

| Pattern | Config key | When to use |
|---------|-----------|-------------|
| Wait for another datasource | `depends_on: elevation_grids_links` | This datasource needs a file produced by another |
| URL list from file | `strategy: explicit_url_list` + `urls.input` | URLs are not predictable; another datasource discovers them |
| Fire once on startup | `trigger.type.name: run_once` | Static dataset; no need to re-fetch on schedule |
| Raster downsample in enrichment | `enrichment_operators: raster_aggregate` | High-resolution raster that is too large for direct mapping |
| External SQL file for mapping | `strategy: sql_template` + `sql_file` | Mapping SQL is complex; better in a dedicated `.sql` file than inline YAML |
| `COALESCE(m.column, 0.0)` in MV | `SELECT` clause | Road segments with no raster coverage get a safe zero default |
