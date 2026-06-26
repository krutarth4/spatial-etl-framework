# Example: Air Quality Config (YAML)

**Source file:** [`data_source_configs/air_quality_data_download.yaml`](../data_source_configs/air_quality_data_download.yaml)  
**Mapper:** [`data_mappers/airQualityDataMapper.py`](../data_mappers/airQualityDataMapper.py) — see [example-air-quality-mapper.md](example-air-quality-mapper.md)

This config fetches air pollution forecast grids (NO2, PM10, PM2.5) from the DCAITI / TU Berlin FairQ API. Data is paged across four URLs (skipping 0, 100k, 200k, 300k rows). It demonstrates:

- `url_template` multi-fetch strategy for paged APIs
- The `idw` (Inverse Distance Weighted) mapping strategy
- `enrichment_filter_sql` to pin interpolation to the latest forecast
- A materialized view with a forecast window CTE

---

## Source and fetch

```yaml
source:
  fetch: http
  mode: multi
  url: https://werkzeug.dcaiti.tu-berlin.de/fairqberlin/
  check_metadata:
    enable: true
    keys: ["last_modified"]
  response_type: json.gz
  save_local: true
  destination: tmp/dcaiti/airqualityAPI/fairq.gz
  multi_fetch:
    enable: true
    strategy: url_template
    url_template: https://werkzeug.dcaiti.tu-berlin.de/fairqberlin/inwt_fairq_cache_skip_{skip}_limit_100000.json.gz
    template_params:
      skip: [0, 100000, 200000, 300000]
```

**`strategy: url_template`**  
The framework expands `template_params.skip` and builds one URL per value by substituting `{skip}` in `url_template`. The four resulting URLs are fetched in parallel:

```
.../inwt_fairq_cache_skip_0_limit_100000.json.gz
.../inwt_fairq_cache_skip_100000_limit_100000.json.gz
.../inwt_fairq_cache_skip_200000_limit_100000.json.gz
.../inwt_fairq_cache_skip_300000_limit_100000.json.gz
```

Each file is processed independently by `read_file_content()`. This is the pattern for any paged HTTP API where pagination is controlled by an offset parameter.

**`response_type: json.gz`**  
Signals to the framework that the file is gzip-compressed JSON, and the mapper's `read_file_content()` handles decompression manually.

---

## Mapping — IDW strategy

```yaml
mapping:
  enable: true
  table_name: air_pollution_grid_mapping
  strategy:
    type: idw
    k: 4
    power: 2
    base_geometry_sql: "COALESCE({base_alias}.geometry_25833, ST_Transform({base_alias}.geometry, 25833))"
    enrichment_geometry_column: geom_25833
    distance_alias: nearest_distance_m
    enrichment_filter_sql: >
      e.no2 IS NOT NULL
      AND e.forecast_time = (SELECT MAX(ee.forecast_time) FROM {enrichment_table} ee)
    value_columns:
      - { name: no2,  type: array }
      - { name: pm10, type: array }
      - { name: pm25, type: array }
```

**IDW (Inverse Distance Weighted)**  
IDW interpolates a continuous field onto each road segment by taking a weighted average of the `k` nearest grid cells, where each cell's weight is `1 / distance^power`. With `k=4, power=2`:
- Closer cells contribute more strongly (weight ∝ 1/d²)
- Using 4 neighbours smooths out the Voronoi-like steps you'd get from a nearest-1 approach

**`base_geometry_sql`**  
Most road segments carry `geometry_25833`, but some older segments may have only `geometry` (4326). The `COALESCE` + `ST_Transform` fallback ensures every segment gets a valid geometry for the distance calculation.

**`enrichment_filter_sql`**  
The enrichment table accumulates multiple forecast runs (different `forecast_time` values). Without this filter, IDW would interpolate across all runs simultaneously — mixing forecasts from different origins. Pinning to `MAX(forecast_time)` ensures only the latest forecast contributes to the mapping.

**`value_columns`**  
Each named column is interpolated element-wise across the `k` neighbours. `type: array` tells the strategy that the column holds a float array — each element (forecast hour) is IDW-interpolated independently.

---

## Storage

```yaml
storage:
  persistent: true
  expires_after: 6h
  staging:
    table_name: air_pollution_grid
    table_class: AirPollutionGridStagingTable
  enrichment:
    table_name: air_pollution_grid_enrichment
    table_class: AirPollutionGridEnrichmentTable
```

`persistent: true` + `expires_after: 6h` means data accumulates via upsert but is considered stale after 6 hours. The framework forces a fresh download after 6 hours regardless of what `check_metadata` reports.

---

## Materialized view

```yaml
materialized_view:
  name: mv_air_pollution
  refresh:
    mode: normal
  depends_on:
    tables:
      - { name: ways_base }
      - { name: air_pollution_grid_enrichment }
      - { name: air_pollution_grid_mapping }
  definition:
    select_sql: |
      WITH forecast_meta AS (
          SELECT
              timezone('UTC', MIN(forecast_time))  AS forecast_start,
              timezone('UTC',
                  MIN(forecast_time)
                  + (MAX(array_length(no2, 1)) - 1) * INTERVAL '1 hour'
              )                                    AS forecast_end
          FROM {schema}.air_pollution_grid_enrichment
          WHERE no2 IS NOT NULL AND forecast_time IS NOT NULL
      )
      SELECT
          w.id, w.way_id, w.way_link_index,
          fm.forecast_start,
          fm.forecast_end,
          m.no2, m.pm10, m.pm25
      FROM {schema}.ways_base w
      CROSS JOIN forecast_meta fm
      LEFT JOIN {schema}.air_pollution_grid_mapping m ON m.way_id = w.id
  indexes:
    - { name: mv_air_pollution_way_id_idx, columns: [way_id] }
    - { name: mv_air_pollution_id_idx, columns: [id], unique: true }
```

**`forecast_meta` CTE**  
Computes the human-readable forecast window (`forecast_start` / `forecast_end`) from the array data itself, without needing a separate metadata table. The `CROSS JOIN forecast_meta` attaches these timestamps to every row so consumers don't need to know the array index → timestamp mapping.

**`CROSS JOIN forecast_meta`**  
Since `forecast_meta` returns exactly one row, the cross join is equivalent to appending two scalar columns to every `ways_base` row. Consumers (the Java router) read `forecast_start` to know what slot 0 in the array corresponds to.

**`depends_on.tables` includes `air_pollution_grid_enrichment`**  
The MV reads from both the mapping table and the enrichment table (via the CTE). Both tables must be listed so the refresh fires only after both are up to date.

---

## Summary of patterns used

| Pattern | Config key | When to use |
|---------|-----------|-------------|
| Paged HTTP API | `url_template` + `template_params.skip` | API uses offset/limit pagination |
| IDW spatial interpolation | `strategy: idw` | Continuous field (pollution, temperature) that should be smoothed |
| Pin enrichment to latest forecast | `enrichment_filter_sql` | Enrichment accumulates multiple runs; only latest matters |
| Array columns per value | `value_columns: [{type: array}]` | Feature has a per-hour time series packed as a float array |
| Forecast window CTE in MV | `WITH forecast_meta` | Consumers need to interpret array indices as timestamps |
