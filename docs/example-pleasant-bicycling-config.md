# Example: Pleasant Bicycling Config (YAML)

**Source file:** [`data_source_configs/pleasant_bicycling.yaml`](../data_source_configs/pleasant_bicycling.yaml)  
**Mapper:** [`data_mappers/pleasantBicyclingMapper.py`](../data_mappers/pleasantBicyclingMapper.py) — see [example-pleasant-bicycling-mapper.md](example-pleasant-bicycling-mapper.md)

This config drives the pleasant bicycling score pipeline. Data is a local parquet file (not an HTTP fetch). It demonstrates:

- `fetch: local` — reading from the filesystem instead of HTTP
- `strategy: custom` — delegating mapping entirely to `mapping_db_query()` in the mapper
- A materialized view that uses `generate_series` + `CROSS JOIN` to build a 24-element hourly array per road segment
- Why `expires_after` is omitted for a high-cost ETL dataset

---

## Source — local file

```yaml
source:
  mode: single
  fetch: local
  file_path: data/pleasant_bicycle/aggregated_metrics.parquet
  response_type: csv     # hint only — mapper overrides read_file_content()
  save_local: true
  destination: tmp/pleasant_bicycle/pleasant.csv
  check_metadata:
    enable: true
    keys: ["content_type"]
```

**`fetch: local`**  
Instead of downloading from HTTP, the framework reads `file_path` directly from the local filesystem. No HTTP handler is invoked. This is useful for datasets delivered by another system (e.g. a data transfer job, mounted network share) rather than a public API.

**`response_type: csv`**  
This is a hint the framework would normally use to choose a reader, but `PleasantBicyclingMapper.read_file_content()` overrides the reader entirely and reads the parquet directly with pandas. The `response_type` here is effectively ignored.

---

## Job

```yaml
job:
  executor: process
  trigger:
    type:
      name: interval
      config:
        hours: 10
```

`executor: process` is important here. The mapper reads ~4M rows from parquet, joins a second file, and processes the result. A subprocess isolates the memory spike from the main pipeline process and releases it after the run.

**Why no `expires_after`?**  
This is an intentional omission. The ETL run takes ~50 minutes end-to-end (4M staging rows + 1M enrichment + full mapping). If `expires_after` is set too aggressively and the container restarts, the framework would force a re-run every time — creating an endless reset loop. Freshness is controlled by the 10-hour job interval instead.

---

## Mapping — custom strategy

```yaml
mapping:
  enable: true
  table_name: pleasant_mapping
  strategy:
    type: custom
  base_table:
    column_name: "pleasant_score"
    column_type: Float
```

`strategy: custom` tells the framework to call `mapping_db_query()` on the mapper class and execute the returned SQL. No built-in PostGIS SQL is generated. Use this when:

- You need `LEFT JOIN LATERAL` to guarantee every base row gets a mapping row
- The mapping involves complex radius + KNN filtering that built-in strategies can't express
- You need `ON CONFLICT … DO UPDATE` semantics with specific column targeting

The mapping SQL (in the mapper) uses `LEFT JOIN LATERAL` with a 5 m `ST_DWithin` filter so every road segment gets exactly one row — `NULL` connection_id for segments with no nearby bicycle data.

---

## Storage

```yaml
storage:
  persistent: true
  staging:
    table_name: pleasant_staging
    table_class: PleasantStagingTable
  enrichment:
    table_name: pleasant_enrichment
    table_class: PleasantEnrichmentTable
```

No `expires_after` (see job section above). `persistent: true` means staging rows accumulate via upsert rather than being truncated each run. The enrichment table is truncated and rebuilt each run by `execute_on_enrichment()` (the mapper overrides the default sync).

---

## Materialized view — 24-element hourly array

```yaml
materialized_view:
  name: mv_pleasant
  description: >
    Per-way hourly SPI (speed performance index) as a 24-element float array.
    spi_hourly[h] is the average SPI for hour h (0–23); -1 means no data.
  depends_on:
    tables:
      - { name: ways_base }
      - { name: pleasant_mapping }
      - { name: pleasant_enrichment }
  definition:
    select_sql: |
      WITH hours AS (
          SELECT generate_series(0, 23) AS hour
      )
      SELECT
          w.id, w.way_id, w.way_link_index,
          array_agg(
              COALESCE(pe.avg_speed_performance_index, -1)
              ORDER BY h.hour
          ) AS spi_hourly
      FROM {schema}.ways_base w
      CROSS JOIN hours h
      LEFT JOIN {schema}.pleasant_mapping pm ON pm.way_id = w.id
      LEFT JOIN {schema}.pleasant_enrichment pe
          ON pe.connection_id = pm.connection_id
         AND pe.hour          = h.hour
      GROUP BY w.id, w.way_id, w.way_link_index
  indexes:
    - { name: idx_mv_pleasant_id, columns: [id], unique: true }
    - { name: idx_mv_pleasant_way_id, columns: [way_id, way_link_index] }
```

**`WITH hours AS (SELECT generate_series(0, 23))`**  
Generates the integers 0–23. The `CROSS JOIN hours` then ensures every road segment is paired with all 24 hours, regardless of whether data exists for each hour.

**`COALESCE(pe.avg_speed_performance_index, -1)`**  
Hours with no bicycle data produce a `NULL` from the `LEFT JOIN`. `COALESCE` replaces it with `-1` as a sentinel value ("no data"). The Java scorer checks for `-1` before using a value.

**`array_agg(... ORDER BY h.hour)`**  
Builds the 24-element array in hour order. `ORDER BY` inside `array_agg` guarantees `spi_hourly[0]` is midnight, `spi_hourly[12]` is noon, etc.

**`depends_on` lists both `pleasant_mapping` and `pleasant_enrichment`**  
The view joins through both tables. Both must be up to date before the MV refresh fires.

---

## Summary of patterns used

| Pattern | Config key | When to use |
|---------|-----------|-------------|
| Local file source | `fetch: local` + `file_path:` | Data is delivered to disk by another system, not an HTTP endpoint |
| No `expires_after` | (omitted) | ETL is expensive; let the job interval control freshness instead |
| Fully custom mapping | `strategy: custom` | Built-in strategies can't express your JOIN logic |
| 24-element time-series array in MV | `generate_series(0,23)` + `CROSS JOIN` + `array_agg` | Result must be a fixed-length array, one slot per time bucket, with missing data filled |
| `-1` as no-data sentinel in array | `COALESCE(..., -1)` | Consumer code distinguishes "no data" from "zero" (e.g. zero speed vs. no measurement) |
