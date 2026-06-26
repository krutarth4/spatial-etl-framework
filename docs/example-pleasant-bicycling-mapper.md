# Example: Pleasant Bicycling Mapper (Python)

**Source file:** [`data_mappers/pleasantBicyclingMapper.py`](../data_mappers/pleasantBicyclingMapper.py)  
**Config:** [`data_source_configs/pleasant_bicycling.yaml`](../data_source_configs/pleasant_bicycling.yaml) — see [example-pleasant-bicycling-config.md](example-pleasant-bicycling-config.md)

This mapper ingests bicycle speed performance data from a local parquet dataset (~4 million 15-minute slots), joins it with a lane geometry parquet, aggregates it into hourly enrichment rows, and maps each road segment to its nearest matching connection. It demonstrates:

- Joining two parquet files in `read_file_content()` — a metrics file and a geometry/lane file
- Overriding `sync_staging_to_enrichment()` to skip the default verbatim copy
- Overriding `execute_on_enrichment()` to run a custom hourly aggregation SQL
- `mapping_db_query()` with `LEFT JOIN LATERAL` to guarantee one mapping row per road segment even when no nearby data exists

---

## Table models

### PleasantStagingTable — 15-minute raw slots

```python
class PleasantStagingTable(StagingTable):
    __tablename__ = "pleasant_staging"

    id              = Column(BigInteger, primary_key=True, autoincrement=True)
    connection_id   = Column(String, nullable=False, index=True)
    interval_start  = Column(BigInteger, nullable=True)   # nanoseconds since epoch
    interval_end    = Column(BigInteger, nullable=True)
    avg_temporal_mean_speed      = Column(Float)
    avg_spatial_mean_speed       = Column(Float)
    avg_naive_mean_speed         = Column(Float)
    avg_speed_performance_index  = Column(Float)
    sample_count    = Column(Integer)
    lane_id         = Column(String, index=True)
    edge_id         = Column(String, index=True)
    geometry        = Column(Text)                       # WKT from source
    geometry_25833  = Column(Geometry("Linestring", srid=25833))
    join_status     = Column(String, default="matched")  # "matched" | "metrics_only" | "lanes_only"

    __table_args__ = (
        UniqueConstraint("connection_id", "interval_start", "interval_end"),
        Index(None, "geometry_25833", postgresql_using="gist"),
    )
```

`interval_start` and `interval_end` are nanosecond Unix timestamps (the source uses nanoseconds). The enrichment aggregation divides them by `3_600_000_000_000` to get hour-of-day.

`join_status` records whether the record has both metrics and geometry (`matched`), only metrics (`metrics_only`), or only geometry (`lanes_only`). Records without a matched geometry still reach staging but cannot participate in spatial mapping.

---

### PleasantEnrichmentTable — hourly aggregates

```python
class PleasantEnrichmentTable(EnrichmentTable):
    __tablename__ = "pleasant_enrichment"

    id            = Column(BigInteger, primary_key=True, autoincrement=True)
    connection_id = Column(String, nullable=False, index=True)
    hour          = Column(Integer, nullable=False)   # 0–23

    avg_temporal_mean_speed      = Column(Float)
    avg_spatial_mean_speed       = Column(Float)
    avg_naive_mean_speed         = Column(Float)
    avg_speed_performance_index  = Column(Float)
    sample_count  = Column(Integer)
    geometry_25833 = Column(Geometry("GEOMETRY", srid=25833))

    __table_args__ = (
        UniqueConstraint("connection_id", "hour"),
        Index(None, "geometry_25833", postgresql_using="gist"),
    )
```

The enrichment table holds ~1 million rows (one per `(connection_id, hour)` pair), down from ~4 million in staging. The geometry is retained here so the mapping `LEFT JOIN LATERAL` can do spatial proximity queries on this table instead of the larger staging table.

---

## Mapper class

### `read_file_content` — two-parquet join

```python
def read_file_content(self, path: str) -> list[dict]:
    # 1. Read metrics parquet
    agg_df = pd.read_parquet(path)   # aggregated_metrics.parquet

    # 2. Read lane geometry parquet (sibling file)
    lanes_path = Path(path).parent / "berlin_lanes.parquet"
    lanes_df = self._read_lanes_dataframe(lanes_path)

    # 3. Normalise connection IDs (strip ":" prefix, replace "_" with "#")
    agg_df["connection_id_norm"] = agg_df["connectionID"].apply(self._normalize_connection_id)
    lanes_df["edge_id_norm"] = lanes_df["edge_id"].apply(self._normalize_connection_id)

    # 4. Deduplicate lanes (one geometry per edge)
    lanes_df = lanes_df.groupby("edge_id_norm").agg(
        lane_id=("lane_id", self._first_not_null),
        geometry_wkt=("geometry_wkt", self._first_not_null),
    )

    # 5. Left-join metrics onto lanes
    merged = agg_df.merge(lanes_df, left_on="connection_id_norm",
                          right_on="edge_id_norm", how="left", indicator=True)

    # 6. Build records, convert WKT geometry → EWKT 25833, NaN → None
    records = [...]
    return self._deduplicate_conflict_keys(records)
```

**Why read a sibling file?**  
The metrics parquet (`aggregated_metrics.parquet`) has speed data but no geometry. The geometry lives in `berlin_lanes.parquet` in the same directory. Both files must be read and joined in Python because they don't share a database table.

**Connection ID normalisation**  
The two files use slightly different ID formats (some have a leading `:` and some use `_` where the other uses `#`). `_normalize_connection_id` strips the leading colon and replaces underscores. Without normalisation, the join produces almost no matches.

**`_deduplicate_conflict_keys`**  
After the join some `(connection_id, interval_start, interval_end)` triples appear more than once (from duplicate lane entries). The dedup keeps the first row but promotes non-null geometry/lane values from later duplicates.

---

### `sync_staging_to_enrichment` — skip the default copy

```python
def sync_staging_to_enrichment(self):
    self.logger.info("Skipping default staging→enrichment sync; "
                     "hourly aggregation will run in execute_on_enrichment.")
```

The default sync copies all staging rows verbatim into enrichment. For this datasource that would produce another 4M-row copy. By overriding with a no-op, the default copy is skipped entirely. The enrichment table is populated by `execute_on_enrichment()` instead.

---

### `execute_on_enrichment` — hourly aggregation SQL

```python
def execute_on_enrichment(self):
    ns_per_hour = 3_600_000_000_000  # 1 hour in nanoseconds

    insert_sql = f"""
        INSERT INTO "{enrich_schema}"."{enrich_table}" (
            connection_id, hour,
            avg_temporal_mean_speed, avg_spatial_mean_speed,
            avg_naive_mean_speed, avg_speed_performance_index,
            sample_count, geometry_25833, ...
        )
        SELECT
            connection_id,
            (interval_start / {ns_per_hour})::int          AS hour,
            AVG(avg_temporal_mean_speed),
            AVG(avg_spatial_mean_speed),
            AVG(avg_naive_mean_speed),
            AVG(avg_speed_performance_index),
            SUM(sample_count),
            MAX(geometry_25833),
            ...
        FROM "{staging_schema}"."{staging_table}"
        WHERE connection_id IS NOT NULL AND interval_start IS NOT NULL
        GROUP BY connection_id, (interval_start / {ns_per_hour})::int
        ON CONFLICT (connection_id, hour) DO UPDATE SET ...
    """

    self.db.call_sql(truncate_sql)   # truncate first (full replace, not incremental)
    self.db.call_sql(insert_sql)
```

`(interval_start / ns_per_hour)::int` converts nanosecond timestamps to hour-of-day (0–23) in one expression. `MAX(geometry_25833)` is safe as an aggregation key because geometry is identical for all 15-minute slots of the same connection in the same hour.

---

### `mapping_db_query` — LEFT JOIN LATERAL

```python
def mapping_db_query(self) -> str:
    return f"""
        INSERT INTO "{map_schema}"."{map_table}" (way_id, connection_id, distance_m)
        SELECT
            b.id,
            e.connection_id,
            ST_Distance(b.geometry_25833, e.geometry_25833) AS distance_m
        FROM "{base_schema}"."{base_table}" b
        LEFT JOIN LATERAL (
            SELECT e2.connection_id, e2.geometry_25833
            FROM "{enrich_schema}"."{enrich_table}" e2
            WHERE ST_DWithin(b.geometry_25833, e2.geometry_25833, 5)
            ORDER BY b.geometry_25833 <-> e2.geometry_25833
            LIMIT 1
        ) e ON TRUE
        ON CONFLICT (way_id)
        DO UPDATE SET connection_id = EXCLUDED.connection_id,
                      distance_m    = EXCLUDED.distance_m
    """
```

**`LEFT JOIN LATERAL`**  
A plain `JOIN` would drop road segments that have no nearby connection. `LEFT JOIN LATERAL ... ON TRUE` guarantees one output row per `ways_base` row — segments without a match get `connection_id = NULL, distance_m = NULL`. The MV and Java scorer both treat `NULL` as "no data".

**`ST_DWithin(..., 5)` + `<->` order**  
The `ST_DWithin` filter limits the lateral search to a 5 m radius before the KNN `<->` sort. This prevents the lateral from sorting the entire enrichment table for roads with no nearby connections — it hits an empty set quickly and moves on.

**The enrichment table has 24 rows per `connection_id`** (one per hour). `LIMIT 1` is fine here because all 24 rows share the same geometry — we only need the `connection_id`, not the per-hour data. The MV later joins back through `connection_id + hour` to get the full hourly array.

---

## How the stages connect

```
aggregated_metrics.parquet
berlin_lanes.parquet (sibling)
        │
        ▼  read_file_content()
        │  → left-join metrics onto lanes
        │  → WKT → EWKT 25833, NaN → None, dedup
        ▼
  pleasant_staging        (4M rows: one per connection_id × 15-min slot)
        │  sync_staging_to_enrichment() → no-op (skip default copy)
        │  execute_on_enrichment() → GROUP BY connection_id, hour
        ▼
  pleasant_enrichment     (~1M rows: one per connection_id × hour)
        │  mapping_db_query() → LEFT JOIN LATERAL within 5 m
        ▼
  pleasant_mapping        (one row per road segment, connection_id or NULL)
        │
        ▼
  mv_pleasant             (24-element SPI array per road segment)
```

---

## Key patterns to reuse

| Pattern | Where | When to use |
|---------|-------|-------------|
| Join two parquet files in `read_file_content` | `_read_lanes_dataframe` + merge | Source data is split across two files that must be combined before staging |
| Skip default staging→enrichment copy | `sync_staging_to_enrichment()` no-op | You want to aggregate staging into enrichment, not copy verbatim |
| Custom aggregation in `execute_on_enrichment` | hourly GROUP BY SQL | Enrichment should be a rollup (hourly, daily) of staging rows |
| `LEFT JOIN LATERAL` for guaranteed coverage | `mapping_db_query()` | Every base table row must appear in mapping, even unmatched ones |
| `ST_DWithin` filter before KNN sort | `WHERE ST_DWithin(..., 5)` | Prevents the lateral from sorting the full enrichment table on every row |
