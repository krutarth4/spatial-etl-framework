# Example: Weather Forecast Mapper + Config

**Mapper:** [`data_mappers/weatherMapper.py`](../data_mappers/weatherMapper.py)  
**Config:** [`data_source_configs/weather_forecast_bright_sky.yaml`](../data_source_configs/weather_forecast_bright_sky.yaml)

This datasource fetches hourly weather forecasts from the Bright Sky API (DWD data) for eight Berlin stations. The per-station responses are flattened into individual hourly records and stored in staging/enrichment. There is no per-way mapping — the forecast data is consumed indirectly via a materialized view that joins through the station mapping. It demonstrates:

- `source_filter()` to flatten a nested per-station JSON response
- `multi_fetch` with a dynamically evaluated `date` parameter (ISO 8601 from `datetime.now()`)
- Keeping staging non-persistent (`staging.persistent: false`) while enrichment persists
- A materialized view that joins across two datasources and builds hourly float arrays with `LEFT JOIN LATERAL`

---

## Table models

```python
class WeatherStagingTable(StagingTable):
    __tablename__ = "weather_staging"

    uid            = Column(Integer, primary_key=True, autoincrement=True)
    source_id      = Column(Integer, nullable=False)
    dwd_station_id = Column(Integer, nullable=False)
    timestamp      = Column(TIMESTAMP(timezone=True), nullable=False)
    temperature    = Column(Float, nullable=False)
    pressure_msl   = Column(Float, nullable=False)
    dew_point      = Column(Float, nullable=False)
    cloud_cover    = Column(Float, nullable=False)
    wind_speed     = Column(Float)
    wind_direction = Column(Float)
    precipitation  = Column(Float, nullable=False)
    sunshine       = Column(Float, nullable=False)

    __table_args__ = (UniqueConstraint("dwd_station_id", "timestamp"),)


class WeatherEnrichmentTable(EnrichmentTable):
    __tablename__ = "weather_enrichment"

    uid            = Column(Integer, primary_key=True, autoincrement=True, index=True)
    dwd_station_id = Column(Integer, nullable=False)
    timestamp      = Column(TIMESTAMP(timezone=True), nullable=False)
    visibility     = Column(Float)
    conditions     = Column(String)
    wind_speed     = Column(Float)
    wind_direction = Column(Float)

    __table_args__ = (UniqueConstraint("dwd_station_id", "timestamp"),)
```

The enrichment table keeps only the columns needed by the MV (`wind_speed`, `wind_direction`, `visibility`, `conditions`) — not the full staging schema. The default staging→enrichment sync copies columns by matching name; columns that exist in staging but not in enrichment are silently dropped.

---

## Mapper class — `source_filter`

```python
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
                enriched["dwd_station_id"] = dwd_station_id   # inject station id
                result.append(enriched)

        return result
```

The Bright Sky API returns one JSON object per station request:
```json
{
  "sources": [{"dwd_station_id": "00399", ...}],
  "weather": [{"timestamp": "...", "wind_speed": 3.2, ...}, ...]
}
```

`data` is a list of these objects (one per station, from the multi-fetch). `source_filter` iterates the list, extracts `dwd_station_id` from `sources[0]`, then flattens `weather` into individual records — each one augmented with `dwd_station_id` so the staging table can link records back to a station.

---

## Config

### Source — multi_fetch with dynamic date

```yaml
source:
  fetch: http
  mode: multi
  url: https://api.brightsky.dev/weather
  multi_fetch:
    enable: true
    strategy: expand_params
    expand:
      dwd_station_id: ["00399", "00403", "00400", "00410", "00420", "00427", "00430", "00433"]
    params:
      date: >
        ${{
        datetime.now(ZoneInfo("Europe/Berlin")).isoformat()
        }}
```

**`expand_params` strategy**  
One request is fired per value in `expand.dwd_station_id`, injecting `dwd_station_id=<value>` as a query parameter alongside the shared `date` param. The eight requests run in parallel.

**Dynamic `date` parameter**  
The `${{ ... }}` syntax is a YAML expression that is evaluated at job runtime. `datetime.now(ZoneInfo("Europe/Berlin")).isoformat()` produces the current local ISO timestamp — so each run fetches the forecast starting from now, not from a fixed date.

### No mapping

```yaml
mapping:
  enable: false
```

Weather readings are per-station, not per-road-segment. The `weather_station_bright_sky` datasource maps each road to its nearest station; the MV then joins through that mapping to assign weather to each road. Separate mapping here would duplicate that work.

### Asymmetric persistence

```yaml
storage:
  persistent: true
  expires_after: 1d
  staging:
    table_name: weather_staging
    table_class: WeatherStagingTable
    persistent: false    # staging is truncated each run
  enrichment:
    table_name: weather_enrichment
    table_class: WeatherEnrichmentTable
    # persistent: true inherited from storage block
```

Staging is truncated each run (`persistent: false` at the table level). Enrichment is kept (`persistent: true` from the parent block). This means:
- Staging holds only the latest fetch (lean, fast inserts)
- Enrichment accumulates historical readings (via upsert on `(dwd_station_id, timestamp)`)
- `expires_after: 1d` forces a re-fetch after 24 hours regardless of metadata headers

### Materialized view — hourly wind arrays across two datasources

```yaml
materialized_view:
  name: mv_weather
  depends_on:
    datasources:
      - weather_forecast_bright_sky
      - weather_station_bright_sky
    tables:
      - { name: weather_enrichment }
      - { name: dwd_station_locations_mapping }
      - { name: ways_base }
  definition:
    select_sql: |
      WITH bounds AS (
          SELECT
              date_trunc('hour', MIN(timestamp)) AS forecast_start,
              date_trunc('hour', MAX(timestamp)) AS forecast_end
          FROM {schema}.weather_enrichment
      ),
      slots AS (
          SELECT gs AS slot_ts,
                 (EXTRACT(EPOCH FROM (gs - b.forecast_start)) / 3600)::int AS slot_idx
          FROM bounds b
          CROSS JOIN generate_series(b.forecast_start, b.forecast_end, INTERVAL '1 hour') AS gs
      )
      SELECT
          w.id, w.way_id, w.way_link_index,
          m.bearing_degree  AS bearing_deg,
          (SELECT forecast_start FROM bounds) AS forecast_start,
          (SELECT forecast_end   FROM bounds) AS forecast_end,
          agg.wind_speed_hourly,
          agg.wind_direction_hourly,
          agg.visibility_hourly
      FROM {schema}.ways_base w
      LEFT JOIN {schema}.dwd_station_locations_mapping m ON m.way_id = w.id
      LEFT JOIN LATERAL (
          SELECT
              array_agg(COALESCE(e.wind_speed, -1)     ORDER BY s.slot_idx) AS wind_speed_hourly,
              array_agg(COALESCE(e.wind_direction, -1) ORDER BY s.slot_idx) AS wind_direction_hourly,
              array_agg(COALESCE(e.visibility, -1)     ORDER BY s.slot_idx) AS visibility_hourly
          FROM slots s
          LEFT JOIN {schema}.weather_enrichment e
              ON e.dwd_station_id::INTEGER = m.dwd_station_id::INTEGER
             AND date_trunc('hour', e.timestamp) = s.slot_ts
      ) agg ON TRUE
```

**`depends_on.datasources`** lists both `weather_forecast_bright_sky` (this datasource) and `weather_station_bright_sky` (the station mapper). The MV only fires after both have completed successfully in the same cycle.

**`bounds` CTE**  
Computes the actual forecast window from `MIN/MAX(timestamp)` in the enrichment table. This adjusts automatically as the enrichment table is updated, without needing to hardcode the window length.

**`slots` CTE + `generate_series`**  
Expands the forecast window into discrete hourly timestamps. `slot_idx` is the integer offset from `forecast_start` — so index 0 = `forecast_start`, index 1 = `forecast_start + 1h`, etc.

**`LEFT JOIN LATERAL` for per-way arrays**  
For each road segment, the lateral subquery builds the three hourly arrays in one pass. Because the station mapping (`dwd_station_locations_mapping`) already assigned one `dwd_station_id` per road, the inner `LEFT JOIN weather_enrichment` filters to that station's readings for each slot.

`bearing_degree` (the road segment's travel direction) comes from the station mapping table — it was computed as `ST_Azimuth` on the road geometry when station mapping ran. The weather scorer uses it to compute headwind/tailwind.

---

## Key patterns to reuse

| Pattern | Where | When to use |
|---------|-------|-------------|
| Flatten per-station arrays in `source_filter` | `source_filter` loop | API returns one envelope per entity; you need one row per child record |
| Dynamic parameter evaluated at runtime | `${{ datetime.now(...) }}` | Parameter must reflect current time or state at fetch time, not config-write time |
| Keep enrichment, truncate staging | `staging.persistent: false` | Raw staging only needed as an import buffer; enrichment is the authoritative store |
| MV across two datasources | `depends_on.datasources` list | View joins data produced by different datasource runs |
| `LEFT JOIN LATERAL` for per-way time arrays | `agg` lateral in MV | Build a fixed-length array from a join with a time-series table |
