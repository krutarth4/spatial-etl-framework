# Step-by-Step Guide: Configuring a New Data Source

This guide walks through adding a new data source to the spatial-etl-framework end-to-end, using a real example from the MDP `config.yaml`: the **DWD Weather Stations** mapper (`weatherStationMapper`). It fetches weather-station metadata from Bright Sky, stores point geometries, and maps each base graph way to its nearest station.

By the end you will know where each piece lives, why it's there, and how to adapt the pattern to your own data source.

---

## Prerequisites

- Framework cloned at `spatial-etl-framework/`
- PostGIS running and the `database:` block in `config_test.yaml` points at it
- Base graph table already exists (`base.table_name` in config — e.g., `ways_base`)

---

## Use Case — What We Are Building

| Property | Value |
|---|---|
| Source | `https://api.brightsky.dev/sources` (DWD station catalog) |
| Fetch mode | Single HTTP request, JSON |
| Data shape | Array of stations with `lat`, `lon`, `dwd_station_id`, etc. |
| ETL stages | staging → enrichment (adds `POINT SRID:4326`) → mapping |
| Mapping | KNN — link each base way to the nearest station |
| Schedule | Every hour |

---

## Step 1 — Create the Mapper Class

Every data source is a subclass of `DataSourceABCImpl` in `data_mappers/`. The filename and class name must match the `class_name` in the YAML config (see [mapper-README.md](mapper-README.md)): `class_name: weatherStation` → `data_mappers/weatherStationMapper.py` → `class WeatherStationMapper`.

Create [data_mappers/weatherStationMapper.py](../data_mappers/weatherStationMapper.py):

```python
from main_core.data_source_abc_impl import DataSourceABCImpl


class WeatherStationMapper(DataSourceABCImpl):
    # (1) Filter the raw upstream response.
    # Brightsky returns { "sources": [ {...station...}, ... ] }.
    # We keep only forecast stations with a recent last_record.
    def source_filter(self, data: dict) -> list:
        stations = data.get("sources", [])
        return [
            s for s in stations
            if s.get("observation_type") == "forecast"
            and s.get("last_record", "")[:4] >= "2024"
        ]

    # (2) Add a PostGIS POINT column during the staging → enrichment copy.
    def enrichment_db_query(self) -> str:
        return """
        INSERT INTO {enrichment_schema}.{enrichment_table}
          (uid, dwd_station_id, station_name, wmo_station_id, point)
        SELECT
          uid,
          dwd_station_id,
          station_name,
          wmo_station_id,
          ST_SetSRID(ST_MakePoint(lon, lat), 4326) AS point
        FROM {staging_schema}.{staging_table}
        ON CONFLICT (dwd_station_id) DO NOTHING;
        """
```

**Which overrides do you actually need?** Only the ones that are dataset-specific.

| Override | When to use |
|---|---|
| `read_file_content(path)` | File parsing (CSV, GPKG, parquet, XYZ, XML…). Return a `list[dict]`. |
| `source_filter(data)` | Reshape nested JSON/XML responses before staging. |
| `staging_db_query()` | Custom raw → staging SQL. |
| `enrichment_db_query()` | Custom staging → enrichment SQL (geometry construction, joins). |
| `mapping_db_query()` | Only when `mapping.strategy.type: custom`. |

For JSON APIs where the response is already a flat list, you don't even need `read_file_content` — the framework's default reader handles it.

---

## Step 2 — Write the Per-Datasource YAML Config

The framework loads datasource configs from `data_folder` (set in `config_test.yaml` → `data_folder: ./data_source_configs/`). Each datasource is a single YAML document registered there.

Create `data_source_configs/weather_station.yaml`:

```yaml
datasources:
  - name: "weather_station_bright_sky"
    description: "DWD weather station catalog from Bright Sky"
    enable: true
    class_name: weatherStation            # → weatherStationMapper.py → WeatherStationMapper
    data_type: static

    # 2a. SOURCE — where the data comes from
    source:
      fetch: http
      mode: single
      check_metadata:
        enable: true
        keys: ["last_modified"]           # skip download when unchanged
      url: https://api.brightsky.dev/sources
      stream: true
      save_local: true
      destination: "tmp/dwd_stations/sources.json"
      response_type: json
      header:
        Accept: "application/json"

    # 2b. JOB — scheduling
    job:
      name: weatherStationJob
      id: weatherStationJob
      trigger:
        type:
          name: interval                  # interval | cron | date
          config:
            hours: 1
      replace_existing: true
      coalesce: true
      max_instances: 1

    # 2c. STORAGE — raw staging + enrichment tables
    storage:
      staging:
        table_name: dwd_station_locations_staging
        table_schema: test_osm_base_graph
        persistent: true
        expires_after: 24h
      enrichment:
        table_name: dwd_station_locations_enrichment
        table_schema: test_osm_base_graph

    # 2d. MAPPING — how rows attach to the base graph
    mapping:
      enable: true
      joins_on: dwd_station_id
      strategy:
        type: knn
        description: "Each base way links to its nearest station"
        link_on:
          mapping_column: dwd_station_id
          base_column: dwd_station_id
          basis: nearest_by_distance
      config:
        base_geometry_column: geometry
        enrichment_geometry_column: point
        distance_sql: ST_Distance({base_geometry}::geography, {enrichment_geometry}::geography)
        order_by_sql: ST_Distance({base_geometry}::geography, {enrichment_geometry}::geography)
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
      table_name: dwd_station_locations_mapping
      table_schema: test_osm_base_graph
```

### Why each block exists

| Block | Purpose |
|---|---|
| `source` | Download mechanics. Swap `fetch: http` → `local` for files on disk; add `multi_fetch:` with `strategy: expand_params \| url_template \| explicit_url_list` for batched fetches. |
| `job` | APScheduler trigger. Use `run_once: true` under `source` for one-shot seed loads. |
| `storage` | Two tables — `staging` (raw, typed) and `enrichment` (adds geometry / derived columns). Both auto-created. |
| `mapping` | Wires enrichment rows onto `ways_base`. Pick the strategy that matches your geometry relation — see the table below. |

### Picking a mapping strategy

| Strategy | Use when |
|---|---|
| `knn` (nearest_by_distance) | Point-to-line nearest match (stations, sensors) |
| `nearest_k` | Top-K nearest features per way |
| `within_distance` | All features within a buffer |
| `aggregate_within_distance` | Aggregate (count, avg) within buffer |
| `intersection` | Polygons / lines that cross the way |
| `attribute_join` | Pure ID join, no spatial logic |
| `sql_template` | Custom SQL with `{base}`/`{enrichment}` placeholders |
| `custom` | Fully hand-written SQL in `mapping_db_query()` |

Full reference: [mapping-strategies-reference.md](mapping-strategies-reference.md).

---

## Step 3 — Register the Datasource

The root `config_test.yaml` points at the folder (`data_folder: ./data_source_configs/`) — any YAML file dropped in there is auto-discovered on next run. No root-config edit is needed.

If you prefer to inline the datasource instead, append it under the root `datasources:` list (see [test.config.yaml](../test.config.yaml) for inlined examples).

---

## Step 4 — Run the Pipeline

```bash
cd spatial-etl-framework
python3 run.py
```

What you should see, in order:

1. `fetch: http` downloads `sources.json` to `tmp/dwd_stations/`.
2. `source_filter()` trims the list to active forecast stations.
3. Rows land in `test_osm_base_graph.dwd_station_locations_staging`.
4. `enrichment_db_query()` copies to `dwd_station_locations_enrichment` with `point GEOMETRY(POINT, 4326)`.
5. KNN mapping writes `(way_id, dwd_station_id, distance, bearing_degree)` into `dwd_station_locations_mapping`.
6. Scheduler queues the next run for one hour later.

Verify:

```sql
SELECT COUNT(*) FROM test_osm_base_graph.dwd_station_locations_enrichment;
SELECT way_id, dwd_station_id, distance, bearing_degree
FROM test_osm_base_graph.dwd_station_locations_mapping LIMIT 5;
```

---

## Step 5 — Iterate

Because `runtime.config_watch.enable: true`, saving any datasource YAML triggers a reload in ~2 s. Typical tweaks:

- **Wrong rows in staging** → fix `source_filter()` or `read_file_content()`.
- **Geometry missing / wrong SRID** → adjust the `ST_*` expression in `enrichment_db_query()`.
- **Too many / too few matches** → switch strategy (`knn` → `within_distance`) or change buffer radius.
- **Job fires too often** → change `job.trigger.type` to `cron` with `hour: "*/6"` etc.

---

## Checklist

- [ ] `data_mappers/<name>Mapper.py` created, class name matches config
- [ ] `data_source_configs/<name>.yaml` with `source`, `job`, `storage`, `mapping`
- [ ] Staging + enrichment schemas exist (or framework can create them)
- [ ] Strategy chosen + `link_on` / geometry columns set correctly
- [ ] Pipeline run, staging row count > 0, mapping row count > 0
- [ ] Base table (`ways_base`) has the expected new column if the mapping writes back to it

---

## Related Docs

- [config-README.md](config-README.md) — every top-level config key explained
- [config-reference.md](config-reference.md) — condensed key reference
- [mapper-README.md](mapper-README.md) — ABC lifecycle, hook ordering
- [mapping-strategies-reference.md](mapping-strategies-reference.md) — every strategy with examples
- [mapping-quick-reference.md](mapping-quick-reference.md) — cheat sheet
- [example-weather-station-simplified.md](example-weather-station-simplified.md) — same example, deeper simplification pass
