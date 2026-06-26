# Spatial ETL Framework

> A config-driven ETL pipeline for continuously enriching an OpenStreetMap / PostGIS road graph with real-world geospatial data.

[![License](https://img.shields.io/badge/License-GPLv3-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/Python-3.11%2B-3776AB?logo=python)](requirements.txt)
[![PostGIS](https://img.shields.io/badge/PostgreSQL-16%20%2B%20PostGIS-336791?logo=postgresql)](Dockerfile)

---

## What it is

Spatial ETL Framework pulls geospatial feeds (weather, air quality, tree locations, elevation, accident data, …), stages and cleans them in PostGIS, and spatially joins each dataset onto an OSM road graph — all declared in YAML. Each road segment ends up with enriched per-edge attributes that any downstream consumer (router, tile server, dashboard, Jupyter notebook) can query.

The pipeline handles scheduling, change detection, bulk ingestion, and materialized view refresh. You declare the source and pick a spatial-join strategy; the framework generates the PostGIS SQL for every stage.

---

## What it does

- **Fetch** from HTTP, WFS, or local files — single or multi-URL batch mode — with ETag / Last-Modified change detection to skip unchanged sources
- **Stage** raw records into PostGIS with configurable batch inserts (default 10 000 rows/batch), then clean with SQL hooks between raw → staging → enrichment
- **Spatially join** to the base road graph using built-in strategies: `knn`, `nearest_k`, `within_distance`, `aggregate_within_distance`, `intersection`, `attribute_join`, `sql_template`, or `custom`
- **Schedule** each datasource independently (cron or interval), hot-reload config edits in ~2 s without restart
- **Refresh** materialized views in topological dependency order after each successful run
- **Inspect** with a FastAPI debug server — `/debug/datasources`, `/debug/mappers/{name}`, GeoJSON mapping visualization at `/debug/mappers/{name}/mapping-visualization`

---

## Quick start

**Prerequisites:** Python 3.13+ and PostgreSQL 16 with PostGIS 3.4, or Docker.

```bash
# Start a PostGIS container
docker run --name postgres \
  -e POSTGRES_PASSWORD=admin123 -e POSTGRES_USER=postgres \
  -p 5432:5432 -d postgis/postgis:16-3.4

# Install and run
pip install -r requirements.txt
DB_HOST=localhost DB_USER=postgres DB_PASSWORD=admin123 DB_NAME=mydb python3 run.py
```

The pipeline reads `config.yaml`, schedules every enabled datasource, and exposes the debug API on `:8000`. Hot-reload picks up `config.yaml` changes in ~2 s.

To run inside Docker alongside PostGIS, see [Docker configuration](docs/docker-configuration.md).

---

## Adding your first datasource

Most datasources need only a YAML config file — no Python:

```yaml
# data_source_configs/my_sensor.yaml
name: my_sensor
enable: true
source:
  fetch: http
  url: "https://api.example.com/sensors.json"
  response_type: json
job:
  trigger:
    type:
      name: interval
      config:
        hours: 6
storage:
  staging:    {table_name: my_sensor_staging,    table_schema: myschema}
  enrichment: {table_name: my_sensor_enrichment, table_schema: myschema}
mapping:
  enable: true
  strategy: {type: knn}
  table_name: my_sensor_mapping
  table_schema: myschema
```

Drop this file in `data_source_configs/` — the framework picks it up automatically on the next reload. For non-standard source formats or custom SQL transforms, you can add a mapper class (a small Python file that overrides only the hooks you need).

→ Full walkthrough: **[Adding a new data source](docs/configure-data-source-step-by-step.md)**

---

## Repository layout

```
spatial-etl-framework/
├── run.py                        # Entry point + config watcher
├── config.yaml                   # Global config (datasources list auto-populated from data_source_configs/)
├── core/                         # FastAPI server, scheduler, debug API
├── main_core/                    # Base mapper class (DataSourceABCImpl) + config loader
├── data_mappers/                 # One Python file per datasource (optional; only if built-in reader isn't enough)
├── data_source_configs/          # Per-datasource YAML files (auto-discovered on startup)
├── database_tables/              # SQLAlchemy table model base classes (StagingTable, EnrichmentTable, MappingTable)
├── materialized_views/           # MV refresh orchestration
├── database/                     # DB connection pool + utilities
├── readers/                      # Format readers (CSV, JSON, GeoPackage, raster, …)
├── handlers/                     # HTTP / file download + ETag metadata checks
├── docs/                         # Reference documentation
└── Dockerfile
```

---

## Documentation

| Doc | What's in it |
|-----|--------------|
| [Getting started](docs/getting-started.md) | **Start here** — zero-to-running: PostGIS setup, first run, ways_base bootstrap, troubleshooting |
| [Adding a new data source](docs/configure-data-source-step-by-step.md) | Table models, YAML config, lifecycle hooks per ETL stage |
| [Mapper lifecycle reference](docs/mapper-README.md) | Every override method with signatures, order, and available `self.*` attributes |
| [Docker configuration](docs/docker-configuration.md) | Env vars, volumes, ports, run modes, compose example, Postgres tuning |
| [Config README](docs/config-README.md) | `config.yaml` top-level sections explained |
| [Config reference](docs/config-reference.md) | Full field-by-field YAML reference |
| [Mapping strategies reference](docs/mapping-strategies-reference.md) | All 8 spatial-join strategies with examples |
| [Mapping quick reference](docs/mapping-quick-reference.md) | One-page strategy cheat sheet |
| [Migration example: tree mapper](docs/migration-example-tree-mapper.md) | Real migration from custom SQL → built-in strategy |
| [Example: weather station (simple)](docs/example-weather-station-simplified.md) | Minimal end-to-end mapper skeleton |
| **Examples — mappers** | |
| [Tree mapper (Python)](docs/example-tree-mapper.md) | Custom `read_file_content`, JSONB staging, WKB geometry, custom MappingTable |
| [Air quality mapper](docs/example-air-quality-mapper.md) | gzip JSON, EWKT geometry, `ARRAY(Float)` columns, CRS transform in enrichment |
| [Elevation mapper](docs/example-elevation-mapper.md) | Override `load()`, XYZ → GeoTIFF, `ST_FromGDALRaster` + `ST_Tile`, raster dedup |
| [Elevation grid links mapper](docs/example-elevation-grid-links.md) | XML parsing in `source_filter`, `after_filter_hook` writes a file, no DB storage |
| [Pleasant bicycling mapper](docs/example-pleasant-bicycling-mapper.md) | Two-parquet join, skip default sync, custom hourly aggregation, `LEFT JOIN LATERAL` mapping |
| [Weather forecast mapper](docs/example-weather-forecast-mapper.md) | `source_filter` flattens nested JSON, dynamic date param, no mapping, per-way time-series MV |
| [Weather station mapper](docs/example-weather-station-mapper.md) | `source_filter` filter, `enrichment_db_query` lat/lon → geometry, `::geography` distance, `bearing_degree` |
| [Graph mapper](docs/example-graph-mapper.md) | Override `execute_run_pipeline()`, OSM file download, `CommService` inter-process signal |
| **Examples — configs** | |
| [Tree config (YAML)](docs/example-tree-config.md) | WFS `multi_fetch`, `aggregate_within_distance`, `enrichment_operators`, inline MV |
| [Air quality config](docs/example-air-quality-config.md) | `url_template` paged fetch, `idw` strategy, `enrichment_filter_sql`, forecast window CTE |
| [Elevation config](docs/example-elevation-config.md) | `depends_on`, `run_once` trigger, `raster_aggregate` operator, `sql_template` mapping |
| [Pleasant bicycling config](docs/example-pleasant-bicycling-config.md) | `fetch: local`, `strategy: custom`, no `expires_after`, `generate_series` hourly array MV |
| [Weather station config](docs/example-weather-station-mapper.md#config) | Multi-value `params`, KNN with `::geography`, `select_columns` for bearing |
| [Batch processing](docs/BATCH_PROCESSING.md) | Bulk-insert sizing and Postgres tuning |
| [Materialized views](docs/materialized-views-reference.md) | MV dependency chains and refresh modes |
| [Debug panel reference](docs/debug-panel-reference.md) | Debug API behavior and coverage calculation |
| [JSON styling](docs/json_styling.md) | JSONPath conventions for source configs |

---

## Tech stack

| Layer | Technology |
|-------|-----------|
| Runtime | Python 3.13, FastAPI, APScheduler |
| Data access | SQLAlchemy, psycopg 3 (binary) |
| Database | PostgreSQL 16 + PostGIS 3 |
| Geospatial readers | GeoPandas, pyogrio, rasterio, GDAL |
| OSM ingestion | osm2pgsql, osmium |

---

## Contributing

- Add a datasource mapper for your city or a new open data feed.
- Add a spatial-join strategy in `main_core/`.
- Improve format readers in `readers/` for additional source types.
- Expand `docs/` with real migration examples from your dataset.

All contributors are expected to follow the project's [Code of Conduct](CODE_OF_CONDUCT.md). Report unacceptable behavior to **krutarthparwal.ai@gmail.com**.
