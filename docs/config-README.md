# Configuration README

This document explains the structure of [config.yaml](/Users/krutarthparwal/Documents/mdp/modular-data-pipeline/config.yaml), what each option means, and which related options the code can also accept.

## Purpose

`config.yaml` drives the full pipeline:

1. API server startup
2. runtime reload behavior
3. scheduler behavior
4. database connectivity
5. graph ingestion
6. datasource ETL jobs
7. mapping into the base graph
8. materialized view refreshes

The runtime entry point is [core/application.py](/Users/krutarthparwal/Documents/mdp/modular-data-pipeline/core/application.py), and the config is loaded through [main_core/core_config.py](/Users/krutarthparwal/Documents/mdp/modular-data-pipeline/main_core/core_config.py).

## Top-level sections

The current file contains these top-level keys:

1. `server`
2. `runtime`
3. `materialized_views`
4. `scheduler`
5. `data_folder`
6. `env_variables`
7. `database`
8. `metadata-datasource`
9. `base`
10. `graph`
11. `datasources`

## Section-by-section reference

### `server`

Controls the optional FastAPI debug server.

| Key | Meaning | Current example | Other supported values |
|---|---|---|---|
| `app_type` | Uvicorn import target | `core.main:app` | Any valid `module:app` string |
| `enable` | Starts or skips the API server | `false` | `true`, `false` |
| `name` | Label only | `fastAPI` | Any string |
| `description` | Human-readable note | text | Any string |
| `host` | Bind address | `0.0.0.0` | Any valid host |
| `port` | Bind port | `8000` | Any free TCP port |
| `reload` | Uvicorn auto-reload | `true` | `true`, `false` |

### `runtime`

Controls process behavior outside the ETL itself.

| Key | Meaning | Current example | Other supported values |
|---|---|---|---|
| `config_watch.enable` | Watches `config.yaml` and restarts process when it changes | `true` | `true`, `false` |
| `config_watch.poll_seconds` | Poll interval for config reload detection | `2` | Any positive number |

### `materialized_views`

Controls post-datasource materialized view refresh.

| Key | Meaning | Current example | Other supported values |
|---|---|---|---|
| `enable` | Global toggle for MV orchestration | `true` | `true`, `false` |
| `views` | List of materialized view configs | one weather MV | Any list of view definitions |

Per-view options:

| Key | Meaning | Current example | Other supported values |
|---|---|---|---|
| `id` | Stable identifier | `mv_weather` | Any string |
| `enable` | Per-view toggle | `true` | `true`, `false` |
| `schema` | View schema | `test_osm_base_graph` | Any DB schema |
| `name` | View name | `mv_weather` | Any DB object name |
| `handler_class` | Handler class name | `WeatherMaterializedViewHandler` | Any handler importable from the configured module |
| `handler_module` | Python module containing the handler | omitted | Defaults to `materialized_views.handlers`, but can be any module path |
| `depends_on.datasources` | Refresh when these datasources succeed | weather datasources | Any datasource names |
| `depends_on.tables` | Informational table dependency list | weather/mapping/base tables | Any table names |
| `refresh.enabled` | Runs refresh after ensure/create | `true` | `true`, `false` |
| `refresh.mode` | Refresh mode | `normal` | `normal`, `concurrently` |
| `refresh.with_data` | Creates or refreshes with data | `true` | `true`, `false` |
| `custom_sql.create` | Full custom CREATE MATERIALIZED VIEW SQL | omitted | Any SQL string |
| `custom_sql.refresh` | Full custom REFRESH SQL | omitted | Any SQL string |
| `select_sql` | Generic `SELECT` used to build the MV | omitted | Any SQL select |
| `mapping_table` | Weather handler mapping table | `dwd_station_locations_mapping` | Handler-specific |
| `weather_table` | Weather handler enrichment table | `weather_enrichment` | Handler-specific |
| `ways_table` | Base graph table for weather handler | `ways_base` | Handler-specific |
| `timestamp_filter` | Fixed timestamp snapshot | `2026-02-24 16:00:00+00` | Timestamp string or `null` |
| `indexes` | Additional indexes for the MV | one `way_id` index | Any handler-supported index list |

### `scheduler`

Controls APScheduler setup.

| Key | Meaning | Current example | Other supported values |
|---|---|---|---|
| `name` | Label only | `APSScheduler` | Any string |
| `enable` | Starts scheduler or not | `false` | `true`, `false` |
| `description` | Human-readable note | text | Any string |
| `timezone` | Scheduler timezone | `Europe/Berlin` | Any valid timezone |
| `scheduler_type` | Stored in config | `BackgroundScheduler` | Current code effectively uses `BackgroundScheduler` |
| `wait_before_shutdown` | APScheduler shutdown wait flag | `false` | `true`, `false` |

### `data_folder`

| Key | Meaning | Current example | Other supported values |
|---|---|---|---|
| `data_folder` | Path containing datasource config fragments or source files | `./data_source_configs/` | Any path |

### `env_variables`

Shared constants loaded into `GlobalConstants`.

| Key | Meaning | Current example | Other supported values |
|---|---|---|---|
| `base_schema` | Shared schema constant | `test_osm_base_graph` | Any scalar |
| `base_table` | Shared base table constant | `ways_base` | Any scalar |
| `...` | Additional shared values | none | Any scalar or string |

These values are useful for YAML anchors and for mapper code that reads global constants.

### `database`

Database connectivity used by the pipeline and metadata services.

| Key | Meaning | Current example | Other supported values |
|---|---|---|---|
| `description` | Human-readable note | text | Any string |
| `enable` | Enables DB-backed flow | `true` | `true`, `false` |
| `driver` | SQLAlchemy driver | `postgresql+psycopg` | Any installed SQLAlchemy driver |
| `url` | Database host | `localhost` | Hostname or IP |
| `port` | Database port | `5432` | Any DB port |
| `database_name` | Database name | `test` | Any database name |
| `database_schema` | Default schema | `test_osm_base_graph` | Any schema |
| `credential.username` | Database user | `postgres` | Any username |
| `credential.password` | Database password | `admin123` | Any password |

Environment overrides also exist:

1. `DB_HOST`
2. `DB_PORT`
3. `DB_NAME`
4. `DB_USER`
5. `DB_PASSWORD`

### `metadata-datasource`

Controls the datasource metadata table.

| Key | Meaning | Current example | Other supported values |
|---|---|---|---|
| `description` | Human-readable note | text | Any string |
| `table_schema` | Schema for metadata tables | `test_osm_base_graph` | Any schema |

### `base`

Controls the base graph table used during mapping.

| Key | Meaning | Current example | Other supported values |
|---|---|---|---|
| `table_name` | Base ways table | `ways_base` | Any table name |
| `table_schema` | Base table schema | `test_osm_base_graph` | Any schema |
| `force_generate` | Recreate the base table | `false` | `true`, `false` |

### `graph`

Controls graph download and base graph preparation.

| Key | Meaning | Current example | Other supported values |
|---|---|---|---|
| `enable` | Enables graph pipeline | `true` | `true`, `false` |
| `tool` | Graph ingestion mode | `external_ingest` | `terminal`, `custom`, `external_ingest` |
| `schema` | Graph schema | `test_osm_base_graph` | Any schema |
| `table_name` | Graph table name | `way_segment` | Any table |
| `osm_file_path` | OSM file location | `./tmp/osm_graph/berlin.osm.pbf` | Any file path |
| `cmd` | External command parts for terminal mode | `osm2pgrouting ...` | Any command list |
| `env` | Extra environment values | `pass: test` | Any key/value map |
| `datasource` | Datasource definitions used to obtain graph input | `osm_graph` | Same shape as normal datasource entries |
| `communication.enable` | Enables coordination task table | `false` | `true`, `false` |
| `communication.tasks` | Predefined communication tasks | several task rows | Any task list |
| `communication.waits.router_coupled` | Optional wait on a router-owned task | configured | Optional wait config |
| `communication.waits.main_ways_before_base` | Optional wait before building base graph | configured | Optional wait config |

Wait object options:

| Key | Meaning | Other supported values |
|---|---|---|
| `enable` | Enables the wait rule | `true`, `false` |
| `task_key` | Task row to watch | Any task key |
| `poll_seconds` | Wait poll interval | Any positive number |
| `timeout_seconds` | Hard timeout | number or `null` |
| `require_is_completed` | Requires completion flag | `true`, `false` |

## `datasources`

Each item in `datasources` maps to the `DataSourceDTO` structure and is executed by [main_core/data_source_mapper.py](/Users/krutarthparwal/Documents/mdp/modular-data-pipeline/main_core/data_source_mapper.py).

Shared datasource fields:

| Key | Meaning | Other supported values |
|---|---|---|
| `name` | Unique datasource identifier | Any unique string |
| `description` | Human-readable note | Any string |
| `enable` | Enables execution | `true`, `false` |
| `class_name` | Mapper class stem | Must resolve to `data_mappers/{class_name}Mapper.py` |
| `data_type` | Informational type | Usually `static` or `dynamic` |
| `debug.endpoint` | Debug API endpoint suffix | Any string |
| `source` | Extraction configuration | See below |
| `job` | Scheduler configuration | See below |
| `mapping` | Mapping-to-base configuration | See below |
| `storage` | Persistence configuration | See below |
| `pre_filter_processing` | Optional save hook before filtering | Optional block |
| `post_filter_processing` | Optional save hook after filtering | Optional block |
| `pre_database_processing` | Optional DB pre-hook config field | Optional block |
| `pro_database_processing` | Optional field in DTO | Optional block |
| `cleanup_processing` | Optional cleanup config field | Optional block |

### `source`

| Key | Meaning | Current examples | Other supported values |
|---|---|---|---|
| `fetch` | Source backend | `http`, `local` | `http`, `https`, `local` |
| `mode` | Single file or expanded set | `single`, `multi` | `single`, `multi` |
| `url` | Remote URL | brightsky, WFS, TU Berlin URLs | Any URL |
| `file_path` | Local input path | parquet path | Any local path |
| `stream` | Streaming download | `true` | `true`, `false` |
| `save_local` | Save remote file locally | `true` | `true`, `false` |
| `destination` | Output path / saved file base | various `tmp/...` paths | Any file path |
| `response_type` | File type hint | `json`, `pbf`, `xml`, `gpkg`, `zip`, `csv`, `json.gz` | Any extension token understood by handlers/readers |
| `headers` | HTTP headers | rarely used | Any header map |
| `params` | HTTP query params | WFS params, station ids | Any parameter map |
| `check_metadata.enable` | Uses metadata change detection | mostly `true` | `true`, `false` |
| `check_metadata.keys` | Metadata keys compared between runs | `last_modified`, `content_type`, `content_length` | Any metadata/header keys |
| `multi_fetch` | Expansion strategy for multiple inputs | configured for several datasources | Optional block |

### `source.multi_fetch`

| Key | Meaning | Other supported values |
|---|---|---|
| `enable` | Enables multi-fetch logic | `true`, `false` |
| `strategy` | Expansion strategy | `expand_params`, `url_template`, `explicit_url_list` |
| `expand` | Lists that will be cartesian-expanded into request params | Any map of lists |
| `params` | Constant params merged into every request | Any map |
| `url_template` | Python format string for URLs or paths | Any `str.format` template |
| `template_params` | Lists interpolated into `url_template` | Any map of equal-length lists |
| `urls` | Explicit list of URLs or a file input descriptor | list of strings or `input` object |

Supported strategies in code:

1. `expand_params`
2. `url_template`
3. `explicit_url_list`

### `job`

| Key | Meaning | Other supported values |
|---|---|---|
| `name` | APScheduler job name | Any string |
| `id` | APScheduler job id | Any string |
| `executor` | Process executor toggle | `process` or omitted |
| `trigger.type.name` | Trigger kind | `interval`, `date`, `cron`, `calendar_interval`, `run_once` |
| `trigger.type.start_date` | Start date or run date | Any parseable datetime |
| `trigger.type.end_date` | Optional DTO field | Any parseable datetime |
| `trigger.type.config` | Trigger-specific kwargs | Any valid APScheduler kwargs for the chosen trigger |
| `replace_existing` | Replace existing job id | `true`, `false` |
| `coalesce` | Coalesce missed runs | `true`, `false` |
| `max_instances` | Max concurrent runs | integer `>= 1` |
| `next_run_time` | Informational in current code path | Any string |

### `mapping`

| Key | Meaning | Other supported values |
|---|---|---|
| `enable` | Enables base mapping | `true`, `false` |
| `joins_on` | Legacy join key name | Any column name |
| `table_name` | Mapping table name | Any table |
| `table_schema` | Mapping table schema | Any schema |
| `base_table.table_name` | Target base table | Any table |
| `base_table.table_schema` | Target base schema | Any schema |
| `base_table.column_name` | Base table column to write/use | Any column |
| `base_table.column_type` | Semantic type hint | Any string |
| `strategy.name` | Mapping strategy | `mapper_sql`, `sql_template`, `none` |
| `strategy.type` | Optional strategy variant | Any string |
| `strategy.link_on.mapping_column` | Mapping-side join column | Any column |
| `strategy.link_on.base_column` | Base-side join column | Any column |
| `strategy.link_on.basis` | Join basis metadata | Any string |
| `config.sql` | SQL template used by `sql_template` strategy | Any SQL string |

Built-in runtime strategies:

1. `mapper_sql`
2. `sql_template`
3. `none`

### `storage`

| Key | Meaning | Other supported values |
|---|---|---|
| `persistent` | Persists transformed data to DB | `true`, `false` |
| `force_create` | Recreate tables before use | `true`, `false` |
| `expires_after` | Retention hint | Any duration-like string |
| `staging.table_name` | Staging table | Any table |
| `staging.table_schema` | Staging schema | Any schema |
| `staging.table_class` | SQLAlchemy class name | Any mapper table class |
| `staging.persistent` | Optional DTO flag | `true`, `false` |
| `enrichment.table_name` | Enrichment table | Any table |
| `enrichment.table_schema` | Enrichment schema | Any schema |
| `enrichment.table_class` | SQLAlchemy class name | Any mapper table class |
| `enrichment.persistent` | Optional DTO flag | `true`, `false` |

## Current datasource inventory

Current datasource entries in [config.yaml](/Users/krutarthparwal/Documents/mdp/modular-data-pipeline/config.yaml):

1. `graph.datasource[0]`: `osm_graph`
2. `datasources[0]`: `weather_station_bright_sky`
3. `datasources[1]`: `weather_forecast_bright_sky`
4. `datasources[2]`: `air_quality_data_download`
5. `datasources[3]`: `elevation_grids`
6. `datasources[4]`: `elevation`
7. `datasources[5]`: `elevation_python`
8. `datasources[6]`: `tree_wfs_capabilities`
9. `datasources[7]`: `tree`
10. `datasources[8]`: `pleasant_bicycling`

Currently enabled:

1. `graph.enable: true`
2. `datasources[8].enable: true`

Most other datasources are present as ready-to-use templates but are disabled.

## What else can be configured

These are the main extension points the current code already supports, even if not all are used in the current file:

1. `source.fetch: https`
2. `source.mode: multi` with `expand_params`, `url_template`, or `explicit_url_list`
3. `job.trigger.type.name: cron`, `date`, `calendar_interval`, `run_once`
4. `mapping.strategy.name: sql_template`
5. custom mapping strategies via mapper override
6. custom materialized view handlers via `handler_module` and `handler_class`
7. local-file datasources by setting `fetch: local` and `file_path`
8. config-reload behavior via `runtime.config_watch`
9. DB environment overrides via `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`

## Practical caveats in the current code

1. Prefer `headers`, not `header`. The DTO expects `headers`, while some current config entries still use `header`.
2. In `source.mode: single`, the current implementation only calls `fetch()` when `check_metadata.enable` is `true`.
3. Hyphenated keys like `pre-filter-processing` do not map cleanly to the DTO snake_case names.
4. Invalid `job.trigger.type.name` values fail only at runtime when the trigger class is resolved.

## Minimal datasource template

```yaml
datasources:
  - name: example_source
    description: Example datasource
    enable: true
    class_name: example
    data_type: static
    debug:
      endpoint: example
    source:
      fetch: http
      mode: single
      check_metadata:
        enable: true
        keys: ["last_modified"]
      url: "https://example.com/data.json"
      stream: true
      save_local: true
      destination: "tmp/example/data.json"
      response_type: json
      headers:
        Accept: "application/json"
      params: {}
    job:
      name: exampleJob
      id: exampleJob
      trigger:
        type:
          name: interval
          config:
            minutes: 30
      replace_existing: true
      coalesce: true
      max_instances: 1
      next_run_time: none
    mapping:
      enable: false
      joins_on: id
      table_name: example_mapping
      table_schema: test_osm_base_graph
      base_table:
        table_name: ways_base
        table_schema: test_osm_base_graph
        column_name: example_score
        column_type: Float
    storage:
      force_create: false
      persistent: true
      expires_after: 6h
      staging:
        table_name: example_staging
        table_schema: test_osm_base_graph
        table_class: ExampleStagingTable
      enrichment:
        table_name: example_enrichment
        table_schema: test_osm_base_graph
        table_class: ExampleEnrichmentTable
```
