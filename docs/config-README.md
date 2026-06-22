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

Controls post-datasource materialized view refresh. In `config.yaml` this section
only holds the global toggle and the (now optional) folder pointer:

| Key | Meaning | Current example | Other supported values |
|---|---|---|---|
| `enable` | Global toggle for MV orchestration | `true` | `true`, `false` |
| `mv_folder` | Optional folder of standalone MV files (empty by default) | `./mv_configs/` | Any path |

Each materialized view is now defined **inline inside the datasource it belongs
to**, under a `materialized_view:` key in that datasource's config file (see
`data_source_configs/`). At load time `CoreConfig._merge_embedded_mv_configs()`
collects every inline block — plus any standalone file in `mv_folder` — into the
`materialized_views.views` list the manager consumes. Shared boilerplate (schema,
handler, build, refresh, `only_on_data_change`) is filled from `mv_defaults` in
`config.yaml`, and the firing/dependency datasource name is auto-filled from the
host datasource.

Per-view options (declared under `materialized_view:`):

| Key | Meaning | Current example | Other supported values |
|---|---|---|---|
| `name` | View name (required) | `mv_weather` | Any DB object name |
| `id` | Stable identifier | defaults to `<schema>.<name>` | Any string |
| `enable` | Per-view toggle | `true` | `true`, `false` |
| `schema` | View schema | filled from `mv_defaults.schema` | Any DB schema |
| `handler.class` / `handler.module` | Handler selection | `GenericMaterializedViewHandler` | Any importable handler |
| `triggers.on_datasource_success` | Refresh when these datasources succeed | auto-filled from host datasource | Any datasource names |
| `depends_on.datasources` | Dependency datasources | auto-filled from host datasource | Any datasource names |
| `depends_on.tables` | Dependency table list (creation is skipped until they exist) | base/mapping/enrichment tables | Any `{name}` entries |
| `refresh.enabled` / `refresh.mode` / `refresh.with_data` | Refresh behavior | `concurrently` (or `normal` without a unique index) | `normal`, `concurrently` |
| `definition.select_sql` | `SELECT` the generic handler wraps into the MV | per datasource | Any SQL select |
| `definition.custom_sql.{create,refresh}` | Full custom DDL run verbatim | omitted | Any SQL string |
| `indexes` | Indexes for the MV (re-asserted on every refresh) | `id` + `way_id` indexes | `{ name, columns, unique, method, where }` |

See [materialized-views-reference.md](materialized-views-reference.md) for the
complete field-by-field schema and handler-selection guidance.

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
| `data_folder` | Directory of per-datasource config files, one datasource per `*.yaml` | `./data_source_configs/` | Any path |

At startup `CoreConfig._load_datasource_configs()` loads every `*.yaml` in this
folder and appends it to `datasources` (after any inline entries). Files are read
through `YamlReader`, so per-file `${{ ... }}` Python blocks and `tmp/...` path
resolution work exactly like the main config. Duplicate `name`s are ignored with
a warning. This is the canonical place to add datasources — `config.yaml` itself
now keeps only the global sections and an empty `datasources: []`.

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

Each datasource lives in its own file under `data_folder` (`./data_source_configs/<name>.yaml`)
and maps to the `DataSourceDTO` structure, executed by [main_core/data_source_mapper.py](/Users/krutarthparwal/Documents/mdp/modular-data-pipeline/main_core/data_source_mapper.py).

**Defaults filled by `CoreConfig._apply_datasource_defaults()`** — a per-file config
only needs to declare what is distinctive; these are filled when omitted:

- `storage.staging.table_schema` / `storage.enrichment.table_schema` → `env_variables.base_schema`
- `storage.{staging,enrichment}.table_name` → `<name>_<stage>` convention (only when omitted)
- `mapping.base_table.table_name` / `.table_schema` → the global `base` block
- `mapping.table_schema` / `incremental` → `mapping_defaults`
- `job.name`/`id` → `<name>Job`; `job.replace_existing`/`coalesce`/`max_instances`/`next_run_time` → `true`/`true`/`1`/`none`

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
| `strategy.type` | Mapping strategy type | `custom`, `sql_template`, `none`, `nearest_neighbour`, `within_distance`, `intersection`, `knn` |
| `strategy.description` | Human-readable mapping note | Any string |
| `strategy.link_on.mapping_column` | Mapping-side join column | Any column |
| `strategy.link_on.base_column` | Base-side join column | Any column |
| `strategy.link_on.basis` | Join basis metadata | Any string |
| `config.sql` | SQL template used by `sql_template` type | Any SQL string |
| `config.k` | Number of nearest neighbors (for `nearest_k`) | Any positive integer |
| `config.max_distance` | Maximum distance threshold (for distance-based strategies) | Any number (meters) |
| `config.base_geometry_column` | Geometry column in base table | Any column (default: `geometry`) |
| `config.enrichment_geometry_column` | Geometry column in enrichment table | Any column (default: `geometry`) |
| `config.distance_sql` | Custom distance calculation SQL template | Any SQL expression |
| `config.order_by_sql` | Custom ordering SQL template | Any SQL expression |
| `config.join_condition_sql` | Custom join condition SQL template | Any SQL expression |
| `config.base_filter_sql` | WHERE clause for base table | Any SQL WHERE condition |
| `config.enrichment_filter_sql` | WHERE clause for enrichment table | Any SQL WHERE condition |
| `config.aggregation_type` | Type of aggregation (for `aggregate_within_distance`) | `jsonb_agg`, `array_agg`, `count`, `avg`, `sum`, `min`, `max` |
| `config.aggregation_column` | Column to aggregate | Any column name |
| `config.aggregation_alias` | Output column name for aggregation | Any string |
| `config.aggregation_expression` | Custom aggregation SQL | Any SQL expression |
| `config.base_join_column` | Join column in base table (for `attribute_join`) | Any column |
| `config.enrichment_join_column` | Join column in enrichment table (for `attribute_join`) | Any column |
| `config.join_type` | Type of SQL join (for `attribute_join`) | `INNER`, `LEFT`, `RIGHT` |
| `config.select_all_enrichment` | Include all enrichment columns | `true`, `false` |
| `config.select_columns` | Additional computed columns | List of strings or dicts with `expression` and `alias` |
| `config.insert.columns` | Columns for INSERT statement | List of column names |
| `config.insert.conflict_columns` | Columns for ON CONFLICT clause | List of column names |
| `config.insert.update_columns` | Columns to update on conflict | List of column names |

Built-in runtime strategies:

**Control strategies:**
1. `custom` - delegates to mapper's `mapping_db_query()` method
2. `sql_template` - uses SQL template string from `mapping.config.sql`
3. `none` - skips mapping entirely

**Spatial strategies (auto-generate PostGIS SQL):**
4. `nearest_neighbour` / `knn` / `nearest_station` - maps to single nearest feature
5. `within_distance` - maps to all features within max distance
6. `intersection` - maps spatially intersecting features
7. `nearest_k` / `k_nearest` / `knn_multiple` - maps to K nearest features
8. `aggregate_within_distance` / `buffer_aggregate` - aggregates features within buffer

**Non-spatial strategies:**
9. `attribute_join` / `id_join` / `key_join` - joins on shared attribute columns

See [mapping-strategies-reference.md](mapping-strategies-reference.md) for detailed documentation.

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

### `enrichment_operators`

Optional, declarative staging→enrichment transforms. When present, the enrichment
step runs these operators instead of (or after) the default verbatim
staging→enrichment copy. They are built into PostGIS SQL by
`main_core/enrichment_operator_builder.py`. Two families exist:

- **In-place** (UPDATE, no row-count change): `make_point`, `reproject`,
  `snap_to_grid`, `derive` (set a column from a SQL expression over the row),
  `normalize` (scale a column table-wide via minmax/zscore).
- **Reshape** (TRUNCATE + INSERT SELECT, bypass the default sync): `aggregate`,
  `spatial_aggregate`, `raster_aggregate`.

| Key | Meaning |
|---|---|
| `operators` | Ordered list of operator blocks (each has a `type` plus type-specific fields like `target_col`, `expression`, `source_col`, `cell_size`, `aggregations`, …) |
| `output_columns` | Optional. When set, the enrichment table is created dynamically from these `{ name, type, index }` specs (used by reshape operators) instead of a SQLAlchemy `table_class` |

The `tree` datasource is the worked example of the in-place path: its enrichment
table is a SQLAlchemy class (`TreeEnrichmentTable`), the default sync copies
`source_id`/`geometry_25833`/`attributes` from staging, then a sequence of
`derive` operators unpacks the raw `attributes` JSONB into normalized columns
(`species_de`, `genus`, `height_m`, a German→English `leaf_type`, a derived
`size_class`, …) so the debug panel shows clean records. `elevation` is the
reshape example (`raster_aggregate`).

## Current datasource inventory

Datasources are one-per-file under `data_source_configs/` (loaded by
`CoreConfig._load_datasource_configs()`). The `osm_graph` datasource still lives
inline under `graph.datasource` in `config.yaml`.

| File | Datasource `name` | Inline MV |
|---|---|---|
| `config.yaml` (`graph.datasource[0]`) | `osm_graph` | — |
| `weather_station_bright_sky.yaml` | `weather_station_bright_sky` | — |
| `weather_forecast_bright_sky.yaml` | `weather_forecast_bright_sky` | `mv_weather` |
| `air_quality_data_download.yaml` | `air_quality_data_download` | `mv_air_pollution` |
| `elevation_grids_links.yaml` | `elevation_grids_links` | — |
| `elevation.yaml` | `elevation` | `mv_ways_with_elevation` |
| `tree.yaml` | `tree` | `mv_tree` |
| `pleasant_bicycling.yaml` | `pleasant_bicycling` | `mv_pleasant` |

Each datasource carries its own `enable` flag; toggle a file's `enable: false`
to take it out of a run.

## What else can be configured

These are the main extension points the current code already supports, even if not all are used in the current file:

1. `source.fetch: https`
2. `source.mode: multi` with `expand_params`, `url_template`, or `explicit_url_list`
3. `job.trigger.type.name: cron`, `date`, `calendar_interval`, `run_once`
4. `mapping.strategy.type: sql_template`
5. custom mapping strategies via mapper override
6. inline materialized views via a `materialized_view:` block per datasource (and custom MV handlers via `handler.class` / `handler.module`)
7. declarative staging→enrichment transforms via `enrichment_operators`
8. local-file datasources by setting `fetch: local` and `file_path`
9. config-reload behavior via `runtime.config_watch`
10. DB environment overrides via `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`

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

    # Optional: per-way materialized view for this datasource. schema, handler,
    # build/refresh boilerplate come from mv_defaults; triggers/depends_on
    # datasources auto-fill from this datasource's name.
    materialized_view:
      name: mv_example
      depends_on:
        tables:
          - { name: ways_base }
          - { name: example_mapping }
      definition:
        select_sql: |
          SELECT w.id, w.way_id, w.way_link_index, m.value
          FROM {schema}.ways_base w
          LEFT JOIN {schema}.example_mapping m ON m.way_id = w.id
      indexes:
        - { name: idx_mv_example_id, columns: [id], unique: true }
```
