# Config Reference (`config.yaml`)

This document explains the full structure used in [config.yaml](/Users/krutarthparwal/Documents/mdp/modular-data-pipeline/config.yaml), what each section does, and which values are allowed by code.

## 1) Top-level structure

`config.yaml` currently contains these top-level keys:

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

## 2) Top-level sections and allowed values

### `server`

Used by `core/init_server.py` (`ServerConfDTO`).

| Key | Type | Allowed values | What it is for |
|---|---|---|---|
| `app_type` | string | Any import path for uvicorn app | FastAPI app import target, e.g. `core.main:app`. |
| `enable` | bool | `true` or `false` | If `false`, API server is not started. |
| `name` | string | Any | Label only. |
| `description` | string | Any | Notes only. |
| `host` | string | Any valid host bind | Uvicorn bind host. |
| `port` | int | Valid TCP port | Uvicorn port. |
| `reload` | bool | `true` or `false` | Uvicorn reload mode. |

### `runtime`

Used in `Application.keep_alive_forever()`.

| Key | Type | Allowed values | What it is for |
|---|---|---|---|
| `config_watch.enable` | bool | `true` or `false` | Enables restart on config file change in keep-alive mode. |
| `config_watch.poll_seconds` | number | `> 0` recommended | Poll interval to detect config changes. |

### `materialized_views`

Used by `materialized_views/manager.py` and handlers.

| Key | Type | Allowed values | What it is for |
|---|---|---|---|
| `enable` | bool | `true` or `false` | Global MV orchestration toggle. |
| `views` | list | List of view configs | Per-MV behavior. |

Per-view keys:

| Key | Type | Allowed values | What it is for |
|---|---|---|---|
| `id` | string | Any | Optional stable identifier. |
| `enable` | bool | `true` or `false` | Per-view toggle. |
| `schema` | string | Existing DB schema | MV schema. |
| `name` | string | Existing/new view name | MV name. |
| `handler_class` | string | Class in handler module | Defaults to `GenericMaterializedViewHandler`. |
| `handler_module` | string | Python module path | Defaults to `materialized_views.handlers`. |
| `depends_on.datasources` | list[string] | Datasource names | MV refresh is triggered when these datasources succeed. |
| `depends_on.tables` | list[string] | Any table refs | Informational dependency list. |
| `refresh.enabled` | bool | `true` or `false` | Whether refresh is executed after ensure. |
| `refresh.mode` | string | `normal`, `concurrently` | Controls `REFRESH MATERIALIZED VIEW` mode. |
| `refresh.with_data` | bool | `true` or `false` | Used by generic handler for `WITH [NO] DATA`. |
| `custom_sql.create` | string | SQL | Optional full custom create SQL. |
| `custom_sql.refresh` | string | SQL | Optional full custom refresh SQL. |
| `select_sql` | string | SQL SELECT | Generic handler create source if `custom_sql.create` absent. |

Weather-specific handler keys in your file:
`mapping_table`, `weather_table`, `ways_table`, `timestamp_filter`, `indexes`.

### `scheduler`

Used by `core/init_scheduler.py` (`SchedulerConfDTO`).

| Key | Type | Allowed values | What it is for |
|---|---|---|---|
| `name` | string | Any | Label only. |
| `enable` | bool | `true` or `false` | If `false`, no scheduler instance is created. |
| `description` | string | Any | Notes only. |
| `timezone` | string | Timezone string | Scheduler timezone. |
| `scheduler_type` | string | Currently effectively `BackgroundScheduler` | Stored in config; current code always instantiates `BackgroundScheduler`. |
| `wait_before_shutdown` | bool | `true` or `false` | Passed to scheduler shutdown wait flag. |

### `data_folder`

| Key | Type | Allowed values | What it is for |
|---|---|---|---|
| `data_folder` | string/path | Any path | Informational in current flow. |

### `env_variables`

Loaded into `GlobalConstants`.

| Key | Type | Allowed values | What it is for |
|---|---|---|---|
| any key | scalar/string | Any | Shared constants for mappers and YAML anchors. |

### `database`

Used by `DbInstance` initialization and env overrides in `CoreConfig`.

| Key | Type | Allowed values | What it is for |
|---|---|---|---|
| `description` | string | Any | Notes only. |
| `enable` | bool | `true` or `false` | DB workflow toggle. |
| `driver` | string | SQLAlchemy driver, e.g. `postgresql+psycopg` | Driver part for DB URL. |
| `url` | string | Host/domain | DB host. Can be overridden by `DB_HOST`. |
| `port` | int | Valid port | DB port. Can be overridden by `DB_PORT`. |
| `database_name` | string | Any | DB name. Can be overridden by `DB_NAME`. |
| `database_schema` | string | Existing schema | Default schema. |
| `credential.username` | string | Any | DB user. Can be overridden by `DB_USER`. |
| `credential.password` | string | Any | DB password. Can be overridden by `DB_PASSWORD`. |

### `metadata-datasource`

Used by `DataSourceMetadataService`.

| Key | Type | Allowed values | What it is for |
|---|---|---|---|
| `description` | string | Any | Notes only. |
| `table_schema` | string | Existing schema | Schema where metadata table is managed. |

### `base`

Used by `BaseGraph`.

| Key | Type | Allowed values | What it is for |
|---|---|---|---|
| `table_name` | string | Table name | Base ways table. |
| `table_schema` | string | Schema name | Base table schema. |
| `force_generate` | bool | `true` or `false` | Rebuild behavior for base tables. |

### `graph`

Used by `InitGraph` and `GraphConfDTO`.

| Key | Type | Allowed values | What it is for |
|---|---|---|---|
| `enable` | bool | `true` or `false` | Graph pipeline toggle. |
| `tool` | string | `terminal`, `custom`, `external_ingest` | Chooses graph ingest mode. |
| `schema` | string | Existing schema | Raw graph schema. |
| `table_name` | string | Existing/raw graph table | Raw ways/link table name. |
| `osm_file_path` | string/path | Existing path | OSM source path. |
| `cmd` | list[string] | Shell command parts | Used when `tool: terminal`. |
| `env` | map[string,string] | Env vars | Command environment for graph ingestion. |
| `datasource` | list[datasource] | Same shape as `datasources[]` | Datasource(s) used for graph download/update. |
| `communication.enable` | bool | `true` or `false` | Enables comm/wait table workflow. |
| `communication.tasks` | list[task] | Any task records | Default task rows in comm table. |
| `communication.waits.router_coupled` | object | See below | Optional wait for router task completion. |
| `communication.waits.main_ways_before_base` | object | See below | Optional wait for `main_ways_table`. |

Wait object keys:
`enable` (bool), `task_key` (string), `poll_seconds` (number), `timeout_seconds` (number or `null`), `require_is_completed` (bool, used by `main_ways_before_base`).

### `datasources`

Primary ETL datasource list. Each entry maps to `DataSourceDTO`.

## 3) Datasource schema (`datasources[]` and `graph.datasource[]`)

### Required/standard fields

| Key | Type | Allowed values | What it is for |
|---|---|---|---|
| `name` | string | Unique name | Primary datasource id. |
| `description` | string | Any | Human description. |
| `enable` | bool | `true` or `false` | If `false`, datasource is skipped. |
| `class_name` | string | Mapper class stem | Used to load `data_mappers/{class_name}Mapper.py`. |
| `data_type` | string | Usually `static` or `dynamic` | Stored in metadata; not hard-enforced by enum. |
| `debug.endpoint` | string | Any | Used by debug fetch APIs. |
| `source` | object | See section below | Extract stage configuration. |
| `job` | object | See section below | Scheduler trigger/job metadata. |
| `mapping` | object | See section below | Mapping table + strategy details. |
| `storage` | object | See section below | Persistence tables + flags. |

Optional processing hooks in DTO:
`pre_filter_processing`, `post_filter_processing`, `pre_database_processing`, `pro_database_processing`, `cleanup_processing`.

### `source` block

| Key | Type | Allowed values | What it is for |
|---|---|---|---|
| `fetch` | string | `http`, `https`, `local` | Fetch backend (`DataSourceABCImpl.FetchTypeEnum`). |
| `mode` | string | `single`, `multi` | Fetch mode (`SourceFetchModeEnum`). |
| `url` | string | URL | Remote source URL for HTTP(S). |
| `file_path` | string/path | Local path | Input path for local fetch. |
| `stream` | bool | `true` or `false` | HTTP streaming flag to handler. |
| `save_local` | bool | `true` or `false` | Whether handler saves fetched file. |
| `destination` | string/path | Local output path | Download target and metadata sidecar base. |
| `response_type` | string | Any extension token (`json`, `xml`, `zip`, `gpkg`, `pbf`, `csv`, ...) | Used by handlers/readers. |
| `headers` | map | Any | HTTP headers. |
| `params` | map | Any | HTTP query parameters. |
| `check_metadata.enable` | bool | `true` or `false` | Enable metadata-change check before download. |
| `check_metadata.keys` | list[string] | Header/meta keys | Compared old vs current metadata to decide fetch. |
| `multi_fetch` | object | See below | Multi-input expansion strategy. |

#### `multi_fetch` block (when `mode: multi`)

| Key | Type | Allowed values | What it is for |
|---|---|---|---|
| `enable` | bool | `true` or `false` | Turns multi strategy on/off. |
| `strategy` | string | `expand_params`, `url_template`, `explicit_url_list` | Multi strategy (`SourceMultiFetchStrategy`). |
| `expand` | map[key, list] | Lists per key | Cartesian product expansion for params. |
| `params` | map | Any fixed params | Added to expanded params. |
| `url_template` | string | Python `str.format` template | URL/path template for `url_template`. |
| `template_params` | map[key, list] | Equal-length lists expected | Values interpolated into template. |
| `urls` | list[string] or object | URL list or `input` file path object | Used by `explicit_url_list`. |

### `job` block

`DataSourceABCImpl.create_job()` supports these trigger names:
`interval`, `date`, `cron`, `calendar_interval`, `run_once`.

| Key | Type | Allowed values | What it is for |
|---|---|---|---|
| `name` | string | Any | APScheduler job name. |
| `id` | string | Any | APScheduler job id (fallbacks to datasource name). |
| `executor` | string/null | `process` or omitted/default | Uses APScheduler `process` executor if set. |
| `trigger.type.name` | string | `interval`, `date`, `cron`, `calendar_interval`, `run_once` | Trigger class selection. |
| `trigger.type.start_date` | datetime/string | Datetime parseable | Start date for applicable triggers. |
| `trigger.type.config` | map | APScheduler trigger kwargs | Per-trigger configuration. |
| `replace_existing` | bool | `true` or `false` | Replace existing job with same id. |
| `coalesce` | bool | `true` or `false` | Coalescing behavior (add_job also forces `coalesce=True`). |
| `max_instances` | int | `>= 1` | Max concurrent job instances. |
| `next_run_time` | string | Any | Stored in DTO; not directly applied in `add_job`. |

### `mapping` block

| Key | Type | Allowed values | What it is for |
|---|---|---|---|
| `enable` | bool | `true` or `false` | Enables map-to-base stage. |
| `joins_on` | string | Column name | Legacy join field for mapper SQL. |
| `table_name` | string | Table name | Mapping table. |
| `table_schema` | string | Schema name | Mapping table schema. |
| `base_table.table_name` | string | Table name | Base graph table name. |
| `base_table.table_schema` | string | Schema | Base graph schema. |
| `base_table.column_name` | string | Column name | Column to update/use in base. |
| `base_table.column_type` | string | Any type label | Mapper-defined semantic type. |
| `strategy.type` | string | `custom`, `sql_template`, `none`, `nearest_neighbour`, `within_distance`, `intersection`, `nearest_station`, `knn` | Runtime mapping strategy discriminator. |
| `strategy.description` | string | Any text | Human-readable note about how the mapping works. |
| `strategy.link_on.mapping_column` | string | Column | Mapping-side column for strategy. |
| `strategy.link_on.base_column` | string | Column | Base-side column for strategy. |
| `strategy.link_on.basis` | string | Any, e.g. `nearest_by_distance` | Strategy basis hint. |
| `config.sql` | string | SQL | Required when `strategy.type: sql_template`. |

Spatial mapper helpers when `strategy.type` is one of the built-in spatial variants:

| Key | Type | What it is for |
|---|---|---|
| `config.base_id_column` | string | Base-table id column to expose as `way_id`. Default: `id`. |
| `config.base_geometry_column` | string | Base geometry column name. Default: `geometry`. |
| `config.enrichment_geometry_column` | string | Enrichment geometry column name. Default: `geometry`. |
| `config.distance_alias` | string | Output alias for distance field. Default: `distance`. |
| `config.distance_sql` | string | Optional custom distance expression using `{base_geometry}` and `{enrichment_geometry}` placeholders. |
| `config.max_distance` | number/string | Required for `within_distance` unless `config.join_condition_sql` is set. |
| `config.join_condition_sql` | string | Optional custom join predicate using `{base_geometry}`, `{enrichment_geometry}`, and `{max_distance}`. |
| `config.order_by_sql` | string | Optional nearest-neighbour ordering expression. |
| `config.base_filter_sql` | string | Optional base-table `WHERE` clause, with or without the `WHERE` keyword. |
| `config.enrichment_filter_sql` | string | Optional enrichment-side filter, with or without the `WHERE` keyword. |
| `config.select_columns` | list | Extra output columns as raw SQL strings or `{ expression, alias }` objects. Strings and expressions may use `{base_alias}`, `{enrichment_alias}`, `{base_geometry_column}`, `{enrichment_geometry_column}`, `{distance_sql}`. |

Mapping runtime behavior:

1. `custom`: call the mapper's `mapping_db_query()` and execute the returned SQL.
2. `sql_template`: read `mapping.config.sql` from config and format placeholders.
3. `none`: skip mapping.
4. Built-in spatial types such as `nearest_neighbour`, `within_distance`, `intersection`, and `knn`: build SQL from the strategy registry.

### `storage` block

| Key | Type | Allowed values | What it is for |
|---|---|---|---|
| `persistent` | bool | `true` or `false` | If `false`, transformed data is not persisted to DB. |
| `force_create` | bool | `true` or `false` | Table recreation behavior at setup. |
| `expires_after` | string | Duration-like text, e.g. `6h` | Retention metadata/hint. |
| `staging.table_name` | string | Table name | Staging table. |
| `staging.table_schema` | string | Schema | Staging schema. |
| `staging.table_class` | string | Class label | Mapper/database table class binding. |
| `staging.persistent` | bool | `true` or `false` | Optional flag in DTO. |
| `enrichment.table_name` | string | Table name | Enrichment table. |
| `enrichment.table_schema` | string | Schema | Enrichment schema. |
| `enrichment.table_class` | string | Class label | Mapper/database table class binding. |
| `enrichment.persistent` | bool | `true` or `false` | Optional flag in DTO. |

## 4) YAML expression support in this project

This codebase supports inline python blocks in YAML via:

`$ {{ ...python... }}` (without the space; shown spaced here for readability)

Actual parser pattern is `${{ ... }}` in `readers/yaml_reader.py`, executed with:

1. `datetime`
2. `ZoneInfo`

This is why values like dynamic current datetime under `multi_fetch.params.date` work.

## 5) Current datasource entries in your `config.yaml`

These are the datasource blocks currently present:

1. `graph.datasource[0]`: `osm_graph` (disabled)
2. `datasources[0]`: `weather_station_bright_sky` (disabled)
3. `datasources[1]`: `weather_forecast_bright_sky` (disabled)
4. `datasources[2]`: `air_quality_data_download` (disabled)
5. `datasources[3]`: `elevation_grids` (disabled)
6. `datasources[4]`: `elevation` (disabled)
7. `datasources[5]`: `elevation_python` (disabled)
8. `datasources[6]`: `tree_wfs_capabilities` (disabled)
9. `datasources[7]`: `tree` (disabled)
10. `datasources[8]`: `pleasant_bicycling` (enabled)

## 6) Practical caveats found while mapping allowed values

1. Config uses `header` in many datasource blocks, while DTO/runtime field is `headers`. To pass headers reliably, use `headers`.
2. In `source.mode: single`, current code only executes `fetch()` when `check_metadata.enable` is `true`. If you set it `false`, extract may return no paths.
3. In `multi_fetch()`, HTTP(S) condition currently behaves as HTTP-only because of `if source.fetch in (FetchTypeEnum.HTTP.value or FetchTypeEnum.HTTPS.value)`. For multi HTTPS, verify behavior before relying on it.
4. Trigger names are not enum-validated at DTO level; invalid `trigger.type.name` fails at runtime when building trigger map.
5. Keys with hyphens (for example `pre-filter-processing`) do not map to DTO snake_case fields unless separately normalized.

## 7) Minimal valid datasource template

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
