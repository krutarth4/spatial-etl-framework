# Materialized Views — Configuration Reference

This document defines the schema for materialized view (MV) configs that live in
[`mv_configs/`](../mv_configs/) and explains **why** each field exists.

> Single source of truth: `config.yaml` enables the subsystem and points at the
> folder; every YAML file in that folder is one MV.

---

## 1. How MVs fit in the pipeline

The pipeline writes per-datasource ETL output to **staging**, **enrichment**,
and **mapping** tables (see `docs/mapping-strategies-reference.md`). Those
tables are normalized for write throughput, not read patterns. A materialized
view denormalizes them into a single, query-shaped table that downstream
consumers (router, API, dashboards) can hit cheaply.

The runtime flow is:

```
ETL run (e.g. weather_forecast_bright_sky) finishes successfully
        │
        ▼
MaterializedViewManager.on_datasource_success(name, sync_result)
        │
        ├── matches MV configs whose `triggers.on_datasource_success` lists `name`
        ├── instantiates the handler class declared in `handler.class`
        ├── handler.ensure()  → CREATE MATERIALIZED VIEW if missing, then indexes
        └── handler.refresh() → REFRESH (skipped if `only_on_data_change` and run touched 0 rows)
```

Source files:
- Manager: [`materialized_views/manager.py`](../materialized_views/manager.py)
- Handlers: [`materialized_views/handlers.py`](../materialized_views/handlers.py)
- Loader: [`main_core/core_config.py`](../main_core/core_config.py) (`_merge_mv_configs`)

---

## 2. Top-level switch in `config.yaml`

```yaml
materialized_views:
  enable: true              # master switch — false disables the entire subsystem
  mv_folder: ./mv_configs/  # every *.yaml in here is loaded as one MV definition
```

The loader globs `mv_folder/*.yaml`, parses each file, and exposes the list as
`materialized_views.views` to the manager. To stop a single MV from running,
set `enable: false` in its file (or delete the file).

---

## 3. MV file schema (v2)

```yaml
# ── Identity ──────────────────────────────────────────────────────────────
id: mv_weather                # stable id for logs / metadata; defaults to "<schema>.<name>"
name: mv_weather              # MV object name in Postgres
schema: trial                 # target schema where the MV is created
description: "..."            # free-text; surfaced in tooling/dependency graphs
enable: true                  # toggle without deleting the file

# ── Handler selection ─────────────────────────────────────────────────────
handler:
  class: GenericMaterializedViewHandler   # which class builds the SQL
  module: materialized_views.handlers     # optional; defaults to materialized_views.handlers

# ── Triggers ──────────────────────────────────────────────────────────────
triggers:
  on_datasource_success:      # refresh after these ETL datasources finish a successful run
    - <datasource_name>
  only_on_data_change: true   # skip REFRESH if the triggering run produced 0 inserts/updates

# ── Dependencies (declarative; informational) ─────────────────────────────
depends_on:
  datasources: [<name>, ...]
  tables:
    - { schema: <schema>, name: <table> }

# ── Definition (provide exactly ONE of select_sql | custom_sql | source) ──
definition:
  select_sql: |               # used by GenericMaterializedViewHandler
    SELECT ... FROM ...

  custom_sql:                 # full control — handler runs these verbatim
    create:  "CREATE MATERIALIZED VIEW ..."
    refresh: "REFRESH MATERIALIZED VIEW ..."

  source:                     # consumed by domain-specific handlers (e.g. WeatherMaterializedViewHandler)
    mapping_table:    { schema: <s>, name: <t> }
    enrichment_table: { schema: <s>, name: <t> }
    base_table:       { schema: <s>, name: <t> }
    filters:
      timestamp_eq: "2026-02-24 16:00:00+00"

# ── Build options ─────────────────────────────────────────────────────────
build:
  with_data: true             # CREATE ... WITH [NO] DATA
  tablespace: null            # optional Postgres tablespace
  storage_parameters: {}      # reserved (e.g. fillfactor) — currently unused

# ── Indexes ───────────────────────────────────────────────────────────────
indexes:
  - name: mv_weather_way_id_idx
    columns: [way_id]
    unique: false
    method: btree             # btree | gist | gin | brin
    where: null               # optional partial-index predicate
  # - sql: "CREATE INDEX ..."  # fully custom

# ── Refresh behavior ──────────────────────────────────────────────────────
refresh:
  enabled: true
  mode: normal                # normal | concurrently  (concurrently needs a UNIQUE index)
  with_data: true             # `false` => REFRESH ... WITH NO DATA
```

---

## 4. Field-by-field — what & why

### Identity

| Field | Required | Why it exists |
|---|---|---|
| `id` | no (defaults to `<schema>.<name>`) | Stable identifier for logs, metrics, and dependency-graph tools. Decouples human-readable id from physical name so you can rename the MV without breaking dashboards. |
| `name` | yes | The actual Postgres MV name. |
| `schema` | yes | Target schema. Kept explicit (not derived from `config.yaml`) so an MV can live in a different schema than its source tables. |
| `description` | no | Self-documentation; useful when the codebase grows past a handful of views. |
| `enable` | no (default `true`) | Disables a single MV without deleting its file — handy for staged rollouts and incident response. |

### `handler`

| Field | Why |
|---|---|
| `class` | Selects the SQL-generation strategy. `GenericMaterializedViewHandler` is fine for ~80% of cases (anything expressible as a single `SELECT`); domain-specific handlers (`WeatherMaterializedViewHandler`, …) exist when the SQL needs runtime logic — multiple filters, conditional joins, post-create steps. |
| `module` | Lets you drop a handler into another package without editing the manager. |

The legacy flat keys `handler_class` / `handler_module` are still accepted.

### `triggers`

| Field | Why |
|---|---|
| `on_datasource_success` | The MV's *event source*. ETL is the system's heartbeat; refreshing on success guarantees the MV reflects the data the user just ingested, without bolting on a separate scheduler. |
| `only_on_data_change` | A REFRESH on a multi-million-row MV is expensive. `sync_result` carries `inserted`/`updated` counts; if both are zero, there is nothing new to materialize and we skip the refresh. |

> Renamed from the old `depends_on.datasources` because *triggering* and
> *dependency* are different concerns: an MV may depend on five tables but only
> need to refresh when one specific datasource updates.

### `depends_on`

Purely declarative — currently used for documentation and future dependency
graphing. Listed here (not under `triggers`) so the dependency surface is still
visible even if you trigger the MV from a cron schedule rather than ETL events.

### `definition` — pick exactly one

Three escape hatches in increasing order of flexibility:

1. **`select_sql`** — you provide just the `SELECT`; the handler wraps it with
   `CREATE MATERIALIZED VIEW … AS …`. Use this when the MV is a pure projection.
2. **`custom_sql.{create,refresh}`** — you provide the full statements. Use
   when you need DDL the handler doesn't generate (e.g. `WITH (fillfactor=70)`,
   stacked statements, post-create `ANALYZE`).
3. **`source`** — structured table references consumed by a *specialized*
   handler (e.g. `WeatherMaterializedViewHandler`). Use when the SQL has to be
   built dynamically (variable filters, column lists derived from config).

This separation is deliberate: the same MV file format covers "trivially
declarative", "I want raw SQL", and "I want a code-driven builder", without
each handler inventing its own keys.

### `build`

| Field | Why |
|---|---|
| `with_data` | `WITH NO DATA` creates the MV unpopulated. Useful when the dataset is huge and you want the *first* refresh to happen on a controlled schedule rather than during the deploy that creates the MV. |
| `tablespace` | Place large MVs on a dedicated disk/SSD tablespace without touching the rest of the schema. |
| `storage_parameters` | Reserved for future per-MV tuning (fillfactor, autovacuum thresholds). Documented now so configs don't sprout ad-hoc keys later. |

### `indexes`

Indexes are first-class config (not buried in `custom_sql`) because:
- They are re-asserted with `IF NOT EXISTS` on every refresh, so a missing index gets healed automatically.
- `mode: concurrently` *requires* a UNIQUE index — making indexes structured lets us validate this combination at config-load time.
- Every supported index shape (`columns`, `unique`, `method`, `where`) maps to a single Postgres flag, with a `sql:` escape hatch for anything exotic.

### `refresh`

| Field | Why |
|---|---|
| `enabled` | Lets you create the MV but skip refreshes (e.g. a one-shot reporting snapshot). |
| `mode: concurrently` | `REFRESH MATERIALIZED VIEW CONCURRENTLY` does not lock readers — essential for any MV consumed by a live API. Requires a UNIQUE index. |
| `with_data` | `REFRESH ... WITH NO DATA` invalidates the MV (useful before a maintenance window where you want to free the storage but keep the schema). |

---

## 5. Choosing a handler

```
Is your MV a single SELECT with no runtime logic?
  └─ yes → GenericMaterializedViewHandler + definition.select_sql

Do you need full DDL control (storage params, multiple statements)?
  └─ yes → GenericMaterializedViewHandler + definition.custom_sql

Does the SQL itself depend on config (variable filters, table refs, etc.)?
  └─ yes → write a domain handler in materialized_views/handlers.py
           and consume definition.source.*
```

A custom handler subclasses `BaseMaterializedViewHandler` and implements
`ensure()` and `refresh()`. Use the inherited `_wrap_create()`,
`_ensure_indexes()`, and `_qualified()` helpers so behavior stays consistent
across handlers (tablespaces, `WITH NO DATA`, index re-assertion).

---

## 6. Full example (the canonical one)

See [`mv_configs/mv_weather.yaml`](../mv_configs/mv_weather.yaml). It uses:
- `WeatherMaterializedViewHandler` (domain handler)
- `definition.source` (structured table refs + a `timestamp_eq` filter)
- A `way_id` btree index
- `triggers.on_datasource_success` for both station and forecast datasources
- `only_on_data_change: true` to skip refreshes when a forecast poll returns
  the cache it already has

---

## 7. Backward compatibility

The handlers still accept the legacy flat keys (`select_sql`, `custom_sql`,
`mapping_table`, `weather_table`, `ways_table`, `timestamp_filter`,
`handler_class`, `handler_module`, `depends_on.datasources`,
`refresh.only_on_data_change`). New configs should use the v2 structure
documented above; old configs keep working untouched.
