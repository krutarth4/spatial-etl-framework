# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

# Spatial ETL Framework — Modular Data Pipeline

Geospatial ETL pipeline for Berlin bicycle routing. Python pipeline serving PostGIS, previously part of the MDP monorepo (now standalone).

**DB:** PostGIS 16, db `test`, user `postgres/admin123`, schema `trial`, base table `trial.ways_base`

## Commands

```bash
# Python pipeline (auto-restarts on config.yaml change)
python3 run.py

# Docker (if running with compose)
docker compose -f docker-compose-init.yaml up --build
```

## Dev Rules

- **Work directly on `develop` branch.** No feature branches.
- **No `git push` or `git commit`.** Local file edits only.
- **Tests can be skipped** — not a priority.
- Pipeline reloads automatically ~2s after `config.yaml` changes.
- **All Claude config (settings, skills, hooks) must be stored in `.claude/` within this repo.** Never write Claude-related config to global `~/.claude/` for anything project-specific.

## Task → File Map

| Task | Files |
|------|-------|
| New data mapper | `data_mappers/<name>Mapper.py` + `data_source_configs/<name>.yaml` (auto-discovered at startup; no registration in `config.yaml`) |
| Debug API endpoint | `core/main.py` |
| Scheduling | `core/init_scheduler.py` + `job:` sections in datasource configs |
| Materialized views | `config.yaml` → `materialized_views:` section |

## Pipeline Architecture

- `config.yaml` is the single source of truth — datasources, schedules, mapping strategies, materialized views.
- Per-datasource config lives in `data_source_configs/<name>.yaml`, auto-discovered at startup by `CoreConfig._load_datasource_configs()` (root `config.yaml` keeps `datasources: []`). Schema, base-table, `<name>_<stage>` table names, common job flags, and `mapping_defaults.config` geometry columns are filled by defaults — declare only what is distinctive.
- Each mapper in `data_mappers/` extends `DataSourceABCImpl`. Override only what you need: `read_file_content()`, `staging_db_query()`, `enrichment_db_query()`, `mapping_db_query()` (last one only for `strategy.type: custom`).
- ETL flow: extract → raw_staging → staging → enrichment → mapping → `trial.ways_base`
- Mapping strategies auto-generate PostGIS SQL: `knn`, `nearest_k`, `within_distance`, `intersection`, `aggregate_within_distance`, `attribute_join`, `sql_template`, `custom`. Full reference: `docs/mapping-strategies-reference.md`

## Memory — Fast Retrieval

When the user asks about any mapper or dataset (elevation, weather, trees, air quality, bicycling, graph, etc.), **first load the project memory** before reading any source files:

- **Mapper architecture reference:** `~/.claude/projects/-Users-krutarthparwal-Documents-spatial-etl-framework/memory/reference_mappers.md`
  - Covers all 12 mappers: data source, ETL tables, mapping strategy, override methods, CRS, notable logic
  - Use this instead of re-reading `data_mappers/` or `data_source_configs/` files
  - Only read the actual source file if you need details beyond what's in the memory

Trigger keywords: mapper, mapping, elevation, weather, trees, air quality, bicycling, graph, ETL, staging, enrichment, datasource, dataset

## New Mapper Skeleton

`data_mappers/<name>Mapper.py`:
```python
from main_core.data_source_abc_impl import DataSourceABCImpl

class MyMapper(DataSourceABCImpl):
    def read_file_content(self, path: str) -> list:
        ...  # parse raw file → list of dicts
```

`data_source_configs/<name>.yaml` (minimum):
```yaml
datasources:
  - name: my_datasource
    enable: true
    class_name: MyMapper
    debug:
      endpoint: my-datasource
    source: {fetch: http, url: "https://...", response_type: json}
    job:
      trigger:
        type: {name: interval, config: {hours: 6}}
    storage:
      staging: {table_name: my_staging, table_schema: test_osm_base_graph}
      enrichment: {table_name: my_enrichment, table_schema: test_osm_base_graph}
    mapping:
      enable: true
      strategy: {type: knn}
      table_name: my_mapping
      table_schema: test_osm_base_graph
```
