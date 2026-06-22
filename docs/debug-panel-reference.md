# Debug Panel — Behaviour Reference

This document explains how two debug-panel features compute their results, so
the numbers and visualisations can be read correctly. Both are served by the
FastAPI debug API (`core/main.py`, prefix `/debug`) and rendered by the Angular
debug components (`frontend-angular/src/app/debug/`).

Related references: [`materialized-views-reference.md`](materialized-views-reference.md),
[`mapping-strategies-reference.md`](mapping-strategies-reference.md).

---

## 1. Enrichment visualisation (with staging fallback)

**Endpoint:** `GET /debug/mappers/{endpoint}/enrichment-visualization`
**Backend:** `core/debug/enrichment_inspector_mixin.py`
**Frontend:** `debug-map.component.ts` (the **Enrichment** button)

The enrichment visualisation renders the raw geometry of a datasource's
enrichment table inside a user-drawn bounding box, transformed to EPSG:4326.

Not every datasource has an enrichment table. When the enrichment table is
either not configured or absent from the database, the endpoint falls back to
the datasource's **staging** table instead of failing. The response then
carries two extra fields:

| Field | Meaning |
|-------|---------|
| `source` | `"enrichment"` (normal) or `"staging"` (fallback in effect) |
| `warning` | Human-readable note explaining the fallback, or `null` |

The frontend shows a `staging fallback` badge and an amber warning line when
`source` is `"staging"`. The geometry-reading logic is identical for both
tables, so the fallback depends on the staging table exposing one of the known
geometry or raster columns (see `_VECTOR_GEOM_CANDIDATES` /
`_RASTER_GEOM_CANDIDATES`). Only when neither the enrichment nor the staging
table can be used does the endpoint return an error.

---

## 2. Mapping coverage

**Served in:** `GET /debug/datasources/{endpoint}` (the dashboard `coverage` block)
**Backend:** `_mapping_coverage()` in `core/debug/mapping_inspector_mixin.py`
**Frontend:** the coverage donut in `debug.component.ts` / `debug.component.html`

Coverage answers one question: **of all road segments in the network, how many
received a real mapped value from this datasource?**

### How it is computed

- **total** is the row count of the base road network (`ways_base`, resolved
  from `mapping.base_table`), not the mapping table's own row count.
- **covered** is the number of distinct road segments that carry a non-null
  value in the mapping table (`COUNT(DISTINCT way_id) WHERE <value_col> IS NOT
  NULL`). `DISTINCT` guards against strategies that emit several rows per
  segment.
- **uncovered** is `total - covered`.
- **covered_pct** is `covered / total * 100`.

Using `ways_base` as the denominator is deliberate. Many strategies drop
unmatched roads, so the mapping table contains only matched segments. Counting
against the mapping table's own size would report close to 100% even when half
the network has no data. Counting real mapped rows against the full network
gives an honest figure.

### Default values count as uncovered

Default and sentinel values (for example `0.0` for elevation, `-1` for the
pleasant-bicycling and weather forecasts, `'[]'` for trees) are injected only
in the **materialized view** through `COALESCE(col, <default>)`. They never
reach the mapping table, where unmatched roads stay null or absent. Because
coverage counts real mapping rows against `ways_base`, a road that only gets a
default value in the view is correctly reported as **uncovered**.

### Which column is tested

The value column is resolved per datasource by `_coverage_value_col()`, which
prefers what the strategy declares, in this order, using the first column that
actually exists in the mapping table:

1. `strategy.value_columns` (for example air quality: `no2`)
2. `strategy.aggregation_alias` (for example tree: `trees`)
3. `base_table.column_name` (a semantic label, used only if it maps to a real
   column)
4. fallback to the first non-id, non-geometry column, excluding the strategy's
   `distance_alias` (for example `nearest_distance_m`)

Datasources that declare no value column in their strategy (`sql_template`,
`custom`) use the fallback. To make their coverage test an exact column, add a
`value_columns` entry to the mapping strategy and the resolver will pick it up.

### Coverage response fields

| Field | Meaning |
|-------|---------|
| `total` | Road segments in the base network (`ways_base`) |
| `covered` | Distinct segments with a real mapped value |
| `uncovered` | Segments with no real data (missing or default-filled) |
| `covered_pct` | `covered / total * 100`, rounded to one decimal |
| `value_column` | The mapping-table column tested for coverage |
| `base_table` | The base table used as the denominator |

---

## 3. Coverage map visualisation

**Endpoint:** `GET /debug/mappers/{endpoint}/coverage-visualization`
**Backend:** `fetch_coverage_visualization()` in `core/debug/mapping_inspector_mixin.py`
**Frontend:** the **Coverage** mode in `debug-map.component.ts`

This renders the base road segments inside a bounding box, each classified as
covered or uncovered, so you can see *where* real data is missing rather than
just the headline percentage. A segment is covered when the mapping table holds
a non-null value for it (same rule as the coverage metric, using the same
`_coverage_value_col()` resolution). Geometry comes from the base network, so
uncovered segments still draw.

The response carries per-view counts plus the network-wide totals:

| Field | Meaning |
|-------|---------|
| `shown` | Segments returned for the current bbox/limit |
| `shown_covered` / `shown_uncovered` | Covered vs uncovered among those shown |
| `value_column` | The mapping-table column tested |
| `base_table` | The base network table |
| `coverage` | The full network-wide coverage block from section 2 (independent of the bbox/limit) |
| `geojson` | FeatureCollection of base segments, each with a `covered` boolean property |
