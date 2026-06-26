# Example: Tree Config (YAML)

**Source file:** [`data_source_configs/tree.yaml`](../data_source_configs/tree.yaml)  
**Mapper:** [`data_mappers/treeMapper.py`](../data_mappers/treeMapper.py) — see [example-tree-mapper.md](example-tree-mapper.md)

This config drives the Berlin street and garden trees datasource. It is a good example of:

- Fetching from a WFS endpoint using `multi_fetch` with `expand_params` (one request per tree layer)
- Using `aggregate_within_distance` to build a per-road JSONB array of nearby trees
- Replacing Python enrichment logic with declarative `enrichment_operators`
- Defining a materialized view inline alongside its datasource

---

## Full annotated config

```yaml
name: tree
description: "Get Trees on the side of the road within 50 m distance"
enable: true
class_name: tree          # → data_mappers/treeMapper.py → TreeMapper
data_type: static         # dataset rarely changes; metadata check skips re-download if unchanged
```

`class_name: tree` follows the naming convention: the file is `treeMapper.py` and the class is `TreeMapper`. `data_type: static` means the framework treats the remote file as immutable once downloaded (until the source reports a change via HTTP metadata headers).

---

## Source and fetch

```yaml
source:
  input: none
  mode: multi
  fetch: http
  url: "https://gdi.berlin.de/services/wfs/baumbestand"
  response_type: gpkg
  save_local: true
  destination: tmp/tree_wfs/tree_ablage/gpkg_packet.gpkg
  check_metadata:
    enable: true
    keys: ["content_type"]
  reader:
    engine: pyogrio
    target_crs: 25833
  multi_fetch:
    enable: true
    strategy: expand_params
    expand:
      typenames:
        - "baumbestand:anlagenbaeume"   # garden/park trees
        - "baumbestand:strassenbaeume"  # street trees
    params:
      service: wfs
      version: "2.0.0"
      request: "GetFeature"
      outputFormat: geopackage
      sortBy: gisid
```

**`mode: multi` + `multi_fetch.strategy: expand_params`**  
Instead of a single URL, the framework expands `expand.typenames` and fires one HTTP request per value, injecting it as `typenames=<value>` into the query string. Each response is saved as a separate GeoPackage file, then processed in parallel by the thread pool.

The resulting URLs look like:
```
https://gdi.berlin.de/services/wfs/baumbestand?service=wfs&version=2.0.0&request=GetFeature
  &outputFormat=geopackage&sortBy=gisid&typenames=baumbestand:anlagenbaeume
https://gdi.berlin.de/services/wfs/baumbestand?service=wfs&version=2.0.0&request=GetFeature
  &outputFormat=geopackage&sortBy=gisid&typenames=baumbestand:strassenbaeume
```

**`check_metadata.keys: ["content_type"]`**  
The WFS endpoint does not send `Last-Modified` or `ETag` headers, so the framework falls back to checking `content_type`. If it matches the last stored value, the download is skipped.

**`reader.engine: pyogrio` and `reader.target_crs: 25833`**  
These are hints for the built-in reader, but `TreeMapper.read_file_content()` overrides the reader entirely — it reads the file with GeoPandas directly and reprojects to 25833 itself. The `reader` block is kept here as documentation and as a fallback.

---

## Job schedule

```yaml
job:
  executor: process     # run in a subprocess (isolates memory; good for large GeoPackage files)
  trigger:
    type:
      name: interval
      start_date: 2025-12-21T11:15:00
      config:
        hours: 1
```

`executor: process` runs the datasource in a separate OS process rather than a thread. This is useful for large GeoPackage files where GeoPandas can hold significant memory — the subprocess releases it when it exits.

---

## Storage

```yaml
storage:
  persistent: true       # do not truncate staging on each run; upsert instead
  expires_after: 6h      # re-fetch even if metadata unchanged after 6 hours
  staging:
    table_name: tree_staging
    table_class: TreeStagingTable
  enrichment:
    table_name: tree_enrichment
    table_class: TreeEnrichmentTable
```

`persistent: true` means rows accumulate across runs via upsert (keyed on `UniqueConstraint("source_id")`). Without this, staging is truncated before each run.

`expires_after: 6h` forces a fresh download even if `check_metadata` reports no change, because some WFS endpoints serve stale headers.

---

## Mapping

```yaml
mapping:
  enable: true
  table_name: tree_mapping
  strategy:
    type: aggregate_within_distance
    max_distance: 50
    aggregation_type: jsonb_build_object
    aggregation_expression: |
      COALESCE(
        jsonb_agg(
          jsonb_build_object(
            'tree_id',   {enrichment_alias}.id,
            'source_id', {enrichment_alias}.source_id,
            'distance_m', ST_Distance({enrichment_alias}.geometry_25833, {base_geometry})
          )
          ORDER BY ST_Distance({enrichment_alias}.geometry_25833, {base_geometry})
        ) FILTER (WHERE {enrichment_alias}.id IS NOT NULL),
        '[]'::jsonb
      )
    aggregation_alias: trees
  base_table:
    column_name: "tree_factor"
    column_type: Integer
```

**`aggregate_within_distance`** generates a single `INSERT … SELECT` that joins every road segment in `ways_base` to all enrichment rows within `max_distance` metres, then applies the `aggregation_expression` to reduce them to one value per road segment.

**Template placeholders** — the framework substitutes these before running the SQL:

| Placeholder | Replaced with |
|-------------|---------------|
| `{enrichment_alias}` | alias for the enrichment table in the generated query |
| `{base_geometry}` | the geometry column of the road segment (`ways_base.geometry_25833`) |

The result for each road segment is a JSONB array like:
```json
[
  {"tree_id": 12345, "source_id": "B-0012345", "distance_m": 4.2},
  {"tree_id": 67890, "source_id": "B-0067890", "distance_m": 31.8}
]
```

stored in `tree_mapping.trees` (the column declared in `TreeMappingTable`). Roads with no trees within 50 m get `'[]'::jsonb`.

---

## Enrichment operators

```yaml
enrichment_operators:
  operators:
    - { type: derive, target_col: species_de,  expression: "attributes->>'art_dtsch'" }
    - { type: derive, target_col: species_bot, expression: "attributes->>'art_bot'" }
    - { type: derive, target_col: genus,       expression: "attributes->>'gattung'" }
    - { type: derive, target_col: street,      expression: "attributes->>'strname'" }
    - { type: derive, target_col: district,    expression: "attributes->>'bezirk'" }
    - { type: derive, target_col: planting_year,          expression: "NULLIF(attributes->>'pflanzjahr','')::int" }
    - { type: derive, target_col: age_years,              expression: "NULLIF(attributes->>'standalter','')::numeric" }
    - { type: derive, target_col: crown_diameter_m,       expression: "NULLIF(attributes->>'kronedurch','')::numeric" }
    - { type: derive, target_col: trunk_circumference_cm, expression: "NULLIF(attributes->>'stammumfg','')::numeric" }
    - { type: derive, target_col: height_m,               expression: "NULLIF(attributes->>'baumhoehe','')::numeric" }
    - type: derive
      target_col: leaf_type
      expression: >
        CASE attributes->>'art_gruppe'
          WHEN 'Laubbäume'  THEN 'deciduous'
          WHEN 'Nadelbäume' THEN 'coniferous'
          ELSE NULLIF(attributes->>'art_gruppe','')
        END
    - type: derive
      target_col: size_class
      expression: >
        CASE
          WHEN height_m >= 20 THEN 'large'
          WHEN height_m >= 10 THEN 'medium'
          WHEN height_m >  0  THEN 'small'
          ELSE 'unknown'
        END
```

Each `derive` operator translates to one `UPDATE tree_enrichment SET <target_col> = <expression>`. Operators run in declaration order — `size_class` is listed last because it reads the already-written `height_m` column.

**Why `NULLIF(..., '')::int` instead of just `::int`?**  
The Berlin WFS sometimes encodes missing numeric values as empty strings `""` in JSONB rather than `null`. `NULLIF` converts the empty string to `NULL` before casting, so the cast doesn't fail.

**Why use operators instead of `enrichment_db_query()`?**  
For simple per-column derivations, operators are cleaner — each line is self-contained and easy to extend without touching Python. Use `enrichment_db_query()` when you need cross-table JOINs, subqueries, or logic that cannot be expressed as a single column expression.

---

## Materialized view

```yaml
materialized_view:
  name: mv_tree
  description: "Per-way tree aggregation (JSONB) within 50 m of each base way"
  refresh:
    mode: normal    # tree_mapping has no unique index → cannot use CONCURRENT refresh
  depends_on:
    tables:
      - {name: ways_base}
      - {name: tree_mapping}
  definition:
    select_sql: |
      SELECT
          w.id,
          w.way_id,
          w.from_node_id,
          w.to_node_id,
          w.way_link_index,
          COALESCE(m.trees, '[]'::jsonb) AS trees
      FROM {schema}.ways_base w
      LEFT JOIN {schema}.tree_mapping m
          ON m.way_id = w.id
  indexes:
    - {name: idx_mv_tree_way_id, columns: [way_id]}
```

The materialized view is declared inline in the datasource config rather than in a separate file under `mv_configs/`. Both approaches work; inline is preferred when the view has a single datasource dependency.

`{schema}` is substituted at refresh time with the active `DB_SCHEMA` value (e.g. `exp_null`).

`LEFT JOIN` ensures every road segment appears in the view even if it has no trees — those rows get `'[]'::jsonb` from `COALESCE`.

`refresh.mode: normal` is required here because `tree_mapping` has no unique index. `CONCURRENT` refresh requires a unique index on the view; without one PostgreSQL rejects it.

---

## Summary of patterns used

| Pattern | Config key | When to use |
|---------|-----------|-------------|
| Fetch two WFS layers in one job | `multi_fetch.expand.typenames` | API exposes layers via a query param; you want both in one datasource |
| Per-road JSONB array of features | `strategy: aggregate_within_distance` + `aggregation_type: jsonb_build_object` | Features are dense; you want all nearby records, not just the nearest |
| Declarative column extraction from JSONB | `enrichment_operators.derive` | Source packs many fields in JSONB; no Python needed for simple extractions |
| Ordered operators (derived-from-derived) | List order in `operators:` | `size_class` must run after `height_m` — declare it last |
| Inline materialized view | `materialized_view:` block in datasource YAML | View depends only on this datasource; keeps config self-contained |
| `NULLIF` before numeric cast | `NULLIF(attributes->>'pflanzjahr','')::int` | Source encodes missing numbers as empty strings |
