# Mapping Strategies Reference

This document provides a comprehensive guide to all available mapping strategies in the Modular Data Pipeline (MDP) and how to configure them.

## Overview

Mapping strategies define how enrichment data (weather stations, trees, air quality, etc.) gets linked to the base graph (`ways_base` table containing road segments). The pipeline supports multiple strategies ranging from simple spatial joins to complex aggregations.

## Strategy Types

### 1. Control Strategies

#### `none`
Skips mapping entirely. Use when you only need staging/enrichment without linking to base graph.

```yaml
mapping:
  enable: false
  strategy:
    type: none
```

#### `custom`
Delegates to mapper class's `mapping_db_query()` method. Use when you need full SQL control.

```yaml
mapping:
  enable: true
  strategy:
    type: custom
```

Then in your mapper class:
```python
def mapping_db_query(self) -> str:
    return """
        INSERT INTO mapping_table (way_id, custom_field)
        SELECT b.id, e.value
        FROM ways_base b
        JOIN enrichment e ON ...
    """
```

#### `sql_template`
Uses a SQL template string from config with placeholder substitution.

```yaml
mapping:
  enable: true
  strategy:
    type: sql_template
  config:
    sql: |
      INSERT INTO {mapping_schema}.{mapping_table} (way_id, station_id)
      SELECT b.id, e.{link_mapping_column}
      FROM {base_schema}.{base_table} b
      JOIN {enrichment_schema}.{enrichment_table} e
        ON ST_DWithin(b.geometry, e.point, 1000)
```

Available placeholders:
- `{datasource_name}`
- `{mapping_table}`, `{mapping_schema}`
- `{staging_table}`, `{staging_schema}`
- `{enrichment_table}`, `{enrichment_schema}`
- `{base_table}`, `{base_schema}`
- `{joins_on}`
- `{strategy_type}`
- `{link_mapping_column}`, `{link_base_column}`, `{link_basis}`

---

### 2. Spatial Strategies (Registry-Backed)

These strategies auto-generate optimized PostGIS SQL based on configuration.

#### `nearest_neighbour` / `knn` / `nearest_station`
Maps each base geometry to its **single nearest** enrichment feature.

**Use Case**: Assign each road segment to its nearest weather station.

```yaml
mapping:
  enable: true
  strategy:
    type: knn
    description: "map each road to nearest weather station"
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
  table_name: station_mapping
  table_schema: test_osm_base_graph
```

**Generated SQL Pattern**:
```sql
SELECT
    b.id AS way_id,
    e.dwd_station_id AS dwd_station_id,
    ST_Distance(b.geometry::geography, e.point::geography) AS distance,
    MOD(...) AS bearing_degree
FROM ways_base b
JOIN LATERAL (
    SELECT *
    FROM enrichment e
    ORDER BY b.geometry <-> e.point
    LIMIT 1
) e ON TRUE
```

**Config Options**:
- `base_geometry_column` (default: `geometry`) - column in base table
- `enrichment_geometry_column` (default: `geometry`) - column in enrichment table
- `distance_sql` (optional) - custom distance calculation
- `order_by_sql` (optional) - custom ordering (default: `<->` operator)
- `select_columns` (optional) - additional computed columns
- `base_filter_sql` (optional) - WHERE clause for base table
- `enrichment_filter_sql` (optional) - WHERE clause for enrichment

---

#### `within_distance`
Maps base geometries to **all** enrichment features within a maximum distance.

**Use Case**: Find all air quality sensors within 500m of each road segment.

```yaml
mapping:
  enable: true
  strategy:
    type: within_distance
  config:
    max_distance: 500  # Required
    base_geometry_column: geometry_25833
    enrichment_geometry_column: geometry_25833
    join_condition_sql: ST_DWithin({base_geometry}, {enrichment_geometry}, {max_distance})
  table_name: air_quality_mapping
  table_schema: test_osm_base_graph
```

**Generated SQL Pattern**:
```sql
SELECT
    b.id AS way_id,
    e.sensor_id,
    ST_Distance(b.geometry_25833, e.geometry_25833) AS distance
FROM ways_base b
JOIN enrichment e
    ON ST_DWithin(b.geometry_25833, e.geometry_25833, 500)
```

**Config Options**:
- `max_distance` (required) - maximum distance threshold
- `join_condition_sql` (optional) - override default ST_DWithin condition
- All options from `nearest_neighbour` also apply

---

#### `intersection`
Maps spatially intersecting features (overlapping polygons, lines crossing, etc.).

**Use Case**: Link road segments to administrative boundaries they intersect.

```yaml
mapping:
  enable: true
  strategy:
    type: intersection
  config:
    base_geometry_column: geometry
    enrichment_geometry_column: boundary_geom
    join_condition_sql: ST_Intersects({base_geometry}, {enrichment_geometry})
  table_name: boundary_mapping
  table_schema: test_osm_base_graph
```

**Generated SQL Pattern**:
```sql
SELECT
    b.id AS way_id,
    e.boundary_id
FROM ways_base b
JOIN enrichment e
    ON ST_Intersects(b.geometry, e.boundary_geom)
```

---

#### `nearest_k` / `k_nearest` / `knn_multiple` ✨ NEW
Maps each base geometry to **K nearest** enrichment features (not just 1).

**Use Case**: Find the 5 nearest parking lots to each road segment.

```yaml
mapping:
  enable: true
  strategy:
    type: nearest_k
  config:
    k: 5  # Number of neighbors
    base_geometry_column: geometry
    enrichment_geometry_column: point
    order_by_sql: ST_Distance({base_geometry}::geography, {enrichment_geometry}::geography)
    select_columns:
      - expression: "{enrichment_alias}.parking_id"
        alias: parking_id
      - expression: "{enrichment_alias}.capacity"
        alias: capacity
  table_name: parking_knn_mapping
  table_schema: test_osm_base_graph
```

**Generated SQL Pattern**:
```sql
SELECT
    b.id AS way_id,
    e.parking_id,
    e.capacity,
    ST_Distance(b.geometry::geography, e.point::geography) AS distance
FROM ways_base b
JOIN LATERAL (
    SELECT *
    FROM enrichment e
    ORDER BY b.geometry <-> e.point
    LIMIT 5  -- K neighbors
) e ON TRUE
```

**Config Options**:
- `k` (default: 1) - number of nearest neighbors to find
- All options from `nearest_neighbour` also apply

---

#### `aggregate_within_distance` / `buffer_aggregate` ✨ NEW
Aggregates **all** enrichment features within a buffer distance into a single row per base geometry.

**Use Case**: Count trees within 50m of each road, or collect all nearby POIs into a JSON array.

```yaml
mapping:
  enable: true
  strategy:
    type: aggregate_within_distance
  config:
    max_distance: 50  # Required - buffer distance
    aggregation_type: jsonb_agg  # Options: jsonb_agg, array_agg, count, avg, sum, min, max
    aggregation_column: tree_id
    aggregation_alias: nearby_trees
    base_geometry_column: geometry_25833
    enrichment_geometry_column: geometry_25833
    select_columns:
      - expression: "COUNT({enrichment_alias}.tree_id)"
        alias: tree_count
  table_name: tree_aggregate_mapping
  table_schema: test_osm_base_graph
```

**Generated SQL Pattern**:
```sql
SELECT
    b.id AS way_id,
    COALESCE(jsonb_agg(e.tree_id), '[]'::jsonb) AS nearby_trees,
    COUNT(e.tree_id) AS tree_count
FROM ways_base b
LEFT JOIN enrichment e
    ON ST_DWithin(b.geometry_25833, e.geometry_25833, 50)
GROUP BY b.id
```

**Config Options**:
- `max_distance` (required) - buffer radius
- `aggregation_type` (default: `jsonb_agg`) - aggregation function:
  - `jsonb_agg` - JSON array of values
  - `array_agg` - PostgreSQL array
  - `count` - count of features
  - `avg`, `sum`, `min`, `max` - numeric aggregations
  - Custom expression starting with `jsonb_build_object`
- `aggregation_column` (required) - column to aggregate
- `aggregation_alias` (optional) - output column name
- `aggregation_expression` (optional) - full custom aggregation SQL
- All options from `nearest_neighbour` also apply

**Advanced Example - Custom JSONB Aggregation**:
```yaml
config:
  aggregation_type: jsonb_build_object
  aggregation_expression: |
    COALESCE(
      jsonb_agg(
        jsonb_build_object(
          'tree_id', {enrichment_alias}.tree_id,
          'species', {enrichment_alias}.species,
          'height_m', {enrichment_alias}.height,
          'distance_m', ST_Distance(
            {enrichment_alias}.geometry_25833,
            {base_geometry}
          )
        )
        ORDER BY ST_Distance({enrichment_alias}.geometry_25833, {base_geometry})
      ) FILTER (WHERE {enrichment_alias}.tree_id IS NOT NULL),
      '[]'::jsonb
    )
```

---

### 3. Non-Spatial Strategies

#### `attribute_join` / `id_join` / `key_join` ✨ NEW
Standard SQL JOIN based on shared attribute columns (no geometry involved).

**Use Case**: Link road segments to external datasets using OSM IDs or road names.

```yaml
mapping:
  enable: true
  strategy:
    type: attribute_join
    link_on:
      base_column: osm_id
      mapping_column: external_osm_id
  config:
    join_type: INNER  # Options: INNER, LEFT, RIGHT
    select_all_enrichment: true  # Include all enrichment columns
  table_name: external_data_mapping
  table_schema: test_osm_base_graph
```

**Alternative - Select Specific Columns**:
```yaml
config:
  join_type: LEFT
  select_columns:
    - traffic_volume
    - road_quality_index
    - expression: "{enrichment_alias}.speed_limit * 1.60934"
      alias: speed_limit_kmh
```

**Generated SQL Pattern**:
```sql
SELECT
    b.id AS way_id,
    e.*
FROM ways_base b
INNER JOIN enrichment e
    ON b.osm_id = e.external_osm_id
```

**Config Options**:
- `base_join_column` or `link_on.base_column` (required) - column in base table
- `enrichment_join_column` or `link_on.mapping_column` (required) - column in enrichment table
- `join_type` (default: `INNER`) - `INNER`, `LEFT`, or `RIGHT`
- `select_all_enrichment` (default: false) - include all enrichment columns
- `select_columns` (optional) - specific columns to select (list of strings or dicts)
- `base_filter_sql` (optional) - WHERE clause for base table
- `enrichment_filter_sql` (optional) - WHERE clause for enrichment

---

## Common Configuration Options

### Geometry Columns
- `base_geometry_column` - name of geometry column in base table (default: `geometry`)
- `enrichment_geometry_column` - name of geometry column in enrichment table (default: `geometry`)

### Filtering
- `base_filter_sql` - WHERE clause to filter base table rows
  ```yaml
  base_filter_sql: "highway IN ('primary', 'secondary')"
  ```
- `enrichment_filter_sql` - WHERE clause to filter enrichment table rows
  ```yaml
  enrichment_filter_sql: "status = 'active'"
  ```

### Custom Expressions
- `select_columns` - additional computed columns:
  ```yaml
  select_columns:
    - expression: "ST_Length({base_geometry}::geography)"
      alias: road_length_m
    - "{enrichment_alias}.attribute_name"  # Simple column reference
  ```

### Placeholders in SQL Templates
Available in `distance_sql`, `order_by_sql`, `join_condition_sql`, `select_columns`:
- `{base_geometry}` - fully qualified base geometry column
- `{enrichment_geometry}` - fully qualified enrichment geometry column
- `{base_alias}` - table alias for base table (usually `b`)
- `{enrichment_alias}` - table alias for enrichment table (usually `e`)
- `{base_geometry_column}` - geometry column name only
- `{enrichment_geometry_column}` - geometry column name only
- `{max_distance}` - max distance value from config

---

## Migration Guide: Converting Custom SQL to Built-in Strategies

### Example 1: Tree Mapper (Before)

**Old Custom SQL** (`treeMapper.py:44`):
```python
def mapping_db_query(self) -> str:
    return f"""
        INSERT INTO {mapping.table_schema}.{mapping.table_name} (way_id, trees)
        SELECT
            w.id AS way_id,
            COALESCE(
                jsonb_agg(
                    jsonb_build_object(
                        'tree_id', t.id,
                        'source_id', t.source_id,
                        'distance_m', ST_Distance(t.geometry_25833, w.geometry_25833)
                    )
                    ORDER BY ST_Distance(t.geometry_25833, w.geometry_25833)
                ) FILTER (WHERE t.id IS NOT NULL),
                '[]'::jsonb
            ) AS trees
        FROM {base.table_schema}.{base.table_name} w
        LEFT JOIN {staging.table_schema}.{staging.table_name} t
          ON ST_DWithin(t.geometry_25833, w.geometry_25833, 50)
        GROUP BY w.id
    """
```

**New Config-Based Approach**:
```yaml
mapping:
  enable: true
  strategy:
    type: aggregate_within_distance
  config:
    max_distance: 50
    aggregation_type: jsonb_build_object
    aggregation_expression: |
      COALESCE(
        jsonb_agg(
          jsonb_build_object(
            'tree_id', {enrichment_alias}.id,
            'source_id', {enrichment_alias}.source_id,
            'distance_m', ST_Distance(
              {enrichment_alias}.geometry_25833,
              {base_geometry}
            )
          )
          ORDER BY ST_Distance({enrichment_alias}.geometry_25833, {base_geometry})
        ) FILTER (WHERE {enrichment_alias}.id IS NOT NULL),
        '[]'::jsonb
      )
    aggregation_alias: trees
    base_geometry_column: geometry_25833
    enrichment_geometry_column: geometry_25833
  table_name: tree_mapping
  table_schema: test_osm_base_graph
```

**Result**: No Python code needed - mapper class can be empty!

---

### Example 2: Weather Station Mapper (Before)

**Old Approach**: Uses built-in `knn` but requires Python override for insert spec.

**New Approach**: Same config works, but now more flexible:
```yaml
mapping:
  enable: true
  strategy:
    type: knn
  config:
    base_geometry_column: geometry
    enrichment_geometry_column: point
    distance_sql: ST_Distance({base_geometry}::geography, {enrichment_geometry}::geography)
    order_by_sql: ST_Distance({base_geometry}::geography, {enrichment_geometry}::geography)
    select_columns:
      - expression: |
          MOD(
            (DEGREES(ST_Azimuth(ST_StartPoint({base_geometry}), ST_EndPoint({base_geometry}))) + 360)::NUMERIC,
            360
          )
        alias: bearing_degree
    insert:
      columns: [way_id, dwd_station_id, distance, bearing_degree]
      conflict_columns: [way_id]
      update_columns: [dwd_station_id, distance, bearing_degree]
```

---

## Best Practices

1. **Start with built-in strategies** - avoid custom SQL unless absolutely necessary
2. **Use `sql_template`** for slight variations of standard patterns
3. **Use `custom`** only for complex multi-step logic or uncommon operations
4. **Test with small datasets first** - some strategies can be expensive on large data
5. **Add indexes** - ensure geometry columns have GIST indexes
6. **Choose appropriate SRID** - use projected CRS (e.g., 25833) for distance calculations
7. **Use filters** - `base_filter_sql` and `enrichment_filter_sql` to reduce processing

---

## Performance Considerations

| Strategy | Performance | Best For | Avoid When |
|----------|-------------|----------|------------|
| `nearest_neighbour` | Fast (uses `<->` operator) | Small enrichment tables | N/A |
| `nearest_k` | Moderate (LATERAL join) | K is small (<10) | K is very large |
| `within_distance` | Fast with GIST index | Known max distance | Unbounded distances |
| `intersection` | Fast with GIST index | Polygon overlays | Point-to-point |
| `aggregate_within_distance` | Moderate (GROUP BY) | Moderate feature counts | Millions of features per buffer |
| `attribute_join` | Very fast (B-tree index) | Non-spatial joins | N/A |

---

## Troubleshooting

### Common Errors

1. **"Strategy requires max_distance"**
   - Solution: Add `max_distance` to `mapping.config`

2. **"attribute_join requires base_join_column"**
   - Solution: Set `link_on.base_column` and `link_on.mapping_column`

3. **Slow mapping performance**
   - Check indexes: `CREATE INDEX ON enrichment USING GIST (geometry);`
   - Add filters to reduce row counts
   - Use projected CRS for distance calculations

4. **Unexpected column names in mapping table**
   - Check `select_columns` and `alias` fields
   - Use `aggregation_alias` for aggregate strategies

---

## Next Steps

- See [config-README.md](config-README.md) for full config schema
- See [mapper-README.md](mapper-README.md) for mapper implementation guide
- Explore existing mappers in `/data_mappers/` for examples
