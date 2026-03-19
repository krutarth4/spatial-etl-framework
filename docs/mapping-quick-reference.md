# Mapping Strategies - Quick Reference Card

One-page cheat sheet for choosing and configuring mapping strategies.

---

## Strategy Decision Tree

```
Do you need to map enrichment data to base graph?
│
├─ NO → Use strategy: none
│
└─ YES → Is it a spatial relationship?
    │
    ├─ NO (join by ID/attribute)
    │   └─ Use: attribute_join
    │
    └─ YES → What kind of relationship?
        │
        ├─ Single nearest feature
        │   └─ Use: nearest_neighbour (knn)
        │
        ├─ K nearest features
        │   └─ Use: nearest_k
        │
        ├─ All features within distance
        │   │
        │   ├─ Need individual rows → Use: within_distance
        │   └─ Need aggregated (count/array/json) → Use: aggregate_within_distance
        │
        ├─ Spatial intersection
        │   └─ Use: intersection
        │
        └─ Complex/custom logic
            └─ Use: custom
```

---

## Strategy Cheat Sheet

| Strategy | When to Use | Key Config |
|----------|-------------|------------|
| `none` | No mapping needed | `enable: false` |
| `custom` | Fully custom SQL logic | Python: `mapping_db_query()` |
| `sql_template` | Template with placeholders | `config.sql: "INSERT..."` |
| `nearest_neighbour` | Find 1 nearest | `base/enrichment_geometry_column` |
| `nearest_k` | Find K nearest | `k: 5` |
| `within_distance` | All within distance (1-to-many) | `max_distance: 100` |
| `aggregate_within_distance` | All within distance (1-to-1 aggregated) | `max_distance: 50`, `aggregation_type` |
| `intersection` | Spatial overlap | `join_condition_sql` (optional) |
| `attribute_join` | Join by ID column | `link_on.base_column`, `join_type` |

---

## Minimal Config Templates

### Nearest Neighbor
```yaml
mapping:
  enable: true
  strategy:
    type: knn
  config:
    base_geometry_column: geometry
    enrichment_geometry_column: point
  table_name: my_mapping
  table_schema: my_schema
  base_table:
    table_name: ways_base
    table_schema: my_schema
```

### K-Nearest
```yaml
strategy:
  type: nearest_k
config:
  k: 5
  base_geometry_column: geometry
  enrichment_geometry_column: point
```

### Aggregate Within Buffer
```yaml
strategy:
  type: aggregate_within_distance
config:
  max_distance: 50
  aggregation_type: jsonb_agg  # or count, array_agg
  aggregation_column: feature_id
  base_geometry_column: geometry_25833
  enrichment_geometry_column: geometry_25833
```

### Join by ID
```yaml
strategy:
  type: attribute_join
  link_on:
    base_column: osm_id
    mapping_column: external_id
config:
  join_type: INNER  # or LEFT, RIGHT
```

---

## Common Config Options

### Geometry & Distance
```yaml
config:
  base_geometry_column: geometry
  enrichment_geometry_column: point
  distance_sql: ST_Distance({base_geometry}::geography, {enrichment_geometry}::geography)
  max_distance: 100
```

### Filtering
```yaml
config:
  base_filter_sql: "highway IN ('primary', 'secondary')"
  enrichment_filter_sql: "status = 'active'"
```

### Custom Columns
```yaml
config:
  select_columns:
    - expression: "ST_Length({base_geometry})"
      alias: road_length
    - "{enrichment_alias}.attribute_name"
```

### Insert Specification
```yaml
config:
  insert:
    columns: [way_id, station_id, distance]
    conflict_columns: [way_id]
    update_columns: [station_id, distance]
```

---

## Aggregation Types

| Type | Output | Example |
|------|--------|---------|
| `jsonb_agg` | JSON array | `[1, 2, 3]` |
| `array_agg` | PG array | `{1, 2, 3}` |
| `count` | Integer | `42` |
| `avg` | Numeric | `12.5` |
| `sum` | Numeric | `150` |
| `min` / `max` | Value | `5` |
| Custom | Any | `jsonb_build_object(...)` |

---

## SQL Placeholders

Use in `distance_sql`, `order_by_sql`, `join_condition_sql`, `select_columns`:

| Placeholder | Example Value |
|-------------|---------------|
| `{base_geometry}` | `b.geometry` |
| `{enrichment_geometry}` | `e.point` |
| `{base_alias}` | `b` |
| `{enrichment_alias}` | `e` |
| `{base_geometry_column}` | `geometry` |
| `{enrichment_geometry_column}` | `point` |
| `{max_distance}` | `100` |

---

## Performance Tips

✅ **DO**:
- Add GIST indexes on geometry columns
- Use projected CRS (e.g., EPSG:25833) for distance
- Add `base_filter_sql` to reduce rows
- Use `enrichment_filter_sql` to pre-filter
- Test on small dataset first

❌ **DON'T**:
- Use geographic CRS for frequent distance calculations
- Set `max_distance` too large
- Forget indexes
- Use `select_all_enrichment: true` with large tables

---

## Common Patterns

### Pattern 1: Nearest Station with Metadata
```yaml
strategy:
  type: knn
config:
  select_columns:
    - expression: "ST_Distance(...)"
      alias: distance_km
    - expression: "{enrichment_alias}.station_name"
      alias: name
```

### Pattern 2: Count Features in Buffer
```yaml
strategy:
  type: aggregate_within_distance
config:
  max_distance: 100
  aggregation_type: count
  aggregation_column: id
  aggregation_alias: feature_count
```

### Pattern 3: Collect Features as JSON
```yaml
strategy:
  type: aggregate_within_distance
config:
  max_distance: 50
  aggregation_type: jsonb_agg
  aggregation_column: id
  aggregation_alias: nearby_features
```

### Pattern 4: Find Top 3 Closest
```yaml
strategy:
  type: nearest_k
config:
  k: 3
  order_by_sql: ST_Distance({base_geometry}::geography, {enrichment_geometry}::geography)
```

---

## Troubleshooting

| Error | Solution |
|-------|----------|
| "requires max_distance" | Add `max_distance` to config |
| "requires base_join_column" | Add `link_on.base_column` |
| Slow performance | Check indexes, add filters |
| Wrong column names | Check `alias` in `select_columns` |
| NULL geometries | Add `enrichment_filter_sql: "geometry IS NOT NULL"` |

---

## Full Documentation

- **Complete Guide**: [mapping-strategies-reference.md](mapping-strategies-reference.md)
- **Migration Example**: [migration-example-tree-mapper.md](migration-example-tree-mapper.md)
- **Mapper Guide**: [mapper-README.md](mapper-README.md)
- **Config Reference**: [config-README.md](config-README.md)

---

## Example: Complete Tree Mapping Config

```yaml
- name: tree
  enable: true
  class_name: tree
  mapping:
    enable: true
    strategy:
      type: aggregate_within_distance
      description: "Collect all trees within 50m of each road"
    config:
      max_distance: 50
      base_geometry_column: geometry_25833
      enrichment_geometry_column: geometry_25833
      aggregation_type: jsonb_build_object
      aggregation_alias: trees
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
      insert:
        columns: [way_id, trees]
        conflict_columns: [way_id]
        update_columns: [trees]
    table_name: tree_mapping
    table_schema: test_osm_base_graph
    base_table:
      table_name: ways_base
      table_schema: test_osm_base_graph
```

**Result**: Zero Python code needed! 🎉
