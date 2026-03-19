# Mapping System Improvements - Summary

## What Changed

The mapping system has been significantly enhanced to be more **general**, **reusable**, and **config-driven**, reducing the need for custom Python code in mapper classes.

## New Built-in Mapping Strategies

### 1. `nearest_k` (K-Nearest Neighbors)
**Purpose**: Find multiple nearest neighbors instead of just one.

**Use Case**: Find the 5 nearest parking lots to each road segment.

**Before**: Required custom SQL in mapper
**Now**: Pure config
```yaml
mapping:
  strategy:
    type: nearest_k
  config:
    k: 5  # Find 5 neighbors
```

---

### 2. `aggregate_within_distance` (Buffer Aggregation)
**Purpose**: Aggregate all features within a distance into a single row.

**Use Case**: Count or collect all trees within 50m of each road.

**Before**: Required complex SQL with GROUP BY and jsonb_agg
**Now**: Pure config
```yaml
mapping:
  strategy:
    type: aggregate_within_distance
  config:
    max_distance: 50
    aggregation_type: jsonb_agg  # or count, array_agg, avg, etc.
```

**Real Example**: The `treeMapper.py` previously had ~35 lines of custom SQL. Now it needs **0 lines** - everything is in config!

---

### 3. `attribute_join` (Non-Spatial Join)
**Purpose**: Join datasets by shared IDs instead of geometry.

**Use Case**: Link road segments to external traffic data using OSM IDs.

**Before**: Required custom SQL
**Now**: Pure config
```yaml
mapping:
  strategy:
    type: attribute_join
    link_on:
      base_column: osm_id
      mapping_column: external_osm_id
  config:
    join_type: INNER
```

---

## Key Benefits

### 1. Less Code, More Config
**Before**:
- Custom Python SQL in every mapper class
- Hard to modify without code changes
- Not reusable

**After**:
- Everything in `config.yaml`
- Easy to adjust parameters
- Patterns reusable across datasources

### 2. Highly Configurable
All strategies support:
- ✅ Custom geometry columns
- ✅ Filter clauses (base and enrichment)
- ✅ Additional computed columns
- ✅ Custom distance/ordering logic
- ✅ Conflict handling (INSERT ... ON CONFLICT)

### 3. Better Documentation
New comprehensive docs:
- **[mapping-strategies-reference.md](mapping-strategies-reference.md)** - Complete strategy guide (600+ lines)
- **[migration-example-tree-mapper.md](migration-example-tree-mapper.md)** - Real migration example
- Updated **[mapper-README.md](mapper-README.md)** and **[config-README.md](config-README.md)**

---

## Migration Path

### Current State
Your existing mappers still work! Nothing breaks.

### Recommended Next Steps

1. **Try the new strategies** on a test datasource
2. **Migrate tree mapper** (see [migration-example-tree-mapper.md](migration-example-tree-mapper.md))
3. **Migrate other mappers** that use similar patterns
4. **Keep custom SQL** for truly unique cases

### What to Migrate

| Mapper | Current Approach | New Strategy | Effort |
|--------|------------------|--------------|--------|
| `weatherStationMapper` | Custom `get_mapping_insert_spec()` | Already using `knn` - just add `config.insert` | Low |
| `treeMapper` | Custom `mapping_db_query()` | `aggregate_within_distance` | Medium |
| `pleasantBicyclingMapper` | Custom `mapping_db_query()` | `within_distance` or `nearest_k` | Medium |
| `airQualityDataMapper` | Unknown | Likely `within_distance` or `aggregate` | TBD |

---

## Real Example: Tree Mapper Migration

### Before (35 lines of Python)
```python
def mapping_db_query(self) -> None | str:
    return f"""
        INSERT INTO {mapping.table_schema}.{mapping.table_name} (way_id, trees)
        SELECT w.id AS way_id,
               COALESCE(jsonb_agg(...), '[]'::jsonb) AS trees
        FROM {base.table_schema}.{base.table_name} w
        LEFT JOIN {staging.table_schema}.{staging.table_name} t
          ON ST_DWithin(t.geometry_25833, w.geometry_25833, 50)
        GROUP BY w.id
        ON CONFLICT (way_id) DO UPDATE SET trees = EXCLUDED.trees;
    """
```

### After (0 lines of Python, ~20 lines of YAML)
```yaml
mapping:
  enable: true
  strategy:
    type: aggregate_within_distance
  config:
    max_distance: 50
    aggregation_type: jsonb_agg
    aggregation_alias: trees
    base_geometry_column: geometry_25833
    enrichment_geometry_column: geometry_25833
    insert:
      columns: [way_id, trees]
      conflict_columns: [way_id]
```

**Result**: Same SQL generated, zero Python code needed!

---

## Configuration Examples

### Example 1: Simple Nearest Neighbor with Distance
```yaml
mapping:
  strategy:
    type: knn
  config:
    base_geometry_column: geometry
    enrichment_geometry_column: point
```

### Example 2: Count Features Within 100m
```yaml
mapping:
  strategy:
    type: aggregate_within_distance
  config:
    max_distance: 100
    aggregation_type: count
    aggregation_column: sensor_id
    aggregation_alias: sensor_count
```

### Example 3: K-Nearest with Custom Ordering
```yaml
mapping:
  strategy:
    type: nearest_k
  config:
    k: 3
    order_by_sql: ST_Distance({base_geometry}::geography, {enrichment_geometry}::geography)
    select_columns:
      - expression: "{enrichment_alias}.name"
        alias: station_name
```

### Example 4: Join by ID (Non-Spatial)
```yaml
mapping:
  strategy:
    type: attribute_join
    link_on:
      base_column: road_id
      mapping_column: osm_road_id
  config:
    join_type: LEFT
    select_columns: [traffic_volume, speed_limit]
```

---

## Advanced Features

### Aggregation Types Supported
- `jsonb_agg` - JSON array
- `array_agg` - PostgreSQL array
- `count` - Count of features
- `avg`, `sum`, `min`, `max` - Numeric aggregations
- Custom expressions with `jsonb_build_object`

### SQL Placeholders in Templates
Use these in `distance_sql`, `order_by_sql`, `select_columns`, etc.:
- `{base_geometry}` - Full qualified geometry column
- `{enrichment_geometry}` - Full qualified geometry column
- `{base_alias}` - Table alias (usually `b`)
- `{enrichment_alias}` - Table alias (usually `e`)

### Filtering Support
```yaml
config:
  base_filter_sql: "highway IN ('primary', 'secondary')"
  enrichment_filter_sql: "status = 'active' AND height > 5.0"
```

---

## Performance Notes

| Strategy | Performance | Best For |
|----------|-------------|----------|
| `nearest_neighbour` | ⚡ Fast | Small enrichment tables |
| `nearest_k` | 🔶 Moderate | K < 10 |
| `within_distance` | ⚡ Fast (with indexes) | Known max distance |
| `aggregate_within_distance` | 🔶 Moderate | Moderate feature counts |
| `attribute_join` | ⚡ Very Fast | Non-spatial joins |

**Tip**: Always ensure GIST indexes on geometry columns!

---

## Testing Your Changes

### 1. Test Individual Strategy
```bash
# Enable only one datasource in config.yaml
python run.py
```

### 2. Verify Output
```sql
-- Check mapping table
SELECT * FROM test_osm_base_graph.your_mapping_table LIMIT 10;

-- Verify aggregations
SELECT way_id, jsonb_array_length(aggregated_field)
FROM test_osm_base_graph.your_mapping_table
LIMIT 10;
```

### 3. Compare Performance
```sql
-- Check query execution time
EXPLAIN ANALYZE SELECT ...
```

---

## Documentation Reference

| Document | Purpose |
|----------|---------|
| [mapping-strategies-reference.md](mapping-strategies-reference.md) | Complete strategy guide with all config options |
| [migration-example-tree-mapper.md](migration-example-tree-mapper.md) | Step-by-step migration example |
| [mapper-README.md](mapper-README.md) | Mapper implementation guide |
| [config-README.md](config-README.md) | Config schema reference |

---

## Questions & Next Steps

### Common Questions

**Q: Do I have to migrate my existing mappers?**
A: No, they still work. Migrate when convenient.

**Q: What if my mapping is too complex for built-in strategies?**
A: Use `strategy: custom` and keep your SQL in Python.

**Q: Can I combine multiple strategies?**
A: Not directly, but you can use `sql_template` to orchestrate multiple steps.

**Q: How do I debug generated SQL?**
A: Enable debug logging or check the executed SQL in database logs.

### Next Steps

1. **Read** [mapping-strategies-reference.md](mapping-strategies-reference.md)
2. **Try** migrating the tree mapper using [migration-example-tree-mapper.md](migration-example-tree-mapper.md)
3. **Test** new strategies on development data first
4. **Migrate** other mappers gradually
5. **Document** your own patterns for the team

---

## Summary

The mapping system is now **significantly more powerful and easier to use**:

- ✅ **3 new built-in strategies** cover most common scenarios
- ✅ **Highly configurable** through YAML
- ✅ **Less Python code** needed (often 0 lines!)
- ✅ **Better documentation** with examples
- ✅ **Backward compatible** - nothing breaks
- ✅ **Performance optimized** - generates efficient PostGIS SQL

**The goal achieved**: You can now add most new spatial datasets **without writing any mapper Python code** - just configure in YAML!
