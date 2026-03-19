# Migration Example: Tree Mapper

This document shows how to migrate the Tree Mapper from custom SQL to the new `aggregate_within_distance` strategy.

## Before: Custom SQL Approach

### Config (`config.yaml`)
```yaml
- name: tree
  enable: true
  class_name: tree
  mapping:
    enable: true
    strategy:
      type: custom  # Uses mapper's mapping_db_query()
    table_name: tree_mapping
    table_schema: test_osm_base_graph
    base_table:
      table_name: ways_base
      table_schema: test_osm_base_graph
```

### Mapper Code (`data_mappers/treeMapper.py`)
```python
class TreeMapper(DataSourceABCImpl):
    def mapping_db_query(self) -> None | str:
        base = self.data_source_config.mapping.base_table
        staging = self.data_source_config.storage.staging
        mapping = self.data_source_config.mapping

        sql = f"""
                INSERT INTO {mapping.table_schema}.{mapping.table_name} (way_id, trees)
                SELECT
                    w.id AS way_id,
                    COALESCE(
                        jsonb_agg(
                            jsonb_build_object(
                                'tree_id', t.id,
                                'source_id', t.source_id,
                                'distance_m', ST_Distance(
                                    t.geometry_25833,
                                    w.geometry_25833
                                )
                            )
                            ORDER BY ST_Distance(
                                t.geometry_25833,
                                w.geometry_25833
                            )
                        ) FILTER (WHERE t.id IS NOT NULL),
                        '[]'::jsonb
                    ) AS trees
                FROM {base.table_schema}.{base.table_name} w
                LEFT JOIN {staging.table_schema}.{staging.table_name} t
                  ON t.geometry_25833 && ST_Expand(w.geometry_25833, 50)
                 AND ST_DWithin(
                        t.geometry_25833,
                        w.geometry_25833,
                        50
                     )
                GROUP BY w.id
                ON CONFLICT (way_id)
                DO UPDATE SET trees = EXCLUDED.trees;
            """
        return sql
```

**Problems**:
1. ❌ Requires writing Python code for simple spatial aggregation
2. ❌ SQL is hardcoded - difficult to adjust parameters
3. ❌ Not reusable for other similar datasources
4. ❌ Table references are manually formatted
5. ❌ Conflict handling must be manually specified

---

## After: Config-Based Approach

### Updated Config (`config.yaml`)
```yaml
- name: tree
  enable: true
  class_name: tree
  mapping:
    enable: true
    strategy:
      type: aggregate_within_distance  # NEW: Built-in strategy
      description: "Aggregate all trees within 50m of each road segment"
    config:
      # Distance configuration
      max_distance: 50  # meters

      # Geometry columns (both in EPSG:25833)
      base_geometry_column: geometry_25833
      enrichment_geometry_column: geometry_25833

      # Aggregation configuration
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

      # Insert specification
      insert:
        columns: [way_id, trees]
        conflict_columns: [way_id]
        update_columns: [trees]

    table_name: tree_mapping
    table_schema: test_osm_base_graph
    base_table:
      table_name: ways_base
      table_schema: test_osm_base_graph

  storage:
    staging:
      table_name: tree_staging
      table_schema: test_osm_base_graph
      table_class: TreeStagingTable
```

### Simplified Mapper Code (`data_mappers/treeMapper.py`)
```python
class TreeMapper(DataSourceABCImpl):
    # mapping_db_query() is NO LONGER NEEDED!
    # The strategy auto-generates the SQL

    def read_file_content(self, path: str):
        # Only data reading logic remains
        gdf = gpd.read_file(path, engine="pyogrio")

        if gdf.crs.to_epsg() != 25833:
            gdf = gdf.to_crs(25833)

        gdf["geometry_wkb"] = gdf.geometry.apply(
            lambda g: g.wkb_hex if g else None
        )

        records = []
        for row in gdf.to_dict(orient="records"):
            records.append({
                "source_id": row.get("gisid"),
                "attributes": {k: v for k, v in row.items()},
                "geometry_25833": row.get("geometry_wkb"),
            })

        return records
```

**Benefits**:
1. ✅ No SQL code in Python - fully declarative config
2. ✅ Easy to adjust parameters (distance, aggregation, columns)
3. ✅ Reusable pattern for other aggregation scenarios
4. ✅ Automatic table reference handling
5. ✅ Built-in conflict resolution from config

---

## Variations

### Variation 1: Count trees instead of collecting details
```yaml
config:
  max_distance: 50
  aggregation_type: count
  aggregation_column: id
  aggregation_alias: tree_count
```

### Variation 2: Simple array of tree IDs
```yaml
config:
  max_distance: 50
  aggregation_type: array_agg
  aggregation_column: id
  aggregation_alias: tree_ids
```

### Variation 3: Add tree count alongside JSONB array
```yaml
config:
  max_distance: 50
  aggregation_type: jsonb_agg
  aggregation_column: id
  aggregation_alias: tree_ids
  select_columns:
    - expression: "COUNT({enrichment_alias}.id)"
      alias: tree_count
    - expression: "AVG({enrichment_alias}.height)"
      alias: avg_tree_height_m
```

### Variation 4: Filter only mature trees
```yaml
config:
  max_distance: 50
  enrichment_filter_sql: "height > 5.0"  # Only trees taller than 5m
  aggregation_type: jsonb_agg
```

---

## Testing the Migration

### Step 1: Update config.yaml
Replace the tree datasource config with the new version above.

### Step 2: Simplify mapper
Remove the `mapping_db_query()` method from `treeMapper.py`.

### Step 3: Test mapping
```bash
# Run just the tree datasource
python run.py  # or your execution command
```

### Step 4: Verify results
```sql
-- Check mapping table structure
SELECT * FROM test_osm_base_graph.tree_mapping LIMIT 5;

-- Verify tree counts
SELECT
    way_id,
    jsonb_array_length(trees) as tree_count,
    trees
FROM test_osm_base_graph.tree_mapping
WHERE jsonb_array_length(trees) > 0
LIMIT 10;

-- Check distance calculations
SELECT
    way_id,
    tree->>'tree_id' as tree_id,
    (tree->>'distance_m')::float as distance_m
FROM test_osm_base_graph.tree_mapping,
     jsonb_array_elements(trees) as tree
WHERE jsonb_array_length(trees) > 0
ORDER BY distance_m DESC
LIMIT 20;
```

---

## Performance Comparison

| Aspect | Custom SQL | New Strategy |
|--------|------------|--------------|
| Development time | ~30 min | ~5 min |
| Code lines | ~35 lines Python | 0 lines Python, ~25 lines YAML |
| Maintainability | Low (SQL in strings) | High (declarative) |
| Reusability | None | High |
| Performance | Same | Same (identical SQL generated) |
| Debugging | Hard (Python + SQL) | Easy (config validation) |

---

## Migration Checklist

- [ ] Backup current `config.yaml`
- [ ] Update datasource config with new strategy
- [ ] Add `config.max_distance`
- [ ] Add `config.aggregation_type` and `config.aggregation_alias`
- [ ] Add `config.base_geometry_column` and `config.enrichment_geometry_column`
- [ ] Add `config.insert` specification
- [ ] Remove `mapping_db_query()` from mapper class
- [ ] Test with small dataset
- [ ] Verify mapping table output
- [ ] Check performance on full dataset
- [ ] Update documentation

---

## Rollback Plan

If you need to revert:

1. Restore original `config.yaml` from backup
2. Restore original `treeMapper.py` with `mapping_db_query()`
3. Re-run the datasource

The mapping table schema doesn't change, so existing data remains valid.

---

## Next Steps

Once tree mapping works with the new strategy:

1. Migrate `pleasantBicyclingMapper.py` (similar pattern)
2. Migrate `airQualityDataMapper.py` if applicable
3. Document your own custom strategies in a team wiki
4. Share successful patterns with team
