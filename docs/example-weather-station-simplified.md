# Example: Simplifying Weather Station Mapper

This example shows how the weather station mapper can be further simplified using the enhanced mapping system.

## Current Implementation

### Config (`config.yaml:200-286`)
```yaml
- name: "weather_station_bright_sky"
  enable: true
  class_name: weatherStation
  mapping:
    enable: true
    joins_on: dwd_station_id
    strategy:
      type: knn
      description: map each base way to its nearest weather station
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
    table_name: dwd_station_locations_mapping
    table_schema: test_osm_base_graph
```

### Mapper Code (`weatherStationMapper.py:78-84`)
```python
def get_mapping_insert_spec(self) -> MappingInsertSpec:
    self.logger.info("Mapping DWD stations to links using strategy-backed SQL builder")
    return MappingInsertSpec(
        columns=["way_id", "dwd_station_id", "distance", "bearing_degree"],
        conflict_columns=["way_id"],
        update_columns=["dwd_station_id", "distance", "bearing_degree"],
    )
```

**Issue**: Still requires Python method override for insert specification.

---

## Simplified Implementation

### Updated Config
```yaml
- name: "weather_station_bright_sky"
  enable: true
  class_name: weatherStation
  mapping:
    enable: true
    strategy:
      type: knn
      description: map each base way to its nearest weather station
      link_on:
        mapping_column: dwd_station_id
        base_column: dwd_station_id
        basis: nearest_by_distance
    config:
      # Geometry columns
      base_geometry_column: geometry
      enrichment_geometry_column: point

      # Distance calculation
      distance_sql: ST_Distance({base_geometry}::geography, {enrichment_geometry}::geography)
      order_by_sql: ST_Distance({base_geometry}::geography, {enrichment_geometry}::geography)

      # Additional computed columns
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

      # Insert specification (NEW - moved from Python to config!)
      insert:
        columns: [way_id, dwd_station_id, distance, bearing_degree]
        conflict_columns: [way_id]
        update_columns: [dwd_station_id, distance, bearing_degree]

    table_name: dwd_station_locations_mapping
    table_schema: test_osm_base_graph
    base_table:
      table_name: ways_base
      table_schema: test_osm_base_graph
```

### Simplified Mapper Code
```python
class WeatherStationMapper(DataSourceABCImpl):

    def source_filter(self, data: list[dict]) -> list[dict]:
        """Custom filter for DWD stations."""
        data = data[0]["sources"]
        filtered = [
            row for row in data
            if row.get("observation_type") == "forecast" and int(row.get("last_record")[:4]) >= 2024
        ]
        self.logger.info(f"Filtered {len(data)} → {len(filtered)} rows")
        return filtered

    def enrichment_db_query(self) -> None | str:
        staging = self.data_source_config.storage.staging
        enrichment = self.data_source_config.storage.enrichment
        sql = f"""
        UPDATE {enrichment.table_schema}.{enrichment.table_name} e
        SET point = ST_SetSRID(
                    ST_MakePoint(s.lon, s.lat),
                    4326
                )
                from {staging.table_schema}.{staging.table_name} s
        WHERE e.dwd_station_id = s.dwd_station_id
            AND e.point IS NULL
        """
        return sql

    # get_mapping_insert_spec() is NO LONGER NEEDED!
    # The insert spec is now in config.yaml
```

**Result**: Removed `get_mapping_insert_spec()` method - everything is in config!

---

## Benefits

1. ✅ **All mapping config in one place** (YAML)
2. ✅ **No Python override needed** for insert specification
3. ✅ **Easier to modify** - just edit config, no code changes
4. ✅ **Consistent pattern** across all datasources
5. ✅ **Better for non-Python users** - everything is declarative

---

## Alternative: Even Simpler with Defaults

If you're okay with default insert behavior (no conflict handling), you can simplify further:

```yaml
config:
  base_geometry_column: geometry
  enrichment_geometry_column: point
  distance_sql: ST_Distance({base_geometry}::geography, {enrichment_geometry}::geography)
  order_by_sql: ST_Distance({base_geometry}::geography, {enrichment_geometry}::geography)
  select_columns:
    - expression: MOD((DEGREES(ST_Azimuth(...)))::NUMERIC, 360)
      alias: bearing_degree
  # No insert spec = default INSERT without conflict handling
```

---

## Migration Steps

1. ✅ Add `config.insert` section to weather station config
2. ✅ Remove `get_mapping_insert_spec()` from `weatherStationMapper.py`
3. ✅ Test mapping
4. ✅ Verify results in database

---

## Testing

```sql
-- Verify mapping table has all expected columns
SELECT way_id, dwd_station_id, distance, bearing_degree
FROM test_osm_base_graph.dwd_station_locations_mapping
LIMIT 10;

-- Check distance calculations
SELECT
    way_id,
    dwd_station_id,
    distance,
    bearing_degree
FROM test_osm_base_graph.dwd_station_locations_mapping
ORDER BY distance DESC
LIMIT 10;

-- Verify no duplicate way_ids (conflict handling works)
SELECT way_id, COUNT(*)
FROM test_osm_base_graph.dwd_station_locations_mapping
GROUP BY way_id
HAVING COUNT(*) > 1;
-- Should return 0 rows
```

---

## Comparison

| Aspect | Before | After |
|--------|--------|-------|
| Config lines | ~30 | ~40 |
| Python lines | ~12 | 0 |
| Total complexity | Medium | Low |
| Ease of modification | Need code change | Just config |
| Consistency | Mixed (config + code) | Pure config |

---

## Next Steps

Apply this pattern to other mappers:
1. ✅ Weather station mapper (shown above)
2. 🔄 Tree mapper (see [migration-example-tree-mapper.md](migration-example-tree-mapper.md))
3. 🔄 Pleasant bicycling mapper
4. 🔄 Air quality mapper

**Goal**: Eventually have all mappers use pure config-based mapping!
