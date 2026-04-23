# Mapping Data onto the Street Graph

> **Read this first.** It explains *which* kind of mapping belongs to *which* kind of data, and why. For how to configure each strategy, see [mapping-quick-reference.md](mapping-quick-reference.md) and [mapping-strategies-reference.md](mapping-strategies-reference.md).

---

## 1. What "mapping onto a street" actually means

The framework keeps a base graph — a table (conventionally `ways_base`) where each row is a road segment stored as a `LINESTRING` with OSM attributes. Every datasource you add produces a separate enrichment table (weather, trees, air quality, …). "Mapping" is the final step that links the two: it writes into a `*_mapping` table keyed by `way_id`, so each road segment gains rows that carry the external data.

Which mapping makes sense is not a question of code — it's a question of **geometry**. A weather station, a tree, a 1 km air-quality cell, and a 1 m elevation raster each have a different spatial relationship to a road, and each relationship has a canonical mapping strategy.

---

## 2. Classify your data first

Before picking a strategy, classify the source by its spatial shape and density. This is the decision driver — everything downstream follows.

| Source shape | Density | Example dataset | Natural relationship | Strategy | Reference mapper |
|---|---|---|---|---|---|
| Sparse points | ~10s across a city | DWD weather stations | Each road belongs to its *single nearest* | `nearest_neighbour` / `knn` | [weatherStationMapper.py](../data_mappers/weatherStationMapper.py) |
| Dense points | ~100k across a city | Street trees | *All* points within a buffer, aggregated per road | `aggregate_within_distance` or `sql_template` | [treeMapper.py](../data_mappers/treeMapper.py) |
| Regular grid | 1 km cells | Air quality (NO₂, PM) | Cell the road intersects / is nearest to, latest forecast per cell | `sql_template` with `DISTINCT ON` | [airQualityDataMapper.py](../data_mappers/airQualityDataMapper.py) |
| Continuous raster | 1 m DEM tiles | Elevation | Sample along the road at intervals, or compute per-way stats | `sql_template` (profile) or `custom` (stats) | [elevationMapper.py](../data_mappers/elevationMapper.py), [elevationPythonMapper.py](../data_mappers/elevationPythonMapper.py) |
| Linear features on their own edge IDs | Parallel to OSM graph | Bike speed / pleasant-bicycling | Match by shared edge ID, fall back to nearest spatial | `nearest_neighbour` with `nearest_within_distance` | [pleasantBicyclingMapper.py](../data_mappers/pleasantBicyclingMapper.py) |
| External rows with a shared key | Any | OSM-linked attributes | Plain SQL join on a shared column, no geometry needed | `attribute_join` | [graphMapper.py](../data_mappers/graphMapper.py) |

If your dataset fits none of these, that's a signal you probably want `sql_template` (declare the SQL) or, as a last resort, `custom` (write Python in `mapping_db_query()`).

---

## 3. The six relationships, in detail

Each subsection explains **what the relationship models**, **when it is the right choice**, **failure modes to watch for**, and a minimal config sketch. The full configuration surface for every strategy lives in [mapping-strategies-reference.md](mapping-strategies-reference.md).

### 3.1 Nearest single feature — `nearest_neighbour` / `knn`

**Models:** "Each road segment *belongs to* exactly one feature."

**When it is right:**
- The source is sparse — so much sparser than the road graph that assigning one feature per road is meaningful.
- The source is authoritative for the attribute you want — e.g., a weather station's reading is the reading for every road in its catchment.

**Failure modes:**
- Dense sources (trees, POIs) collapse to one-per-road and lose almost all information. Use an aggregate strategy instead.
- On a projected vs. geographic CRS mismatch, nearest-by-distance silently becomes nearest-by-degree. See [§4](#4-crs-choosing-the-right-coordinate-system).

**Sketch:**
```yaml
strategy:
  type: knn
  link_on:
    mapping_column: dwd_station_id
    basis: nearest_by_distance
config:
  base_geometry_column: geometry
  enrichment_geometry_column: point
```

### 3.2 K nearest features — `nearest_k`

**Models:** "Each road is influenced by its *K closest* features."

**When it is right:**
- You want to interpolate across several stations (e.g., triangulate temperature from the three nearest weather stations) rather than snapping to one.
- Redundancy matters — the single nearest might be offline, so carry the next few.

**Failure modes:**
- Choosing K blind of geometry: with a very sparse source, the 5th-nearest may be far enough away to be noise.
- `nearest_k` expands the mapping table by a factor of K — size the downstream schema accordingly.

**Sketch:**
```yaml
strategy:
  type: nearest_k
config:
  k: 3
  order_by_sql: ST_Distance({base_geometry}::geography, {enrichment_geometry}::geography)
```

### 3.3 All within a buffer, aggregated — `aggregate_within_distance`

**Models:** "For each road, summarize *every* nearby feature into a single row."

**When it is right:**
- Dense point clouds where the quantity of neighbours is itself the signal: tree count, POI density, crash history.
- The question is "how many / which are near" rather than "which is *the* nearest".

**Failure modes:**
- Buffer distance is the single most impactful knob and the easiest to get wrong. For trees the canopy-influence radius is ~20–50 m; for "nearby amenities" 100–300 m. Too large and every road looks the same; too small and most roads carry empty arrays.
- Aggregation choice (`count`, `jsonb_agg`, `avg`, …) determines what ends up in the mapping row. Pick the smallest representation that answers the downstream question.

**Sketch:**
```yaml
strategy:
  type: aggregate_within_distance
config:
  max_distance: 50
  aggregation_type: jsonb_agg
  aggregation_column: id
  base_geometry_column: geometry_25833
  enrichment_geometry_column: geometry_25833
```

The tree mapper's real implementation uses `sql_template` to build a richer per-tree JSON object (id, source_id, distance); both approaches are valid — the aggregate strategy is the declarative shortcut, `sql_template` is the full SQL escape hatch. See the live SQL at [config.yaml](../config.yaml#L852-L883).

### 3.4 Spatial intersection — `intersection`

**Models:** "Does the road *physically pass through* this polygon or grid cell?"

**When it is right:**
- Area features: admin boundaries, land-use polygons, pollution grid cells, quiet zones.
- You want the portion of the road inside the feature, not just proximity. `ST_Intersection` gives you a length weight.

**Failure modes:**
- Touching-edge-only intersections count as "intersects"; if that is not what you want, use `ST_DWithin` inside the buffer or filter by intersection length `> 0`.
- Very many small cells over a long road produces a many-rows-per-way mapping. Aggregate downstream or switch to `DISTINCT ON` if you only want one best-match per road (pattern below).

**Sketch:** see the air-quality SQL in [config.yaml](../config.yaml#L430-L457) — it intersects each road with the nearest latest-forecast grid cell and stores the intersection length in metres as a weight.

### 3.5 Along-line sampling — `sql_template` + `ST_Segmentize` / `ST_DumpPoints`

**Models:** "Walk along the road, ask the surface at each step."

**When it is right:**
- Continuous surfaces: rasters (elevation, noise, temperature grids), cost fields.
- You want a *profile* along each road, not one value.

**Failure modes:**
- Sampling interval is a tradeoff: too fine and the mapping table explodes, too coarse and you miss curvature. Elevation uses 100 m in the SQL path and 25 m in the Python path — both viable, driven by what the downstream consumer needs.
- No native "along-line" strategy exists in the registry — this relationship is always built via `sql_template` (declarative) or `custom` (when the sampling loop is cleaner in Python, as in `elevationPythonMapper.py`).

**Sketch:** see the elevation-profile SQL at [config.yaml](../config.yaml#L590-L638). The `ST_Segmentize` + `ST_DumpPoints` pattern is the backbone; swap the raster lookup to hit any other surface.

### 3.6 Attribute join — `attribute_join`

**Models:** "The source already knows which edge it belongs to."

**When it is right:**
- Upstream data was produced against the same graph (bike-speed metrics keyed by `connection_id`, OSM-derived attributes keyed by `osm_id`).
- No geometry operation is needed — it's a plain SQL JOIN on a shared column.

**Failure modes:**
- ID drift: if the source's edge IDs came from a different snapshot of the graph than your base table, join hit-rate silently drops. Log matched / unmatched counts.
- When hit-rate is low, combine with a spatial fallback — pleasant-bicycling uses `nearest_neighbour` with `nearest_within_distance` as an ID-free path, then joins on `connection_id` where it is available.

**Sketch:**
```yaml
strategy:
  type: attribute_join
  link_on:
    base_column: osm_id
    mapping_column: external_id
config:
  join_type: LEFT
```

---

## 4. CRS: choosing the right coordinate system

Every spatial strategy that uses distance or buffers is silently wrong on a geographic CRS, because `ST_Distance` on `geometry(4326)` returns degrees, not metres. The convention in this framework:

- **EPSG:4326** (WGS84, lat/lon) — storage, interchange, display.
- **EPSG:25833** (ETRS89 / UTM zone 33N) — Berlin's local projected CRS, used for everything that measures distance.

Existing mappers that do distance work keep a `geometry_25833` column alongside `geometry` and use it in all mapping SQL (see `treeMapper.py`, `pleasantBicyclingMapper.py`, `airQualityDataMapper.py`). If you are adapting the framework to another city, replace `25833` with the appropriate local UTM zone and re-run enrichment.

Two rules:

1. **Use a projected CRS for every distance, buffer, and intersection.** Cast with `::geography` only when a metre-accurate globe-distance is worth the cost — it is much slower than projected-metres.
2. **Store 4326 for portability**, but always project before measuring.

---

## 5. Temporal data on top of spatial

Many sources vary in time: weather is hourly, air quality is per-forecast-run. Temporal slicing is not the job of the mapping strategy — it belongs one layer up, in either the mapping SQL or a materialized view.

**Pattern A — latest snapshot in the mapping SQL.**
When each spatial unit (grid cell, station) has many timestamped rows and you want "the latest that applies to this road", use `DISTINCT ON` in the mapping SQL:

```sql
JOIN LATERAL (
  SELECT DISTINCT ON (e.grid_id) e.*
  FROM enrichment_table e
  WHERE ST_DWithin(w.geometry_25833, e.geom_25833, 36)
  ORDER BY e.grid_id, e.forecast_time DESC NULLS LAST
) latest ON ...
```

The air-quality mapper uses exactly this pattern: the enrichment table keeps all forecast rows, and mapping selects the latest per grid cell. See [config.yaml](../config.yaml#L430-L457).

**Pattern B — fixed timestamp in a materialized view.**
When you want an analyst-facing snapshot pinned to a specific hour, the mapping stays cell-to-road and a materialized view layered on top picks the timestamp. Weather works this way via `timestamp_filter` on `mv_weather` (see [config.yaml](../config.yaml#L25-L56)).

**Which to use:**
- Operational routing that always wants "right now" → Pattern A.
- Research / comparisons / dashboards pinned to a reference time → Pattern B.

---

## 6. Decision flow

Start here:

```
What is the spatial shape of your source data?
│
├─ No geometry — it carries an ID that matches my base graph
│     → attribute_join  (§3.6)
│        If hit rate is low, add a spatial fallback (§3.1 or §3.3)
│
├─ Points
│   │
│   ├─ Sparse, one-per-catchment (stations)
│   │     → knn / nearest_neighbour  (§3.1)
│   │
│   ├─ Several nearest matter (interpolation)
│   │     → nearest_k  (§3.2)
│   │
│   └─ Dense point cloud (trees, POIs, crashes)
│         → aggregate_within_distance  (§3.3)
│            or sql_template for richer per-feature JSON
│
├─ Polygons or grid cells
│     → intersection  (§3.4)
│        Add DISTINCT ON if you need the latest per cell (§5.A)
│
├─ Raster / continuous surface
│     → sql_template with ST_Segmentize + ST_DumpPoints  (§3.5)
│        Or write the sampler in Python with custom + mapping_db_query()
│
└─ Linear features aligned to roads
      → knn with nearest_within_distance, optionally combined with attribute_join
         (§3.6 + §3.1)
```

Once you know the strategy, go to:
- [mapping-quick-reference.md](mapping-quick-reference.md) — config templates and common patterns.
- [mapping-strategies-reference.md](mapping-strategies-reference.md) — full per-strategy reference.
- [configure-data-source-step-by-step.md](configure-data-source-step-by-step.md) — wiring a new datasource end-to-end.
