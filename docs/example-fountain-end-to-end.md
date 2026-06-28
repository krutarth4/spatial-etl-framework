# Example: Fountain Dataset — End to End

**ETL mapper:** [`data_mappers/fountainMapper.py`](../data_mappers/fountainMapper.py)  
**ETL config:** [`data_source_configs/fountain.yaml`](../data_source_configs/fountain.yaml)  
**Java scorer:** [`javaosmrouter-master/.../routing/scorer/FountainScorer.java`](../../javaosmrouter-master/src/main/java/de/fraunhofer/fokus/asct/josmr/routing/scorer/FountainScorer.java)

This document traces the fountain dataset from raw OSM data to a route score and badge in the Java router. It is a good example of:

- Parsing an OSM PBF file with `osmium` inside `read_file_content()`
- Converting lat/lon to a projected CRS in `enrichment_db_query()` instead of staging
- Using `aggregate_within_distance` with a plain integer count (the simplest possible aggregation)
- Declaring a materialized view inline in the datasource YAML
- Writing a jOOQ-based route scorer that reads the MV and accumulates per-segment counts
- Adding a new badge type to `RouteBadge`

---

## Overview of the full flow

```
berlin.osm.pbf
      │
      ▼  FountainMapper.read_file_content()
      │  osmium extracts fountain / drinking_water / water_tap nodes
      │  → list of {osm_id, name, amenity, access, lat, lon}
      ▼
fountain_staging          (osm_id, name, amenity, access, lat, lon)
      │
      ▼  FountainMapper.enrichment_db_query()
      │  ST_Transform(ST_SetSRID(ST_MakePoint(lon, lat), 4326), 25833)
      ▼
fountain_enrichment       (osm_id, name, amenity, access, geometry_25833)
      │
      ▼  aggregate_within_distance  max_distance=150 m  aggregation_type=count
      ▼
fountain_mapping          (way_id → fountain_count INTEGER)
      │
      ▼  materialized view refresh
      ▼
mv_fountain               (way_id, way_link_index, fountain_count, has_fountain_nearby)
      │
      ▼  FountainScorer (Java router)
      ▼
RouteCandidate.scores["fountain_count"]  +  FOUNTAIN badge
```

---

## Step 1 — Table models

All three table classes live in `fountainMapper.py`. The framework auto-creates and migrates them on startup.

### FountainStaging

```python
class FountainStaging(StagingTable):
    __tablename__ = "fountain_staging"

    id      = Column(BigInteger, primary_key=True, autoincrement=True)
    osm_id  = Column(BigInteger, nullable=False)
    name    = Column(String,     nullable=True)
    amenity = Column(String,     nullable=True)   # "fountain" | "drinking_water" | "water_tap"
    access  = Column(String,     nullable=True)
    lat     = Column(Float,      nullable=False)
    lon     = Column(Float,      nullable=False)

    __table_args__ = (UniqueConstraint("osm_id"),)
```

Geometry is **not** stored here. The OSM node gives raw WGS84 lat/lon; projection to EPSG:25833 happens later in the enrichment step. This keeps staging as a clean mirror of the source.

`UniqueConstraint("osm_id")` allows upserts — re-runs update changed nodes rather than duplicating them.

### FountainEnrichmentTable

```python
class FountainEnrichmentTable(EnrichmentTable):
    __tablename__ = "fountain_enrichment"

    id              = Column(BigInteger, primary_key=True, autoincrement=True)
    osm_id          = Column(BigInteger, nullable=False)
    name            = Column(String,     nullable=True)
    amenity         = Column(String,     nullable=True)
    access          = Column(String,     nullable=True)
    geometry_25833  = Column(Geometry(geometry_type="POINT", srid=25833), index=True)

    __table_args__ = (UniqueConstraint("osm_id"),)
```

The enrichment table drops `lat`/`lon` and replaces them with a proper PostGIS geometry column in EPSG:25833 — the same CRS used by `ways_base.geometry_25833`. Both sides must share a CRS for the spatial join in the mapping step.

### FountainMappingTable

```python
class FountainMappingTable(MappingTable):
    __tablename__ = "fountain_mapping"

    id              = Column(BigInteger, primary_key=True, autoincrement=True)
    fountain_count  = Column(Integer,    nullable=False, default=0)
```

A minimal mapping table. `aggregate_within_distance` with `aggregation_type: count` produces a single integer per road segment — no JSONB array needed. The column name `fountain_count` matches the `aggregation_alias` in the YAML config.

---

## Step 2 — Mapper class

```python
class FountainMapper(DataSourceABCImpl):

    def read_file_content(self, path) -> list:
        handler = _FountainHandler()
        handler.apply_file(str(path), locations=True)
        self.logger.info(f"Extracted {len(handler.records)} fountain features from {path}")
        return handler.records

    def enrichment_db_query(self) -> str | None:
        staging    = self.data_source_config.storage.staging
        enrichment = self.data_source_config.storage.enrichment
        return f"""
            INSERT INTO {enrichment.table_schema}.{enrichment.table_name}
                (osm_id, name, amenity, access, geometry_25833)
            SELECT
                s.osm_id,
                s.name,
                s.amenity,
                s.access,
                ST_Transform(
                    ST_SetSRID(ST_MakePoint(s.lon, s.lat), 4326),
                    25833
                ) AS geometry_25833
            FROM {staging.table_schema}.{staging.table_name} s
            ON CONFLICT (osm_id) DO UPDATE
                SET name           = EXCLUDED.name,
                    amenity        = EXCLUDED.amenity,
                    access         = EXCLUDED.access,
                    geometry_25833 = EXCLUDED.geometry_25833
        """
```

**`read_file_content`** — delegates to `_FountainHandler`, an `osmium.SimpleHandler` subclass that filters nodes by tag and returns a list of plain dicts. The base class inserts these dicts directly into staging.

**`enrichment_db_query`** — overridden because staging has no geometry column. The query reads `lat`/`lon` from staging and builds EPSG:25833 geometry with `ST_MakePoint → ST_SetSRID(4326) → ST_Transform(25833)`. `ON CONFLICT (osm_id) DO UPDATE` keeps enrichment in sync across re-runs without duplicates.

Nothing else is overridden — the base class handles download, staging insert, and mapping.

### The OSM handler

```python
class _FountainHandler(osmium.SimpleHandler):
    _AMENITY_VALUES = {"fountain", "drinking_water"}
    _MAN_MADE_VALUES = {"water_tap"}

    def __init__(self):
        super().__init__()
        self.records = []

    def node(self, n):
        amenity  = n.tags.get("amenity", "")
        man_made = n.tags.get("man_made", "")
        if amenity not in self._AMENITY_VALUES and man_made not in self._MAN_MADE_VALUES:
            return
        if not n.location.valid():
            return
        self.records.append({
            "osm_id":  n.id,
            "name":    n.tags.get("name") or None,
            "amenity": amenity or man_made,
            "access":  n.tags.get("access") or None,
            "lat":     n.location.lat,
            "lon":     n.location.lon,
        })
```

`locations=True` in `apply_file()` forces osmium to resolve node coordinates (required — without it, `n.location` is always invalid). The handler captures three OSM amenity types under a unified `amenity` field: `fountain`, `drinking_water`, and `water_tap` (the last stored via the `man_made` tag).

---

## Step 3 — YAML config

```yaml
name: fountain
description: "OSM fountains and drinking water taps within 150 m of each road segment"
enable: true
class_name: fountainMapper
data_type: static

source:
  mode: single
  fetch: local
  file_path: tmp/osm_graph/berlin.osm.pbf
  response_type: pbf

job:
  trigger:
    type:
      name: interval
      config:
        hours: 168   # weekly — OSM extract refreshes ~weekly

storage:
  persistent: true
  staging:
    table_class: FountainStaging
  enrichment:
    table_class: FountainEnrichmentTable

mapping:
  enable: true
  table_name: fountain_mapping
  table_class: FountainMappingTable
  strategy:
    type: aggregate_within_distance
    max_distance: 150
    aggregation_type: count
    aggregation_alias: fountain_count
  base_geometry_column: geometry_25833
  enrichment_geometry_column: geometry_25833
  base_table:
    column_name: fountain_count
    column_type: Integer
```

**`source.fetch: local` + `file_path`** — the Berlin OSM PBF is already on disk (downloaded by the graph mapper). No HTTP fetch needed; the file path is read directly.

**`response_type: pbf`** — signals that `read_file_content()` handles parsing. The built-in readers don't support PBF; the mapper's override takes full control.

**`storage.persistent: true`** — staging rows accumulate via upsert (keyed on `osm_id`) rather than being truncated each run. Fountain locations change rarely; this avoids unnecessary churn.

**`strategy.aggregate_within_distance` with `aggregation_type: count`** — the framework generates:

```sql
INSERT INTO {schema}.fountain_mapping (way_id, fountain_count)
SELECT
    b.id AS way_id,
    COUNT(e.id) AS fountain_count
FROM {schema}.ways_base b
LEFT JOIN {schema}.fountain_enrichment e
    ON ST_DWithin(b.geometry_25833, e.geometry_25833, 150)
GROUP BY b.id
```

Every road segment in `ways_base` is joined to all enrichment rows within 150 m and the count is stored. Segments with no fountains nearby get `COUNT = 0` (due to `LEFT JOIN`).

**`base_table.column_name: fountain_count`** — the framework adds a `fountain_count INTEGER` column to `ways_base` and writes the per-segment count there as well, making it available for non-MV queries.

---

## Step 4 — Materialized view

```yaml
materialized_view:
  name: mv_fountain
  description: "Per-way fountain count within 150 m of each base way"
  refresh:
    mode: normal
  depends_on:
    tables:
      - { name: ways_base }
      - { name: fountain_mapping }
  definition:
    select_sql: |
      SELECT
          w.id,
          w.way_id,
          w.from_node_id,
          w.to_node_id,
          w.way_link_index,
          COALESCE(m.fountain_count, 0)                                    AS fountain_count,
          CASE WHEN COALESCE(m.fountain_count, 0) > 0 THEN true ELSE false
               END                                                          AS has_fountain_nearby
      FROM {schema}.ways_base w
      LEFT JOIN {schema}.fountain_mapping m
          ON m.way_id = w.id
  indexes:
    - { name: idx_mv_fountain_way_id, columns: [way_id] }
```

`mv_fountain` joins `ways_base` (which has `way_id` and `way_link_index`) to `fountain_mapping` (which has one row per `ways_base.id`). The result has one row per road sub-segment — the granularity the Java router works at.

`COALESCE(m.fountain_count, 0)` — ensures segments with no mapping row still appear with `fountain_count = 0` rather than `NULL`.

`has_fountain_nearby` — a derived boolean; useful for boolean filtering queries or frontend display without re-thresholding the count.

`refresh.mode: normal` — `CONCURRENT` refresh requires a unique index on the MV. `fountain_mapping` has no unique index on its PK relative to the MV's grain, so `normal` is used instead (locks the view briefly during refresh).

---

## Step 5 — Rebuild the router (jOOQ code generation)

Once `mv_fountain` exists in the database, rebuild the router. jOOQ's `generateJooq` task connects to the live DB, introspects the schema, and regenerates all table classes under `src/main/generated/` — including a new `MvFountain.java` and `MvFountainRecord.java` with the correct column types.

```bash
cd javaosmrouter-master
./gradlew generateJooq --no-daemon   # requires PostgreSQL on 5432 with mv_fountain present
./gradlew clean installDist -x test --no-daemon
```

This is the supported way to add any new MV or table to the router. Do not write the jOOQ table class by hand — run `generateJooq` after the ETL pipeline has created the view.

---

## Step 6 — Route badge

`RouteBadge.BadgeType` in the router gets one new enum value:

```java
FOUNTAIN(
    "FOUNTAIN",
    "Route with the most fountains and drinking water taps nearby.",
    "fountain_count"
)
```

The third argument is the score key — the key under which `FountainScorer` stores its result in `RouteCandidate.scores`. `assignBadge()` uses this to look up the score without hard-coding the string.

---

## Step 7 — FountainScorer (Java)

```java
public class FountainScorer implements RouteScorer {

    private static final String SCORE_KEY = "fountain_count";
    private static final RouteBadge.BadgeType BADGE = RouteBadge.BadgeType.FOUNTAIN;

    private final DSLContext dsl;
    private volatile boolean available;

    public FountainScorer(DSLContext dsl) {
        this.dsl = dsl;
        this.available = ScorerTableChecker.exists(dsl, DatabaseConfig.getSchema(), "mv_fountain");
    }

    @Override
    public boolean isAvailable() {
        if (available) return true;
        available = ScorerTableChecker.exists(dsl, DatabaseConfig.getSchema(), "mv_fountain");
        return available;
    }

    @Override
    public void score(List<RouteCandidate> candidates, Set<WaySegmentKey> uniqueKeys) {
        if (!available || candidates == null || candidates.isEmpty()) return;

        Set<WaySegmentKey> keys = uniqueKeys != null
                ? uniqueKeys
                : RouteCandidate.extractUniqueWaySegments(candidates);

        Map<WaySegmentKey, Integer> counts = fetchFountainCounts(keys);

        for (RouteCandidate route : candidates) {
            double total = 0d;
            for (int i = 0; i < route.steps.size(); i++) {
                RoutingStep step = route.steps.get(i);
                WaySegmentKey key = new WaySegmentKey(
                        step.getLinkAndDirection().link.wayId,
                        step.getLinkAndDirection().link.wayLinkIndex);
                double v = counts.getOrDefault(key, 0);
                route.putStepScore(i, SCORE_KEY, v);
                total += v;
            }
            route.putScore(SCORE_KEY, total);
        }
    }

    @Override
    public void assignBadge(List<RouteCandidate> candidates) {
        // badge = highest fountain density (count per metre of route)
        candidates.stream()
                .filter(r -> r.getScore(SCORE_KEY) != null && r.lengthMeter > 0)
                .max(Comparator.comparingDouble(r -> r.getScore(SCORE_KEY) / r.lengthMeter))
                .ifPresent(r -> r.badges.add(new RouteBadge(BADGE)));
    }

    private Map<WaySegmentKey, Integer> fetchFountainCounts(Set<WaySegmentKey> keys) {
        Map<WaySegmentKey, Integer> result = new HashMap<>();
        List<Long> wayIds = keys.stream().map(WaySegmentKey::getWayId).distinct().toList();

        for (List<Long> batch : BatchUtils.partition(new ArrayList<>(wayIds), 5000)) {
            dsl.select(
                    MvFountain.MV_FOUNTAIN.WAY_ID,
                    MvFountain.MV_FOUNTAIN.WAY_LINK_INDEX,
                    MvFountain.MV_FOUNTAIN.FOUNTAIN_COUNT)
               .from(MvFountain.MV_FOUNTAIN)
               .where(MvFountain.MV_FOUNTAIN.WAY_ID.in(batch))
               .fetch()
               .forEach(row -> {
                   WaySegmentKey k = new WaySegmentKey(row.value1(), row.value2());
                   if (keys.contains(k))
                       result.putIfAbsent(k, row.value3() == null ? 0 : row.value3());
               });
        }
        return result;
    }
}
```

**`isAvailable()` lazy re-check** — on startup, `mv_fountain` may not exist yet (the ETL pipeline hasn't run). The scorer sets `available = false` and re-checks on every routing request until the view appears, at which point it enables itself permanently without a restart.

**Batch DB fetch** — all unique `way_id` values across all route candidates are fetched in one query (batched at 5 000 to stay within the `IN` clause limit). This avoids N+1 queries regardless of route length.

**Per-step scoring** — each routing step gets its segment's count stored under `SCORE_KEY`. The route total is the sum across all steps.

**Badge: density not total** — `assignBadge()` divides by `lengthMeter` before comparing. This prevents a long route from winning purely by traversing more segments near the same fountain cluster.

**Wiring into `Router.java`:**

```java
this.scorers = List.of(
    new FastestRouteScorer(),
    new WindRouteScorer(dsl),
    new ElevationRouteScorer(elevationDataSource),
    new PleasantBicycleScorer(dsl),
    new TreeScorer(dsl),
    new FountainScorer(dsl),        // ← added
    new AccidentRouteScorer(this.bicycleAccidentData),
    new AirQualityRouteScorer(dsl)
);
```

---

## Why the route total is much larger than the visible fountain count

The per-segment `fountain_count` in `mv_fountain` is the number of fountain nodes within 150 m of **that road sub-segment**. A single physical fountain can be within 150 m of many consecutive sub-segments along a road. The scorer sums these counts across all steps, so the same fountain is counted once per sub-segment it neighbours.

A route with 50 sub-segments passing near 3 fountains can easily produce a total score of 100+, while only 3 fountains are visible on the map. The score is meaningful only as a **relative comparison between routes**, not as an absolute fountain count.

---

## Summary of decisions

| Decision | Why |
|----------|-----|
| No geometry in staging — only lat/lon | Raw OSM nodes give WGS84 coordinates; projection to 25833 is a DB operation, not a parsing step |
| `enrichment_db_query()` override | Staging has no geometry column — the default sync would copy nothing useful |
| `aggregate_within_distance` with `count` | Fountains are sparse amenity points; per-road count is the right aggregation (no need for a JSONB array of IDs) |
| 150 m buffer | Typical walking detour distance; balances coverage with noise |
| `data_type: static` + `persistent: true` | OSM fountain locations change rarely; upsert avoids truncating and re-inserting thousands of unchanged rows weekly |
| Rebuild router after ETL run | jOOQ `generateJooq` introspects the live DB — `mv_fountain` must exist before the task runs; this is the supported way to pick up any new MV |
| Badge uses density not total | Prevents long routes from winning unfairly — a 10 km route with 5 fountains should not beat a 1 km route with 4 |
