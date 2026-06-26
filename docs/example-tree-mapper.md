# Example: Tree Mapper (Python)

**Source file:** [`data_mappers/treeMapper.py`](../data_mappers/treeMapper.py)  
**Config:** [`data_source_configs/tree.yaml`](../data_source_configs/tree.yaml) — see [example-tree-config.md](example-tree-config.md)

This mapper ingests Berlin street and garden trees from the Berlin WFS as GeoPackage files. It is a good example of:

- Writing a custom `read_file_content()` override for vector files that carry geometry
- Packing raw source fields into a single `JSONB` column in staging, then unpacking them into typed enrichment columns using `derive` operators in YAML
- Defining a custom `MappingTable` when the standard mapping schema is not enough

---

## Table models

Three table model classes live at the top of the mapper file. The framework auto-creates and migrates them on startup.

### TreeStagingTable

```python
class TreeStagingTable(StagingTable):
    __tablename__ = "tree_staging"

    id             = Column(BigInteger, primary_key=True, autoincrement=True)
    source_id      = Column(String, nullable=True, index=True)   # Berlin gisid
    attributes     = Column(JSONB, nullable=False)               # all raw cadastre fields
    geometry_25833 = Column(Geometry("POINT", srid=25833), nullable=False)

    __table_args__ = (
        UniqueConstraint("source_id"),
        Index(None, "geometry_25833", postgresql_using="gist"),
    )
```

**Why JSONB for attributes?**  
The Berlin tree cadastre exports ~20 German-named fields (`art_dtsch`, `gattung`, `pflanzjahr`, …) that change between releases. Packing them into a single `JSONB` column keeps the staging schema stable regardless of source schema changes. The enrichment stage then picks out only the fields that are useful.

**Why geometry in staging?**  
The source is a GeoPackage — geometry arrives with the raw data. Storing it directly in staging avoids a round-trip through lat/lon columns and lets the enrichment table inherit geometry without re-encoding.

---

### TreeEnrichmentTable

```python
class TreeEnrichmentTable(EnrichmentTable):
    __tablename__ = "tree_enrichment"

    id             = Column(BigInteger, primary_key=True, autoincrement=True)
    source_id      = Column(String, nullable=True, index=True)
    attributes     = Column(JSONB, nullable=True)                  # verbatim copy from staging
    geometry_25833 = Column(Geometry("POINT", srid=25833), nullable=True)

    # Normalized columns — filled by `derive` operators declared in tree.yaml
    species_de             = Column(String, nullable=True)   # attributes->>'art_dtsch'
    species_bot            = Column(String, nullable=True)   # attributes->>'art_bot'
    genus                  = Column(String, nullable=True)   # attributes->>'gattung'
    leaf_type              = Column(String, nullable=True)   # 'deciduous' | 'coniferous' | NULL
    street                 = Column(String, nullable=True)
    district               = Column(String, nullable=True)
    planting_year          = Column(Integer, nullable=True)
    age_years              = Column(Float, nullable=True)
    crown_diameter_m       = Column(Float, nullable=True)
    trunk_circumference_cm = Column(Float, nullable=True)
    height_m               = Column(Float, nullable=True)
    size_class             = Column(String, nullable=True)   # derived from height_m

    __table_args__ = (
        UniqueConstraint("source_id"),
        Index(None, "geometry_25833", postgresql_using="gist"),
    )
```

**How the normalized columns get populated:**  
The default staging→enrichment sync copies `source_id`, `attributes`, and `geometry_25833` verbatim (they share the same column name in both tables). The normalized columns (`species_de`, `height_m`, etc.) start as `NULL`. The `enrichment_operators` block in `tree.yaml` then runs a series of `derive` operators — each one issues a single `UPDATE` statement that reads from `attributes` JSONB and writes one normalized column. No Python needed for this stage.

See [example-tree-config.md](example-tree-config.md#enrichment-operators) for the full operator list.

---

### TreeMappingTable

```python
class TreeMappingTable(MappingTable):
    __tablename__ = "tree_mapping"

    id    = Column(BigInteger, primary_key=True, autoincrement=True)
    trees = Column(JSONB, nullable=False)
```

The `aggregate_within_distance` strategy stores its aggregation result in whatever column name you declare. Here it is `trees` — a JSONB array of `{tree_id, source_id, distance_m}` objects, one per tree within 50 m of the road segment. The column is declared here; the aggregation expression is in `tree.yaml`.

For the default `knn` strategy you do not need a custom `MappingTable` at all — the base class provides `way_id` and the enrichment FK. A custom table is only needed when the strategy result has a non-standard shape (like a JSONB array here).

---

## Mapper class

```python
class TreeMapper(DataSourceABCImpl):

    def read_file_content(self, path: str):
        gdf = gpd.read_file(path, engine="pyogrio")

        if gdf.empty:
            return []

        if "geometry" not in gdf.columns:
            raise ValueError("No geometry column found")

        if gdf.crs is None:
            raise ValueError("Input dataset has no CRS defined")

        # Reproject to EPSG:25833 if the source uses a different CRS
        if gdf.crs.to_epsg() != 25833:
            gdf = gdf.to_crs(25833)

        # Encode geometry as WKB hex — PostGIS accepts this on insert
        gdf["geometry_wkb"] = gdf.geometry.apply(
            lambda g: g.wkb_hex if g else None
        )
        gdf = gdf.drop(columns=["geometry"])

        records = []
        for row in gdf.to_dict(orient="records"):
            geometry_wkb = row.pop("geometry_wkb", None)
            source_id = row.get("gisid")

            # NaN floats are not JSON-serializable — replace with None
            cleaned_attributes = {
                k: (None if isinstance(v, float) and math.isnan(v) else v)
                for k, v in row.items()
            }

            records.append({
                "source_id": source_id,
                "attributes": cleaned_attributes,
                "geometry_25833": geometry_wkb,
            })

        return records
```

**Why override `read_file_content()`?**  
The built-in GeoPackage reader (via `response_type: gpkg`) drops the geometry column and returns the attribute columns as a flat dict. For trees that is not enough — geometry has to reach the staging table. Overriding `read_file_content()` lets us:

1. Read the GeoPackage ourselves with GeoPandas
2. Reproject to EPSG:25833 if needed
3. Encode geometry as WKB hex (the format PostGIS accepts via SQLAlchemy)
4. Pack all remaining columns into `attributes` JSONB and clean out `NaN` values that would break JSON serialization

**The three output keys** map directly to the three non-PK columns of `TreeStagingTable`:

| Key | Type | Column |
|-----|------|--------|
| `source_id` | `str` | `tree_staging.source_id` |
| `attributes` | `dict` | `tree_staging.attributes` (JSONB) |
| `geometry_25833` | WKB hex `str` | `tree_staging.geometry_25833` |

---

## How the stages connect

```
WFS → GeoPackage file
        │
        ▼  read_file_content()
        │  → reprojects to 25833
        │  → geometry_wkb + attributes JSONB
        ▼
  tree_staging          (source_id, attributes, geometry_25833)
        │  default sync copies matching columns
        ▼
  tree_enrichment       (source_id, attributes, geometry_25833 — copied)
        │  derive operators (tree.yaml) unpack attributes → typed columns
        ▼
  tree_enrichment       (+ species_de, height_m, leaf_type, size_class …)
        │  aggregate_within_distance strategy
        ▼
  tree_mapping          (way_id → trees JSONB array, one entry per tree ≤ 50 m)
        │
        ▼
  mv_tree               (ways_base LEFT JOIN tree_mapping)
```

---

## Key patterns to reuse

| Pattern | Where | When to use |
|---------|-------|-------------|
| Pack source fields into JSONB staging column | `TreeStagingTable.attributes` | Source schema changes between releases; you want a stable staging table |
| Unpack JSONB in enrichment using `derive` operators | `tree.yaml` `enrichment_operators` | Avoid Python for simple column extractions and casts |
| WKB-encode geometry in `read_file_content` | `TreeMapper.read_file_content` | Source is a vector file that the built-in reader would drop geometry from |
| Custom `MappingTable` with JSONB column | `TreeMappingTable.trees` | Mapping strategy produces an array or complex object per road segment |
| NaN → None cleaning | `read_file_content` attribute loop | Any GeoPandas source — float NaN in JSONB columns breaks psycopg insert |
