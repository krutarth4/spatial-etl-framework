# Example: Elevation Mapper (Python)

**Source file:** [`data_mappers/elevationMapper.py`](../data_mappers/elevationMapper.py)  
**Config:** [`data_source_configs/elevation.yaml`](../data_source_configs/elevation.yaml) — see [example-elevation-config.md](example-elevation-config.md)

This mapper ingests 1 m Digital Elevation Model (DEM) tiles for Berlin, builds PostGIS raster tiles in staging, downsamples them in enrichment, then samples each road segment to derive per-way ascent, descent, and slope. It is the most technically complex mapper and demonstrates:

- Overriding `load()` entirely to replace the default `bulk_insert` with a raster-specific SQL insert
- Two-pass XYZ → GeoTIFF conversion using rasterio (memory-stable for ~4M-row files)
- Using `ST_FromGDALRaster` + `ST_Tile` to split the GeoTIFF into 1000×1000 PostGIS raster tiles
- Storing a raster hash for deduplication (`ON CONFLICT (raster_hash) DO NOTHING`)
- Enrichment and mapping driven entirely by config — no Python SQL methods needed

---

## Table models

```python
class ElevationStagingTable(StagingTable):
    __tablename__ = "elevation_staging"

    id           = Column(Integer, primary_key=True, autoincrement=True, index=True)
    rast         = Column(Raster)             # PostGIS raster tile (1000×1000 pixels, 1 m)
    raster_hash  = Column(String, index=True) # MD5 of raster binary — used for deduplication
    dataset_id   = Column(String)             # filename of the source ZIP

    __table_args__ = (
        Index("idx_elevation_staging_rast_gist",
              func.ST_ConvexHull(rast),
              postgresql_using="gist"),
        UniqueConstraint("raster_hash"),
    )

class ElevationEnrichmentTable(EnrichmentTable):
    __tablename__ = "elevation_enrichment"

    id   = Column(Integer, primary_key=True, autoincrement=True, index=True)
    rast = Column(Raster)   # 10 m averaged raster produced by raster_aggregate operator

    __table_args__ = (
        Index("idx_elevation_enrichment_rast_gist",
              func.ST_ConvexHull(rast),
              postgresql_using="gist"),
    )

class ElevationMappingTable(MappingTable):
    __tablename__ = "elevation_mapping"

    id            = Column(BigInteger, primary_key=True, autoincrement=True)
    total_ascent  = Column(Float, nullable=False)
    total_descent = Column(Float, nullable=False)
    max_slope     = Column(Float, nullable=False)
    avg_slope     = Column(Float, nullable=False)
    sample_count  = Column(Integer, nullable=True)
```

**`Column(Raster)`**  
`geoalchemy2.Raster` maps to the PostGIS `raster` type. The staging table holds high-resolution 1 m tiles; the enrichment table holds coarser 10 m tiles produced by the `raster_aggregate` enrichment operator.

**GiST index on `ST_ConvexHull(rast)`**  
PostGIS raster tiles don't have a native geometry column, but the convex hull of the tile extent is a polygon that GiST can index. This makes spatial queries (e.g. "which tiles overlap this road segment?") fast.

**`raster_hash` for deduplication**  
The pipeline may process overlapping tile downloads across runs. `MD5(ST_AsBinary(rast))` produces a stable hash; `ON CONFLICT (raster_hash) DO NOTHING` skips identical tiles already in the table.

---

## Mapper class

### `read_file_content` — ZIP extraction + XYZ → GeoTIFF

```python
def read_file_content(self, path: str) -> dict:
    # 1. Unzip the downloaded archive, extract the .xyz file
    with zipfile.ZipFile(path, "r") as z:
        xyz_names = [name for name in z.namelist() if name.endswith(".xyz")]
        xyz_path = os.path.join(extract_dir, os.path.basename(xyz_names[0]))
        z.extract(xyz_names[0], extract_dir)

    # 2. Convert XYZ → GeoTIFF (two-pass, memory-stable)
    tif_path = self.create_raster(xyz_path)

    return {"name": os.path.basename(tif_path), "path": tif_path}
```

Instead of returning a list of dicts (the usual contract), this returns a single dict with the path to the generated GeoTIFF. The custom `load()` method below then reads that path.

### `create_raster` — two-pass XYZ → GeoTIFF

```python
@measure_time("raster creation")
def create_raster(self, xyz_path: str, pixel_size: float = 1.0) -> str:
    # Pass 1: scan the file to find bounds (min/max X, Y)
    with open(xyz_path, "r") as f:
        for line in f:
            x, y, _ = parsed
            min_x, max_x = min(min_x, x), max(max_x, x)
            ...

    # Allocate a numpy float32 array (nodata = -9999)
    raster = np.full((height, width), -9999, dtype=np.float32)

    # Pass 2: fill the array
    with open(xyz_path, "r") as f:
        for line in f:
            x, y, z = parsed
            raster[row, col] = z

    # Write GeoTIFF with rasterio
    with rasterio.open(tif_path, "w", crs=CRS.from_epsg(25833), ...) as dst:
        dst.write(raster, 1)

    return str(tif_path)
```

**Why two passes?**  
Reading all 4M XYZ points into memory to find bounds first would require ~300 MB. Two sequential file reads use only the memory for one tile at a time. Pass 1 just accumulates 4 floats (min/max X/Y); Pass 2 fills the pre-allocated array.

**`nodata = -9999`**  
Missing pixels (e.g. bodies of water or edge padding) are filled with `-9999`. The elevation SQL template filters `WHERE z IS NOT NULL AND z <> -9999` before computing ascent/descent.

### `load()` — raster insert via `ST_FromGDALRaster` + `ST_Tile`

```python
def load(self, file_info):
    tif_path = file_info["path"]
    with open(absolute_path, "rb") as f:
        raster_bytes = f.read()

    query = f"""
        INSERT INTO {source_schema}.{source_name} (rast, raster_hash, dataset_id)
        SELECT
            tile.rast,
            md5(ST_AsBinary(tile.rast)),
            :dataset_id
        FROM (
            SELECT ST_Tile(
                ST_FromGDALRaster(:raster_data),
                1000, 1000                           -- 1000×1000 pixel tiles
            ) AS rast
        ) AS tile
        ON CONFLICT (raster_hash) DO NOTHING;
    """

    self.execute_query("custom", query, {
        "raster_data": raster_bytes,
        "dataset_id": os.path.basename(tif_path),
    })
```

**`ST_FromGDALRaster(:raster_data)`**  
Converts the GeoTIFF bytes directly into a PostGIS in-memory raster — no file path on the server needed. The entire TIFF is passed as a binary parameter.

**`ST_Tile(..., 1000, 1000)`**  
Splits the large raster into 1000×1000 pixel tiles. Smaller tiles make the spatial index (GiST on convex hull) more selective — a road segment query hits only the tiles that actually overlap the segment, not the entire dataset.

**Why override `load()` instead of `read_file_content()`?**  
The default `load()` calls `bulk_insert(table, schema, records_list)`. Raster data isn't a list of dicts — it's a binary blob that needs tiling SQL. Overriding `load()` replaces the insert entirely.

---

## How the stages connect

```
ZIP files (one per DEM tile)
        │
        ▼  read_file_content()
        │  → unzip → two-pass XYZ → GeoTIFF
        ▼  load()  (overrides bulk_insert)
        │  → ST_FromGDALRaster → ST_Tile(1000×1000) → staging
        ▼
  elevation_staging   (1 m PostGIS raster tiles)
        │  raster_aggregate operator (elevation.yaml)
        ▼
  elevation_enrichment (10 m averaged raster tiles)
        │  sql_template strategy (mapping_sql/elevation_raster.sql)
        ▼
  elevation_mapping   (way_id → total_ascent, total_descent, max_slope, avg_slope)
        │
        ▼
  mv_ways_with_elevation
```

---

## Key patterns to reuse

| Pattern | Where | When to use |
|---------|-------|-------------|
| Override `load()` instead of `read_file_content` | `ElevationMapper.load()` | Data is not a list of dicts; needs custom SQL insert |
| `ST_FromGDALRaster` + `ST_Tile` | `insert_into_staging()` | Loading a raster file into PostGIS raster tiles |
| Two-pass XYZ file processing | `create_raster()` | Large point files where holding all points in memory is impractical |
| `MD5(ST_AsBinary(rast))` for dedup | `ON CONFLICT (raster_hash)` | Avoid re-inserting identical raster tiles across runs |
| GiST index on `ST_ConvexHull(rast)` | `ElevationStagingTable.__table_args__` | Spatial queries on raster tiles |
