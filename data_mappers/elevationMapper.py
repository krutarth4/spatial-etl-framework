"""Raster-based elevation mapper.

ETL flow:
  staging      — XYZ DEM tiles parsed → 1 m PostGIS RASTER (ST_Tile 1000×1000)
  enrichment   — config-driven `raster_aggregate` operator downsamples the 1 m
                 raster to a coarser averaged grid (e.g. 10 m, 'Average')
  mapping      — `sql_template` (mapping_sql/elevation_raster.sql) samples the
                 averaged raster along each way → per-way ascent/descent/slope

Replaces the old numpy/cKDTree per-way approach, now kept in
elevationNumpyDeprecatedMapper.py.
"""
import os
import zipfile
from pathlib import Path

from geoalchemy2 import Raster
from sqlalchemy import (
    Column, Integer, BigInteger, Float, String, UniqueConstraint, Index, func,
)

from database_tables.enrichment_table import EnrichmentTable
from database_tables.mapping_table import MappingTable
from database_tables.staging_table import StagingTable
from main_core.data_source_abc_impl import DataSourceABCImpl

import numpy as np
import rasterio
from rasterio.transform import from_origin
from pyproj import CRS

from utils.execution_time import measure_time


class ElevationStagingTable(StagingTable):
    """1 m DEM raster tiles (EPSG:25833)."""
    __tablename__ = "elevation_staging"

    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    rast = Column(Raster)
    raster_hash = Column(String, index=True)
    dataset_id = Column(String)

    __table_args__ = (
        Index(
            "idx_elevation_staging_rast_gist",
            func.ST_ConvexHull(rast),
            postgresql_using="gist",
        ),
        UniqueConstraint("raster_hash"),
    )


class ElevationEnrichmentTable(EnrichmentTable):
    """Coarse averaged raster produced by the `raster_aggregate` operator."""
    __tablename__ = "elevation_enrichment"

    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    rast = Column(Raster)

    __table_args__ = (
        Index(
            "idx_elevation_enrichment_rast_gist",
            func.ST_ConvexHull(rast),
            postgresql_using="gist",
        ),
    )


class ElevationMappingTable(MappingTable):
    """Per-way elevation stats sampled from the averaged raster.

    `way_id` (unique FK to ways_base) is inherited from MappingTable.
    """
    __tablename__ = "elevation_mapping"

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    total_ascent = Column(Float, nullable=False)
    total_descent = Column(Float, nullable=False)
    max_slope = Column(Float, nullable=False)
    avg_slope = Column(Float, nullable=False)
    sample_count = Column(Integer, nullable=True)


class ElevationMapper(DataSourceABCImpl):
    """Builds the elevation raster in staging; enrichment + mapping are config-driven."""

    @staticmethod
    def _parse_xyz_line(line: str):
        line = line.strip()
        if not line:
            return None
        parts = line.replace(",", " ").split()
        if len(parts) < 3:
            return None
        try:
            return float(parts[0]), float(parts[1]), float(parts[2])
        except ValueError:
            return None

    @measure_time("raster creation")
    def create_raster(self, xyz_path: str, pixel_size: float = 1.0) -> str:
        """Streaming XYZ → GeoTIFF. Memory-stable for large files (~4M rows)."""
        xyz_path = Path(xyz_path)
        tif_path = xyz_path.with_suffix(".tif")
        if tif_path.exists():
            return str(tif_path)

        min_x = float("inf")
        max_x = float("-inf")
        min_y = float("inf")
        max_y = float("-inf")

        # -------- PASS 1: determine bounds --------
        valid_points = 0
        with open(xyz_path, "r") as f:
            for line in f:
                parsed = self._parse_xyz_line(line)
                if parsed is None:
                    continue
                x, y, _ = parsed
                min_x = min(min_x, x)
                max_x = max(max_x, x)
                min_y = min(min_y, y)
                max_y = max(max_y, y)
                valid_points += 1

        if valid_points == 0:
            raise ValueError(f"No valid XYZ points found in file: {xyz_path}")

        width = int((max_x - min_x) / pixel_size) + 1
        height = int((max_y - min_y) / pixel_size) + 1

        raster = np.full((height, width), -9999, dtype=np.float32)

        # -------- PASS 2: fill raster --------
        with open(xyz_path, "r") as f:
            for line in f:
                parsed = self._parse_xyz_line(line)
                if parsed is None:
                    continue
                x, y, z = parsed

                col = int((x - min_x) / pixel_size)
                row = int((max_y - y) / pixel_size)

                if 0 <= row < height and 0 <= col < width:
                    raster[row, col] = z

        origin_x = min_x - (pixel_size / 2)
        origin_y = max_y + (pixel_size / 2)
        transform = from_origin(origin_x, origin_y, pixel_size, pixel_size)

        with rasterio.open(
                tif_path,
                "w",
                driver="GTiff",
                height=height,
                width=width,
                count=1,
                dtype=raster.dtype,
                crs=CRS.from_epsg(25833),
                transform=transform,
                nodata=-9999,
        ) as dst:
            dst.write(raster, 1)

        return str(tif_path)

    def read_file_content(self, path: str) -> dict:
        base_dir = os.path.dirname(path)
        extract_dir = os.path.join(base_dir, "extracted")
        os.makedirs(extract_dir, exist_ok=True)

        with zipfile.ZipFile(path, "r") as z:
            xyz_names = [name for name in z.namelist() if name.endswith(".xyz")]
            if not xyz_names:
                raise ValueError("No .xyz file found inside ZIP")

            xyz_name = xyz_names[0]
            xyz_path = os.path.join(extract_dir, os.path.basename(xyz_name))

            if not os.path.exists(xyz_path):
                z.extract(xyz_name, extract_dir)
                extracted_full_path = os.path.join(extract_dir, xyz_name)
                if extracted_full_path != xyz_path:
                    os.replace(extracted_full_path, xyz_path)

        # 🔥 Create TIFF immediately
        tif_path = self.create_raster(xyz_path)

        return {
            "name": os.path.basename(tif_path),
            "path": tif_path,
        }

    def load(self, file_info):
        if not self.data_source_config.storage.persistent:
            self.logger.warning("Persistence disabled.")
            return

        if self.db is None:
            raise RuntimeError("DB not initialized")

        if isinstance(file_info, list):
            file_info = file_info[0]

        tif_path = file_info["path"]
        self.logger.info(f"Loading raster from TIFF file {tif_path}")

        self.insert_into_staging(
            self.data_source_config.storage.staging.table_schema,
            ElevationStagingTable.__tablename__,
            tif_path,
        )

    def insert_into_staging(self, source_schema, source_name, file_path):
        self.logger.info(f"Inserting into staging table {source_schema}.{source_name} -> {file_path}")

        try:
            absolute_path = os.path.abspath(file_path)

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
                        1000,
                        1000
                    ) AS rast
                ) AS tile
                ON CONFLICT (raster_hash) DO NOTHING;
            """

            self.execute_query(
                "custom",
                query,
                {
                    "raster_data": raster_bytes,
                    "dataset_id": os.path.basename(file_path),
                },
            )

        except Exception as e:
            self.logger.error(e)

    def ensure_raster_constraints(self, schema, table):
        """Ensure raster constraints exist (runs once if not already applied)."""
        check_query = """
                           SELECT 1
                           FROM raster_columns
                           WHERE r_table_schema = :schema
                             AND r_table_name = :table
                           """

        result = self.execute_query("custom", check_query, {
            "schema": schema,
            "table": table,
        })

        if not result.fetchone():
            self.logger.info("Applying raster constraints...")

            constraint_query = """
                SELECT AddRasterConstraints(
                    :schema,
                    :table,
                    'rast'
                );
            """

            self.execute_query("custom", constraint_query, {
                "schema": schema,
                "table": table,
            })

            self.logger.info("Raster constraints applied.")
