import binascii
import os
import zipfile
from pathlib import Path

from geoalchemy2 import Geometry, Raster
from pyproj import Transformer
from sqlalchemy import Column, Integer, Float, ARRAY, UniqueConstraint, Index, func

from database_tables.enrichment_table import EnrichmentTable
from database_tables.mapping_table import MappingTable
from database_tables.staging_table import StagingTable
from main_core.data_source_abc_impl import DataSourceABCImpl

import numpy as np
import pandas as pd
import rasterio
from rasterio.transform import from_origin
from pyproj import CRS


class ElevationTable(StagingTable):
    __tablename__ = "elevation_staging"

    id = Column(Integer, primary_key=True, autoincrement=True,
                index=True)  # make sure to create indexing for the table for better query and fast computation
    rast = Column(Raster)

    __table_args__ = (
        Index(
            "elevation_staging_rast_gix",
            func.ST_ConvexHull(rast),
            postgresql_using="gist"
        ),
    )


class ElevationEnrichmentTable(EnrichmentTable):
    __tablename__ = "elevation_enrichment"

    id = Column(Integer, primary_key=True, autoincrement=True,
                index=True)  # make sure to create indexing for the table for better query and fast computation
    rast = Column(Raster)

    __table_args__ = (
        UniqueConstraint("id", "rast"),
    )


class ElevationMappingTable(MappingTable):
    __tablename__ = "elevation_mapping"

    id = Column(Integer, primary_key=True,
                autoincrement=True)  # make sure to create indexing for the table for better query and fast computation
    altitude = Column(Float)
    linked_points = Column(ARRAY(Integer))
    difference = Column(Float)


class ElevationMapper(DataSourceABCImpl):
    transformer = Transformer.from_crs(25833, 4326, always_xy=True)
    docker_container_path = "/extracted/"

    def create_raster(self, xyz_path: str, pixel_size: float = 1.0) -> str:
        """
        Streaming XYZ → GeoTIFF.
        Memory stable for large files (~4M rows).
        """

        xyz_path = Path(xyz_path)
        tif_path = xyz_path.with_suffix(".tif")

        min_x = float("inf")
        max_x = float("-inf")
        min_y = float("inf")
        max_y = float("-inf")

        # -------- PASS 1: determine bounds --------
        with open(xyz_path, "r") as f:
            for line in f:
                x, y, _ = map(float, line.strip().split())
                min_x = min(min_x, x)
                max_x = max(max_x, x)
                min_y = min(min_y, y)
                max_y = max(max_y, y)

        width = int((max_x - min_x) / pixel_size) + 1
        height = int((max_y - min_y) / pixel_size) + 1

        raster = np.full((height, width), -9999, dtype=np.float32)

        # -------- PASS 2: fill raster --------
        with open(xyz_path, "r") as f:
            for line in f:
                x, y, z = map(float, line.strip().split())

                col = int((x - min_x) / pixel_size)
                row = int((max_y - y) / pixel_size)

                raster[row, col] = z
        origin_x = min_x - (pixel_size / 2)
        origin_y = max_y + (pixel_size / 2)

        transform = from_origin(origin_x, origin_y, pixel_size, pixel_size)

        # transform = from_origin(min_x, max_y, pixel_size, pixel_size)
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
        # with rasterio.open(
        #         tif_path,
        #         "w",
        #         driver="GTiff",
        #         height=height,
        #         width=width,
        #         count=1,
        #         dtype=raster.dtype,
        #         crs=CRS.from_epsg(25833),
        #         transform=transform,
        #         nodata=-9999,
        #         compress="LZW",  # 🔥 important for size
        #         tiled=True,  # 🔥 important for performance
        #         blockxsize=256,
        #         blockysize=256,
        # ) as dst:
        #     dst.write(raster, 1)

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
            "path": tif_path
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
            ElevationTable.__tablename__,
            tif_path
        )

        # Optional: remove TIFF after insert
        # self.ensure_raster_constraints(self.data_source_config.storage.staging.table_schema,ElevationTable.__tablename__)
        # os.remove(tif_path)



    def insert_into_staging(self, source_schema, source_name, file_path):
        self.logger.info(f"Inserting into staging table {source_schema}.{source_name} -> {file_path}")
        try:
            absolute_path = os.path.abspath(file_path)
            with open(absolute_path, "rb") as f:
                raster_bytes = f.read()
            # hex_string = binascii.hexlify(raster_bytes).decode("ascii")

            query = f"""
                INSERT INTO {source_schema}.{source_name} (rast)
                SELECT ST_Tile(
                    ST_FromGDALRaster(:raster_data),
                    1000,
                    1000
                );
            """

            self.execute_query("custom", query, {"raster_data": raster_bytes})
        except Exception as e:
            self.logger.error(e)

    def ensure_raster_constraints(self, schema, table):
        """
        Ensure raster constraints exist.
        Runs only once if not already applied.
        """

        check_query = """
                           SELECT 1
                           FROM raster_columns
                           WHERE r_table_schema = :schema
                             AND r_table_name = :table
                           """

        result = self.execute_query("custom", check_query, {
            "schema": schema,
            "table": table
        })

        if not result.fetchone():
            self.logger.info("Applying raster constraints...")

            constraint_query = f"""
                SELECT AddRasterConstraints(
                    :schema,
                    :table,
                    'rast'
                );
            """

            self.execute_query("custom", constraint_query, {
                "schema": schema,
                "table": table
            })

            self.logger.info("Raster constraints applied.")