import os
import zipfile
from pathlib import Path

from geoalchemy2 import Geometry, Raster
from pyproj import Transformer
from sqlalchemy import Column, Integer, Float, ARRAY, UniqueConstraint, Index, func, String
from sqlalchemy.dialects.postgresql import JSONB

from database_tables.enrichment_table import EnrichmentTable
from database_tables.mapping_table import MappingTable
from database_tables.staging_table import StagingTable
from main_core.data_source_abc_impl import DataSourceABCImpl

import numpy as np
import rasterio
from rasterio.transform import from_origin
from pyproj import CRS

from utils.execution_time import measure_time


class ElevationTable(StagingTable):
    __tablename__ = "elevation_staging"

    id = Column(Integer, primary_key=True, autoincrement=True,
                index=True)  # make sure to create indexing for the table for better query and fast computation
    rast = Column(Raster)
    raster_hash = Column(String, index=True)
    dataset_id = Column(String)

    __table_args__ = (
        Index(
            "elevation_staging_rast_gix",
            func.ST_ConvexHull(rast),
            postgresql_using="gist"
        ),
        UniqueConstraint("raster_hash")
    )


class ElevationEnrichmentTable(EnrichmentTable):
    __tablename__ = "elevation_enrichment"

    id = Column(Integer, primary_key=True, autoincrement=True, index=True)

    dataset_id = Column(String, nullable=False, default="default", index=True)

    tile_x = Column(Integer, nullable=False, index=True)
    tile_y = Column(Integer, nullable=False, index=True)
    rast = Column(Raster)
    raster_hash = Column(String, index=True)

    footprint_4326 = Column(Geometry("POLYGON", srid=4326), index=True)

    __table_args__ = (
        Index(
            "elevation_enrichment_rast_gix",
            func.ST_ConvexHull(rast),
            postgresql_using="gist"
        ),
        Index(
            "elevation_enrichment_footprint",
            func.ST_ConvexHull(footprint_4326),
            postgresql_using="gist"
        ),
        UniqueConstraint(
            "dataset_id",
            "tile_x",
            "tile_y",
        ),
    )


class ElevationMappingTable(MappingTable):
    __tablename__ = "elevation_mapping"

    id = Column(Integer, primary_key=True,
                autoincrement=True)  # make sure to create indexing for the table for better query and fast computation
    elevation_profile = Column(JSONB)



class ElevationMapper(DataSourceABCImpl):
    transformer = Transformer.from_crs(25833, 4326, always_xy=True)
    docker_container_path = "/extracted/"

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

    def sync_staging_to_enrichment(self):
        return

    def mapping_db_query(self) -> str:
        self.logger.info("Mapping Elevation to links (insert into mapping table)")

        base = self.data_source_config.mapping.base_table
        enrichment = self.data_source_config.storage.enrichment
        mapping = self.data_source_config.mapping

        sql = f"""
            WITH sampled_points AS (
                SELECT
                    w.id AS way_id,
                    dp.path[1] AS path_idx,
                    dp.geom AS geom_25833
                FROM {base.table_schema}.{base.table_name} w
                CROSS JOIN LATERAL ST_DumpPoints(
                    ST_Segmentize(
                        ST_Transform(w.geometry, 25833),
                        100.0
                    )
                ) AS dp
            ),
            points_with_elevation AS (
                SELECT
                    sp.way_id,
                    sp.path_idx,
                    sp.geom_25833,
                    z_lookup.z
                FROM sampled_points sp
                LEFT JOIN LATERAL (
                    SELECT z
                    FROM (
                        SELECT ST_Value(r.rast, sp.geom_25833) AS z
                        FROM {enrichment.table_schema}.{enrichment.table_name} r
                        WHERE ST_Intersects(r.footprint_4326, ST_Transform(sp.geom_25833, 4326))
                    ) sampled_z
                    WHERE sampled_z.z IS NOT NULL
                    LIMIT 1
                ) AS z_lookup ON TRUE
            )
            INSERT INTO {mapping.table_schema}.{mapping.table_name} (way_id, elevation_profile)
            SELECT
                p.way_id AS way_id,
                jsonb_agg(
                    jsonb_build_object(
                        'x', ST_X(ST_Transform(p.geom_25833, 4326)),
                        'y', ST_Y(ST_Transform(p.geom_25833, 4326)),
                        'z', p.z
                    )
                    ORDER BY p.path_idx
                ) AS elevation_profile
            FROM points_with_elevation p
            WHERE p.z IS NOT NULL
            GROUP BY p.way_id
            ON CONFLICT (way_id)
            DO UPDATE SET
                elevation_profile = EXCLUDED.elevation_profile;
        """

        return sql
    def enrichment_db_query(self) -> None | str:
        staging = self.data_source_config.storage.staging
        enrichment = self.data_source_config.storage.enrichment
        sql = f"""
                    INSERT INTO {enrichment.table_schema}.{enrichment.table_name}
                          (dataset_id, tile_x, tile_y, rast, raster_hash, footprint_4326)
                        SELECT
                        dataset_id,
                        
                          floor(ST_UpperLeftX(s.rast) / 1000)::int AS tile_x,
                          floor(ST_UpperLeftY(s.rast) / 1000)::int AS tile_y,
                        
                          s.rast,
                          s.raster_hash,
                          ST_Transform(ST_ConvexHull(s.rast), 4326) AS footprint_4326
                        
                        FROM {staging.table_schema}.{staging.table_name} s
                        WHERE s.raster_hash IS NOT NULL
                        
                        ON CONFLICT (dataset_id, tile_x, tile_y)
                        DO UPDATE SET
                            rast = EXCLUDED.rast,
                            raster_hash = EXCLUDED.raster_hash,
                            footprint_4326 = EXCLUDED.footprint_4326
                        
                        -- Only update if content actually changed
                        WHERE {enrichment.table_schema}.{enrichment.table_name}.raster_hash
                              IS DISTINCT FROM EXCLUDED.raster_hash;

              """

        return sql
    @measure_time("raster creation")
    def create_raster(self, xyz_path: str, pixel_size: float = 1.0) -> str:
        """
        Streaming XYZ → GeoTIFF.
        Memory stable for large files (~4M rows).
        """



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
                    "dataset_id": os.path.basename(file_path)
                }
            )

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
