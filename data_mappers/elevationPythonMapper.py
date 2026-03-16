import binascii
import hashlib
import os
import zipfile
from pathlib import Path

from geoalchemy2 import Geometry, Raster
from pyproj import Transformer
from shapely import wkb
from shapely.geometry import box
from sqlalchemy import Column, Integer, Float, ARRAY, UniqueConstraint, Index, func, String, BigInteger, DateTime
from sqlalchemy.dialects.postgresql import JSONB

from database_tables.enrichment_table import EnrichmentTable
from database_tables.mapping_table import MappingTable
from database_tables.staging_table import StagingTable
from main_core.data_source_abc_impl import DataSourceABCImpl

import numpy as np
from shapely.ops import transform as shp_transform

from scipy.spatial import cKDTree


class ElevationPythonStagingTable(StagingTable):
    __tablename__ = "elevation_python_staging"
    id = Column(BigInteger, primary_key=True, autoincrement=True)

    way_id = Column(BigInteger, nullable=False, index=True)

    total_ascent = Column(Float, nullable=False)
    total_descent = Column(Float, nullable=False)
    max_slope = Column(Float, nullable=False)
    avg_slope = Column(Float, nullable=False)
    tile_name = Column(ARRAY(String), nullable=True)
    sample_count = Column(Integer, nullable=True)

    created_at = Column(DateTime(timezone=True), default=func.now())

    __table_args__ = (
        UniqueConstraint("way_id"),
    )


# class ElevationPythonEnrichmentTable(EnrichmentTable):
#     __tablename__ = "elevation_python_enrichment"
#
#     id = Column(Integer, primary_key=True, autoincrement=True, index=True)
#
#     dataset_id = Column(String, nullable=False, default="default", index=True)
#
#     tile_x = Column(Integer, nullable=False, index=True)
#     tile_y = Column(Integer, nullable=False, index=True)
#     rast = Column(Raster)
#     raster_hash = Column(String, index=True)
#
#     footprint_4326 = Column(Geometry("POLYGON", srid=4326), index=True)
#
#     __table_args__ = (
#         Index(
#             "elevation_enrichment_rast_gix",
#             func.ST_ConvexHull(rast),
#             postgresql_using="gist"
#         ),
#         Index(
#             "elevation_enrichment_footprint",
#             func.ST_ConvexHull(footprint_4326),
#             postgresql_using="gist"
#         ),
#         UniqueConstraint(
#             "dataset_id",
#             "tile_x",
#             "tile_y",
#         ),
#     )


class ElevationPythonMapper(DataSourceABCImpl):
    transformer = Transformer.from_crs(4326, 25833, always_xy=True)
    _metrics = {}  # way_id -> dict(ascent, descent, max_slope, last_z, last_pt)

    def post_database_processing(self):

        records = []

        for way_id, m in self._metrics.items():
            records.append({
                "way_id": way_id,
                "total_ascent": m["ascent"],
                "total_descent": m["descent"],
                "max_slope": m["max_slope"],
                "avg_slope": 0.0,  # compute if needed
                "sample_count": None,
                "tile_name": m["tile_name"]
            })

        self.logger.info(f"Writing {len(records)} slope records to staging")
        #
        self.db.bulk_insert(
            self.data_source_config.storage.staging.table_name,
            self.data_source_config.storage.staging.table_schema,
            records
        )

    def load(self, data):
        return

    _missing_inside = set()
    _zero_length = set()
    _single_sample = set()
    def pre_filter_processing(self, result):
        result = result[0]
        x = result["x"]
        y = result["y"]
        z = result["z"]
        tile_name = result["tile_name"]
        # --- 1) tile bounds in EPSG:25833 ---
        min_x = float(np.min(x))
        max_x = float(np.max(x))
        min_y = float(np.min(y))
        max_y = float(np.max(y))


        self.logger.info(
            f"Elevation tile bounds (25833): "
            f"minx={min_x:.2f}, miny={min_y:.2f}, maxx={max_x:.2f}, maxy={max_y:.2f} "
            f"points={len(z)}"
        )

        xy = np.column_stack((x.astype(np.float32), y.astype(np.float32)))
        zz = z.astype(np.float32)
        tree = cKDTree(xy)
        tile_polygon = box(min_x, min_y, max_x, max_y)

        sql = """
              SELECT id, ST_AsBinary(geometry) AS geom_wkb
              FROM test_osm_base_graph.ways_base
              WHERE ST_Intersects(
                        ST_Transform(geometry, 25833),
                        ST_MakeEnvelope(:minx, :miny, :maxx, :maxy, 25833)
                )
              """

        ways = self.db.fetch_query(sql, {
            "minx": min_x, "miny": min_y, "maxx": max_x, "maxy": max_y
        })
        self.logger.info(f"Candidate ways in tile bbox: {len(ways)}")
        step_m = 25.0  # tune later

        def to_utm(xlon, ylat):
            return self.transformer.transform(xlon, ylat)

        for way_id, geom_wkb in ways:
            try:
                geom_wgs84 = wkb.loads(bytes(geom_wkb))
            except Exception:
                # Sometimes driver already returns bytes; keep robust
                geom_wgs84 = wkb.loads(geom_wkb)

            geom_utm = shp_transform(to_utm, geom_wgs84)

            length = geom_utm.length
            if not length or length <= 0:
                self._zero_length.add(way_id)
                continue

            # 🔄 CHANGED: Clip geometry to tile polygon
            clipped = geom_utm.intersection(tile_polygon)

            # --- Fallback 1: if clipping removed geometry ---
            if clipped.is_empty or clipped.length <= 0:
                # fallback to full geometry endpoints
                coords = np.array([
                    [geom_utm.coords[0][0], geom_utm.coords[0][1]],
                    [geom_utm.coords[-1][0], geom_utm.coords[-1][1]]
                ], dtype=np.float32)
            else:
                clipped_length = clipped.length

                if clipped_length <= step_m:
                    distances = np.array([0.0, float(clipped_length)], dtype=np.float64)
                else:
                    distances = np.arange(0.0, float(clipped_length), step_m, dtype=np.float64)
                    if distances[-1] != clipped_length:
                        distances = np.append(distances, float(clipped_length))

                pts = [clipped.interpolate(d) for d in distances]
                coords = np.array([[p.x, p.y] for p in pts], dtype=np.float32)


            if len(coords) == 1:
                self._single_sample.add(way_id)
                coords = np.array([
                    [geom_utm.coords[0][0], geom_utm.coords[0][1]],
                    [geom_utm.coords[-1][0], geom_utm.coords[-1][1]]
                ], dtype=np.float32)

            # --- Elevation lookup ---
            _, idx = tree.query(coords, k=1)
            elev = zz[idx]

            # compute partial metrics for these in-tile samples
            m = self._metrics.get(way_id)
            if m is None:
                m = {"ascent": 0.0, "descent": 0.0, "max_slope": 0.0, "last_x": None, "last_y": None, "last_z": None, "tile_name": []}
                self._metrics[way_id] = m

            # iterate in order as they appear along the line
            # NOTE: coords_in are in the order of original sampling because inside is a boolean mask.
            m["tile_name"].append(tile_name)
            for i in range(len(coords)):
                cx, cy = float(coords[i, 0]), float(coords[i, 1])
                cz = float(elev[i])

                if m["last_x"] is not None:
                    dx = ((cx - m["last_x"]) ** 2 + (cy - m["last_y"]) ** 2) ** 0.5
                    if dx > 0:
                        dz = cz - m["last_z"]

                        if dz > 0:
                            m["ascent"] += dz
                        else:
                            m["descent"] += -dz

                        slope = abs(dz / dx)
                        if slope > m["max_slope"]:
                            m["max_slope"] = slope

                m["last_x"], m["last_y"], m["last_z"] = cx, cy, cz
        self.logger.info(f"Zero-length ways: {len(self._zero_length)}")
        self.logger.info(f"Missing inside samples: {len(self._missing_inside)}")
        self.logger.info(f"Single-sample ways: {len(self._single_sample)}")
        print("Total processed ways:", len(self._metrics))

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

            # 🔥 Read immediately
        data = np.loadtxt(xyz_path, dtype=np.float64)
        x = data[:, 0]
        y = data[:, 1]
        z = data[:, 2]

        return {
            "tile_name": os.path.basename(xyz_path),
            "x": x,
            "y": y,
            "z": z
        }
