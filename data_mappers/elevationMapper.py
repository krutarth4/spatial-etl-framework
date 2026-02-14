import zipfile

from geoalchemy2 import Geometry
from pyproj import Transformer
from sqlalchemy import Column, Integer, Float, ARRAY, UniqueConstraint

from database_tables.enrichment_table import EnrichmentTable
from database_tables.mapping_table import MappingTable
from database_tables.staging_table import StagingTable
from main_core.data_source_abc_impl import DataSourceABCImpl


class ElevationTable(StagingTable):
    __tablename__ = "elevation_staging"

    id = Column(Integer, primary_key=True, autoincrement=True,
                 index=True)  # make sure to create indexing for the table for better query and fast computation
    lat = Column(Float)
    lon = Column(Float)
    altitude = Column(Float)

    __table_args__ = (
        UniqueConstraint("id", "altitude"),
    )


class ElevationEnrichmentTable(EnrichmentTable):
    __tablename__ = "elevation_enrichment"

    id = Column(Integer, primary_key=True, autoincrement=True,
                 index=True)  # make sure to create indexing for the table for better query and fast computation
    lat = Column(Float)
    lon = Column(Float)
    altitude = Column(Float)
    geom = Column(Geometry(geometry_type="POINT", srid=4326))

    __table_args__ = (
        UniqueConstraint("id","altitude"),
    )


class ElevationMappingTable(MappingTable):
    __tablename__ = "elevation_mapping"

    id = Column(Integer, primary_key=True, autoincrement=True)  # make sure to create indexing for the table for better query and fast computation
    altitude= Column(Float)
    linked_points = Column(ARRAY(Integer))
    difference = Column(Float)


class ElevationMapper(DataSourceABCImpl):
    transformer = Transformer.from_crs(25833, 4326, always_xy=True)

    def read_file_content(self, path):
        rows =[]
        with zipfile.ZipFile(path, "r") as z:
            for name in z.namelist():
                with z.open(name) as f:
                    if name.endswith(".xyz"):
                        for line in f:
                            # line = line.decode("utf-8").strip()
                            # if not line:
                            #     continue
                            lat, lon, altitude = map(float, line.split())
                            # lat, lon = self.transformer.transform(lat, lon)
                            # point = WKTElement(f"POINT({lon} {lat})", srid=4326)
                            # create geom into this also

                            rows.append({"lat": lat,
                                         "lon": lon,
                                         "altitude": altitude,
                                         # "geom": point
                                         })
                            # print(rows[-1])
        # print("content is ", content)
        return rows

# TODO geometry operations ins