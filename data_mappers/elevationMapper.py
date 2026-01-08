import json
import zipfile

from geoalchemy2 import Geometry, WKTElement
from pyproj import Transformer
from sqlalchemy import Column, Integer, Float

from database.base import Base
from main_core.data_source_abc_impl import DataSourceABCImpl


class ElevationTable(Base):
    __tablename__ = "elevation_upsert"

    id = Column(Integer, primary_key=True, autoincrement=True,
                 index=True)  # make sure to create indexing for the table for better query and fast computation
    lat = Column(Float)
    lon = Column(Float)
    altitude = Column(Float)
    geom = Geometry(geometry_type="POINT", srid=4326)


class ElevationMapper(DataSourceABCImpl):
    transformer = Transformer.from_crs(25833, 4326, always_xy=True)

    def read_file_content(self, path):
        rows =[]
        with zipfile.ZipFile(path, "r") as z:
            for name in z.namelist():
                with z.open(name) as f:
                    if name.endswith(".xyz"):
                        for line in f:
                            line = line.decode("utf-8").strip()
                            if not line:
                                continue
                            lat, lon, altitude = map(float, line.split())
                            lat, lon = self.transformer.transform(lat, lon)
                            point = WKTElement(f"POINT({lon} {lat})", srid=4326)
                            # create geom into this also

                            rows.append({"lat": lat,
                                         "lon": lon,
                                         "altitude": altitude,
                                         "geom": point
                                         })
                            # print(rows[-1])
        # print("content is ", content)
        return rows
