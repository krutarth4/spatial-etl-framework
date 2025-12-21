import json
import zipfile

from sqlalchemy import Column, Integer, Float

from database.base import Base
from main_core.data_source_abc_impl import DataSourceABCImpl


class ElevationTable(Base):
    __tablename__ = "elevation"

    id = Column(Integer, primary_key=True, autoincrement=True,
                 index=True)  # make sure to create indexing for the table for better query and fast computation
    lat = Column(Float)
    lon = Column(Float)
    altitude = Column(Float)


class ElevationMapper(DataSourceABCImpl):

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
                            rows.append({"lat": lat, "lon": lon, "altitude": altitude})
        # print("content is ", content)
        return rows
