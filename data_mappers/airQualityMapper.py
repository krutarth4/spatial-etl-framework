from geoalchemy2 import Geometry
from sqlalchemy import Column, Integer, ARRAY, String, Float, DateTime

from database.base import Base
from main_core.data_source_abc_impl import DataSourceABCImpl


# class AirPollutionGrid(Base):
#     __tablename__ = "air_pollution_grid"
#
#     id = Column(Integer, primary_key=True)  # grid ID (from JSON)
#     forecast_time = Column(DateTime, nullable=False)
#     forecast_range = Column(String(100), nullable=False)
#
#     no2 = Column(ARRAY(Float))
#     pm10 = Column(ARRAY(Float))
#     pm25 = Column(ARRAY(Float))
#
#     # coordinates in both systems
#     x_utm = Column(Float, nullable=False)
#     y_utm = Column(Float, nullable=False)
#     lat = Column(Float)
#     lon = Column(Float)
#
#     # PostGIS geometry (EPSG:25833)
#     geom = Column(Geometry("POINT", srid=25833), nullable=False)


class AirQualityMapper(DataSourceABCImpl):
    pass