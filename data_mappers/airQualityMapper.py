from geoalchemy2 import Geometry
from sqlalchemy import Column, Integer, ARRAY, String, Float, DateTime

from database.base import Base
from main_core.data_source_abc_impl import DataSourceABCImpl


class AirQualityMapper(DataSourceABCImpl):
    pass