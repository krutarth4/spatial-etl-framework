from geoalchemy2 import Geometry
from sqlalchemy import Column, Integer, Float, UniqueConstraint

from database.base import Base
from main_core.data_source_abc_impl import DataSourceABCImpl


class TestTable(Base):
    __tablename__ = "test"

    uid = Column(Integer, primary_key=True, autoincrement=True,
                 index=True)# make sure to create indexing for the table for better query and fast computation
    id = Column(Integer)
    price = Column(Float)
    rating = Column(Float)
    stock = Column(Float)
    UniqueConstraint('id', name='test_table_unique_id')

    # geom = Geometry(geometry_type="POINT", srid=4326)


class TestMapper(DataSourceABCImpl):
    pass