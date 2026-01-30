from sqlalchemy import Column, Integer, ForeignKey

from core.globalconstants import GlobalConstants
from database.base import Base


class MappingTable(Base):
    __abstract__ = True

    way_id = Column(Integer,ForeignKey(f"{GlobalConstants.base_schema}.{GlobalConstants.base_table}.id"), unique=True, nullable=False)
