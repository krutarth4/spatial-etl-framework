from sqlalchemy import Column, Integer, ForeignKey, UniqueConstraint

from core.globalconstants import GlobalConstants
from database.base import Base


class MappingTable(Base):
    __abstract__ = True

    way_id = Column(Integer,ForeignKey(f"{GlobalConstants.base_schema}.{GlobalConstants.base_table}.id"),index=True, unique=True, nullable=False)
    __table_args__ = (
        UniqueConstraint("way_id"),
    )