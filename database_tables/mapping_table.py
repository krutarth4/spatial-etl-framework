from sqlalchemy import Column, Integer, ForeignKey

from database.base import Base


class MappingTable(Base):
    __abstract__ = True

    way_id = Column(Integer,ForeignKey("ways_base.id"), unique=True, nullable=False)