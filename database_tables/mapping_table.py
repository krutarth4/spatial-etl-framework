from sqlalchemy import Column, Integer

from database.base import Base


class MappingTable(Base):
    __abstract__ = True

    way_id = Column(Integer, unique=True, nullable=False)