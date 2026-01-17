from sqlalchemy import Column, Integer

from database.base import Base


class StagingTable(Base):
    __abstract__ = True