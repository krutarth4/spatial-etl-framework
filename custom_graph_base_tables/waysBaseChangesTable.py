from sqlalchemy import Column, Integer, BigInteger, String, Index

from database.base import Base


class WaysBaseChangesTable(Base):
    __tablename__ = "ways_base_changes"

    base_id = Column(BigInteger, primary_key=True)
    way_id = Column(BigInteger, nullable=False)
    way_link_index = Column(Integer, nullable=False)
    op = Column(String(16), nullable=False)  # 'added' | 'removed' | 'modified'

    __table_args__ = (
        Index(None, "op"),
    )
