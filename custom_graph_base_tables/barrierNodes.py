import enum

from sqlalchemy import Column, Integer, BigInteger, Enum, ARRAY, String

from database.base import Base


# 2️⃣ Define model
class BarrierNode(Base):
    __tablename__ = "barrier_nodes"

    id = Column(Integer, primary_key=True, autoincrement=True)
    node_id = Column(BigInteger, unique=True, nullable=False)
    barrier_type = Column(String, nullable=False)
    modes_allowed = Column(ARRAY(String), nullable=True)
    modes_restricted = Column(ARRAY(String), nullable=True)
