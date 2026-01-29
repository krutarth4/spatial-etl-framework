from geoalchemy2 import Geometry
from sqlalchemy import Column, Integer, BigInteger, Enum, ARRAY, String

from database.base import Base


# 2️⃣ Define model
class WaysBaseTable(Base):
    __tablename__ = "ways_base"

    id = Column(Integer, primary_key=True, autoincrement=True)
    way_id = Column(BigInteger, unique=False, nullable=False)
    start_node_id = Column(BigInteger, nullable=False)
    end_node_id = Column(BigInteger, nullable=False)
    way_link_index = Column(Integer, nullable=False)

    #Postgis Geometry
    geometry = Column(Geometry(geometry_type="LINESTRING", srid=4326), nullable=False)
