
from geoalchemy2 import Geometry
from sqlalchemy import Column, Integer, BigInteger, ARRAY, String, Float

from database.base import Base


# 2️⃣ Define model
class LinksTable(Base):
    __tablename__ = "links"

    id = Column(Integer, primary_key=True, autoincrement=True)
    way_id = Column(BigInteger, unique=False, nullable=False)
    name = Column(String, nullable=True)
    # highway_type = Column(String, nullable=True)
    smoothness = Column(String, nullable=True)
    start_node_id = Column(BigInteger, nullable=False)
    end_node_id = Column(BigInteger, nullable=False)
    way_link_index = Column(Integer, nullable=False)
    # attributes = Column(ARRAY(String), nullable=True)
    max_speed_forward = Column(Float, nullable=True)
    max_speed_reverse = Column(Float, nullable=True)
    meters = Column(Float, nullable=True)
    # surface = Column(String, nullable=True)
    # travel_mode_dots = Column(String, nullable=True)

    #Postgis Geometry
    geometry = Column(Geometry(geometry_type="LINESTRING", srid=4326), nullable=False)
