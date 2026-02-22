from geoalchemy2 import Geometry
from sqlalchemy import Column, Integer, BigInteger, Enum, ARRAY, String, Index

from database.base import Base


# 2️⃣ Define model
class WaysBaseTable(Base):
    __tablename__ = "ways_base"

    id = Column(Integer, primary_key=True, autoincrement=True)
    way_id = Column(BigInteger, unique=False, nullable=False)
    from_node_id = Column(BigInteger, nullable=False)
    to_node_id = Column(BigInteger, nullable=False)
    way_link_index = Column(Integer, nullable=False)
    length_m = Column(Integer)

    #Postgis Geometry
    geometry = Column(Geometry(geometry_type="LINESTRING", srid=4326), nullable=False)
    geometry_25833 = Column(Geometry(geometry_type="LINESTRING", srid=25833), nullable=True,default="NULL")

    __table_args__ = (
        Index(
            None,
            "geometry",
            postgresql_using="gist"
        ),
        Index(
            None,
            "geometry_25833",
            postgresql_using="gist"
        ),
    )
