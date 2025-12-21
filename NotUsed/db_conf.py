from log_manager.logger_manager import LoggerManager
from main_core.core_config import CoreConfig
from sqlalchemy import create_engine, inspect, text, MetaData
from sqlalchemy.orm import sessionmaker, Session, declarative_base
from dataclasses import dataclass
from typing import Optional
from dacite import from_dict
from geoalchemy2 import Geometry

@dataclass
class Credential:
    username: str
    password: str

@dataclass
class DBConfig:
    driver: str
    url: str
    port: int
    database_name: str
    database_schema: Optional[str]  # not required
    credential: Credential

class DbConf:
    # SQLALCHEMY_DATABASE_URL = 'postgresql://postgres:Bright#1270@localhost/fastapi'

    def __init__(self,core_config:DBConfig, schema:str = "public"):
        self.core_config = from_dict(data_class=DBConfig, data=core_config)
        self.logger = LoggerManager(self.__class__.__name__).get_logger()
        self.db_url = self.create_db_url(self.core_config)
        self.print_db_url()
        self.engine = self.create_engine()
        self.session =self.create_session()
        self.schema =schema
        self.metadata = self.create_metadata()
        self.inspector = inspect(self.engine)
        self.logger.debug(f"db schema {self.schema}")


    def create_metadata(self):
        # return Base.metadata
        return MetaData(schema = self.schema)

    def create_db_url(self,core_config: DBConfig):
        return (f"{core_config.driver}://{core_config.credential.username}:{core_config.credential.password}@"
                f"{core_config.url}:{core_config.port}/{core_config.database_name}")

    def print_db_url(self):
        self.logger.debug(f"Testing connection with url {self.db_url}")
        # print(f"Testing connection with url {self.db_url}")

    def get_db_url(self):
        return self.db_url

    def create_engine(self):
        return create_engine(self.db_url, echo=False, plugins=["geoalchemy2"])

    def create_session(self):
        return sessionmaker(bind=self.engine, autocommit=False, autoflush=False)

    def get_session(self):
        if self.session is not None:
            return self.session()
        return None
    def initialize(self):
        pass
    def update_metadata(self):
        self.metadata.reflect(bind=self.engine)

    def inspect_session(self):
        inspector = inspect(self.engine)
        self.logger.info(f"The tables found {inspector.get_table_names()}")
        for table_name in inspector.get_table_names(schema=self.schema):
            # self.logger.info("Table:", table_name) #-> leads to an error as it can return a dictionary
            print(table_name)


if __name__ == '__main__':
    raw:DBConfig = CoreConfig("../config.yaml").get_value("db")

    db_conf = DbConf(raw, "public")
    db_conf.inspect_session()
    sess = db_conf.session

    def get_nearest_node(session, lon: float, lat: float) -> int:
        query = text(
            """
            SELECT id
            FROM ways_vertices_pgr
            ORDER BY the_geom <-> ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)
            LIMIT 1
            """
        )
        result = session().execute(query, {"lon": lon, "lat": lat}).fetchone()
        return result[0] if result else None


    def get_route(session, start_lon: float, start_lat: float, end_lon: float, end_lat: float):
        start_node = get_nearest_node(session, start_lon, start_lat)
        end_node = get_nearest_node(session, end_lon, end_lat)
        print(f"{start_node} and {end_node}")

        if not start_node or not end_node:
            return {"error": "Could not find nearest nodes"}

        sql = text("""
                   WITH route AS (SELECT *
                                  FROM pgr_dijkstra(
                                          'SELECT gid AS id, source, target, cost, reverse_cost FROM ways',
                                          :start_id,
                                          :end_id,
                                          directed := false
                                       ))
                   SELECT ST_AsGeoJSON(w.the_geom) AS geom
                   FROM route r
                            JOIN ways w ON r.edge = w.gid;
                   """)

        rows = session().execute(sql, {"start_id": start_node, "end_id": end_node}).fetchall()
        features = [eval(row[0]) for row in rows if row[0]]

        return {
            "type": "FeatureCollection",
            "features": [{"type": "LineString", "geometry": f} for f in features]
        }
    # ernst reuter platz location and route handling
    a = get_route(sess, 13.3231919715,52.5109321435, 13.324586511, 52.51260044 )

    print(a)


