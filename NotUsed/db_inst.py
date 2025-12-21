from datetime import datetime
from zoneinfo import ZoneInfo

import requests
from geoalchemy2 import Geometry
from sqlalchemy import text, Column, Integer, Float, String, DateTime, Table, Date, \
    UniqueConstraint, select, BIGINT, ARRAY
from sqlalchemy.dialects.postgresql import insert

from NotUsed.db_conf import DbConf, DBConfig
from log_manager.logger_manager import LoggerManager


class DBInst(DbConf):
    # TODO: look into automap_base to get the power of ORM and sqlAlchemy

    def __init__(self, conf: DBConfig, schema = "public"):
        super().__init__(conf, schema)
        self.logger = LoggerManager(self.__class__.__name__).get_logger()
        self.schema = schema

        if self.get_session() is None :
            print("No db session found")
        else:
            self.db = self.get_session()

    def get_db(self):
        try:
            yield self.db
        finally:
            self.db.close()

    def execute(self, query, params = None):
        result  =  self.db.execute(query, params or {}).fetchall()
        return result

    def get_nearest_node(self, lon: float, lat: float) -> int:
        query = text(
            """
            SELECT id
            FROM ways_vertices_pgr
            ORDER BY the_geom <-> ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)
            LIMIT 1
            """
        )
        result = self.db.execute(query, {"lon": lon, "lat": lat}).fetchone()
        return result[0] if result else None

    def get_route(self, start_lon: float, start_lat: float, end_lon: float, end_lat: float, dijkstra = False):
        start_node = self.get_nearest_node( start_lon, start_lat)
        end_node = self.get_nearest_node( end_lon, end_lat)
        print(f"{start_node} and {end_node}")

        if not start_node or not end_node:
            return {"error": "Could not find nearest nodes"}
        # add something like this
        # create index
        # CREATE
        # INDEX
        # idx_vertices_geom
        # ON
        # ways_vertices_pgr
        # USING
        # GIST(the_geom);
        # TODO: for a condition that it is in the 100 m range
        # SELECT
        # id
        # FROM
        # ways_vertices_pgr
        # WHERE
        # ST_DWithin(
        #     the_geom,
        #     ST_SetSRID(ST_MakePoint(: lon,:lat), 4326),
        # 0.001 - - ~100
        # m
        # at
        # latitude
        # 52
        # )
        # ORDER
        # BY
        # the_geom <-> ST_SetSRID(ST_MakePoint(: lon,:lat), 4326)
        # LIMIT
        # 1;
        sql_dikstra = text("""
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

        sql_aStar = text("""
                           WITH route AS (SELECT *
                                          FROM pgr_aStar(
                                                  'SELECT gid AS id, source, target, cost, reverse_cost,x1,y1,x2,y2 FROM ways',
                                                  :start_id,
                                                  :end_id,
                                                  directed => false, heuristic => 2
                                               ))
                           SELECT ST_AsGeoJSON(w.the_geom) AS geom
                           FROM route r
                                    JOIN ways w ON r.edge = w.gid;
                           """)

        rows = self.db.execute(sql_dikstra if dijkstra else sql_aStar , {"start_id": start_node, "end_id": end_node}).fetchall()
        features = [eval(row[0]) for row in rows if row[0]]

        return {
            "type": "FeatureCollection",
            "features": [{"type": "Feature", "geometry": f} for f in features]
        }

    def close(self):
        self.db.close()

    def __enter__(self):
        print("in enter for dblocal instance")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    def get_metadata(self):
        self.update_metadata()
        return self.metadata

    def create_weather_table(self):
        """Create a weather_data table with FK to ways_vertices_pgr."""
        print("⏳ Creating weather_data table...")
        engine = self.engine
        metadata = self.get_metadata()
        if metadata.tables.get("pgrouting.weather_data") is not None:
            print(f"weather table exist")
            return

        weather_data = Table(
            "weather_data",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            # Column("node_id", Integer, ForeignKey("ways_vertices_pgr.id", ondelete="CASCADE")),
            Column("temperature", Float),
            Column("humidity", Float),
            Column("wind_speed", Float),
            Column("wind_direction", Float),
            Column("condition", String(100)),  # e.g. "clear", "rain"
            Column("timestamp", DateTime, server_default=text("NOW()")),
        )

        metadata.create_all(engine)
        print("✅ weather_data table created successfully.")

    def create_station_table(self):
        engine = self.engine
        metadata = self.get_metadata()
        if metadata.tables.get("pgrouting.brightsky_stations") is not None:
            return
        brightsky_stations = Table(
            "brightsky_stations",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("dwd_station_id", String(10 ),nullable=False),
            Column("observation_type", String(100),nullable=False),
            Column("lat", Float),
            Column("lon", Float),
            Column("height", Float),
            Column("station_name", String(255), nullable=False),
            Column("wmo_station_id", String(20)),
            Column("first_record", Date),
            Column("last_record", Date),
        UniqueConstraint("dwd_station_id", "observation_type", name="uq_dwd_obs_type"),
            schema=self.schema
        )

        metadata.create_all(engine)

    def insert_brightsky_stations(self, stations: list[dict]):
        """Insert multiple BrightSky stations into the database."""
        engine = self.engine
        metadata = self.get_metadata()
        print(f"tables{metadata.tables}")
        brightsky_stations = metadata.tables["pgrouting.brightsky_stations"]
        # brightsky_stations = Table("brightsky_stations",metadata,autoload_with=engine,schema="pgrouting")

        # If the table metadata isn’t loaded yet, reflect it
        if brightsky_stations is None:
            metadata.reflect(engine, only=["brightsky_stations"])
            brightsky_stations = metadata.tables["brightsky_stations"]
            print(f"brightsky table not found and created a new one ")

        with engine.begin() as conn:
            for s in stations:
                print(s)
                stmt = insert(brightsky_stations).on_conflict_do_nothing(
                    index_elements=["dwd_station_id", "observation_type"]
                ).values(
                    id=s.get("id"),
                    dwd_station_id=s.get("dwd_station_id"),
                    observation_type=s.get("observation_type"),
                    lat=s.get("lat"),
                    lon=s.get("lon"),
                    height=s.get("height"),
                    station_name=s.get("station_name").lower(),
                    wmo_station_id=s.get("wmo_station_id"),
                    first_record=s.get("first_record"),
                    last_record=s.get("last_record"),
                    )
                conn.execute(stmt)

    def get_dwd_station_ids(self):
        engine = self.engine
        metadata = self.get_metadata()
        brightsky_stations = Table("brightsky_stations", metadata, autoload_with=engine, schema="pgrouting")

        with engine.connect() as conn:
            query = select(brightsky_stations.c.dwd_station_id).where(brightsky_stations.c.observation_type =="forecast")
            result = conn.execute(query)
            station_ids = [row[0] for row in result.fetchall()]
            return station_ids

    def create_weather_observations_table(self):
        """
        Create the weather_observations table linked to brightsky_stations.
        """
        engine = self.engine
        metadata = self.get_metadata()  # or however you're handling schema

        if metadata.tables.get("pgrouting.weather_observations") is not None:
            return

        weather_observations = Table(
            "weather_observations",
            metadata,
            Column("id", BIGINT, primary_key=True, autoincrement=True),
            Column("station_id", String(5)), # ForeignKey("pgrouting.brightsky_stations.dwd_station_id", ondelete="CASCADE")),
            Column("timestamp", DateTime(timezone=True), nullable=False),
            Column("temperature", Float),
            Column("relative_humidity", Float),
            Column("pressure_msl", Float),
            Column("dew_point", Float),
            Column("cloud_cover", Float),
            Column("visibility", Float),
            Column("condition", String(50)),
            Column("icon", String(50)),
            Column("wind_speed", Float),
            Column("wind_direction", Float),
            Column("precipitation", Float),
            Column("sunshine", Float),
            Column("source_id", Integer),
            UniqueConstraint("station_id", "timestamp", name="uq_station_timestamp")
        )

        metadata.create_all(engine)
        print("✅ Table 'weather_observations' created successfully in schema pgrouting.")

    def insert_weather_observations(self):
        """
        Fetch and insert hourly weather data for multiple Brightsky stations.
        Skips duplicates (same station_id + timestamp).
        """
        engine = self.engine
        metadata = self.get_metadata()
        brightsky_stations = metadata.tables["pgrouting.brightsky_stations"]
        weather_observations = metadata.tables["pgrouting.weather_observations"]

        # Step 1: Get mapping of dwd_station_id -> station.id
        station_ids = self.get_dwd_station_ids()
        print(f"stationids {len(station_ids)}")
        date  = datetime.now(ZoneInfo("Europe/Berlin")).strftime("%Y-%m-%d")

        # Step 2: Loop over stations and call Brightsky API
        with engine.begin() as conn:  # ensures commit/rollback safety
            for dwd_id in station_ids:
                print(f"Fetching data for station {dwd_id} ...")
                url = f"https://api.brightsky.dev/weather?date={date}&dwd_station_id={dwd_id}"
                response = requests.get(url)
                data = response.json()

                # Skip if API returns no data
                if "weather" not in data:
                    print(f"⚠️ No weather data for {dwd_id}")
                    continue

                rows_to_insert = []
                for record in data["weather"]:
                    rows_to_insert.append({
                        "station_id": dwd_id,
                        "timestamp": record["timestamp"],
                        "temperature": record["temperature"],
                        "relative_humidity": record["relative_humidity"],
                        "pressure_msl": record["pressure_msl"],
                        "dew_point": record["dew_point"],
                        "cloud_cover": record["cloud_cover"],
                        "visibility": record["visibility"],
                        "condition": record["condition"],
                        "icon": record["icon"],
                        "wind_speed": record["wind_speed"],
                        "wind_direction": record["wind_direction"],
                        "precipitation": record["precipitation"],
                        "sunshine": record["sunshine"],
                        "source_id": record["source_id"],
                    })

                if not rows_to_insert:
                    print(f"⚠️ No data to insert for station {dwd_id}")
                    continue

                # Step 3: Insert while skipping duplicates
                stmt = insert(weather_observations).values(rows_to_insert)
                stmt = stmt.on_conflict_do_nothing(index_elements=["station_id", "timestamp"])
                conn.execute(stmt)

                print(f"✅ Inserted data for station {dwd_id} ({len(rows_to_insert)} records)")

    def create_air_pollution_table(self):
        """
        Create a PostGIS table for INWT Berlin air pollution grid data.
        Each row corresponds to one grid cell (feature) in one forecast run.
        """
        engine = self.engine
        metadata = self.get_metadata()
        if metadata.tables.get("pgrouting.air_pollution_grid") is not None:
            return


        air_pollution = Table(
            "air_pollution_grid",
            metadata,
            Column("id", Integer, primary_key=True),  # grid ID (from JSON)
            Column("forecast_time", DateTime, nullable=False),
            Column("forecast_range", String(100), nullable=False),
            Column("no2", ARRAY(Float)),
            Column("pm10", ARRAY(Float)),
            Column("pm25", ARRAY(Float)),

            # 💡 Coordinates in both systems
            Column("x_utm", Float, nullable=False),
            Column("y_utm", Float, nullable=False),
            Column("lat", Float),  # EPSG:4326 latitude
            Column("lon", Float),  # EPSG:4326 longitude

            # PostGIS geometry in projected CRS
            Column("geom", Geometry("POINT", srid=25833), nullable=False),

            UniqueConstraint("id", "forecast_time", name="uq_airgrid_forecast"),
        )

        metadata.create_all(engine)
        print("✅ Table 'air_pollution_grid' created successfully.")

