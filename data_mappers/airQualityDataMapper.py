import gzip

import ijson
from geoalchemy2 import Geometry, WKTElement
from pyproj import Transformer
from sqlalchemy import Integer, Column, DateTime, Float, ARRAY, UniqueConstraint, String

from database.base import Base
from handlers.file_handler import FileHandler
from handlers.http_handler import HttpHandler
from main_core.data_source_abc_impl import DataSourceABCImpl

class AirPollutionGrid(Base):
    __tablename__ = "air_pollution_grid"

    # Primary Key
    id = Column(Integer, primary_key=True, nullable=False)

    # Forecast metadata
    forecast_time = Column(DateTime, nullable=True)
    forecast_range = Column(String(100), nullable=True)

    # Pollutant arrays
    no2 = Column(ARRAY(Float))
    pm10 = Column(ARRAY(Float))
    pm25 = Column(ARRAY(Float))

    # Coordinates
    x_utm = Column(Float, nullable=False)
    y_utm = Column(Float, nullable=False)
    lat = Column(Float)
    lon = Column(Float)

    # PostGIS geometry column
    geom = Column(Geometry("POINT", srid=25833), nullable=False)

    # Unique constraint (same as metadata version)
    __table_args__ = (
        UniqueConstraint("id", "forecast_time", name="uq_airgrid_forecast"),
    )
class AirQualityDataMapper(DataSourceABCImpl):
    skips = [0, 100000, 200000, 300000]
    transformer = Transformer.from_crs(25833, 4326, always_xy=True)
# f"https://werkzeug.dcaiti.tu-berlin.de/fairqberlin/inwt_fairq_cache_skip_{skip}_limit_100000.json.gz",f"./airw_{skip}.json.gz")
#     def fetch(self):
#         http_handler = HttpHandler()
#         source = self.data_source_config.source
#         data_mapper = []
#         file_handler = FileHandler(source.destination)
#
#         for skip in self.skips:
#             self.logger.info(f"fetching data from {source}:{skip}")
#             destination = f"{source.destination}/airw_{skip}.{source.response_type}"
#             url = f"{source.url}inwt_fairq_cache_skip_{skip}_limit_100000.json.gz"
#             a = file_handler.get_latest_data_file(f"airw_{skip}",source.response_type)
#             print("file handler", a)
#             # result = self.load_and_store_gz_json(a)
#
#             result = http_handler.call(uri=url, params=source.params, headers=source.headers,
#                                        destination_path=destination,
#                                        save=source.save_local)
#
#
#             data_mapper.extend(result)
#
#         return data_mapper

    def read_file_content(self, path):
        return self.load_and_store_gz_json(path)

    def load_and_store_gz_json(self, gz_path):
        print(f"📖 Reading and inserting data from {gz_path}")
        row_to_insert = []
        with gzip.open(gz_path, "rt", encoding="utf-8") as f:
            # Stream each feature
            for feature in ijson.items(f, "features.item"):
                try:
                    props = feature["properties"]
                    geom = feature["geometry"]
                    x, y = geom["coordinates"]
                    lon, lat = self.transformer.transform(x, y)

                    point = WKTElement(f"POINT({x} {y})", srid=25833)
                    row = {
                        "id": props["id"],
                        "forecast_time": props["date_time_forecast_iso8601"],
                        "forecast_range": props["forecast_range_iso8601"],
                        "no2": props.get("no2"),
                        "pm10": props.get("pm10"),
                        "pm25": props.get("pm2.5"),
                        "x_utm": x,
                        "y_utm": y,
                        "lon": lon,
                        "lat": lat,
                        "geom": point,
                    }
                    row_to_insert.append(row)
                    # return row_to_insert
                except Exception as e:
                    print(f"⚠️ Skipped one feature due to error: {e}")

        # print(f"✅ Finished inserting {count} records from {gz_path}")
        return row_to_insert