from datetime import datetime
from typing import Any, List

from zoneinfo import ZoneInfo

from sqlalchemy import Column, Integer, String, Float, TIMESTAMP, DATETIME, DateTime, UniqueConstraint, ForeignKey


from database.base import Base
from handlers.http_handler import HttpHandler
from main_core.data_source_abc_impl import DataSourceABCImpl

#
class WeatherTable(Base):
    __tablename__ = "weather"

    uid = Column(Integer, primary_key=True, autoincrement=True, index=True) # make sure to create indexing for the table for better query and fast computation
    source_id = Column(Integer, nullable=False)
    station_id = Column(Integer)
    dwd_station_id = Column(String)
    timestamp = Column(TIMESTAMP(timezone=True), nullable=False)
    temperature = Column(Float, nullable=False)
    relative_humidity = Column(Float)
    pressure_msl = Column(Float, nullable=False)
    dew_point = Column(Float, nullable=False)
    cloud_cover = Column(Float, nullable=False)
    visibility = Column(Float)
    conditions = Column(String)
    wind_speed = Column(Float)
    wind_direction = Column(Float, nullable=False)
    precipitation = Column(Float, nullable=False)
    sunshine = Column(Float, nullable=False)
    UniqueConstraint('source_id', "timestamp", name='station_time_unique_id')
    # station = relationship("DwdStationsTable", backref="weather_rows")

class WeatherMapper(DataSourceABCImpl):
    pass
    # https: // brightsky.dev / docs /  # /operations/getWeather#Query-Parameters
    # dwd_station_ids = ["00399", "00403", "00400", "00410", "00420", "00427", "00430", "00433"]
    # data_mapper = []


    # def fetch(self):
    #     url = self.data_source_config.source.url
    #     print(url)
    #     http_handler = HttpHandler()
    #     source = self.data_source_config.source
    #     date = datetime.now(ZoneInfo("Europe/Berlin")).isoformat()
    #     source.params = {"date":date}
    #     for station in self.dwd_station_ids:
    #         source.params = {**source.params,"dwd_station_id":station}
    #         result = http_handler.call(uri=source.url, params=source.params, headers=source.headers,destination_path=source.destination, save=source.save_local)
    #         self.data_mapper.extend(result["weather"])
    #         # print(result)
    #     return self.data_mapper

    def source_filter(self, data: list[Any]) -> List[dict]:
        result: List[dict] = []

        for content in data:
            sources = content.get("sources", [])
            if not sources:
                continue  # or raise, depending on strictness

            dwd_station_id = sources[0].get("dwd_station_id")

            for weather in content.get("weather", []):
                # copy to avoid mutating original payload
                enriched = dict(weather)
                enriched["dwd_station_id"] = dwd_station_id
                result.append(enriched)

        return result





