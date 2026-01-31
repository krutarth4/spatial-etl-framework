from datetime import datetime
from typing import Any, List

from zoneinfo import ZoneInfo

from sqlalchemy import Column, Integer, String, Float, TIMESTAMP, DATETIME, DateTime, UniqueConstraint, ForeignKey
from sqlalchemy.orm import relationship

from database.base import Base
from database_tables.enrichment_table import EnrichmentTable
from database_tables.mapping_table import MappingTable
from database_tables.staging_table import StagingTable
from handlers.http_handler import HttpHandler
from main_core.data_source_abc_impl import DataSourceABCImpl

#
class WeatherStagingTable(StagingTable):
    __tablename__ = "weather_staging"

    uid = Column(Integer, primary_key=True, autoincrement=True) # make sure to create indexing for the table for better query and fast computation
    source_id = Column(Integer, nullable=False)
    dwd_station_id = Column(String, nullable=False)
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
    __table_args__ = (
        UniqueConstraint('dwd_station_id', "timestamp", name='uniq_weatherstaging'),
    )

class WeatherEnrichmentTable(EnrichmentTable):
    __tablename__ = "weather_enrichment"

    uid = Column(Integer, primary_key=True, autoincrement=True, index=True) # make sure to create indexing for the table for better query and fast computation
    dwd_station_id = Column(String, nullable=False)
    timestamp = Column(TIMESTAMP(timezone=True), nullable=False)
    visibility = Column(Float)
    conditions = Column(String)
    wind_speed = Column(Float)
    wind_direction = Column(Float, nullable=False)
    __table_args__ = (
        UniqueConstraint('dwd_station_id', "timestamp", name='uniq_weatherenrichment'),
    )


class WeatherMapper(DataSourceABCImpl):
    pass
    # https: // brightsky.dev / docs /  # /operations/getWeather#Query-Parameters


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





