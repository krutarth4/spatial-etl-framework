from geoalchemy2 import Geometry
from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, UniqueConstraint, ForeignKeyConstraint

from core.globalconstants import GlobalConstants
from database_tables.enrichment_table import EnrichmentTable
from database_tables.mapping_table import MappingTable
from database_tables.staging_table import StagingTable
from main_core.data_source_abc_impl import DataSourceABCImpl


class DwdStationsTable(StagingTable):
    # Make sure no indexing and constrains are added here other than a PK
    __tablename__ = "dwd_station_locations_staging"
    uid = Column(Integer, primary_key=True, autoincrement=True)
    id = Column(Integer)
    dwd_station_id = Column(Integer, unique=True, nullable=False)
    station_name = Column(String)
    observation_type = Column(String)
    lat = Column(Float)
    lon = Column(Float)
    height = Column(Float)
    wmo_station_id = Column(String)
    first_record = Column(DateTime(timezone=True))
    last_record = Column(DateTime(timezone=True))


class DwdWeatherStationEnrichmentTable(EnrichmentTable):
    __tablename__ = "dwd_station_locations_enrichment"
    uid = Column(Integer, primary_key=True, autoincrement=True)
    dwd_station_id = Column(Integer, unique=True, nullable=False)
    point = Column(Geometry(geometry_type="POINT", srid=4326), index=True)


class DwdMappingTable(MappingTable):
    __tablename__ = "dwd_station_locations_mapping"
    uid = Column(Integer, primary_key=True, autoincrement=True)
    dwd_station_id = Column(Integer, ForeignKey(
        f"{GlobalConstants.base_schema}.{DwdWeatherStationEnrichmentTable.__tablename__}.dwd_station_id", ondelete="Cascade"),
                        nullable=False)
    distance = Column(Float, nullable=False)
    bearing_degree = Column(Float, nullable=True)


class WeatherStationMapper(DataSourceABCImpl):

    def source_filter(self, data: list[dict]) -> list[dict]:
        """Custom filter for DWD stations."""

        data = data[0]["sources"]
        # filter only historical observation type
        filtered = [
            row for row in data
            if row.get("observation_type") == "forecast" and int(row.get("last_record")[:4]) >= 2024
        ]

        self.logger.info(f"Filtered {len(data)} → {len(filtered)} rows")
        return filtered

    def enrichment_db_query(self) -> None | str:
        staging = self.data_source_config.storage.staging
        enrichment = self.data_source_config.storage.enrichment
        sql = f"""
        UPDATE {enrichment.table_schema}.{enrichment.table_name} e 
        SET point = ST_SetSRID(
                    ST_MakePoint(s.lon, s.lat),
                    4326
                )
                from {staging.table_schema}.{staging.table_name} s
        WHERE e.dwd_station_id = s.dwd_station_id
            AND e.point IS NULL
        
            
              """

        return sql

