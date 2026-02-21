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
    def mapping_db_query(self) -> str:
        self.logger.info("Mapping DWD stations to links (insert into mapping table)")

        base = self.data_source_config.mapping.base_table
        enrichment = self.data_source_config.storage.enrichment
        mapping = self.data_source_config.mapping

        sql = f"""
            INSERT INTO {mapping.table_schema}.{mapping.table_name} (way_id, dwd_station_id, distance, bearing_degree)
            SELECT
                w.id AS way_id,
                s.dwd_station_id AS dwd_station_id,
                ST_Distance(
                    w.geometry::geography,
                    s.point::geography
                ) AS distance,
                MOD(
                    (DEGREES(
                      ST_Azimuth(
                        ST_StartPoint(w.geometry),
                        ST_EndPoint(w.geometry)
                      )
                    ) + 360)::NUMERIC,
                    360
                  ) AS bearing_degree
            FROM {base.table_schema}.{base.table_name} w
            JOIN LATERAL (
                SELECT
                    en.uid,
                    en.dwd_station_id,
                    en.point
                FROM {enrichment.table_schema}.{enrichment.table_name} en
                ORDER BY
                    ST_Distance(
                        w.geometry::geography,
                        en.point::geography
                    )
                LIMIT 1
            ) s ON TRUE;
        
        """

        return sql
