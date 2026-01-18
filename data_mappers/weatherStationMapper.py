from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, UniqueConstraint

from database_tables.enrichment_table import EnrichmentTable
from database_tables.mapping_table import MappingTable
from database_tables.staging_table import StagingTable
from main_core.data_source_abc_impl import DataSourceABCImpl


class DwdStationsTable(StagingTable):
    # Make sure no indexing and constrains are added here other than a PK
    __tablename__ = "dwd_station_locations_staging"
    uid = Column(Integer, primary_key=True, autoincrement=True)
    id = Column(Integer)
    dwd_station_id = Column(Integer, nullable=False)
    station_name = Column(String)
    observation_type = Column(String)
    lat = Column(Float)
    lon = Column(Float)
    height = Column(Float)
    wmo_station_id = Column(String)
    first_record = Column(DateTime(timezone=True))
    last_record = Column(String)

class DwdWeatherStationEnrichmentTable(EnrichmentTable):
    __tablename__ = "dwd_weather_station_enrichment"
    uid = Column(Integer, primary_key=True, autoincrement=True)
    station_id = Column(Integer, unique=True, nullable=False)
    station_name = Column(String)
    weight = Column(Float)

class DwdMappingTable(MappingTable):
    __tablename__ = "dwd_mapping_stations"
    uid = Column(Integer, primary_key=True, autoincrement=True)
    station_id = Column(Integer,ForeignKey("test.dwd_station_locations_staging.uid", ondelete="Cascade") , unique=True, nullable=False)
    distance = Column(Float)


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

    #  This check is called right after the source filter if we should continue
    # def check_before_update(self, old_data, new_data) -> bool:
    #     if len(old_data) != len(new_data):
    #         return True
    #     return False

    # def map_to_link_db_query(self) -> None | str:
    #     self.logger.info(f"Mapping DWD Stations to links through sql query")
    #     sql = f"""
    #         UPDATE {self.data_source_config.mapping.table_schema}
    #         .{self.data_source_config.mapping.table_name} AS w
    #         SET
    #             {self.data_source_config.mapping.base_table.column_name} = (
    #         SELECT d.dwd_station_id
    #         FROM {self.data_source_config.storage.table_schema}.{self.data_source_config.storage.table_name} AS d
    #         ORDER BY ST_SetSRID(ST_MakePoint(d.lon, d.lat), 4326) <->
    #                 w.geom
    #         LIMIT 1
    #             )
    #     """
    #     return sql


def map_to_link_db_query(self) -> str:
    self.logger.info("Mapping DWD stations to links (insert into mapping table)")

    base = self.data_source_config.mapping.base_table
    staging = self.data_source_config.storage
    mapping = self.data_source_config.mapping.mapping_table

    sql = f"""
    INSERT INTO {mapping.table_schema}.{mapping.table_name} (
        station_id,
        link_id,
        distance
    )
    SELECT
        d.dwd_station_id,
        w.id AS link_id,
        'nearest_station' AS mapping_method
    FROM {staging.table_schema}.{staging.table_name} d
    JOIN LATERAL (
        SELECT id
        FROM {base.table_schema}.{base.table_name}
        ORDER BY
            ST_SetSRID(ST_MakePoint(d.lon, d.lat), 4326) <-> geom
        LIMIT 1
    ) w ON TRUE
    WHERE NOT EXISTS (
        SELECT 1
        FROM {mapping.table_schema}.{mapping.table_name} m
        WHERE m.station_id = d.dwd_station_id
    );
    """

    return sql