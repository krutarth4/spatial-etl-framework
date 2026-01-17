from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, UniqueConstraint

from database_tables.enrichment_table import EnrichmentTable
from database_tables.mapping_table import MappingTable
from database_tables.staging_table import StagingTable
from main_core.data_source_abc_impl import DataSourceABCImpl


class DwdStationsTable(StagingTable):
    # Make sure no indexing and constrains are added here other than a PK
    __tablename__ = "dwd_station_locations"

    uid = Column(Integer, primary_key=True, autoincrement=True,
                 index=True)  # make sure to create indexing for the table for better query and fast computation
    id = Column(Integer)
    dwd_station_id = Column(Integer, unique=True, nullable=False)
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

class DwdMappingTable(MappingTable):
    __tablename__ = "dwd_mapping_stations"
    uid = Column(Integer, primary_key=True, autoincrement=True)
    station_id = Column(Integer, unique=True, nullable=False)
    way_id = Column(Integer, unique=True, nullable=False)

# class StationLocationLink(Base):
#     __tablename__ = "station_location_link"
#     uid = Column(Integer, primary_key=True, autoincrement=True)
#
#     link_id = Column(
#         Integer,
#         ForeignKey("test_runner.ways_base.id", ondelete="CASCADE"),
#         nullable=False,
#         index=True,
#     )
#
#     station_id = Column(
#         Integer,
#         ForeignKey("test_runner.dwd_station_locations.uid", ondelete="CASCADE"),
#         nullable=False,
#         index=True,
#     )
#
#     __table_args__ = (
#         UniqueConstraint(
#             "link_id",
#             "station_id",
#             name="uq_station_link"
#         ),)


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

    # TODO: mapper to understand how the data is mapped -> for the routing servce to unserstand how the data is mapped
    # def map_to_link_db_query(self) -> None | str:
    #     self.logger.info(f"Mapping DWD Stations to links through sql query")
    #     sql = f"""
    #         UPDATE {self.data_source_config.mapping.base_table.table_schema}
    #         .{self.data_source_config.mapping.base_table.table_name} AS w
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
