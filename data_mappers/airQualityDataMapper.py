import gzip

import ijson
from geoalchemy2 import Geometry, WKTElement
from pyproj import Transformer
from sqlalchemy import Integer, Column, DateTime, Float, ARRAY, UniqueConstraint, String, Index, ForeignKey

from core.globalconstants import GlobalConstants
from database_tables.enrichment_table import EnrichmentTable
from database_tables.mapping_table import MappingTable
from database_tables.staging_table import StagingTable
from main_core.data_source_abc_impl import DataSourceABCImpl


class AirPollutionGridStagingTable(StagingTable):
    __tablename__ = "air_pollution_grid"

    uid = Column(Integer, primary_key=True, autoincrement=True)
    grid_id = Column(Integer, nullable=False, index=True)

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

    # Geometry in source CRS (ETRS89 / UTM zone 33N)
    geom_25833 = Column(Geometry("POINT", srid=25833), nullable=False)

    __table_args__ = (
        UniqueConstraint("grid_id", "forecast_time"),
        Index(None, "geom_25833", postgresql_using="gist"),
    )


class AirPollutionGridEnrichmentTable(EnrichmentTable):
    __tablename__ = "air_pollution_grid_enrichment"

    uid = Column(Integer, primary_key=True, autoincrement=True)
    grid_id = Column(Integer, nullable=False, index=True)

    forecast_time = Column(DateTime, nullable=True, index=True)
    forecast_range = Column(String(100), nullable=True)

    no2 = Column(ARRAY(Float))
    pm10 = Column(ARRAY(Float))
    pm25 = Column(ARRAY(Float))

    x_utm = Column(Float, nullable=False)
    y_utm = Column(Float, nullable=False)
    lat = Column(Float)
    lon = Column(Float)

    geom_25833 = Column(Geometry("POINT", srid=25833), nullable=False)
    geom_4326 = Column(Geometry("POINT", srid=4326))

    __table_args__ = (
        UniqueConstraint("grid_id", "forecast_time"),
        Index(None, "grid_id", "forecast_time", "uid"),
        Index(None, "geom_25833", postgresql_using="gist"),
        Index(None, "geom_4326", postgresql_using="gist"),
    )


class AirPollutionGridMappingTable(MappingTable):
    __tablename__ = "air_pollution_grid_mapping"

    # Override parent's way_id to remove column-level unique=True (this table has N rows per way)
    way_id = Column(Integer, ForeignKey(f"{GlobalConstants.base_schema}.{GlobalConstants.base_table}.id"), nullable=False)

    uid = Column(Integer, primary_key=True, autoincrement=True)
    grid_uid = Column(Integer, nullable=False, index=True)
    grid_id = Column(Integer, nullable=False, index=True)
    intersection_length_m = Column(Float, nullable=False)

    __table_args__ = (
        UniqueConstraint("way_id", "grid_id"),
    )


class AirQualityDataMapper(DataSourceABCImpl):
    # f"https://werkzeug.dcaiti.tu-berlin.de/fairqberlin/inwt_fairq_cache_skip_{skip}_limit_100000.json.gz",f"./airw_{skip}.json.gz")

    transformer = Transformer.from_crs(25833, 4326, always_xy=True)

    def enrichment_db_query(self) -> None | str:
        enrichment = self.data_source_config.storage.enrichment
        sql = f"""
            UPDATE {enrichment.table_schema}.{enrichment.table_name}
            SET geom_4326 = ST_Transform(geom_25833, 4326)
            WHERE geom_25833 IS NOT NULL
              AND geom_4326 IS NULL;
        """
        return sql

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

                    point_25833 = WKTElement(f"POINT({x} {y})", srid=25833)
                    row = {
                        "grid_id": props["id"],
                        "forecast_time": props["date_time_forecast_iso8601"],
                        "forecast_range": props["forecast_range_iso8601"],
                        "no2": props.get("no2"),
                        "pm10": props.get("pm10"),
                        "pm25": props.get("pm2.5"),
                        "x_utm": x,
                        "y_utm": y,
                        "lon": lon,
                        "lat": lat,
                        "geom_25833": point_25833,
                    }
                    row_to_insert.append(row)
                    # return row_to_insert
                except Exception as e:
                    print(f"⚠️ Skipped one feature due to error: {e}")

        # print(f"✅ Finished inserting {count} records from {gz_path}")
        return row_to_insert
