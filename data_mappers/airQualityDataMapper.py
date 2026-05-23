import gzip

import orjson
from geoalchemy2 import Geometry
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

    # Coordinates (UTM 33N, mirrors geom_25833 for convenience)
    x_utm = Column(Float, nullable=False)
    y_utm = Column(Float, nullable=False)

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
        self.logger.info(f"Reading and parsing {gz_path}")
        with gzip.open(gz_path, "rb") as f:
            payload = orjson.loads(f.read())

        features = payload.get("features", []) or []
        rows = []
        skipped = 0
        for feature in features:
            try:
                props = feature["properties"]
                x, y = feature["geometry"]["coordinates"]
                rows.append({
                    "grid_id": props["id"],
                    "forecast_time": props["date_time_forecast_iso8601"],
                    "forecast_range": props["forecast_range_iso8601"],
                    "no2": props.get("no2"),
                    "pm10": props.get("pm10"),
                    "pm25": props.get("pm2.5"),
                    "x_utm": x,
                    "y_utm": y,
                    "geom_25833": f"SRID=25833;POINT({x} {y})",
                })
            except Exception:
                skipped += 1
        if skipped:
            self.logger.warning(f"Skipped {skipped} malformed features in {gz_path}")
        return rows
