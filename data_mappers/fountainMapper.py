import osmium
from geoalchemy2 import Geometry
from sqlalchemy import Column, BigInteger, Integer, String, Float, UniqueConstraint

from database_tables.enrichment_table import EnrichmentTable
from database_tables.staging_table import StagingTable
from database_tables.mapping_table import MappingTable
from main_core.data_source_abc_impl import DataSourceABCImpl


class FountainStaging(StagingTable):
    __tablename__ = "fountain_staging"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    osm_id = Column(BigInteger, nullable=False)
    name = Column(String, nullable=True)
    amenity = Column(String, nullable=True)
    access = Column(String, nullable=True)
    lat = Column(Float, nullable=False)
    lon = Column(Float, nullable=False)

    __table_args__ = (UniqueConstraint("osm_id"),)


class FountainEnrichmentTable(EnrichmentTable):
    __tablename__ = "fountain_enrichment"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    osm_id = Column(BigInteger, nullable=False)
    name = Column(String, nullable=True)
    amenity = Column(String, nullable=True)
    access = Column(String, nullable=True)
    geometry_25833 = Column(Geometry(geometry_type="POINT", srid=25833), index=True)

    __table_args__ = (UniqueConstraint("osm_id"),)


class FountainMappingTable(MappingTable):
    __tablename__ = "fountain_mapping"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    fountain_count = Column(Integer, nullable=False, default=0)


class _FountainHandler(osmium.SimpleHandler):
    _AMENITY_VALUES = {"fountain", "drinking_water"}
    _MAN_MADE_VALUES = {"water_tap"}

    def __init__(self):
        super().__init__()
        self.records = []

    def node(self, n):
        amenity = n.tags.get("amenity", "")
        man_made = n.tags.get("man_made", "")
        if amenity not in self._AMENITY_VALUES and man_made not in self._MAN_MADE_VALUES:
            return
        if not n.location.valid():
            return
        self.records.append(
            {
                "osm_id": n.id,
                "name": n.tags.get("name") or None,
                "amenity": amenity or man_made,
                "access": n.tags.get("access") or None,
                "lat": n.location.lat,
                "lon": n.location.lon,
            }
        )


class FountainMapper(DataSourceABCImpl):

    def read_file_content(self, path) -> list:
        handler = _FountainHandler()
        handler.apply_file(str(path), locations=True)
        self.logger.info(f"Extracted {len(handler.records)} fountain features from {path}")
        return handler.records

    def enrichment_db_query(self) -> str | None:
        staging = self.data_source_config.storage.staging
        enrichment = self.data_source_config.storage.enrichment
        return f"""
            INSERT INTO {enrichment.table_schema}.{enrichment.table_name}
                (osm_id, name, amenity, access, geometry_25833)
            SELECT
                s.osm_id,
                s.name,
                s.amenity,
                s.access,
                ST_Transform(
                    ST_SetSRID(ST_MakePoint(s.lon, s.lat), 4326),
                    25833
                ) AS geometry_25833
            FROM {staging.table_schema}.{staging.table_name} s
            ON CONFLICT (osm_id) DO UPDATE
                SET name          = EXCLUDED.name,
                    amenity       = EXCLUDED.amenity,
                    access        = EXCLUDED.access,
                    geometry_25833 = EXCLUDED.geometry_25833
        """
