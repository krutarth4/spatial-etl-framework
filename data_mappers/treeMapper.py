import math

from geoalchemy2 import Geometry
from sqlalchemy import Column, BigInteger, Integer, String, UniqueConstraint, Float, Index
from sqlalchemy.dialects.postgresql import JSONB, ARRAY

from database_tables.enrichment_table import EnrichmentTable
from database_tables.mapping_table import MappingTable
from database_tables.staging_table import StagingTable
from main_core.data_source_abc_impl import DataSourceABCImpl
import geopandas as gpd


class TreeStagingTable(StagingTable):
    __tablename__ = "tree_staging"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    source_id = Column(String, nullable=True, index=True)

    attributes = Column(JSONB, nullable=False)

    geometry_25833 = Column(
        Geometry("POINT", srid=25833),
        nullable=False,
    )



    __table_args__ = (
        UniqueConstraint("source_id"),
        Index(
            None,
        "geometry_25833",
        postgresql_using="gist")
    )

class TreeEnrichmentTable(EnrichmentTable):
    """Normalized per-tree record shown in the debug panel.

    The staging table keeps the raw Berlin tree cadastre fields packed in a
    single `attributes` JSONB. This table is the staging row with those raw
    German fields unpacked into clean, typed, English-named columns. The
    extraction and the derived `leaf_type` / `size_class` values are produced
    by the declarative `derive` operators in data_source_configs/tree.yaml,
    not here. `source_id`, `geometry_25833`, and `attributes` are copied
    verbatim from staging by the default staging→enrichment sync; everything
    else stays NULL until the operators run.
    """
    __tablename__ = "tree_enrichment"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    source_id = Column(String, nullable=True, index=True)

    # Copied verbatim from staging (matching column names) by the default sync.
    attributes = Column(JSONB, nullable=True)
    geometry_25833 = Column(Geometry("POINT", srid=25833), nullable=True)

    # Normalized columns — populated by the derive operators in tree.yaml.
    species_de = Column(String, nullable=True)              # art_dtsch
    species_bot = Column(String, nullable=True)             # art_bot
    genus = Column(String, nullable=True)                   # gattung
    leaf_type = Column(String, nullable=True)               # art_gruppe -> deciduous/coniferous
    street = Column(String, nullable=True)                  # strname
    district = Column(String, nullable=True)                # bezirk
    planting_year = Column(Integer, nullable=True)          # pflanzjahr
    age_years = Column(Float, nullable=True)                # standalter
    crown_diameter_m = Column(Float, nullable=True)         # kronedurch
    trunk_circumference_cm = Column(Float, nullable=True)   # stammumfg
    height_m = Column(Float, nullable=True)                 # baumhoehe
    size_class = Column(String, nullable=True)              # derived from height_m

    __table_args__ = (
        UniqueConstraint("source_id"),
        Index(
            None,
            "geometry_25833",
            postgresql_using="gist",
        ),
    )


class TreeMappingTable(MappingTable):
    __tablename__ = "tree_mapping"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    trees = Column(JSONB, nullable=False)


class TreeMapper(DataSourceABCImpl):

    def read_file_content(self, path: str):

        # Read entire GeoPackage
        gdf = gpd.read_file(path, engine="pyogrio")

        if gdf.empty:
            return []

        if "geometry" not in gdf.columns:
            raise ValueError("No geometry column found")

        if gdf.crs is None:
            raise ValueError("Input dataset has no CRS defined")

        # Ensure CRS = 25833
        if gdf.crs.to_epsg() != 25833:
            gdf = gdf.to_crs(25833)

        # Convert geometry to WKB hex
        gdf["geometry_wkb"] = gdf.geometry.apply(
            lambda g: g.wkb_hex if g else None
        )

        gdf = gdf.drop(columns=["geometry"])

        records = []

        for row in gdf.to_dict(orient="records"):
            geometry_wkb = row.pop("geometry_wkb", None)

            source_id = row.get("gisid")

            cleaned_attributes = {
                k: (None if isinstance(v, float) and math.isnan(v) else v)
                for k, v in row.items()
            }

            records.append({
                "source_id": source_id,
                "attributes": cleaned_attributes,
                "geometry_25833": geometry_wkb,
            })

        print(f"Total records: {len(records)}")
        return records

