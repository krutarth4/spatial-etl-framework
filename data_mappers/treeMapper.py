import math

from geoalchemy2 import Geometry
from sqlalchemy import Column, BigInteger, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB

from database_tables.staging_table import StagingTable
from main_core.data_source_abc_impl import DataSourceABCImpl
import geopandas as gpd


class TreeStagingTable(StagingTable):
    __tablename__ = "tree_staging"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    source_id = Column(String, nullable=True)

    attributes = Column(JSONB, nullable=True)

    geometry = Column(
        Geometry("POINT", srid=4326),
        nullable=False
    )

    __table_args__ = (
        UniqueConstraint(id, geometry),
    )



class TreeMapper(DataSourceABCImpl):

    def read_file_content(self, path: str):

        # Read entire GeoPackage
        gdf = gpd.read_file(path, engine="pyogrio")

        if gdf.empty:
            return []

        if "geometry" not in gdf.columns:
            raise ValueError("No geometry column found")

        if gdf.crs.to_epsg() != 4326:
            gdf = gdf.to_crs(4326)

        # Convert geometry column to WKB hex in vectorized way
        gdf["geometry_wkb"] = gdf.geometry.apply(lambda g: g.wkb_hex if g else None)

        # Drop original geometry column
        gdf = gdf.drop(columns=["geometry"])
        records = []
        for row in gdf.to_dict(orient="records"):

            geometry_wkb = row.pop("geometry_wkb", None)

            # Extract source_id (gisid)
            source_id = row.get("gisid")

            # Clean NaN values for JSONB
            cleaned_attributes = {}
            for k, v in row.items():
                if isinstance(v, float) and math.isnan(v):
                    cleaned_attributes[k] = None
                else:
                    cleaned_attributes[k] = v

            records.append({
                "source_id": source_id,
                "attributes": cleaned_attributes,
                "geometry": geometry_wkb
            })
        print(f"Total records: {len(records)}")
        return records