import math

from geoalchemy2 import Geometry
from sqlalchemy import Column, BigInteger, String, UniqueConstraint, Float, Index
from sqlalchemy.dialects.postgresql import JSONB, ARRAY

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

class TreeMappingTable(MappingTable):
    __tablename__ = "tree_mapping"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    trees = Column(JSONB, nullable=False)


class TreeMapper(DataSourceABCImpl):


    def mapping_db_query(self) -> None | str:
        base = self.data_source_config.mapping.base_table
        staging= self.data_source_config.storage.staging
        mapping = self.data_source_config.mapping

        sql = f"""
                INSERT INTO {mapping.table_schema}.{mapping.table_name} (way_id, trees)
                SELECT
                    w.id AS way_id,
                    COALESCE(
                        jsonb_agg(
                            jsonb_build_object(
                                'tree_id', t.id,
                                'source_id', t.source_id,
                                'distance_m', ST_Distance(
                                    t.geometry_25833,
                                    w.geometry_25833
                                )
                            )
                            ORDER BY ST_Distance(
                                t.geometry_25833,
                                w.geometry_25833
                            )
                        ) FILTER (WHERE t.id IS NOT NULL),
                        '[]'::jsonb
                    ) AS trees
                FROM {base.table_schema}.{base.table_name} w
                LEFT JOIN {staging.table_schema}.{staging.table_name} t
                  ON t.geometry_25833 && ST_Expand(w.geometry_25833, 50)
                 AND ST_DWithin(
                        t.geometry_25833,
                        w.geometry_25833,
                        50
                     )
                GROUP BY w.id
                ON CONFLICT (way_id)
                DO UPDATE SET trees = EXCLUDED.trees;
            """
        return sql

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

