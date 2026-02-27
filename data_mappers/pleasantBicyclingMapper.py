from sqlalchemy import Column, Integer, Float, ARRAY, UniqueConstraint, Index, func, String, BigInteger, DateTime
from sqlalchemy.dialects.postgresql import JSONB

from database_tables.enrichment_table import EnrichmentTable
from database_tables.mapping_table import MappingTable
from database_tables.staging_table import StagingTable
from main_core.data_source_abc_impl import DataSourceABCImpl



class PleasantStagingTable(StagingTable):
    pass
    # __tablename__ = "elevation_python_staging"
    # id = Column(BigInteger, primary_key=True, autoincrement=True)
    #
    # way_id = Column(BigInteger, nullable=False, index=True)
    #
    # total_ascent = Column(Float, nullable=False)
    # total_descent = Column(Float, nullable=False)
    # max_slope = Column(Float, nullable=False)
    # avg_slope = Column(Float, nullable=False)
    # tile_name = Column(ARRAY(String), nullable=True)
    # sample_count = Column(Integer, nullable=True)
    #
    # created_at = Column(DateTime(timezone=True), default=func.now())
    #
    # __table_args__ = (
    #     UniqueConstraint("way_id"),
    # )

class PleasantBicyclingMapper(DataSourceABCImpl):
    pass