from database.db_instancce import DbInstance

from sqlalchemy import String, DateTime, Column, Integer, Text, Boolean, JSON, func

from database.base import Base


class DataSourceMetadata(Base):
    __tablename__ = "data_source_metadata"

    id = Column(Integer, primary_key=True)

    # --- identity
    source_key = Column(String, nullable=False, unique=True)
    source_name = Column(String, nullable=False)
    description = Column(Text)

    # --- file / input tracking
    source_type = Column(String, nullable=False)
    # e.g. "osm_pbf", "csv", "api", "geojson"

    file_path = Column(String)
    file_checksum = Column(String)
    file_size_bytes = Column(Integer)

    # --- timestamps
    last_ingested_at = Column(DateTime)
    last_checked_at = Column(DateTime)
    last_successful_run_at = Column(DateTime)

    # --- status
    is_active = Column(Boolean, default=True)
    last_run_status = Column(String)
    last_run_message = Column(Text)

    # --- config tracking (VERY useful)
    config_hash = Column(String)
    config_snapshot = Column(JSON)

    # --- audit
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, onupdate=func.now())


class DataSourceMetadataRepository:
    table_name  = DataSourceMetadata.__tablename__
    def __init__(self, db: DbInstance, schema: str) :
        if db is None:
            return
        self.db = db
        self.schema = schema

    def is_metadata_table_present(self) -> bool:
        self.db.table_exists(self.table_name, self.schema)

    def create_metadata_table(self) -> None:
        self.db.create_table_if_not_exist(self.table_name, self.schema)

    def delete_metadata_table(self, schema: str) -> None:
        pass

    def get_metadata(self, data_source_unique_name:str) -> DataSourceMetadata:
        pass
