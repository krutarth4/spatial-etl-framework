from database.db_instancce import DbInstance

from sqlalchemy import String, DateTime, Column, Integer, Text, Boolean, JSON, func, select
from sqlalchemy.dialects.postgresql import ARRAY

from database.base import Base
from log_manager.logger_manager import LoggerManager


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

    file_path = Column(ARRAY(String))
    file_checksum = Column(String)
    file_size_bytes = Column(Integer)

    # --- timestamps
    last_ingested_at = Column(DateTime)
    last_checked_at = Column(DateTime)
    last_successful_run_at = Column(DateTime)

    # --- status
    is_active = Column(Boolean, default=True)
    current_run_status = Column(String, default="idle")
    last_run_status = Column(String)
    last_run_message = Column(Text)

    # --- config tracking (VERY useful)
    config_hash = Column(String)
    config_snapshot = Column(JSON)

    # --- audit
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, onupdate=func.now())


class DataSourceMetadataRepository:
    table_name = DataSourceMetadata.__tablename__

    def __init__(self, db: DbInstance, schema: str):
        if db is None:
            return
        self.db = db
        self.schema = schema
        self.logger = LoggerManager(type(self).__name__)
        self._bind_model_schema()

    def _bind_model_schema(self) -> None:
        # Queries use the ORM model directly, so bind its table schema dynamically
        # to the configured metadata schema (otherwise SQL defaults to public).
        if self.schema:
            DataSourceMetadata.__table__.schema = self.schema

    def is_metadata_table_present(self) -> bool:
        return self.db.table_exists(self.table_name, self.schema)

    def create_metadata_table(self) -> None:
        # Ensure target schema exists before creating the table.
        try:
            self.db.create_schema_if_not_exists()
        except Exception:
            # Fall back to create_table_if_not_exist error handling/logging.
            pass
        self.db.create_table_if_not_exist(self.table_name, self.schema)

    def delete_metadata_table(self, schema: str = None) -> None:
        self.db.drop_table(self.table_name, schema or self.schema)

    def get_metadata(self, source_key: str) -> DataSourceMetadata | None:
        with self.db.session_scope() as session:
            stmt = (
                select(DataSourceMetadata)
                .where(DataSourceMetadata.source_key == source_key)
            )
            return session.execute(stmt).scalar_one_or_none()

    def get_metadata_file_paths(self, source_key: str) -> list[str] | None:
        with self.db.session_scope() as session:
            stmt = (
                select(DataSourceMetadata.file_path)
                .where(DataSourceMetadata.source_key == source_key)
            )
            return session.execute(stmt).scalar_one_or_none()

    def upsert_metadata(
            self,
            source_key: str,
            defaults: dict,
    ) -> DataSourceMetadata:
        with self.db.session_scope() as session:
            metadata = session.execute(
                select(DataSourceMetadata)
                .where(DataSourceMetadata.source_key == source_key)
            ).scalar_one_or_none()

            if metadata is None:
                metadata = DataSourceMetadata(
                    source_key=source_key,
                    **defaults,
                )
                session.add(metadata)
            else:
                for field, value in defaults.items():
                    if hasattr(metadata, field):
                        setattr(metadata, field, value)

            session.flush()
            return metadata

    def update_metadata(
            self,
            source_key: str,
            **updates,
    ) -> DataSourceMetadata:
        try:
            with self.db.session_scope() as session:
                metadata = session.execute(
                    select(DataSourceMetadata)
                    .where(DataSourceMetadata.source_key == source_key)
                ).scalar_one()

                for field, value in updates.items():
                    if hasattr(metadata, field):
                        setattr(metadata, field, value)

                session.flush()
                return metadata
        except Exception as e:
            self.logger.error(e)

    def update_run_status(
            self,
            source_key: str,
            status: str,
            message: str | None = None,
            success: bool = False,
    ):
        updates = {
            "last_run_status": status,
            "last_run_message": message,
            "last_checked_at": func.now(),
        }

        if success:
            updates["last_successful_run_at"] = func.now()

        return self.update_metadata(source_key, **updates)
