"""ETL table creation, cloning, index management, and cleanup.

Reads from self:  self.db, self.data_source_config, self.logger,
                  self.data_source_name, self.raw_staging_table,
                  self.raw_staging_schema
Writes to self:   self.raw_staging_schema, self.raw_staging_table
"""
from main_core.safe_class import safe_class


@safe_class
class TableManagementMixin:
    """Creates and maintains the ETL tables (staging, enrichment, mapping)."""

    def create_data_tables(self):
        """Create all configured ETL tables (staging, enrichment, mapping) if they don't exist."""
        if self.data_source_config.storage.persistent and self.db is not None:
            self.logger.info("Creating table")
            storage_data = self.data_source_config.storage
            force_create = storage_data.force_create
            if storage_data.staging:
                self.create_staging_tables(
                    storage_data.staging.table_name,
                    storage_data.staging.table_schema,
                    force_create,
                )
            if storage_data.enrichment:
                self.create_enrichment_tables(
                    storage_data.enrichment.table_name,
                    storage_data.enrichment.table_schema,
                    force_create,
                )
            if (
                self.data_source_config.mapping.table_name
                and self.data_source_config.mapping.enable
            ):
                self.create_mapping_tables(
                    self.data_source_config.mapping.table_name,
                    self.data_source_config.mapping.table_schema,
                    force_create,
                )

    def create_staging_tables(self, table_name: str, schema: str, force_create: bool):
        """Create the staging table and its raw-staging clone used for bulk COPY."""
        raw_staging_table_name = f"{table_name}_raw_staging"
        self.db.create_table_if_not_exist(
            table_name,
            table_schema=schema or None,
            force_create=force_create,
            create_without_indexes=True,
        )
        self.raw_staging_schema, self.raw_staging_table = self.db.clone_table_structure(
            schema, table_name, schema, raw_staging_table_name
        )

    def create_enrichment_tables(self, table_name: str, schema: str, force_create: bool):
        """Create the enrichment table.

        If enrichment_operators with output_columns are configured, the table
        schema is derived dynamically from those operator definitions.
        """
        operators_config = getattr(self.data_source_config, "enrichment_operators", None)
        if operators_config is not None and operators_config.output_columns:
            self._create_enrichment_table_from_operators(
                table_name, schema, force_create, operators_config.output_columns
            )
            return
        self.db.create_table_if_not_exist(
            table_name,
            table_schema=schema or None,
            force_create=force_create,
            create_without_indexes=True,
        )

    def _create_enrichment_table_from_operators(
        self, table_name, schema, force_create, output_columns
    ):
        """Dynamically create the enrichment table from operator-declared output_columns."""
        from sqlalchemy import Column, BigInteger, MetaData, Table

        if force_create:
            self.db.drop_table(table_name, schema, backup=True, check_exist=True, cascade=True)
        if self.db.table_exists(table_name, schema):
            self.logger.info(
                f"Enrichment table '{schema}.{table_name}' exists, skipping creation."
            )
            return

        meta = MetaData(schema=schema)
        columns = [Column("uid", BigInteger, primary_key=True, autoincrement=True)]
        for col_spec in output_columns:
            sa_type = self.db.resolve_sqlalchemy_type(col_spec.type)
            columns.append(Column(col_spec.name, sa_type))

        table = Table(table_name, meta, *columns, schema=schema)
        meta.create_all(bind=self.db.engine, tables=[table], checkfirst=True)
        self.logger.info(
            f"Enrichment table '{schema}.{table_name}' created from operator output_columns."
        )

        for col_spec in output_columns:
            if col_spec.index == "gist":
                self.db.create_ways_base_geometry_index(
                    schema,
                    table_name,
                    geometry_column=col_spec.name,
                    index_name=f"idx_{table_name}_{col_spec.name}_gist",
                )

    def create_mapping_tables(self, table_name: str, schema: str, force_create: bool):
        """Create the mapping table if it does not exist."""
        self.db.create_table_if_not_exist(
            table_name,
            table_schema=schema or None,
            force_create=force_create,
            create_without_indexes=True,
        )

    def create_indexes_for_table(self, table_kind: str):
        """Create deferred indexes for the given table kind ('staging', 'enrichment', 'mapping').

        Indexes are deferred at table creation (create_without_indexes=True) so that
        bulk inserts run without index overhead; this method adds them afterwards.
        """
        if self.db is None or not self.data_source_config.storage.persistent:
            return

        try:
            if table_kind == "staging" and self.data_source_config.storage.staging:
                table_name = self.data_source_config.storage.staging.table_name
                table_schema = self.data_source_config.storage.staging.table_schema
            elif table_kind == "enrichment" and self.data_source_config.storage.enrichment:
                table_name = self.data_source_config.storage.enrichment.table_name
                table_schema = self.data_source_config.storage.enrichment.table_schema
            elif (
                table_kind == "mapping"
                and self.data_source_config.mapping.enable
                and self.data_source_config.mapping.table_name
            ):
                table_name = self.data_source_config.mapping.table_name
                table_schema = self.data_source_config.mapping.table_schema
            else:
                return

            if table_name in getattr(self.db, "table_index_map", {}):
                self.db.create_indexes(table_name, table_schema)
        except Exception as e:
            self.logger.error(
                f"Failed creating {table_kind} indexes for datasource {self.data_source_name}: {e}"
            )
            self._note_stage_warning(f"create_indexes:{table_kind}", e)

    def recreate_table_indexes(self):
        """Rebuild all indexes for staging, enrichment, and mapping tables."""
        if self.db is not None and self.data_source_config.storage.persistent:
            if self.data_source_config.storage.enrichment:
                self.db.create_indexes(
                    self.data_source_config.storage.enrichment.table_name,
                    self.data_source_config.storage.enrichment.table_schema,
                )
            if self.data_source_config.storage.staging:
                self.db.create_indexes(
                    self.data_source_config.storage.staging.table_name,
                    self.data_source_config.storage.staging.table_schema,
                )
            if (
                self.data_source_config.mapping.table_name
                and self.data_source_config.mapping.enable
            ):
                self.db.create_indexes(
                    self.data_source_config.mapping.table_name,
                    self.data_source_config.mapping.table_schema,
                )

    def clean_raw_staging_table(self, backup: bool):
        """Drop the raw-staging table (with optional backup) after successful sync."""
        if self.raw_staging_table is None:
            return
        self.db.drop_table(
            self.raw_staging_table, self.raw_staging_schema, backup, True, True
        )
