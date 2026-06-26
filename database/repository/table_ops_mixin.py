"""Table creation, cloning, dropping, indexing, reflection, and row counts.

Mixin extracted from the original DBRepository god-class; composed back
together (with DbConfiguration) in database/db_repository.py. Methods are
unchanged — they run on the composed instance and call peers via self.*
"""
import csv
import threading
from contextlib import contextmanager
from datetime import datetime
import time
from io import StringIO
import json
from typing import Text

from dacite import from_dict
from geoalchemy2 import Geometry
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import select, update, insert, Column, BigInteger, text, func, Row, RowMapping, TIMESTAMP, Numeric, \
    Table, or_, MetaData, UniqueConstraint

from utils.data_source_config_dto import BaseGraphDTO
from database.base import Base
from database.db_configuration import DbConfiguration
from sqlalchemy.dialects.postgresql import Insert, JSONB, ARRAY, UUID as PG_UUID
from typing import Union, List, Any, Sequence

from utils.logger_manager import LoggerManager
from sqlalchemy import (
    Integer, Float, String, Boolean, Date, DateTime, JSON
)
import re
from utils.execution_time import measure_time

SQLALCHEMY_TYPE_MAP = {
    # Basic
    "integer": Integer,
    "int": Integer,
    "bigint": BigInteger,
    "float": Float,
    "double": Float,
    "string": String,
    "varchar": String,
    "text": Text,
    "bool": Boolean,
    "boolean": Boolean,

    # Time
    "date": Date,
    "datetime": DateTime,
    "timestamp": TIMESTAMP,

    # Numeric
    "numeric": Numeric,
    "decimal": Numeric,

    # PostgreSQL JSON
    "json": JSON,
    "jsonb": JSONB,

    # PostgreSQL uuid
    "uuid": PG_UUID,

    # Geometry
    "geometry": Geometry,

    # Array
    "array": ARRAY
}


class TableOpsMixin:
    def get_table_count(
            self,
            table_name: str,
            table_schema: str,
            estimate: bool = False,
    ) -> int:
        """
        Return total row count of a table.

        Args:
            table_name: table name
            table_schema: schema name
            estimate: if True, return the fast planner estimate from
                pg_class.reltuples instead of an exact COUNT(*). The estimate
                needs no table scan and is accurate enough for dashboards and
                health probes. Exact COUNT(*) on multi-million-row tables is
                slow, and under heavy ETL write load it can take longer than a
                health probe allows, which is what previously flipped the
                pipeline to unhealthy.

        Returns:
            int: number of rows in the table (exact, or estimated if estimate=True)
        """

        if estimate:
            try:
                with self.session_scope() as session:
                    result = session.execute(
                        text(
                            "SELECT GREATEST(c.reltuples, 0)::bigint "
                            "FROM pg_class c "
                            "JOIN pg_namespace n ON n.oid = c.relnamespace "
                            "WHERE n.nspname = :schema AND c.relname = :table"
                        ),
                        {"schema": table_schema, "table": table_name},
                    ).scalar()
                return int(result) if result is not None else 0
            except Exception as e:
                self.logger.error(
                    f"Failed to estimate row count for "
                    f"'{table_schema}.{table_name}': {e}"
                )
                return 0

        table: Table | None = self.get_table(table_name, table_schema)

        if table is None:
            raise ValueError(
                f"Table '{table_schema}.{table_name}' does not exist"
            )

        stmt = select(func.count()).select_from(table)

        try:
            with self.session_scope() as session:
                result = session.execute(stmt).scalar_one()

            self.logger.info(
                f"Row count for '{table_schema}.{table_name}': {result}"
            )

            return result
        except Exception as e:
            self.logger.error(
                f"Failed to get row count for "
                f"'{table_schema}.{table_name}': {e}"
            )
            raise
    def create_base_table_clone(self, source_schema, source_table):
        self.logger.info(f"creating base table {self.base_table.table_name} in schema {self.base_table.table_schema}")
        self.clone_table_with_data(
            source_schema, source_table,
            self.base_table.table_schema, self.base_table.table_name,
            True
        )
    @measure_time(label="bulk clone")
    def clone_table_data(
            self,
            source_table_name: str,
            source_table_schema: str,
            target_table_name: str,
            target_table_schema: str,
            exclude_columns: set[str] | None = None,
    ):
        """
        Clone data from source table into target table by automatically
        selecting matching columns.

        Only columns that:
        - exist in BOTH tables
        - are NOT primary keys
        - are NOT autoincrement
        - are NOT explicitly excluded
        will be copied.

        Returns:
            Number of inserted rows
        """
        if not self.table_exists(source_table_name, source_table_schema):
            self.logger.error(f"Source table does not exist {source_table_name} in schema {source_table_schema}")
            raise Exception(f"Source table does not exist {source_table_name} in schema {source_table_schema}")

        exclude_columns = exclude_columns or set()

        source_table: Table | None = self.get_table(
            source_table_name, source_table_schema
        )
        target_table: Table | None = self.get_table(
            target_table_name, target_table_schema
        )

        if source_table is None:
            raise ValueError(
                f"Source table '{source_table_schema}.{source_table_name}' does not exist"
            )

        if target_table is None:
            raise ValueError(
                f"Target table '{target_table_schema}.{target_table_name}' does not exist"
            )

        # --- resolve common columns ---
        target_columns = []
        source_columns = []

        for target_col in target_table.columns:
            if (
                    target_col.primary_key
                    or target_col.autoincrement
                    or target_col.name in exclude_columns
            ):
                continue

            if target_col.name in source_table.c:
                target_columns.append(target_col)
                source_columns.append(source_table.c[target_col.name])

        if not target_columns:
            raise ValueError(
                f"No compatible columns found between "
                f"{source_table_schema}.{source_table_name} and "
                f"{target_table_schema}.{target_table_name}"
            )

        insert_stmt = insert(target_table).from_select(
            target_columns,
            select(*source_columns),
        )

        try:
            with self.session_scope() as session:
                result = session.execute(insert_stmt)

            self.logger.info(
                f"Cloned data from "
                f"{source_table_schema}.{source_table_name} → "
                f"{target_table_schema}.{target_table_name}"
            )


        except Exception as e:
            self.logger.error(
                f"Clone failed from "
                f"{source_table_schema}.{source_table_name} → "
                f"{target_table_schema}.{target_table_name}: {e}"
            )
            raise
    def create_ways_base_geometry_index(
            self,
            table_schema: str,
            table_name: str,
            geometry_column: str = "geometry",
            index_name: str = "idx_ways_base_geometry_index",
    ) -> None:
        """
        Create GiST index on ways geometry column (idempotent).
        """

        stmt = text(f"""
            CREATE INDEX IF NOT EXISTS {index_name}
            ON {table_schema}.{table_name}
            USING GIST ({geometry_column});
        """)

        with self.session_scope() as session:
            session.execute(stmt)

        self.logger.info(
            f"GiST index '{index_name}' ensured on "
            f"{table_schema}.{table_name}({geometry_column})"
        )
    def reflect_base_tables(self, schema: str, table_name: str):
        Table(
            table_name,
            self.base.metadata,
            schema=schema,
            autoload_with=self.engine,
        )
    def create_table_if_not_exist(self, table_name: str, table_schema: str = None, force_create: bool = False,
                                  create_without_indexes: bool = False):
        """Create table defined in Base.metadata. for the data sources."""
        try:

            self.logger.info(f"create table {table_name}")
            self.update_metadata(table_schema)
            table = self.base.metadata.tables[self.normalize_table_name(table_name, table_schema, False)]
            if force_create:
                self.drop_table(table_name, table_schema, True, True, True)

            if not self.table_exists(table_name, table_schema):
                original_indexes = set(table.indexes)
                schema = table_schema or self.schema

                # If a previous CREATE TABLE failed midway, PostgreSQL can be left
                # with orphaned index names. Clear only indexes this table would create.
                self._drop_orphan_table_indexes(table, schema)

                if create_without_indexes:
                    self.table_index_map[table_name] = original_indexes
                    table.indexes.clear()
                table.schema = schema
                self.base.metadata.create_all(bind=self.engine, tables=[table], checkfirst=True)
                self.logger.info(f"Table '{table_name}' created successfully.")
            else:
                schema = table_schema or self.schema
                if not self.table_schema_matches(table_name, schema):
                    self.logger.info("Table schema doesn't match")
                    self.create_table_if_not_exist(table_name, force_create=True)
                else:
                    self.logger.info("Table exists, skipping the creation of the table")
        except Exception as e:
            self.logger.error(f"error creating table {table_name} : {e}")
    def _drop_orphan_table_indexes(self, table, schema: str) -> None:
        for idx in list(table.indexes):
            if not idx.name:
                continue
            if self.index_exists(idx.name, schema):
                self.logger.warning(
                    f"Dropping stale index before table create: {schema}.{idx.name}"
                )
                with self.session_scope() as session:
                    session.execute(text(f'DROP INDEX IF EXISTS "{schema}"."{idx.name}"'))
    @staticmethod
    def get_staging_table_name(name) -> str:
        return f"{name}_staging"
    def create_unlogged_staging_table(self, table_name: str):
        staging_table = self.get_staging_table_name(table_name)

        if self.table_exists(staging_table):
            self.logger.info(f"Staging table {staging_table} already exists")
            return

        sql = text(f"""
            CREATE UNLOGGED TABLE {self.schema}.{staging_table}
            (LIKE {self.schema}.{table_name} INCLUDING DEFAULTS)
        """)

        with self.engine.begin() as conn:
            conn.execute(sql)

        self.logger.info(f"Created UNLOGGED staging table {staging_table}")
    def index_exists(self, index_name: str, schema: str) -> bool:
        sql = text("""
                   SELECT 1
                   FROM pg_indexes
                   WHERE schemaname = :schema
                     AND indexname = :index
                   """)
        with self.engine.connect() as conn:
            return conn.execute(
                sql, {"schema": schema, "index": index_name}
            ).scalar() is not None
    def materialized_view_exists(self, view_name: str, schema: str) -> bool:
        sql = text("""
            SELECT 1
            FROM pg_matviews
            WHERE schemaname = :schema
              AND matviewname = :view_name
        """)
        with self.engine.connect() as conn:
            return conn.execute(sql, {"schema": schema, "view_name": view_name}).scalar() is not None
    @measure_time(label="create indexes")
    def create_indexes(self, table_name: str, schema: str = None):
        try:
            table = Base.metadata.tables[self.normalize_table_name(table_name, schema, False)]
            if not self.table_exists(table_name, schema):
                self.logger.warning(f"Table '{table_name}' doesn't exist. For recreating indexes...")
                return
            schema = schema or self.schema
            if table_name in self.table_index_map:
                table.indexes.update(self.table_index_map[table_name])

                for idx in table.indexes:
                    if self.index_exists(idx.name, schema):
                        self.logger.info(f"Index exists, skipping: {idx.name}")
                        continue

                    self.logger.info(f"Creating index: {idx.name}")
                    idx.create(bind=self.engine)
                #     Delete the mapping from the internal storage after successful creation
                if self.table_index_map[table_name]:
                    del self.table_index_map[table_name]
            else:
                self.logger.warning(f"Table '{table_name}' index skipped as the table indexes doesnt exist")
        except Exception as e:
            self.logger.error(f"error creating indexes : {e}")
    def drop_table(
            self,
            table_name: str,
            schema: str | None = None,
            backup: bool = False,
            check_exist: bool = True,
            cascade: bool = True,
    ):
        """
        Drop or backup a table safely.

        :param table_name: table name (without schema)
        :param schema: schema name (defaults to self.schema)
        :param backup: if True, rename table instead of dropping
        :param check_exist: skip if table does not exist
        :param cascade: drop dependent objects
        """
        schema_name = schema or self.schema
        full_name = f"{schema_name}.{table_name}"

        try:
            inspector = self.get_inspector()

            if check_exist and not inspector.has_table(table_name, schema=schema_name):
                self.logger.warning(
                    f"Table '{full_name}' does not exist — skipping."
                )
                return

            with self.engine.begin() as conn:
                if backup:
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    backup_name = f"{table_name}_backup_{timestamp}"

                    self.logger.warning(
                        f"Renaming table '{full_name}' → '{schema_name}.{backup_name}'"
                    )

                    # 1️⃣ Drop constraints
                    conn.execute(
                        text(
                            f"""
                                    DO $$
                                    DECLARE r RECORD;
                                    BEGIN
                                        FOR r IN (
                                            SELECT conname
                                            FROM pg_constraint
                                            WHERE conrelid = '{schema_name}.{table_name}'::regclass
                                            AND contype NOT IN ('p', 'n')
                                        ) LOOP
                                            EXECUTE format(
                                                'ALTER TABLE "{schema_name}"."{table_name}" DROP CONSTRAINT %I',
                                                r.conname
                                            );
                                        END LOOP;
                                    END $$;
                                    """
                        )
                    )

                    # 2️⃣ Drop indexes that are NOT owned by constraints
                    # (PK/UNIQUE/EXCLUSION constraints own backing indexes and must not be dropped directly)
                    conn.execute(
                        text(
                            f"""
                                    DO $$
                                    DECLARE r RECORD;
                                    BEGIN
                                        FOR r IN (
                                            SELECT i.relname AS indexname
                                            FROM pg_class t
                                            JOIN pg_namespace ns ON ns.oid = t.relnamespace
                                            JOIN pg_index ix ON ix.indrelid = t.oid
                                            JOIN pg_class i ON i.oid = ix.indexrelid
                                            LEFT JOIN pg_constraint c ON c.conindid = i.oid
                                            WHERE ns.nspname = '{schema_name}'
                                              AND t.relname = '{table_name}'
                                              AND c.oid IS NULL
                                        ) LOOP
                                            EXECUTE format(
                                                'DROP INDEX IF EXISTS "{schema_name}".%I',
                                                r.indexname
                                            );
                                        END LOOP;
                                    END $$;
                                    """
                        )
                    )


                    conn.execute(
                        text(
                            f'ALTER TABLE "{schema_name}"."{table_name}" '
                            f'RENAME TO "{backup_name}"'
                        )
                    )
                    return

                self.logger.warning(f"Dropping table '{full_name}' ...")

                cascade_sql = "CASCADE" if cascade else ""
                conn.execute(
                    text(
                        f'DROP TABLE "{schema_name}"."{table_name}" {cascade_sql}'
                    )
                )

            self.logger.info(f"Table '{full_name}' dropped successfully.")

        except Exception as e:
            self.logger.error(
                f"Failed to drop table '{full_name}': {e}"
            )
    def clone_table_with_data(self, source_schema: str, table_name: str, target_schema: str,
                              target_table_name: str, copy_data: bool = False):
        """
        Clone a table's schema (structure only) from one schema into another.

        :param source_schema: The schema where the original table resides.
        :param table_name: The name of the table to clone.
        :param target_schema: The schema where the new table will be created.
        """

        create_schema_sql = f"""
        CREATE SCHEMA IF NOT EXISTS {target_schema};
        """

        clone_sql = f"""
        CREATE TABLE IF NOT EXISTS {target_schema}.{target_table_name} 
        (LIKE {source_schema}.{table_name} INCLUDING ALL);
        """

        with self.engine.begin() as conn:  # engine.begin() = auto-commit transaction
            conn.execute(text(create_schema_sql))
            conn.execute(text(clone_sql))

            if copy_data:
                conn.execute(text(f"""
                        ALTER TABLE {target_schema}.{target_table_name}
                        ALTER COLUMN id SET GENERATED BY DEFAULT;
                    """))
                copy_data_sql = f"""
                        INSERT INTO {target_schema}.{target_table_name}
                        SELECT * FROM {source_schema}.{table_name};
                        """
                conn.execute(text(copy_data_sql))

        return f"{target_schema}.{target_table_name}"
    def clone_table_structure(self, source_schema: str, source_table_name: str, target_schema: str,
                              target_table_name: str):
        self.logger.info(f"Creating raw staging table {target_table_name} ...")
        source_table = self.get_table(source_table_name, source_schema)
        target_table = self.get_table(target_table_name, target_schema)
        if target_table is not None:
            self.drop_table(target_table_name, target_schema, False, True, True)
        if source_table is None:
            raise ValueError(f"Source table {source_schema}.{source_table_name} not found")
        metadata = MetaData(schema=target_schema)
        raw_table = Table(
            target_table_name,  # ✅ positional
            metadata,  # ✅ positional
            *(
                c.copy(
                    autoincrement=True,
                    unique=False,
                    index=False,
                )
                for c in source_table.columns
            ),
            schema=target_schema,
        )
        raw_table.metadata.create_all(bind=self.engine, checkfirst=True)
        self.update_metadata(target_schema)
        self.logger.info(f"Table '{target_table_name}' created successfully.")
        return target_schema, target_table_name
