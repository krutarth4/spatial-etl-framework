import csv
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from io import StringIO
from typing import Text

from dacite import from_dict
from geoalchemy2 import Geometry
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import select, update, insert, Column, BigInteger, text, func, Row, RowMapping, TIMESTAMP, Numeric, \
    Table, or_, MetaData, UniqueConstraint

from data_config_dtos.data_source_config_dto import BaseGraphDTO
from database.base import Base
from database.db_configuration import DbConfiguration
from sqlalchemy.dialects.postgresql import Insert, JSONB, ARRAY, UUID as PG_UUID
from typing import Optional, Union, List, Any, Sequence

from log_manager.logger_manager import LoggerManager
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


class DBRepository(DbConfiguration):

    def __init__(self, db_conf, base, raw_graph):

        self.base_table = from_dict(BaseGraphDTO, base)
        self.base_graph = raw_graph
        super().__init__(db_conf, self.base_table)
        self.logger = LoggerManager(type(self).__name__)
        self.table_index_map = {}

    def get_table_count(
            self,
            table_name: str,
            table_schema: str,
    ) -> int:
        """
        Return total row count of a table.

        Args:
            table_name: table name
            table_schema: schema name

        Returns:
            int: number of rows in the table
        """

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

                if create_without_indexes:
                    self.table_index_map[table_name] = original_indexes
                    table.indexes.clear()
                table.schema = table_schema or self.schema
                self.base.metadata.create_all(bind=self.engine, tables=[table], checkfirst=True)
                self.logger.info(f"Table '{table_name}' created successfully.")
            else:
                schema = table_schema or self.schema
                if not self.table_schema_matches(table_name, schema):
                    self.logger.info("Table schema doesn't match")
                    self.create_table_if_not_exist(table_name, True)
                else:
                    self.logger.info("Table exists, skipping the creation of the table")
        except Exception as e:
            self.logger.error(f"error creating table {table_name} : {e}")

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

    @measure_time(label="create indexes")
    def create_indexes(self, table_name: str, schema: str = None):
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

    @contextmanager
    def raw_pg_connection(self):
        conn = self.engine.raw_connection()
        try:
            yield conn
            conn.commit()
        except:
            conn.rollback()
            raise
        finally:
            conn.close()

    @measure_time(label="bulk insert")
    def bulk_insert(
            self,
            table_name: str,
            table_schema: str,
            data_list: list[dict],
            staging: bool = False,
    ):
        if not data_list:
            self.logger.info("No data to insert.")
            return
        # if staging:
        #     table_name = self.get_staging_table_name(table_name)
        table = self.get_table(table_name, table_schema)
        if table is None:
            raise ValueError(f"Table '{table_name}' does not exist")

        # Determine allowed columns (exclude autoincrement PKs)
        insert_columns = [
            c.name
            for c in table.columns
            if not (c.primary_key or c.autoincrement)

        ]

        if not insert_columns:
            raise ValueError("No insertable columns found")

        # Build CSV buffer
        buffer = StringIO()

        writer = csv.writer(
            buffer,
            delimiter=",",
            quotechar='"',
            quoting=csv.QUOTE_MINIMAL,
            lineterminator="\n",
        )

        for row in data_list:
            writer.writerow(
                [
                    row.get(col)
                    for col in insert_columns
                ]
            )
            # buffer.write(
            #     ",".join(
            #         "" if row.get(col) is None else str(row[col])
            #         for col in insert_columns
            #     )
            #     + "\n"
            # )

        buffer.seek(0)

        # COPY into table
        # copy_sql = f"""
        #     COPY {table.schema}.{table.name}
        #     ({", ".join(insert_columns)})
        #     FROM STDIN WITH CSV
        # """

        copy_sql = f"""
            COPY {table.schema}.{table.name}
            ({", ".join(insert_columns)})
            FROM STDIN WITH (
                FORMAT csv,
                DELIMITER ',',
                QUOTE '"',
                ESCAPE '"',
                NULL ''
            )
        """

        try:
            with self.raw_pg_connection() as conn:
                with conn.cursor() as cur:
                    with cur.copy(copy_sql) as copy:
                        copy.write(buffer.getvalue())

            self.logger.info(
                f"Inserted {len(data_list)} rows into '{table_name}'"
            )

        except Exception as e:
            self.logger.error(f"Bulk insert failed for '{table_name}': {e}")
            raise

    def resolve_sqlalchemy_type(self, type_str: str):
        """
        Convert string like:
          - 'String(100)'
          - 'Numeric(10,2)'
          - 'Geometry(Point,4326)'
          - 'ARRAY(Integer)'
        into SQLAlchemy type objects.
        """

        if not isinstance(type_str, str):
            return type_str  # Already a Column type

        s = type_str.strip()

        # -------------------------------------------------------
        # 1. Handle Geometry: e.g. Geometry(Point,4326)
        # -------------------------------------------------------
        geom_match = re.match(r"Geometry\((.*)\)", s, re.IGNORECASE)
        if geom_match:
            parts = [p.strip() for p in geom_match.group(1).split(",")]
            geom_type = parts[0]
            srid = int(parts[1]) if len(parts) > 1 else 4326
            return Geometry(geom_type, srid)

        # -------------------------------------------------------
        # 2. Handle ARRAY: e.g. ARRAY(Integer)
        # -------------------------------------------------------
        arr_match = re.match(r"ARRAY\((.*)\)", s, re.IGNORECASE)
        if arr_match:
            inner_type_str = arr_match.group(1).strip()
            inner_type = self.resolve_sqlalchemy_type(inner_type_str)
            return ARRAY(inner_type)

        # -------------------------------------------------------
        # 3. Handle Numeric(10,2)
        # -------------------------------------------------------
        num_match = re.match(r"Numeric\((\d+),\s*(\d+)\)", s, re.IGNORECASE)
        if num_match:
            precision = int(num_match.group(1))
            scale = int(num_match.group(2))
            return Numeric(precision, scale)

        # -------------------------------------------------------
        # 4. Any type with parentheses, e.g. String(255)
        # -------------------------------------------------------
        generic_match = re.match(r"(\w+)\((.*)\)", s)
        if generic_match:
            base = generic_match.group(1).lower()
            param = generic_match.group(2).strip()

            if base not in SQLALCHEMY_TYPE_MAP:
                raise ValueError(f"Unknown SQLAlchemy type: {base}")

            # Parameters are comma-separated → turn them into python ints if possible
            params = [p.strip() for p in param.split(",")]
            cast_params = [int(p) if p.isdigit() else p for p in params]

            return SQLALCHEMY_TYPE_MAP[base](*cast_params)

        # -------------------------------------------------------
        # 5. Simple type like "Integer", "Boolean", "UUID"
        # -------------------------------------------------------
        base = s.lower()
        if base in SQLALCHEMY_TYPE_MAP:
            return SQLALCHEMY_TYPE_MAP[base]()

        raise ValueError(f"Unknown SQLAlchemy type string: {type_str}")

    def add_column(
            self,
            table_name: str,
            column_name: str,
            column_type: Column,
            schema: str | None = None,
            default_value: Any = None,
            if_not_exists: bool = True,
    ):
        """
        Add a new column to an existing table and optionally fill it with a default value.

        Args:
            table_name (str): Table to modify.
            column_name (str): New column name.
            column_type (Column): SQLAlchemy Column type, e.g. Integer(), String(100), Boolean().
            default_value (Any): Value to set for existing rows.
            schema (str): Override schema. If None → use class default.
            if_not_exists (bool): Skip if column already exists.
        """

        schema_name = schema or self.schema
        full_name = f"{schema_name}.{table_name}"

        # ---- CHECK IF COLUMN ALREADY EXISTS ----
        table_info = self.inspector.get_columns(table_name, schema=schema_name)
        existing_columns = {col["name"] for col in table_info}

        if column_name in existing_columns:
            if if_not_exists:
                self.logger.info(f"Column '{column_name}' already exists in '{full_name}', skipping.")
                return
            else:
                raise ValueError(f"Column '{column_name}' already exists in table '{full_name}'")

        # ---- ADD COLUMN ----
        self.logger.warning(f"Adding column '{column_name}' to table '{full_name}' ...")
        column_type = self.resolve_sqlalchemy_type(column_type)
        # Convert SQLAlchemy type object → raw SQL type
        coltype_sql = column_type.compile(self.engine.dialect)

        sql_add = text(
            f'ALTER TABLE "{schema_name}"."{table_name}" '
            f'ADD COLUMN "{column_name}" {coltype_sql}'
        )

        try:
            with self.engine.begin() as conn:
                conn.execute(sql_add)
        except Exception as e:
            self.logger.error(f"Failed to add column: {e}")
            raise

        self.logger.info(f"Column '{column_name}' added successfully.")

        # ---- APPLY DEFAULT TO EXISTING ROWS ----
        if default_value is not None:
            self.logger.warning(
                f"Updating existing rows in '{full_name}' with default value: {default_value}"
            )

            sql_update = text(
                f'UPDATE "{schema_name}"."{table_name}" '
                f'SET "{column_name}" = :val'
            )

            with self.engine.begin() as conn:
                conn.execute(sql_update, {"val": default_value})

            self.logger.info(f"Default value applied to existing rows.")

    def add_column_to_base(
            self,
            column_name: str,
            column_type: Column,
            default_value: Any = None,
    ):

        self.add_column(
            self.base_config.table_name,
            column_name,
            column_type,
            self.base_config.table_schema,
            default_value,
            True
        )

    def update_column_data_in_db(self):
        """
        this function helps to update column data in the database.
        The update and processing takes place in the database side rather than local in pipeline
        """

    def update_column_data(self):
        """
        this function helps to update column data in the database.
        The processing takes place locally first and then is rewritten into the DB
        """
        pass

    def fetch_columns_with_limits(
            self,
            table_name: str,
            column_names: Union[str, List[str], None] = None,
            limits: int | None = None
    ) -> list[Any] | Sequence[Row[Any] | RowMapping | Any] | Sequence[Row[tuple[Any, ...] | Any]]:
        """
        Fetch one or more columns from a table with an optional LIMIT.
        Supports both single-column and multi-column queries.
        """

        table = self.get_table(table_name)
        if table is None:
            self.logger.warning(f"Table '{table_name}' does not exist")
            return []

        # --- IF NO COLUMN NAMES → SELECT EVERYTHING ---
        if not column_names:  # handles None or empty list
            stmt = select(table)
        else:
            # Convert single column -> list
            if isinstance(column_names, str):
                column_names = [column_names]

            # Validate
            missing = [c for c in column_names if c not in table.columns]
            if missing:
                raise ValueError(f"Columns {missing} do not exist in table '{table_name}'")

            # Build SELECT
            columns = [table.c[c] for c in column_names]
            stmt = select(*columns)

        if limits is not None:
            stmt = stmt.limit(limits)

        with self.session_scope() as session:
            result = session.execute(stmt)

            # Single-column result → return flat list
            # If selecting specific columns → return structured result
            if column_names:
                if len(column_names) == 1:
                    return result.scalars().all()
                return result.fetchall()

            # Multi-column result → return list of tuples
            return result.fetchall()

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

                    # 2️⃣ Drop indexes (except PK)
                    conn.execute(
                        text(
                            f"""
                                    DO $$
                                    DECLARE r RECORD;
                                    BEGIN
                                        FOR r IN (
                                            SELECT indexname
                                            FROM pg_indexes
                                            WHERE schemaname = '{schema_name}'
                                              AND tablename = '{table_name}'
                                              AND indexname NOT LIKE '%_pkey'
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


    # -----------------------------
    # UPDATE
    # -----------------------------
    def update(self, table_name: str, filters: dict, updates: dict):
        """Update records in table where filters match."""
        table = self.get_table(table_name)
        stmt = update(table).where(
            *[table.c[k] == v for k, v in filters.items()]
        ).values(**updates)

        try:
            with self.engine.begin() as conn:
                result = conn.execute(stmt)
                self.logger.info(f"Updated {result.rowcount} row(s) in '{table_name}'")
        except SQLAlchemyError as e:
            self.logger.error(f"Update failed for '{table_name}': {e}")
            raise

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
        self.metadata.create_all(bind=self.engine, checkfirst=True, tables=[raw_table])
        self.logger.info(f"Table '{target_table_name}' created successfully.")
        return target_schema, target_table_name

    @staticmethod
    def resolve_conflict_columns(table):
        """
        Determine business-key (conflict) columns from a SQLAlchemy Table.

        Resolution order:
        1. Single UniqueConstraint → use its columns
        2. Column(unique=True) → use those columns
        3. Otherwise → error
        """

        # 1️⃣ Explicit UNIQUE CONSTRAINTS
        unique_constraints = [
            c for c in table.constraints
            if isinstance(c, UniqueConstraint)
        ]

        if len(unique_constraints) == 1:
            return [col.name for col in unique_constraints[0].columns]

        if len(unique_constraints) > 1:
            raise ValueError(
                f"Multiple UniqueConstraints found on {table.fullname}. "
                "Cannot infer business key automatically."
            )

        # 2️⃣ Column-level unique=True
        unique_columns = [
            c.name for c in table.columns
            if isinstance(c, Column) and c.unique
        ]

        if unique_columns:
            return unique_columns

        # 3️⃣ Nothing usable
        raise ValueError(
            f"No UNIQUE constraint or unique=True columns found on {table.fullname}. "
            "Business key must be defined."
        )

    @staticmethod
    def resolve_update_columns(
            table,
            conflict_cols: list[str],
            exclude_cols: list[str] | None = None,
    ):
        """
        Determine which columns should be updated on change.
        """
        exclude_cols = set(exclude_cols or [])

        update_cols = [
            c.name
            for c in table.columns
            if not c.primary_key
               and c.name not in conflict_cols
               and c.name not in exclude_cols
        ]

        if not update_cols:
            raise ValueError(
                f"No updatable columns found for table {table.fullname}"
            )

        return update_cols

    def sync_staging_to_enrichment(
            self,
            staging_schema: str,
            staging_table_name: str,
            enrichment_schema: str,
            enrichment_table_name: str,
    ):
        """
        Sync data from staging → enrichment.

        - inserts new rows
        - updates rows only if enrichment-relevant columns changed
        - ignores unchanged rows
        """

        staging_table = self.get_table(staging_table_name, staging_schema)
        enrichment_table = self.get_table(enrichment_table_name, enrichment_schema)

        if staging_table is None:
            raise ValueError(
                f"Staging table {staging_schema}.{staging_table_name} not found"
            )
        if enrichment_table is None:
            raise ValueError(
                f"Enrichment table {enrichment_schema}.{enrichment_table_name} not found"
            )

        # 🔑 infer business key from enrichment table
        conflict_cols = DBRepository.resolve_conflict_columns(enrichment_table)

        # 🔄 determine which columns enrichment actually stores
        update_cols = [
            c.name
            for c in enrichment_table.columns
            if not c.primary_key
               and c.name not in conflict_cols
               and c.name in staging_table.c
        ]

        if not update_cols:
            self.logger.warning(
                f"No updatable enrichment columns found for "
                f"{enrichment_schema}.{enrichment_table_name}"
                f" -> this means there is not copying of columns present in staging to enrichment, which is not a PK, FK, or any other constraints."
            )

        insert_cols = conflict_cols + update_cols

        base_insert = Insert(enrichment_table).from_select(
            insert_cols,
            select(*(staging_table.c[c] for c in insert_cols))
        )

        update_map = {
            col: getattr(base_insert.excluded, col)
            for col in update_cols
        }

        where_clause = or_(
            *(
                enrichment_table.c[col].is_distinct_from(
                    getattr(base_insert.excluded, col)
                )
                for col in update_cols
            )
        )

        if not update_cols:
            upsert_stmt = base_insert.on_conflict_do_nothing(
                index_elements=conflict_cols
            )
        else:
            upsert_stmt = base_insert.on_conflict_do_update(
                index_elements=conflict_cols,
                set_=update_map,
                where=where_clause,
            )

        try:
            with self.session_scope() as session:
                result = session.execute(upsert_stmt)

            self.logger.info(
                f"Synced staging → enrichment: "
                f"{staging_schema}.{staging_table_name} → "
                f"{enrichment_schema}.{enrichment_table_name}"
            )

            return result.rowcount

        except Exception as e:
            self.logger.error(
                f"Failed syncing staging → enrichment: {e}"
            )
            raise

    @measure_time(label= "Syncing source to target table")
    def sync_source_to_target_table(
            self,
            raw_schema: str,
            raw_table_name: str,
            staging_schema: str,
            staging_table_name: str,
    ):
        """
        Sync data from raw_staging → staging.

        - inserts new rows
        - updates rows only if data actually changed
        - ignores unchanged rows
        """

        raw_table = self.get_table(raw_table_name, raw_schema)
        staging_table = self.get_table(staging_table_name, staging_schema)

        if raw_table is None:
            raise ValueError(f"Raw table {raw_schema}.{raw_table_name} not found")
        if staging_table is None:
            raise ValueError(f"Staging table {staging_schema}.{staging_table_name} not found")

        conflict_cols = DBRepository.resolve_conflict_columns(staging_table)
        update_cols = DBRepository.resolve_update_columns(
            staging_table,
            conflict_cols,
            # exclude_cols=["ingested_at"]
        )

        insert_cols = conflict_cols + update_cols

        base_insert = Insert(staging_table).from_select(
            insert_cols,
            select(*(raw_table.c[c] for c in insert_cols))
        )

        update_map = {
            col: getattr(base_insert.excluded, col)
            for col in update_cols
        }

        where_clause = or_(
            *(
                staging_table.c[col].is_distinct_from(
                    getattr(base_insert.excluded, col)
                )
                for col in update_cols
            )
        )

        upsert_stmt = (base_insert.on_conflict_do_update(
            index_elements=conflict_cols,
            set_=update_map,
            where=where_clause,
        ).returning(
            staging_table.c.uid,
            text("xmax")
        )
        )

        try:
            with self.session_scope() as session:
                result = session.execute(upsert_stmt)
                rows = result.fetchall()

            inserted = sum(1 for r in rows if r.xmax == 0)
            updated = len(rows) - inserted

            self.logger.info(
                f"Synced raw → staging: {raw_schema}.{raw_table_name} → "
                f"{staging_schema}.{staging_table_name}"
            )

            return {
                "inserted": inserted,
                "updated": updated,
                "total": len(rows),
            }

        except Exception as e:
            self.logger.error(
                f"Failed syncing raw → staging for "
                f"{raw_schema}.{raw_table_name}: {e}"
            )
            raise
    @measure_time(label= "SQL execution time: ")
    def call_sql(self, sql: str):
        """
        Execute raw SQL using the session_scope().
        Ensures the same transactional behavior as ORM operations.
        """
        self.logger.info(f"Executing SQL:\n{sql}")

        try:
            with self.session_scope() as session:
                result = session.execute(text(sql))
            self.logger.info("SQL execution completed.")
            return result
        except Exception as e:
            self.logger.error(f"SQL execution failed: {e}")



if __name__ == "__main__":
    # db = DBRepository()
    pass
