"""Column add/update and column-limit introspection (schema evolution).

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

from data_config_dtos.data_source_config_dto import BaseGraphDTO
from database.base import Base
from database.db_configuration import DbConfiguration
from sqlalchemy.dialects.postgresql import Insert, JSONB, ARRAY, UUID as PG_UUID
from typing import Union, List, Any, Sequence

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


class SchemaOpsMixin:
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
