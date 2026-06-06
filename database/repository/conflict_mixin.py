"""ON CONFLICT / primary-key / update column resolution for upserts.

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


class ConflictMixin:
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
    def resolve_primary_key_columns(self, table):
        return [col.name for col in table.primary_key.columns]
