"""Engine/session bootstrap, raw SQL execution, batching, and low-level helpers.

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


class ConnectionMixin:
    def __init__(self, db_conf, base, raw_graph):

        self.base_table = from_dict(BaseGraphDTO, base)
        self.base_graph = raw_graph
        super().__init__(db_conf, self.base_table)
        self.logger = LoggerManager(type(self).__name__)
        self.table_index_map = {}
    @contextmanager
    def _sql_execution_heartbeat(self, sql: str, interval_seconds: int = 5):
        """
        Emit periodic logs while a blocking SQL call is still executing.
        Useful for long-running INSERT/UPDATE/CTE/PostGIS queries.
        """
        if interval_seconds <= 0:
            yield
            return

        stop_event = threading.Event()
        started_at = time.perf_counter()
        sql_preview = " ".join((sql or "").strip().split())
        if len(sql_preview) > 220:
            sql_preview = f"{sql_preview[:100]}..."

        def _heartbeat():
            # First tick after interval to avoid noise for fast queries.
            while not stop_event.wait(interval_seconds):
                elapsed = time.perf_counter() - started_at
                self.logger.info(
                    f"SQL still executing... elapsed={elapsed:.1f}s | query={sql_preview}"
                )

        thread = threading.Thread(target=_heartbeat, name="sql-heartbeat", daemon=True)
        thread.start()
        try:
            yield
        finally:
            stop_event.set()
            # Short join so the daemon thread can exit cleanly without blocking shutdown.
            thread.join(timeout=0.2)
    @contextmanager
    def raw_pg_connection(self):
        conn = self.engine.raw_connection()
        try:
            yield conn
            conn.commit()
        except:
            self.logger.error(f"Error committing to database{conn}")
            conn.rollback()
            raise
        finally:
            conn.close()
    def pg_array_literal(self, value):
        if value is None:
            return None

        if not isinstance(value, (list, tuple)):
            raise ValueError(f"Expected list/tuple for ARRAY column, got {type(value)}")

        # Escape quotes inside elements
        escaped = [
            '"' + str(v).replace('"', '\\"') + '"'
            for v in value
        ]

        return "{" + ",".join(escaped) + "}"
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
    def fetch_query(self, query, params):
        """
        Execute a SELECT query and return all rows.
        """

        try:
            with self.session_scope() as session:
                result = session.execute(text(query), params or {})
                rows = result.fetchall()
                return rows

        except Exception as e:
            self.logger.error(f"Fetch query failed: {e}")
            raise
    @measure_time(label= "SQL execution time: ")
    def call_sql(self, sql: str, params: Any = None, raise_on_error: bool = False):
        """
        Execute raw SQL using the session_scope().
        Ensures the same transactional behavior as ORM operations.
        """
        # self.logger.debug(f"Executing SQL:\n{sql}")

        try:
            self.logger.info("Starting SQL execution...")
            with self._sql_execution_heartbeat(sql):
                with self.session_scope() as session:
                    if params:
                        session.execute(text(sql), params=params)
                    else:
                        session.execute(text(sql))
            self.logger.info("SQL execution completed.")
            # return result
        except Exception as e:
            self.logger.error(f"SQL execution failed: {e}")
            if raise_on_error:
                raise
    def call_sql_with_advisory_lock(self, sql: str, lock_key: str, raise_on_error: bool = False):
        """
        Run ``sql`` while holding a transaction-level advisory lock derived from
        ``lock_key``, both inside a single transaction.

        Used to serialize concurrent ``CREATE INDEX IF NOT EXISTS`` calls. That
        statement is NOT atomic: two sessions can both pass the catalog check and
        then collide on pg_class's unique index (pg_class_relname_nsp_index). This
        happens when one MV is triggered from multiple datasources at the same
        time (e.g. mv_weather fired by two weather datasources in parallel). The
        advisory lock makes the second session wait for the first to commit, after
        which IF NOT EXISTS correctly short-circuits.
        """
        try:
            self.logger.info("Starting SQL execution (advisory-locked)...")
            with self._sql_execution_heartbeat(sql):
                with self.session_scope() as session:
                    session.execute(
                        text("SELECT pg_advisory_xact_lock(hashtext(:lock_key))"),
                        {"lock_key": lock_key},
                    )
                    session.execute(text(sql))
            self.logger.info("SQL execution completed.")
        except Exception as e:
            self.logger.error(f"SQL execution failed: {e}")
            if raise_on_error:
                raise
    def _extract_base_table_from_sql(self, sql: str) -> tuple[str, str] | None:
        """
        Extract base table name and schema from SQL query for batching.
        Looks for patterns like: FROM schema.table alias
        """
        import re
        # Match FROM schema.table or FROM "schema"."table"
        pattern = r'FROM\s+(?:"?(\w+)"?\.)?"?(\w+)"?\s+(\w+)'
        match = re.search(pattern, sql, re.IGNORECASE)
        if match:
            schema = match.group(1)
            table = match.group(2)
            alias = match.group(3)
            return (schema, table, alias)
        return None
    def _detect_batchable_query(self, sql: str) -> bool:
        """
        Detect if a query is suitable for batching.
        Detects any INSERT INTO ... SELECT ... FROM shape (with or without JOIN),
        so straight raw->staging copies can also be sliced.
        """
        sql_upper = sql.upper().strip()
        return (
            "INSERT INTO" in sql_upper
            and "SELECT" in sql_upper
            and "FROM" in sql_upper
        )
    @measure_time(label="Batched SQL execution time: ")
    def call_sql_batched(
        self,
        sql: str,
        batch_size: int = 10000,
        base_id_column: str = "id",
        params: Any = None,
        raise_on_error: bool = False
    ):
        """
        Execute large INSERT INTO ... SELECT queries in batches to avoid long locks.

        Args:
            sql: The SQL query to execute
            batch_size: Number of rows to process per batch
            base_id_column: Column name to use for batching (default: "id")
            params: Optional query parameters
            raise_on_error: Whether to raise exceptions

        This method:
        1. Extracts the base table from the query
        2. Counts total rows to process
        3. Processes in batches using ID ranges or LIMIT/OFFSET
        4. Commits after each batch to release locks
        5. Provides progress logging
        """

        if not self._detect_batchable_query(sql):
            self.logger.info("Query not suitable for batching, executing normally")
            return self.call_sql(sql, params, raise_on_error)

        try:
            # Extract base table info
            table_info = self._extract_base_table_from_sql(sql)
            if not table_info:
                self.logger.warning("Could not extract base table from query, executing without batching")
                return self.call_sql(sql, params, raise_on_error)

            base_schema, base_table, base_alias = table_info
            self.logger.info(f"Batching query on table: {base_schema}.{base_table} (alias: {base_alias})")

            # Get total row count from base table
            count_sql = f"SELECT COUNT(*) FROM {base_schema}.{base_table}"
            with self.session_scope() as session:
                total_rows = session.execute(text(count_sql)).scalar()

            if total_rows == 0:
                self.logger.info("No rows to process")
                return

            self.logger.info(f"Total rows to process: {total_rows}, batch size: {batch_size}")

            # Calculate number of batches
            num_batches = (total_rows + batch_size - 1) // batch_size

            # Process in batches using ID ranges
            self.logger.info(f"Processing in {num_batches} batches...")

            for batch_num in range(num_batches):
                offset = batch_num * batch_size

                # Modify SQL to add LIMIT and OFFSET
                # We wrap the base table in a subquery with LIMIT/OFFSET
                batched_sql = self._add_batch_limits_to_query(
                    sql, base_schema, base_table, base_alias,
                    batch_size, offset, base_id_column
                )

                self.logger.info(
                    f"Processing batch {batch_num + 1}/{num_batches} "
                    f"(rows {offset + 1}-{min(offset + batch_size, total_rows)})"
                )

                start_time = time.perf_counter()
                with self.session_scope() as session:
                    result = session.execute(text(batched_sql), params or {})
                    row_count = result.rowcount if hasattr(result, 'rowcount') else 0

                elapsed = time.perf_counter() - start_time
                self.logger.info(
                    f"Batch {batch_num + 1} completed in {elapsed:.2f}s "
                    f"({row_count} rows affected)"
                )

            self.logger.info(f"All {num_batches} batches completed successfully")

        except Exception as e:
            self.logger.error(f"Batched SQL execution failed: {e}")
            if raise_on_error:
                raise
    def _add_batch_limits_to_query(
        self,
        sql: str,
        base_schema: str,
        base_table: str,
        base_alias: str,
        limit: int,
        offset: int,
        order_column: str = "id",
    ) -> str:
        """
        Modify SQL query to add LIMIT/OFFSET by wrapping the base table.

        Converts:
            FROM schema.table b
        To:
            FROM (SELECT * FROM schema.table ORDER BY <pk> LIMIT x OFFSET y) b
        """
        # ORDER BY makes LIMIT/OFFSET windows stable across batches; without it,
        # Postgres can revisit or skip rows under concurrent writes.
        import re

        order_clause = f'ORDER BY "{order_column}"'

        pattern = rf'(FROM\s+)(?:"{base_schema}"\.)?"{base_table}"(\s+{base_alias})'
        replacement = rf'\1(SELECT * FROM "{base_schema}"."{base_table}" {order_clause} LIMIT {limit} OFFSET {offset}) {base_alias}'

        modified_sql = re.sub(pattern, replacement, sql, count=1, flags=re.IGNORECASE)

        if modified_sql == sql:
            pattern = rf'(FROM\s+){base_schema}\.{base_table}(\s+{base_alias})'
            replacement = rf'\1(SELECT * FROM {base_schema}.{base_table} {order_clause} LIMIT {limit} OFFSET {offset}) {base_alias}'
            modified_sql = re.sub(pattern, replacement, sql, count=1, flags=re.IGNORECASE)

        return modified_sql
