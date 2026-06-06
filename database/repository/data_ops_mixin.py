"""Bulk insert, update, upsert, and staging/source table synchronisation.

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


class DataOpsMixin:
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

        # Determine allowed columns (exclude autoincrement PKs). Skip columns that
        # are absent from every row so database-side defaults can still apply.
        insert_columns = []
        for column in table.columns:
            if column.primary_key or column.autoincrement:
                continue
            if any(column.name in row for row in data_list):
                insert_columns.append(column.name)

        if not insert_columns:
            raise ValueError("No insertable columns found")

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

        # Pre-resolve column objects once to avoid repeated dict lookups per row
        col_meta = [(col, table.columns.get(col)) for col in insert_columns]

        def _format_row(row: dict) -> list:
            formatted = []
            for col, column_obj in col_meta:
                val = row.get(col)
                if isinstance(column_obj.type, ARRAY):
                    val = self.pg_array_literal(val)
                elif isinstance(column_obj.type, JSONB):
                    if val is not None:
                        val = json.dumps(val)
                formatted.append(val)
            return formatted

        thread = threading.current_thread()

        t0 = time.monotonic()
        self.logger.info(
            f"[DB WAIT] thread={thread.name} rows={len(data_list)}"
        )

        # Stream rows in chunks so we never hold more than COPY_CHUNK_SIZE rows
        # as a CSV string in memory at once (avoids OOM on large datasets).
        COPY_CHUNK_SIZE = 50_000

        try:
            with self.raw_pg_connection() as conn:
                self.logger.info(
                    f"[DB ACQUIRED] thread={thread.name}"
                )
                with conn.cursor() as cur:
                    with cur.copy(copy_sql) as copy:
                        chunk_buf = StringIO()
                        writer = csv.writer(
                            chunk_buf,
                            delimiter=",",
                            quotechar='"',
                            quoting=csv.QUOTE_MINIMAL,
                            lineterminator="\n",
                        )
                        for i, row in enumerate(data_list):
                            writer.writerow(_format_row(row))
                            if (i + 1) % COPY_CHUNK_SIZE == 0:
                                chunk_buf.seek(0)
                                copy.write(chunk_buf.read())
                                chunk_buf.seek(0)
                                chunk_buf.truncate(0)
                        # flush remainder
                        chunk_buf.seek(0)
                        remainder = chunk_buf.read()
                        if remainder:
                            copy.write(remainder)

            self.logger.info(
                f"Inserted {len(data_list)} rows into '{table_name}'"
            )
            self.logger.critical(
                f"[DB DONE] thread={thread.name} "
                f"time={time.monotonic() - t0:.2f}s")

        except Exception as e:
            self.logger.error(f"Bulk insert failed for '{table_name}': {e}")
            raise
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
    def _upsert_in_pk_batches(
            self,
            source_table: Table,
            target_table: Table,
            insert_cols: list[str],
            conflict_cols: list[str],
            update_cols: list[str],
            batch_size: int,
            return_xmax: bool = False,
    ):
        """
        Walk source_table in PK-range slices and copy rows into target_table per slice.
        Each batch commits independently — failures mid-stream leave prior batches durable.

        Fast path: if target_table is empty, skip the ON CONFLICT machinery entirely
        and emit a plain INSERT … SELECT per batch. This is the common case for
        experimentation runs (tables get dropped + reloaded), and avoids the per-row
        `IS DISTINCT FROM` comparison across every update column.

        Slow path: ON CONFLICT DO UPDATE with `IS DISTINCT FROM` filter, but without
        RETURNING xmax — we rely on `result.rowcount` for affected-row counts instead
        of materializing every PK back into Python.
        """
        source_pk_cols = [c.name for c in source_table.primary_key.columns]
        if len(source_pk_cols) != 1:
            raise ValueError(
                f"PK-range batching needs a single-column PK on "
                f"{source_table.fullname}; found {source_pk_cols}"
            )
        pk_name = source_pk_cols[0]
        pk_col = source_table.c[pk_name]

        with self.session_scope() as session:
            bounds = session.execute(
                select(func.min(pk_col), func.max(pk_col))
            ).one()
            # Cheap empty-target probe — LIMIT 1 instead of COUNT(*)
            target_empty = session.execute(
                select(text("1")).select_from(target_table).limit(1)
            ).first() is None
        pk_min, pk_max = bounds[0], bounds[1]

        if pk_min is None:
            self.logger.info(
                f"No rows in {source_table.fullname}, skipping batched upsert"
            )
            if return_xmax:
                return {"inserted": 0, "updated": 0, "total": 0, "success": True}
            return 0

        total_rows = 0
        lo = pk_min
        batch_num = 0
        approx_batches = max(1, (pk_max - pk_min) // batch_size + 1)
        mode = "plain INSERT (target empty)" if target_empty else "ON CONFLICT upsert"
        self.logger.info(
            f"Batched copy {source_table.fullname} -> {target_table.fullname} [{mode}]: "
            f"pk range {pk_min}..{pk_max}, batch_size={batch_size}, "
            f"~{approx_batches} batches"
        )

        while lo <= pk_max:
            hi = lo + batch_size
            batch_num += 1

            base_insert = Insert(target_table).from_select(
                insert_cols,
                select(*(source_table.c[c] for c in insert_cols))
                    .where(pk_col >= lo)
                    .where(pk_col < hi)
            )

            if target_empty:
                stmt = base_insert
            elif update_cols:
                update_map = {
                    col: getattr(base_insert.excluded, col)
                    for col in update_cols
                }
                where_clause = or_(
                    *(
                        target_table.c[col].is_distinct_from(
                            getattr(base_insert.excluded, col)
                        )
                        for col in update_cols
                    )
                )
                stmt = base_insert.on_conflict_do_update(
                    index_elements=conflict_cols,
                    set_=update_map,
                    where=where_clause,
                )
            else:
                stmt = base_insert.on_conflict_do_nothing(
                    index_elements=conflict_cols
                )

            t0 = time.perf_counter()
            with self.session_scope() as session:
                result = session.execute(stmt)
                batch_rows = result.rowcount if result.rowcount >= 0 else 0
                total_rows += batch_rows

            elapsed = time.perf_counter() - t0
            self.logger.info(
                f"Batch {batch_num}/{approx_batches} "
                f"(pk {lo}..{hi - 1}) completed in {elapsed:.2f}s "
                f"({batch_rows} rows, total {total_rows})"
            )

            lo = hi

        if return_xmax:
            # `rowcount` counts all affected rows (inserts + updates). We report it
            # under `inserted` so MaterializedViewManager._has_new_data still triggers
            # refreshes when data changed. We no longer distinguish inserted vs updated.
            return {
                "inserted": total_rows,
                "updated": 0,
                "total": total_rows,
                "success": True,
            }
        return total_rows
    def sync_staging_to_enrichment(
            self,
            staging_schema: str,
            staging_table_name: str,
            enrichment_schema: str,
            enrichment_table_name: str,
            batch_size: int | None = None,
    ):
        """
        Sync data from staging → enrichment.

        - inserts new rows
        - updates rows only if enrichment-relevant columns changed
        - ignores unchanged rows

        When batch_size is a positive int, the upsert is sliced over staging's PK
        range and committed per batch.
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
        conflict_cols = self.resolve_conflict_columns(enrichment_table)

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

        if batch_size and batch_size > 0:
            try:
                rowcount = self._upsert_in_pk_batches(
                    source_table=staging_table,
                    target_table=enrichment_table,
                    insert_cols=insert_cols,
                    conflict_cols=conflict_cols,
                    update_cols=update_cols,
                    batch_size=batch_size,
                    return_xmax=False,
                )
                self.logger.info(
                    f"Synced staging → enrichment (batched): "
                    f"{staging_schema}.{staging_table_name} → "
                    f"{enrichment_schema}.{enrichment_table_name}"
                )
                return rowcount
            except Exception as e:
                self.logger.error(
                    f"Failed batched sync staging → enrichment: {e}"
                )
                raise

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
            batch_size: int | None = None,
    ):
        """
        Sync data from raw_staging → staging.

        - inserts new rows
        - updates rows only if data actually changed
        - ignores unchanged rows

        When batch_size is a positive int, the upsert is sliced over raw_staging's
        PK range and committed per batch.
        """

        raw_table = self.get_table(raw_table_name, raw_schema)
        staging_table = self.get_table(staging_table_name, staging_schema)

        if raw_table is None:
            raise ValueError(f"Raw table {raw_schema}.{raw_table_name} not found")
        if staging_table is None:
            raise ValueError(f"Staging table {staging_schema}.{staging_table_name} not found")

        conflict_cols = self.resolve_conflict_columns(staging_table)
        update_cols = self.resolve_update_columns(
            staging_table,
            conflict_cols,
            # exclude_cols=["ingested_at"]
        )

        insert_cols = conflict_cols + update_cols

        if batch_size and batch_size > 0:
            try:
                stats = self._upsert_in_pk_batches(
                    source_table=raw_table,
                    target_table=staging_table,
                    insert_cols=insert_cols,
                    conflict_cols=conflict_cols,
                    update_cols=update_cols,
                    batch_size=batch_size,
                    return_xmax=True,
                )
                self.logger.info(
                    f"Synced raw → staging (batched): {raw_schema}.{raw_table_name} → "
                    f"{staging_schema}.{staging_table_name} "
                    f"(inserted={stats['inserted']}, updated={stats['updated']})"
                )
                return stats
            except Exception as e:
                self.logger.error(
                    f"Failed batched sync raw → staging for "
                    f"{raw_schema}.{raw_table_name}: {e}"
                )
                return {"success": False}

        # Same empty-target fast path as the batched code path: skip ON CONFLICT
        # entirely when staging is empty (typical for experimentation runs that
        # drop + reload every dataset).
        with self.session_scope() as session:
            target_empty = session.execute(
                select(text("1")).select_from(staging_table).limit(1)
            ).first() is None

        base_insert = Insert(staging_table).from_select(
            insert_cols,
            select(*(raw_table.c[c] for c in insert_cols))
        )

        if target_empty:
            stmt = base_insert
        else:
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
            stmt = base_insert.on_conflict_do_update(
                index_elements=conflict_cols,
                set_=update_map,
                where=where_clause,
            )

        try:
            with self.session_scope() as session:
                result = session.execute(stmt)
                affected = result.rowcount if result.rowcount and result.rowcount >= 0 else 0

            self.logger.info(
                f"Synced raw → staging: {raw_schema}.{raw_table_name} → "
                f"{staging_schema}.{staging_table_name} "
                f"({'plain insert' if target_empty else 'upsert'}, {affected} rows)"
            )

            # `inserted` carries the affected-rows total so MV refresh still triggers.
            return {
                "inserted": affected,
                "updated": 0,
                "total": affected,
                "success": True
            }

        except Exception as e:
            self.logger.error(
                f"Failed syncing raw → staging for "
                f"{raw_schema}.{raw_table_name}: {e}"
            )
            return {
                "success": False
            }
