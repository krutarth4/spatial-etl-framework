from dacite import from_dict
from sqlalchemy import text

from custom_graph_base_tables import (
    WaysBaseChangesConsumedTable,
    WaysBaseChangesStateTable,
    WaysBaseChangesTable,
    WaysBaseTable,
)
from utils.data_source_config_dto import BaseGraphDTO
from database.db_instance import DbInstance


# Columns of trial.ways_base copied from the source `way_segment` table during
# the upsert path. Hash is computed over the attributes (not the natural key).
_WAYS_BASE_DATA_COLUMNS = ["way_id", "way_link_index", "from_node_id", "to_node_id", "length_m", "geometry"]
_WAYS_BASE_NATURAL_KEY = ["way_id", "way_link_index"]
_WAYS_BASE_ATTR_COLUMNS = ["from_node_id", "to_node_id", "length_m", "geometry"]

_CHANGES_TABLE_NAME = "ways_base_changes"
_PREV_SNAPSHOT_TABLE_NAME = "ways_base_prev_snapshot"
_CHANGES_STATE_TABLE_NAME = "ways_base_changes_state"
_CHANGES_CONSUMED_TABLE_NAME = "ways_base_changes_consumed"


def _content_hash_expr(geometry_col: str = "geometry") -> str:
    """SQL expression producing a 16-byte md5 over the row's content.

    ST_SnapToGrid is used to absorb sub-millimeter float jitter that would
    otherwise mark unchanged geometries as 'modified' on every regeneration.
    """
    return (
        "decode(md5("
        f"COALESCE(ST_AsBinary(ST_SnapToGrid({geometry_col}, 1e-7))::text, '') || '|' || "
        "COALESCE(from_node_id::text, '') || '|' || "
        "COALESCE(to_node_id::text, '') || '|' || "
        "COALESCE(length_m::text, '')"
        "), 'hex')"
    )


class BaseGraph:
    def __init__(self, db: DbInstance | None, base_graph_conf):
        if db is not None:
            self.db = db
            self.base_graph_conf = from_dict(BaseGraphDTO, base_graph_conf)

    def create_base_graph_tables(self):
        self.db.create_table_if_not_exist(self.base_graph_conf.table_name,
                                          self.base_graph_conf.table_schema,
                                          self.base_graph_conf.force_generate)
        # Co-located changes table used by incremental mapping. Always created
        # alongside ways_base; safe to call repeatedly.
        self.db.create_table_if_not_exist(_CHANGES_TABLE_NAME,
                                          self.base_graph_conf.table_schema,
                                          False)
        # Bookkeeping for per-datasource consumption of the change-set. State
        # holds the current generation (bumped by populate); consumed records
        # the latest generation each datasource has mapped.
        self.db.create_table_if_not_exist(_CHANGES_STATE_TABLE_NAME,
                                          self.base_graph_conf.table_schema,
                                          False)
        self.db.create_table_if_not_exist(_CHANGES_CONSUMED_TABLE_NAME,
                                          self.base_graph_conf.table_schema,
                                          False)

    def populate_base_graph_table(self, source_name: str, source_schema: str):
        """Upsert ways_base from the source way-segments table and materialize
        the per-segment change-set into ways_base_changes.

        Steps:
            1. Snapshot existing (id, natural_key, content_hash) into a prev table.
            2. Upsert from source on (way_id, way_link_index).
            3. Recompute geometry_25833 + content_hash for rows whose hash changed.
            4. Delete rows no longer present in source.
            5. Diff snapshot vs current state, write ops into ways_base_changes.
        """
        schema = self.base_graph_conf.table_schema
        table = self.base_graph_conf.table_name
        prev = f"{schema}.{_PREV_SNAPSHOT_TABLE_NAME}"
        changes = f"{schema}.{_CHANGES_TABLE_NAME}"
        target = f"{schema}.{table}"
        source = f"{source_schema}.{source_name}"

        data_cols_csv = ", ".join(_WAYS_BASE_DATA_COLUMNS)
        attr_cols_update_csv = ",\n                ".join(
            f"{c} = EXCLUDED.{c}" for c in _WAYS_BASE_ATTR_COLUMNS
        )
        hash_expr = _content_hash_expr("geometry")

        # 1. Snapshot existing state (natural key + hash + id) BEFORE the upsert.
        self.db.call_sql(f"DROP TABLE IF EXISTS {prev};", raise_on_error=True)
        self.db.call_sql(
            f"CREATE TABLE {prev} AS "
            f"SELECT id, way_id, way_link_index, content_hash FROM {target};",
            raise_on_error=True,
        )

        # 2. Upsert from source on (way_id, way_link_index).
        self.db.call_sql(
            f"INSERT INTO {target} ({data_cols_csv}) "
            f"SELECT {data_cols_csv} FROM {source} "
            f"ON CONFLICT (way_id, way_link_index) DO UPDATE SET "
            f"{attr_cols_update_csv};",
            raise_on_error=True,
        )

        # 3. Refresh geometry_25833 + content_hash, but only where content actually changed.
        self.db.call_sql(
            f"UPDATE {target} SET "
            f"geometry_25833 = ST_Transform(geometry, 25833), "
            f"content_hash = {hash_expr} "
            f"WHERE content_hash IS DISTINCT FROM {hash_expr};",
            raise_on_error=True,
        )

        # 4. Remove segments no longer present in source.
        self.db.call_sql(
            f"DELETE FROM {target} t "
            f"WHERE NOT EXISTS ("
            f"  SELECT 1 FROM {source} s "
            f"  WHERE s.way_id = t.way_id AND s.way_link_index = t.way_link_index"
            f");",
            raise_on_error=True,
        )

        # 5. Materialize change-set. base_id is the *current* ways_base.id where
        #    available, else the previous id (so 'removed' rows still carry an id
        #    that mapping tables can DELETE FROM ... WHERE way_id = base_id).
        self.db.call_sql(f"TRUNCATE {changes};", raise_on_error=True)
        self.db.call_sql(
            f"INSERT INTO {changes} (base_id, way_id, way_link_index, op) "
            f"SELECT "
            f"  COALESCE(n.id, p.id) AS base_id, "
            f"  COALESCE(n.way_id, p.way_id) AS way_id, "
            f"  COALESCE(n.way_link_index, p.way_link_index) AS way_link_index, "
            f"  CASE "
            f"    WHEN p.id IS NULL THEN 'added' "
            f"    WHEN n.id IS NULL THEN 'removed' "
            f"    ELSE 'modified' "
            f"  END AS op "
            f"FROM {target} n "
            f"FULL OUTER JOIN {prev} p "
            f"  ON n.way_id = p.way_id AND n.way_link_index = p.way_link_index "
            f"WHERE p.id IS NULL "
            f"   OR n.id IS NULL "
            f"   OR n.content_hash IS DISTINCT FROM p.content_hash;",
            raise_on_error=True,
        )

        self.db.call_sql(f"DROP TABLE IF EXISTS {prev};", raise_on_error=False)

        # 6. Bump change-set generation iff diff produced rows. Consumers gate
        # on (current_gen > consumed_gen), so leaving generation untouched on
        # an empty diff means no datasource is woken up to map nothing.
        self._bump_generation_if_changes_present()

    def drop_base_graph_table(self):
        self.db.drop_table(self.base_graph_conf.table_name,self.base_graph_conf.table_schema,True,True,True)

    def check_base_graph_table_exists(self):
        return self.db.has_base_tables()

    def get_base_graph_row_counts(self):
        return self.db.get_table_count(self.base_graph_conf.table_name,self.base_graph_conf.table_schema)

    # --- Incremental-mapping support ---------------------------------------------------

    def get_changes_table_fqn(self) -> str:
        """Fully-qualified name of the change-set table (schema.table)."""
        return f"{self.base_graph_conf.table_schema}.{_CHANGES_TABLE_NAME}"

    def has_pending_changes(self) -> bool:
        """True iff ways_base_changes has at least one row. Retained for
        callers that don't care about per-datasource bookkeeping."""
        try:
            return self.db.get_table_count(_CHANGES_TABLE_NAME, self.base_graph_conf.table_schema) > 0
        except Exception:
            return False

    # --- Per-datasource generation tracking ---

    def _state_table_fqn(self) -> str:
        return f"{self.base_graph_conf.table_schema}.{_CHANGES_STATE_TABLE_NAME}"

    def _consumed_table_fqn(self) -> str:
        return f"{self.base_graph_conf.table_schema}.{_CHANGES_CONSUMED_TABLE_NAME}"

    def _bump_generation_if_changes_present(self) -> None:
        try:
            if self.db.get_table_count(_CHANGES_TABLE_NAME, self.base_graph_conf.table_schema) == 0:
                return
        except Exception:
            return
        state = self._state_table_fqn()
        self.db.call_sql(
            f"INSERT INTO {state} (id, generation) VALUES (1, 1) "
            f"ON CONFLICT (id) DO UPDATE SET generation = {state}.generation + 1;",
            raise_on_error=True,
        )

    def current_generation(self) -> int:
        """Returns the latest published change-set generation. 0 if no populate
        has produced a non-empty diff yet."""
        try:
            with self.db.session_scope() as session:
                row = session.execute(text(
                    f"SELECT generation FROM {self._state_table_fqn()} WHERE id = 1"
                )).first()
                return int(row[0]) if row else 0
        except Exception:
            return 0

    def consumed_generation_for(self, datasource_name: str) -> int | None:
        """Latest generation that `datasource_name` has fully mapped, or None
        if it has never consumed."""
        try:
            with self.db.session_scope() as session:
                row = session.execute(
                    text(
                        f"SELECT consumed_generation FROM {self._consumed_table_fqn()} "
                        f"WHERE datasource_name = :ds"
                    ),
                    {"ds": datasource_name},
                ).first()
                return int(row[0]) if row else None
        except Exception:
            return None

    def mark_consumed(self, datasource_name: str, generation: int) -> None:
        """Record that `datasource_name` has mapped through `generation`."""
        self.db.call_sql(
            f"INSERT INTO {self._consumed_table_fqn()} (datasource_name, consumed_generation) "
            f"VALUES (:ds, :gen) "
            f"ON CONFLICT (datasource_name) DO UPDATE SET consumed_generation = EXCLUDED.consumed_generation;",
            params={"ds": datasource_name, "gen": int(generation)},
            raise_on_error=True,
        )

    def has_pending_changes_for(self, datasource_name: str) -> bool:
        """True iff the current change-set is non-empty and `datasource_name`
        hasn't caught up to the current generation."""
        try:
            if self.db.get_table_count(_CHANGES_TABLE_NAME, self.base_graph_conf.table_schema) == 0:
                return False
        except Exception:
            return False
        current = self.current_generation()
        if current == 0:
            return False
        consumed = self.consumed_generation_for(datasource_name)
        if consumed is None:
            return True
        return current > consumed

    def pending_change_count(self, ops: tuple[str, ...] = ("added", "modified")) -> int:
        """Number of change-set rows matching `ops`. Used to size mapping work
        (e.g. decide whether batching is worth the overhead)."""
        if not ops:
            return 0
        ops_csv = ", ".join(f"'{op}'" for op in ops)
        try:
            with self.db.session_scope() as session:
                row = session.execute(text(
                    f"SELECT COUNT(*) FROM {self.base_graph_conf.table_schema}.{_CHANGES_TABLE_NAME} "
                    f"WHERE op IN ({ops_csv})"
                )).first()
                return int(row[0]) if row else 0
        except Exception:
            return 0
