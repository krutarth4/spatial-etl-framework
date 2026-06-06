"""DBRepository — thin combiner over the focused database mixins.

The original ~1640-line god-class has been split into focused mixins under
database/repository/.  This module only composes them (plus DbConfiguration)
behind the original name, so existing imports keep working unchanged:

    from database.db_repository import DBRepository

Mixin responsibilities:
  ConnectionMixin  — __init__, engine/session, raw SQL, batching, low-level helpers
  TableOpsMixin    — table create/clone/drop/index/reflect, row counts
  DataOpsMixin     — bulk insert, update, upsert, staging/source sync
  SchemaOpsMixin   — column add/update, column-limit introspection
  ConflictMixin    — ON CONFLICT / primary-key / update column resolution

ConnectionMixin.__init__ calls super().__init__(...), which resolves through the
MRO to DbConfiguration (kept last) for the real engine/session setup.
"""
from database.db_configuration import DbConfiguration
from database.repository.connection_mixin import ConnectionMixin
from database.repository.table_ops_mixin import TableOpsMixin
from database.repository.data_ops_mixin import DataOpsMixin
from database.repository.schema_ops_mixin import SchemaOpsMixin
from database.repository.conflict_mixin import ConflictMixin


class DBRepository(
    ConnectionMixin,
    TableOpsMixin,
    DataOpsMixin,
    SchemaOpsMixin,
    ConflictMixin,
    DbConfiguration,
):
    """Composed database repository (see database/repository/ for each mixin)."""
