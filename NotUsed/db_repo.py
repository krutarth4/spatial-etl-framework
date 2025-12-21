from sqlalchemy import MetaData, update, delete, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects.postgresql import Insert, insert

from NotUsed.db_conf import DBConfig, DbConf
from database.db_instancce import DbInstance
from graph.osm_barrier_type import OsmBarrierType
from graph.osm_travel_modes import OsmTravelModes
from log_manager.logger_manager import LoggerManager
from NotUsed.db_connect import DBConnect
from main_core.core_config import CoreConfig

# Important do not remove - all Base data class will be created automatically
import data_tables
class DbRepo:
    """
    Generic database repository that can insert, update, delete, and query any table
    by name and schema using SQLAlchemy Core.
    """

    def __init__(self, db: DbInstance, schema: str = "public"):
        self.db = db
        self.engine = db.engine
        self.logger = LoggerManager(self.__class__.__name__).get_logger()
        self.schema = schema or db.db_conf.schema
        self.metadata = MetaData(schema=self.schema)
        self.metadata.reflect(bind=self.engine)

    # -----------------------------
    # Helper: get table object
    # -----------------------------
    def get_table(self, table_name: str):

        table_name = f"{self.schema}.{table_name}"
        if table_name not in self.metadata.tables:
            self.logger.warning(f"Table '{table_name}' not found in schema '{self.schema}'. Reflecting metadata...")
            self.metadata.reflect(bind=self.engine)
            print(self.metadata.tables)
            if table_name not in self.metadata.tables:
                raise ValueError(f"Table '{table_name}' does not exist in schema '{self.schema}'")
        return self.metadata.tables[f"{table_name}"]

    # -----------------------------
    # INSERT
    # -----------------------------

    def insert(self, table_name: str, data_values: dict, column_id : str =None):
        """Insert a single row into the given table."""
        table = self.get_table(table_name)
        stmt = insert(table).values(**data_values)
        # stmt = stmt.on_conflict_do_update(
        #     index_elements=[table.c[column_id]],
        #     set_={c.name: getattr(stmt.excluded, c.name) for c in table.columns if c.name != "node_id"})
        # stmt = stmt.on_conflict_do_nothing()
        try:
            with self.engine.begin() as conn:
                conn.execute(stmt)
            self.logger.info(f"Inserted data into '{table_name}': {data_values}")
        except SQLAlchemyError as e:
            self.logger.error(f"Insert failed for '{table_name}': {e}")
            raise


    def bulk_upsert(self, table_name: str, data_list: list[dict], conflict_column: str = "node_id"):
        """Efficiently insert or update multiple rows using ON CONFLICT (PostgreSQL)."""
        if not data_list:
            return

        table = self.get_table(table_name)
        stmt = Insert(table).values(data_list)

        # prepare update dict — exclude conflict column
        update_dict = {c.name: stmt.excluded[c.name] for c in table.columns if c.name not in (conflict_column, "id")}

        stmt = stmt.on_conflict_do_update(
            index_elements=[table.c[conflict_column]],
            set_=update_dict
        )

        try:
            with self.engine.begin() as conn:
                conn.execute(stmt)
            self.logger.info(f"Bulk upserted {len(data_list)} rows into '{table_name}'")
        except SQLAlchemyError as e:
            self.logger.error(f"Bulk upsert failed for '{table_name}': {e}")
            raise

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

    # -----------------------------
    # DELETE
    # -----------------------------
    def delete(self, table_name: str, filters: dict):
        """Delete rows from table matching filters."""
        table = self.get_table(table_name)
        stmt = delete(table).where(
            *[table.c[k] == v for k, v in filters.items()]
        )

        try:
            with self.engine.begin() as conn:
                result = conn.execute(stmt)
                self.logger.info(f"Deleted {result.rowcount} row(s) from '{table_name}'")
        except SQLAlchemyError as e:
            self.logger.error(f"Delete failed for '{table_name}': {e}")
            raise

    # -----------------------------
    # FETCH / QUERY
    # -----------------------------
    def fetch_all(self, table_name: str, filters: dict | None = None):
        """Fetch all rows (optionally filtered) from the given table."""
        table = self.get_table(table_name)
        stmt = select(table)
        if filters:
            stmt = stmt.where(*[table.c[k] == v for k, v in filters.items()])

        try:
            with self.engine.begin() as conn:
                result = conn.execute(stmt)
                rows = [dict(r._mapping) for r in result]
                self.logger.info(f"Fetched {len(rows)} rows from '{table_name}'")
                return rows
        except SQLAlchemyError as e:
            self.logger.error(f"Fetch failed for '{table_name}': {e}")
            raise

    def fetch_one(self, table_name: str, filters: dict):
        """Fetch a single row matching the filters."""
        rows = self.fetch_all(table_name, filters)
        return rows[0] if rows else None


if __name__=='__main__':

    core_db_conf: DBConfig = CoreConfig().get_value("db2")
    dbConf = DbConf(core_db_conf)
    conn = DBConnect(dbConf)
    conn.reflect_existing_table_tems()
    conn.create_all_tables()
    db_crud =  DbRepo(conn)
    data = {
        "node_id": 2002,
        "barrier_type": OsmBarrierType.GATE.osm_name,
        "modes_allowed": [OsmTravelModes.BICYCLE.key, OsmTravelModes.MOTORCAR.key],
        "modes_restricted": [OsmTravelModes.MOTORCAR.key],

    }
    # print(data)
    # db_crud.insert(BarrierNode.__tablename__, data)