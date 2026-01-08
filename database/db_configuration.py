from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

import geopandas as gpd
from dacite import from_dict
from geoalchemy2.shape import to_shape
from numpy.ma.core import max_filler
from sqlalchemy import inspect, MetaData, create_engine, select, delete, update, insert, Column, Integer, BigInteger, \
    String, text, func, Row, RowMapping
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.dialects.postgresql import Insert

from database.base import Base
from log_manager.logger_manager import LoggerManager
from main_core.core_config import CoreConfig


@dataclass
class CredentialDTO:
    username: str
    password: str


@dataclass
class DBConfigDTO:
    description: str
    enable: bool
    driver: str
    url: str
    port: int
    database_name: str
    database_schema: str  # not required
    credential: CredentialDTO


class DbConfiguration:
    """
    This class will take care of configuration level database instance
    """
    base = Base

    def __init__(self, core_config, base_config):
        self.core_config = from_dict(DBConfigDTO, core_config)
        self.logger = LoggerManager(type(self).__name__)
        self.db_url = self.create_db_url()
        self.print_db_url()
        self.engine = self.create_engine()
        self.session_factory = self.create_session_factory()
        self.scoped_session = scoped_session(self.session_factory)
        self.schema = self.core_config.database_schema or "public"
        self.create_schema_if_not_exists()
        self.metadata = self.create_metadata()
        self.inspector = inspect(self.engine)
        self.base_config = base_config
        self.logger.debug(f"db schema {self.schema}")

        # create all the tables linked to the Base
        self.update_metadata()

    # Main engine based function of DDL functions
    def create_schema_if_not_exists(self):
        """Create the database schema if it does not exist."""
        schema = self.schema

        sql = text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')

        try:
            with self.engine.begin() as conn:
                conn.execute(sql)
            self.logger.info(f"Schema '{schema}' ensured.")
        except Exception as e:
            self.logger.error(f"Failed to create schema '{schema}': {e}")
            raise

    # this should be moved to the repository

    # -----------------------------
    # Helper: get table object
    # -----------------------------
    def get_table(self, table_name: str):

        table_name = self.normalize_table_name(table_name, True)
        if table_name not in self.metadata.tables:
            self.logger.warning(f"Table '{table_name}' not found in schema '{self.schema}'. Reflecting metadata...")
            self.update_metadata()
            # return None
            if table_name not in self.metadata.tables:
                return None
                raise ValueError(f"Table '{table_name}' does not exist in schema '{self.schema}'")

        return self.metadata.tables[f"{table_name}"]

    def table_exists(self, table_name: str) -> bool:
        """Check if table already exists in database schema."""
        # Table names passed are with schema and inspector just gives out just names with <dbName>.*
        try:

            exists = self.inspector.has_table(table_name.split(".")[-1], schema=self.schema)
            self.logger.info(f"Table '{table_name}' exists: {exists}")
            return exists
        except Exception as e:
            self.logger.error(f"Table '{table_name}' does not exist in schema '{self.schema}'")
            self.logger.error(e)

    # -----------------------------
    # INSERT
    # -----------------------------

    def insert(self, table_name: str, data_values: dict, column_id: str):
        """Insert a single row into the given table."""
        table = self.get_table(table_name)
        stmt = insert(table).values(**data_values)
        stmt = stmt.on_conflict_do_update(
            index_elements=[table.c[column_id]],
            set_={c.name: getattr(stmt.excluded, c.name) for c in table.columns if c.name != "node_id"})

        try:
            with self.engine.begin() as conn:
                conn.execute(stmt)
            self.logger.info(f"Inserted data into '{table_name}': {data_values}")
        except SQLAlchemyError as e:
            self.logger.error(f"Insert failed for '{table_name}': {e}")
            raise

    # -----------------------------
    # FETCH / QUERY
    # -----------------------------

    def create_scoped_session(self):
        return scoped_session(self.session_factory)

    def create_metadata(self):
        # return Base.metadata
        return MetaData(schema=self.schema)

    def create_db_url(self):
        core_config = self.core_config
        return (f"{core_config.driver}://{core_config.credential.username}:{core_config.credential.password}@"
                f"{core_config.url}:{core_config.port}/{core_config.database_name}")

    def print_db_url(self):
        self.logger.info(f"Testing connection with url {self.db_url}")

    def get_db_url(self):
        return self.db_url

    def create_engine(self):
        return create_engine(self.db_url, echo=False, plugins=["geoalchemy2"])

    def create_session_factory(self):
        return sessionmaker(bind=self.engine,
                            autocommit=False,
                            autoflush=False,
                            pool_size=10,
                            max_overflow = 20,
                            pool_pre_ping=True
                            )

    def get_new_session(self):
        return self.create_scoped_session()

    # def get_session_factory(self):
    #     if self.session_factory is not None:
    #         return self.session(
    #     return None

    def update_metadata(self):
        self.metadata.reflect(bind=self.engine)

    def inspect_session(self):
        inspector = inspect(self.engine)
        self.logger.info(f"The tables found {inspector.get_table_names()}")
        for table_name in inspector.get_table_names():
            # self.logger.info("Table:", table_name) #-> leads to an error as it can return a dictionary
            print(table_name)

    @contextmanager
    def session_scope(self):
        """Provide a transactional scope around a series of operations."""
        session = self.scoped_session()
        try:
            yield session
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            # session.remove()
            # TODO: check if the session should be removed or just closed as we are creating new session each time
            # TODO : Evaluate
            session.close()

    def get_table_row_count(self, table_name: str) -> int:
        """
        Return total number of rows in the given table.
        """
        table = self.get_table(table_name)

        stmt = select(func.count()).select_from(table)

        with self.session_scope() as session:
            return session.execute(stmt).scalar_one()

    def fetch_one_session(self, table_name: str, filters: dict):
        try:
            table = self.get_table(table_name)

            with self.session_scope() as session:
                stmt = select(table).where(*[table.c[k] == v for k, v in filters.items()])
                result = session.execute(stmt).first()

                return dict(result._mapping) if result else None
        except SQLAlchemyError as e:
            self.logger.error(f"fetch data from failed !!! '{table_name}': {e} ")
            return None

    def get_all_db_tables(self):
        """Return a list of all table names in the configured schema. Based on inspector linked through DB itself """
        try:
            tables = self.inspector.get_table_names(schema=self.schema)
            self.logger.info(f"Tables found in schema '{self.schema}': {tables}")
            return tables
        except Exception as e:
            self.logger.error(f"Failed to fetch table list: {e}")
            raise

    def has_base_tables(self):
        """Return a list of all table names in the configured schema. Based on inspector linked through DB itself """
        try:

            return self.inspector.has_table(table_name=self.base_config.table_name,
                                            schema=self.base_config.table_schema)

        except Exception as e:
            self.logger.error(f"Failed to check  table list: {e}")
            raise

    def get_orm_columns(self, table_name: str):
        """Return column names of ORM BASE model."""
        table_name = f"{self.schema}.{table_name}"
        table = Base.metadata.tables.get(table_name)
        print(f"ORM tavble {table}")
        if table is None:
            raise ValueError(f"ORM table '{table_name}' not found!")
        return [col.name for col in table.columns]

    def get_db_columns(self, table_name: str):
        """Return column names of a table from the DB."""
        cols = self.inspector.get_columns(table_name, schema=self.schema)
        return [col["name"] for col in cols]

    def get_db_column_info(self, table_name: str):
        """Return full DB column metadata (name → attributes)."""
        table_name = self.normalize_table_name(table_name, False)
        columns = self.inspector.get_columns(table_name, schema=self.schema)

        db_info = {}

        for col in columns:
            db_info[col["name"]] = {
                # "type": str(col["type"]),  # DB type
                "nullable": col["nullable"],  # nullable?
                # "default": str(col.get("default")),  # default value
                # "autoincrement": col.get("autoincrement"),
                "python_type": col["type"].python_type or None,
            }

        return db_info

    def normalize_table_name(self, table_name: str, with_schema_prefix: bool = False):
        # print(f"before table name normalization {table_name}")
        name = table_name.split(".")
        size = len(name)
        if with_schema_prefix:
            res = table_name if size > 1 else f"{self.schema}.{table_name}"

        else:
            res = table_name if size == 1 else name[-1]
            # print(f"result for normalization {res}")
        return res

    def get_orm_column_info(self, table_name: str):
        """Return ORM model metadata for columns."""
        table_name = self.normalize_table_name(table_name, False)
        table = Base.metadata.tables.get(table_name)
        # print(f"get orm column info ORM table {table}")
        if table is None:
            # workaround for local class testing

            table = Base.metadata.tables.get(self.normalize_table_name(table_name, True))
            if table is None:
                raise ValueError(f"ORM table '{table_name}' not found!")

        orm_info = {}

        for col in table.columns:
            orm_info[col.name] = {
                # "type": str(col.type),  # ORM type
                "nullable": col.nullable,  # nullable?
                # "default": str(col.default),  # default?
                # DB inteprets the default value as false hence added a check
                # "autoincrement": False if col.autoincrement == "auto" else col.autoincrement,
                # "all": col,
                "python_type": col.type.python_type or None,
            }
        return orm_info
    #TODO: implement to be used by both the factors
    @staticmethod
    def create_table_schema_comparator(key: str, value: Any):
        return
    def table_schema_matches(self, table_name: str) -> bool | None:
        """Compare DB table structure vs ORM table structure (deep check)."""
        try:
            db_info = self.get_db_column_info(table_name)
            orm_info = self.get_orm_column_info(table_name)

            self.logger.debug(f"DB table {table_name}: {db_info}")
            self.logger.debug(f"ORM table {table_name}: {orm_info}")

            db_cols = set(db_info.keys())
            orm_cols = set(orm_info.keys())

            # First: compare column existence
            if db_cols != orm_cols:
                self.logger.warning(
                    f"Column mismatch in '{table_name}': "
                    f"DB columns = {db_cols}, ORM columns = {orm_cols}"
                )
                return False

            # Second: compare column properties
            for col in db_cols:
                db_col = db_info[col]
                orm_col = orm_info[col]

                if db_col != orm_col:
                    self.logger.warning(
                        f"Column mismatch in '{table_name}.{col}':\n"
                        f"DB  = {db_col}\n"
                        f"ORM = {orm_col}"
                    )
                    return False

            return True
        except Exception as e:
            self.logger.error(f"Error occurred {e}", e)
            return None


if __name__ == "__main__":
    class TestDB(Base):
        __tablename__ = "test"
        __table_args__ = {"schema": "test"}
        id = Column(Integer, primary_key=True, autoincrement=True)
        source = Column(BigInteger, nullable=False)
        name = Column(String, nullable=False)
        address = Column(String, nullable=False)


    conf = CoreConfig()
    conf = conf.get_value("database")
    db = DbConfiguration(conf)
    # db.create_table("test")
    # db.create_all_tables()

    #     start with new execution
    #     res = db.fetch_one_session("test", {"source": 2})
    #     print(res)

    tables = db.get_all_db_tables()
    print(f"table : {tables}")

    # check the db for adding colum

    # db.drop_table("test", True, True)
    # if r:
    #     print("dropped successfully")

    r = db.fetch_columns_with_limits("dwd_station_locations", "dwd_station_id", 3)
    print(r)

    print("Cloning the tables .....")
    # clone_table = db.clone_table_schema_with_data("public","ways", "test", "ways", False )
    # print(clone_table)

    print("already cloned present now lets start with mapping")
    res = db.fetch_columns_with_limits("ways    ", ["id", "osm_id", "geom"], 3)
    print(res)
    print(type(res[0][1]))
    gdf = gpd.GeoDataFrame(
        [(osm, to_shape(geom)) for osm, geom in res],
        columns=["osm_id", "geometry"],
        geometry="geometry",
        crs="EPSG:4326")

    print(gdf)
