from database.base import Base
from NotUsed.db_conf import DbConf
from log_manager.logger_manager import LoggerManager

from sqlalchemy import Table

class DBConnect:
    """
    A unified database manager for handling connections, sessions, metadata,
    table creation, and schema validation using SQLAlchemy ORM.
    """
    base = Base

    def __init__(self, db_conf: DbConf):
        self.logger = LoggerManager(self.__class__.__name__).get_logger()
        self.db_conf = db_conf
        # Setup engine and session
        self.engine = self.db_conf.engine
        self.session_factory = self.db_conf.session
        self.Session = self.db_conf.get_session()

        # Metadata
        self.metadata = self.db_conf.metadata
        self.inspector = self.db_conf.inspector
        self.logger.info("Database connection established.")

    # -----------------------------
    # Session management
    # -----------------------------
    def get_session(self):
        return self.Session()

    def close_session(self):
        self.Session.remove()
        self.logger.debug("Database session closed.")

    # -----------------------------
    # Table management
    # -----------------------------


    def table_exists(self, table_name: str) -> bool:
        """Check if table already exists in database schema."""
        # self.logger.info(f"Checking if table exist in inspector {self.inspector.get_table_names()}")
        #Table names passed are with schema and inspector just gives out just names with <dbName>.*
        exists = self.inspector.has_table(table_name.split(".")[-1], schema=self.db_conf.schema)
        self.logger.info(f"Table '{table_name}' exists: {exists}")
        return exists

    def get_existing_columns(self, table_name: str):
        """Return a list of existing column names for the given table."""
        if not self.table_exists(table_name):
            return []
        columns = [col["name"] for col in self.inspector.get_columns(table_name, schema=self.db_conf.schema)]
        return columns

    def compare_table_structure(self, model_class) -> bool:
        """
        Compare ORM model structure to existing DB table columns.
        Returns True if structures match, False if not.
        """
        table_name = model_class.__tablename__
        if not self.table_exists(table_name):
            return False

        db_cols = set(self.get_existing_columns(table_name))
        model_cols = set(model_class.__table__.columns.keys())

        if db_cols == model_cols:
            self.logger.info(f"Table '{table_name}' already matches ORM definition.")
            return True
        else:
            self.logger.warning(f"Schema mismatch in table '{table_name}':")
            self.logger.warning(f"  DB Columns: {db_cols}")
            self.logger.warning(f"  ORM Columns: {model_cols}")
            return False

    def create_table_if_not_exists(self, model_class):
        """
        Create a single table if it does not exist.
        If it exists, verify schema and skip creation if it matches.
        """
        table_name = model_class.__tablename__
        if self.table_exists(table_name):
            if self.compare_table_structure(model_class):
                self.logger.info(f"Skipping creation — table '{table_name}' already up-to-date.")
            else:
                self.logger.warning(f"Table '{table_name}' exists but schema differs.")
                self.logger.warning("Consider running migrations or manually syncing schema.")
            return

        self.logger.info(f"Creating new table '{table_name}'...")
        model_class.__table__.create(self.engine)
        self.logger.info(f"Table '{table_name}' created successfully.")

    def create_all_tables(self):
        # Base.metadata.create_all(bind=self.engine)

        """Create all missing tables defined in Base.metadata."""
        self.logger.info(f"create all tables {self.metadata.tables.items()}")
        # self.logger.info(f"create all tables2 {Base.metadata.tables.keys()}")
        for name, table in Base.metadata.tables.items():
            self.logger.info(f"in base mode; check all tables {name} {table}")
            if not self.table_exists(name):
                self.logger.info(f"Creating missing table '{name}'...")
                # table.create(self.engine)
                Base.metadata.create_all(bind=self.engine, tables=[table], checkfirst=True)

            else:
                self.logger.info(f"Table '{name}' already exists — skipping creation.")
        self.logger.info("Table creation check complete.")

    def reflect_existing_table_tems(self):
        """Reflect database schema into metadata."""
        self.metadata.reflect(bind=self.engine)
        self.logger.info(f"Reflected tables: {list(self.metadata.tables.items())}")

    def get_table(self, table_name: str) -> Table | None:
        if table_name in self.metadata.tables:
            return self.metadata.tables[table_name]
        self.logger.warning(f"Table '{table_name}' not found in metadata.")
        return None