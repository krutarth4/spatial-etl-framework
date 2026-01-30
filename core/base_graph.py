from dacite import from_dict

from data_config_dtos.data_source_config_dto import BaseGraphDTO
from database.db_instancce import DbInstance


class BaseGraph:
    def __init__(self, db: DbInstance | None, base_graph_conf):
        if db is not None:
            self.db = db
            self.base_graph_conf = from_dict(BaseGraphDTO, base_graph_conf)
            self.create_base_graph_tables()

    def create_base_graph_tables(self):
        self.db.create_table_if_not_exist(self.base_graph_conf.table_name,
                                          self.base_graph_conf.table_schema,
                                          self.base_graph_conf.force_generate)

    def populate_base_graph_table(self, source_name: str, source_schema: str):
        self.db.clone_table_data(source_name, source_schema, self.base_graph_conf.table_name,
                                 self.base_graph_conf.table_schema)

    def drop_base_graph_table(self):
        self.db.drop_table(self.base_graph_conf.table_name,self.base_graph_conf.table_schema,True,True,True)

    def check_base_graph_table_exists(self):
        return self.db.has_base_tables()

    def get_base_graph_row_counts(self):
        return self.db.get_table_count(self.base_graph_conf.table_name,self.base_graph_conf.table_schema)
