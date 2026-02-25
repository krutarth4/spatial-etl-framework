from main_core.data_source_abc_impl import DataSourceABCImpl


class GraphMapper(DataSourceABCImpl):

    def execute_run_pipeline(self):
        path = self.extract()
        return