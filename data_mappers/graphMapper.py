from main_core.data_source_abc_impl import DataSourceABCImpl


class GraphMapper(DataSourceABCImpl):

    def execute_run_pipeline(self):
        paths = self.extract()
        self._update_metadata_runtime_paths(paths)
        return self.run_job_response("Graph source prepared")
