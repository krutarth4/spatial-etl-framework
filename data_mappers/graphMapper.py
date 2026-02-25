from communication.comm_service import CommService
from main_core.data_source_abc_impl import DataSourceABCImpl


class GraphMapper(DataSourceABCImpl):
    _osm_download_task_key = "osm_file_download"

    def _get_comm_service(self) -> CommService | None:
        if self.db is None:
            return None
        schema = None
        if self.metadata_service is not None and getattr(self.metadata_service, "metadata_conf", None) is not None:
            schema = getattr(self.metadata_service.metadata_conf, "table_schema", None)
        elif hasattr(self.db, "schema"):
            schema = self.db.schema
        return CommService(self.db, schema)

    def execute_run_pipeline(self):
        comm_service = self._get_comm_service()
        if comm_service is not None:
            comm_service.ensure_task(self._osm_download_task_key, owner="pipeline", current_status="idle")
            comm_service.update_status(
                self._osm_download_task_key,
                owner="pipeline",
                current_status="running",
                last_run_status="running",
                last_run_message="Checking metadata / downloading OSM file",
                is_completed=False,
            )

        try:
            paths = self.extract()
            self._update_metadata_runtime_paths(paths)
            downloaded = self._last_fetch_performed_download
            if comm_service is not None:
                if downloaded is True:
                    msg = "Downloaded new OSM file"
                elif downloaded is False:
                    msg = "OSM file already available (metadata unchanged)"
                else:
                    msg = "OSM file prepared"
                comm_service.update_status(
                    self._osm_download_task_key,
                    owner="pipeline",
                    current_status="idle",
                    last_run_status="success",
                    last_run_message=msg,
                    success=True,
                    is_completed=True,
                )
            return self.run_job_response("Graph source prepared")
        except Exception as e:
            if comm_service is not None:
                comm_service.update_status(
                    self._osm_download_task_key,
                    owner="pipeline",
                    current_status="failed",
                    last_run_status="failed",
                    last_run_message=str(e),
                    is_completed=False,
                )
            raise
