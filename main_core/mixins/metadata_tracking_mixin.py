"""Run-lifecycle metadata: registration, start/finish marking, path tracking.

Reads from self:  self.metadata_service, self.data_source_name,
                  self.data_source_config, self.logger
"""
from main_core.safe_class import safe_class


@safe_class
class MetadataTrackingMixin:
    """Persists run-lifecycle events to the metadata store."""

    def _register_datasource_metadata(self):
        """Register this datasource in the metadata store on first init."""
        if self.metadata_service is None:
            return
        try:
            self.metadata_service.register_data_source(self.data_source_config)
        except Exception as e:
            self.logger.error(
                f"Datasource metadata registration failed for {self.data_source_name}: {e}"
            )

    def _mark_metadata_run_started(self):
        """Record that a new run has started."""
        if self.metadata_service is None:
            return
        try:
            self.metadata_service.mark_run_started(self.data_source_name)
        except Exception as e:
            self.logger.error(
                f"Failed to mark metadata run start for {self.data_source_name}: {e}"
            )

    def _mark_metadata_run_finished(
        self,
        succeeded: bool,
        run_result=None,
        error: Exception | None = None,
        duration_seconds: int | None = None,
    ):
        """Record run outcome (success/failure), final message, and duration."""
        if self.metadata_service is None:
            return
        try:
            message = None
            if isinstance(run_result, dict):
                message = run_result.get("message")
            if error is not None:
                message = str(error)
            self.metadata_service.mark_run_finished(
                self.data_source_name, succeeded, message, duration_seconds
            )
        except Exception as e:
            self.logger.error(
                f"Failed to update metadata run status for {self.data_source_name}: {e}"
            )

    def _note_stage_warning(self, stage: str, error: Exception):
        """Record a non-fatal stage failure so run() can report the run as degraded."""
        if not hasattr(self, "_run_stage_warnings"):
            self._run_stage_warnings = []
        self._run_degraded = True
        self._run_stage_warnings.append((stage, str(error)))

    def _update_metadata_runtime_paths(self, paths):
        """Overwrite the runtime file-path list in metadata (used for lineage)."""
        if self.metadata_service is None:
            return
        try:
            self.metadata_service.update_runtime_file_paths(self.data_source_name, paths)
        except Exception as e:
            self.logger.error(
                f"Failed to update runtime file paths in metadata for {self.data_source_name}: {e}"
            )

    def _append_metadata_runtime_paths(self, paths):
        """Append to the runtime file-path list in metadata."""
        if self.metadata_service is None:
            return
        try:
            self.metadata_service.append_runtime_file_paths(self.data_source_name, paths)
        except Exception as e:
            self.logger.error(
                f"Failed to append runtime file paths in metadata for {self.data_source_name}: {e}"
            )
