import importlib
from typing import Any

from log_manager.logger_manager import LoggerManager
from materialized_views.handlers import GenericMaterializedViewHandler


class MaterializedViewManager:
    _default_handler = "GenericMaterializedViewHandler"
    _handler_module = "materialized_views.handlers"

    def __init__(self, db, global_conf: dict[str, Any] | None):
        self.db = db
        self.global_conf = global_conf or {}
        self.logger = LoggerManager(type(self).__name__).get_logger()

    def _is_enabled(self) -> bool:
        return bool(self.global_conf.get("enable", False))

    def _iter_matching_views(self, datasource_name: str):
        for view_conf in self.global_conf.get("views", []) or []:
            if not view_conf.get("enable", True):
                continue
            deps = (view_conf.get("depends_on", {}) or {})
            datasources = deps.get("datasources", []) or []
            if datasource_name in datasources:
                yield view_conf

    def _build_handler(self, view_conf: dict[str, Any]):
        class_name = view_conf.get("handler_class") or self._default_handler
        module_name = view_conf.get("handler_module") or self._handler_module
        module = importlib.import_module(module_name)
        handler_cls = getattr(module, class_name)
        return handler_cls(self.db, view_conf)

    @staticmethod
    def _has_new_data(sync_result: dict | None) -> bool:
        if not isinstance(sync_result, dict):
            return True
        return (sync_result.get("inserted", 0) + sync_result.get("updated", 0)) > 0

    def on_datasource_success(self, datasource_name: str, sync_result: dict | None = None):
        if self.db is None:
            self.logger.info("Materialized view manager skipped: db is None")
            return
        if not self._is_enabled():
            self.logger.info("Materialized view manager skipped: materialized_views.enable=false")
            return

        matched = False
        for view_conf in self._iter_matching_views(datasource_name):
            matched = True
            view_id = view_conf.get("id") or f'{view_conf.get("schema")}.{view_conf.get("name")}'
            refresh_conf = view_conf.get("refresh", {}) or {}
            try:
                self.logger.info(f"Materialized view trigger matched for datasource '{datasource_name}' -> {view_id}")
                handler = self._build_handler(view_conf)
                handler.ensure()
                if not refresh_conf.get("enabled", True):
                    continue
                if refresh_conf.get("only_on_data_change", False) and not self._has_new_data(sync_result):
                    self.logger.info(f"Skipping MV refresh for {view_id}: no new or updated rows from '{datasource_name}'")
                    continue
                handler.refresh()
            except Exception as e:
                self.logger.error(f"Materialized view processing failed for {view_id}: {e}")
        if not matched:
            self.logger.info(f"No materialized view configured for datasource '{datasource_name}'")
