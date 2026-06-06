import importlib
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

from data_config_dtos.data_source_config_dto import DataSourceDTO
from dacite import from_dict, Config

from log_manager.logger_manager import LoggerManager, REPORT_LEVEL
from main_core.safe_class import safe_class
from metadata.data_source_metadata_service import DataSourceMetadataService


@safe_class
class DataSourceMapper:
    _prefix_path = "data_mappers"

    def __init__(self, sources, db_instance, scheduler_core, base_graph_conf, metadata_service: DataSourceMetadataService | None):
        self.logger = LoggerManager(type(self).__name__).get_logger()
        self.db_instance = db_instance
        self.scheduler_core = scheduler_core
        self.data_sources = sources
        self.metadata_service = metadata_service
        self.base_graph_conf = base_graph_conf
        self.logger.info(f"Found {len(self.data_sources)} data sources")
        self._register_all_datasource_metadata()
        all_sources = list(self.data_sources)  # keep full list for status table
        # name -> DataSourceDTO for EVERY datasource (incl. disabled) so dependents
        # can resolve an upstream's enable flag and output tables by name.
        self._peer_configs = self._build_peer_configs(all_sources)
        self.data_sources = self.check_enable_data_sources()
        self._print_datasource_table(all_sources, self.data_sources)

    def _build_peer_configs(self, all_sources) -> dict:
        peers: dict = {}
        for source in all_sources or []:
            dto = self._to_datasource_dto(source)
            if dto is not None and dto.name:
                peers[dto.name] = dto
        return peers

    @staticmethod
    def _to_datasource_dto(source) -> DataSourceDTO | None:
        if isinstance(source, DataSourceDTO):
            return source
        if isinstance(source, dict):
            return from_dict(DataSourceDTO, data=source, config=Config(cast=[dict]))
        return None

    def _register_all_datasource_metadata(self):
        if self.metadata_service is None:
            return
        for source in self.data_sources or []:
            try:
                dto = self._to_datasource_dto(source)
                if dto is None:
                    continue
                self.metadata_service.register_data_source(dto)
                if not dto.enable:
                    self.metadata_service.update(
                        dto.name,
                        {
                            "is_active": False,
                            "current_run_status": "disabled",
                            "last_run_status": "disabled",
                            "last_run_message": "Datasource disabled in config",
                        },
                    )
            except Exception as e:
                source_name = source.get("name") if isinstance(source, dict) else getattr(source, "name", "unknown")
                self.logger.error(f"Metadata registration failed for datasource {source_name}: {e}")

    @staticmethod
    def _parse_env_list(var: str) -> set[str]:
        raw = os.getenv(var, "").strip()
        if not raw:
            return set()
        return {name.strip() for name in raw.split(",") if name.strip()}

    def check_enable_data_sources(self):
        only = self._parse_env_list("ETL_ONLY")
        disable = self._parse_env_list("ETL_DISABLE")
        if only:
            self.logger.info(f"ETL_ONLY set — activating only: {only}")
        if disable:
            self.logger.info(f"ETL_DISABLE set — skipping: {disable}")

        try:
            result = []
            for source in self.data_sources:
                data = self._to_datasource_dto(source)
                if data is None:
                    continue
                name = data.name

                if only:
                    active = name in only
                elif disable:
                    active = name not in disable and data.enable
                else:
                    active = data.enable

                if active:
                    result.append(data)
                elif (only or disable) and self.metadata_service:
                    reason = "excluded by ETL_ONLY" if only else "excluded by ETL_DISABLE"
                    self.metadata_service.update(name, {
                        "is_active": False,
                        "current_run_status": "disabled",
                        "last_run_status": "disabled",
                        "last_run_message": reason,
                    })
            return result
        except Exception as e:
            self.logger.error(f"Error loading data sources: {e}")

    def _print_datasource_table(self, all_sources: list, active_sources: list):
        """Print a color-coded table of all datasources and their activation status."""
        active_names = {getattr(s, "name", None) for s in active_sources}

        GREEN = "\033[92m"
        RED   = "\033[91m"
        DIM   = "\033[2m"
        BOLD  = "\033[1m"
        CYAN  = "\033[96m"
        RESET = "\033[0m"

        rows = []
        for source in all_sources:
            dto = self._to_datasource_dto(source)
            if dto is None:
                continue
            name  = dto.name or ""
            cls   = (dto.class_name or "").strip()
            schedule = ""
            try:
                trigger = dto.job.trigger.type if dto.job and dto.job.trigger else None
                schedule = getattr(trigger, "name", "") if trigger else ""
            except Exception:
                pass

            is_active = name in active_names
            status = f"{GREEN}● active{RESET}" if is_active else f"{DIM}○ disabled{RESET}"
            rows.append((name, cls, schedule, status, is_active))

        if not rows:
            return

        # Column widths (plain text, no ANSI codes)
        w_name  = max(len("Datasource"),   max(len(r[0]) for r in rows))
        w_cls   = max(len("Class"),        max(len(r[1]) for r in rows))
        w_sched = max(len("Schedule"),     max(len(r[2]) for r in rows))
        w_stat  = max(len("Status"),       len("○ disabled"))

        def row_line(name, cls, sched, status_plain, color):
            n  = name.ljust(w_name)
            c  = cls.ljust(w_cls)
            s  = sched.ljust(w_sched)
            st = f"{color}{status_plain.ljust(w_stat)}{RESET}"
            return f"  │ {n}  │ {c}  │ {s}  │ {st} │"

        h_sep  = f"  ├─{'─┼─'.join('─' * w for w in [w_name, w_cls, w_sched, w_stat])}─┤"
        top    = f"  ┌─{'─┬─'.join('─' * w for w in [w_name, w_cls, w_sched, w_stat])}─┐"
        bot    = f"  └─{'─┴─'.join('─' * w for w in [w_name, w_cls, w_sched, w_stat])}─┘"
        header = (f"  │ {'Datasource'.ljust(w_name)}  │ {'Class'.ljust(w_cls)}"
                  f"  │ {'Schedule'.ljust(w_sched)}  │ {'Status'.ljust(w_stat)} │")

        active_count   = sum(1 for r in rows if r[4])
        disabled_count = len(rows) - active_count
        title = (
            f"\n{BOLD}{CYAN}  Datasource Registry"
            f"  {RESET}{DIM}({active_count} active, {disabled_count} disabled){RESET}"
        )
        lines = [title, top, header, h_sep]
        for name, cls, sched, _, is_active in rows:
            color  = GREEN if is_active else DIM
            plain  = "● active" if is_active else "○ disabled"
            lines.append(row_line(name, cls, sched, plain, color))
        lines.append(bot)
        print("\n".join(lines))

    def _run_one_datasource(self, source):
        data = source
        class_name = (data.class_name or "").strip()
        if class_name.endswith("Mapper"):
            class_name = class_name[:-6]
        try:
            module_path = f"{self._prefix_path}.{class_name}Mapper"
            module = importlib.import_module(module_path)
            mapper_class = getattr(module, f"{class_name[0].upper() + class_name[1:]}Mapper")
            instance_data_source = mapper_class(data, self.db_instance, self.scheduler_core, self.base_graph_conf, self.metadata_service)
            instance_data_source.peer_configs = self._peer_configs
            instance_data_source.execute()
            self.logger.info(f"{mapper_class.__name__} configuration step finished!!")
        except Exception as e:
            self.logger.error(f"Error running data source {class_name}: {e}")

    def run_data_source_mapper(self):
        n = len(self.data_sources)
        # Warm the mapper package in the main thread first. data_mappers/__init__.py
        # eagerly imports every mapper module; importing it concurrently from N worker
        # threads races on Python's per-module import locks and can deadlock
        # (_DeadlockError on _ModuleLock). Pre-importing single-threaded makes the
        # threaded import_module() calls below pure cache hits.
        try:
            importlib.import_module(self._prefix_path)
        except Exception as e:
            self.logger.error(f"Error pre-importing mapper package '{self._prefix_path}': {e}")
        self.logger.info(f"Starting {n} datasources in parallel")
        with ThreadPoolExecutor(max_workers=n, thread_name_prefix="DSMapper") as pool:
            futures = {pool.submit(self._run_one_datasource, src): src for src in self.data_sources}
            for fut in as_completed(futures):
                try:
                    fut.result()
                except Exception as e:
                    src = futures[fut]
                    self.logger.error(f"Datasource {getattr(src, 'name', src)} failed: {e}")

    def start_execution(self):
        self.run_data_source_mapper()


if __name__ == "__main__":
    dsm = DataSourceMapper("weather", None, None)
