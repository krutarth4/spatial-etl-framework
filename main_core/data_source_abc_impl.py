import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from enum import Enum
from itertools import product
from pathlib import Path
from typing import Any, List

from apscheduler.triggers.calendarinterval import CalendarIntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger

from core.base_graph import BaseGraph
from core.init_scheduler import InitScheduler
from database.db_instancce import DbInstance
from handlers.file_handler import FileHandler
from handlers.http_handler import HttpHandler
from log_manager.logger_manager import LoggerManager
from main_core.data_source_abc import DataSourceABC
from data_config_dtos.data_source_config_dto import DataSourceDTO, SourceFetchModeEnum, SourceMultiFetchStrategy, \
    SourceInputDTO, SourceDTO
from main_core.safe_class import safe_class
from utils.execution_time import format_duration


class FetchTypeEnum(Enum):
    HTTP = "http"
    HTTPS = "https"
    LOCAL = "local"

    @classmethod
    def has_value(cls, value):
        return value in cls._value2member_map_


class TriggerTypeEnum(Enum):
    CRON = "cron"
    DATE = "date"
    INTERVAL = "interval"

    @classmethod
    def has_value(cls, value):
        return value in cls._value2member_map_


@safe_class
class DataSourceABCImpl(DataSourceABC):

    def __init__(self, data_source_conf: DataSourceDTO, db_instance: DbInstance | None, scheduler_core: InitScheduler,
                 base_graph_conf, metadata_service):

        self.metadata_service = metadata_service
        # self.source_result: List | None = None
        self.logger = LoggerManager(type(self).__name__)
        self.logger.info(f"Initializing {type(self).__name__}")
        self.base_graph = BaseGraph(db_instance, base_graph_conf)
        self.data_source_config = data_source_conf
        self.data_source_name = data_source_conf.name
        self.db = db_instance
        self.job_configuration = data_source_conf.job
        self.start_timer = None
        self.end_timer = None
        self.raw_staging_table = None
        self.raw_staging_schema = None

        if scheduler_core is not None:
            self.scheduler = scheduler_core
            self.create_job()
        else:
            self.logger.debug(f"No scheduler found, using default setting")
            self.run()

    def create_data_tables(self):
        if self.data_source_config.storage.persistent and self.db is not None:
            self.logger.info(f"Creating table")
            storage_data = self.data_source_config.storage
            force_create = storage_data.force_create
            if storage_data.staging.table_name:
                self.create_staging_tables(storage_data.staging.table_name, storage_data.staging.table_schema,
                                           force_create)
            if storage_data.enrichment.table_name:
                self.create_enrichment_tables(storage_data.enrichment.table_name, storage_data.enrichment.table_schema,
                                              force_create)
            if self.data_source_config.mapping.table_name and self.data_source_config.mapping.enable:
                self.create_mapping_tables(self.data_source_config.mapping.table_name,
                                           self.data_source_config.mapping.table_schema, force_create)

    def create_staging_tables(self, table_name: str, schema: str, force_create: bool):
        raw_staging_table_name = f"{table_name}_raw_staging"
        self.db.create_table_if_not_exist(table_name, table_schema=schema or None,
                                          force_create=force_create, create_without_indexes=True)
        self.raw_staging_schema, self.raw_staging_table = self.db.clone_table_structure(schema, table_name, schema,
                                                                                        raw_staging_table_name)

    def create_enrichment_tables(self, table_name: str, schema: str, force_create: bool):
        self.db.create_table_if_not_exist(table_name, table_schema=schema or None,
                                          force_create=force_create, create_without_indexes=False)

    def create_mapping_tables(self, table_name: str, schema: str, force_create: bool):
        self.db.create_table_if_not_exist(table_name, table_schema=schema or None,
                                          force_create=force_create, create_without_indexes=False)

    def check_before_update(self) -> bool:
        """
        After the fetch define some criteria to check if the new data is available or not , if not then return otherwise continue with the run method as usual

        """
        # get the data from the raw file that we always save / or from the database metadata

        # if same metadta then let it be otherwise continue with the job

        # found_new_data = True
        # if self.data_source_config.check_before_update:
        #     if self.db is not None and self.data_source_config.storage.persistent:
        #         self.logger.info(f"Checking for changes before update {self.data_source_config.name} ......")
        #         old_data = self.db.fetch_columns_with_limits(self.data_source_config.storage.table_name)
        #         found_new_data = DataSourceABCImpl.check_before_update_condition(old_data, self.source_result)
        # else:
        #     self.logger.warning(f"Check on the file disabled  {self.data_source_config.name}")
        # return found_new_data
        return True

    @staticmethod
    def check_before_update_condition(self, old_data: Any, new_data: Any):
        if old_data is None or new_data is None or (len(old_data) != len(new_data)):
            return True
        else:
            return False

    def fetch(self):
        source = self.data_source_config.source
        paths: list[str] = []
        if source.fetch in (FetchTypeEnum.HTTP.value, FetchTypeEnum.HTTPS.value):
            # check the metadata here if same then no call just file path otherwise new http request
            check = self.is_metadata_for_single_fetch_changed()
            if check:
                http_handler = HttpHandler()
                path = http_handler.call(uri=source.url, destination_path=source.destination, stream=source.stream,
                                         headers=source.headers, params=source.params,
                                         file_extension=source.response_type)
                paths.append(path)
            else:
                paths.append(source.destination)

        elif source.fetch in FetchTypeEnum.LOCAL.value:
            path = Path(source.file_path)
            paths.append(path)
        else:
            self.logger.error(f"Invalid fetch type: {source.fetch}")
            return None
        return paths

    def check_multi_metadata_before_fetch(self, url, headers, params, path) -> bool:
        source = self.data_source_config.source
        current_metadata = HttpHandler().call_remote_metadata(uri=url, headers=headers,
                                                              params=params)

        file_handler = FileHandler(path)
        name = path.split("/")[-1].split(".")
        old_metadata = file_handler.read_metadata('.'.join(name[:-1]))

        if self.is_metadata_changed(old_metadata, current_metadata, source.check_metadata.keys):
            self.logger.info("New UPDATES available for MULTI Metadata checks. Need to fetch new data ...... ")
            return True
        else:
            self.logger.warning(
                f"No new data found for metadata before fetch check. Hence skipping the rest of processing steps ")
            return False

    @staticmethod
    def create_file_name_for_multi_fetch_expand_params(source, param) -> str:
        base, ext = source.destination.rsplit(".", 1)

        # Sort keys for deterministic filename
        parts = []
        for k in sorted(param.keys()):
            v = str(param[k])
            # Replace unsafe characters
            v = re.sub(r"[^\w\-\.]", "_", v)
            parts.append(f"{k}-{v}")

        suffix = "_".join(parts)

        return f"{base}_{suffix}.{ext}"

    def process_multi_fetch_expand_list(self, source, urls) -> list[str]:
        http_handler = HttpHandler()
        paths = []
        self.logger.info(f" no. of urls: {len(urls)}, process starting ......")
        for i, url in enumerate(urls):
            url_name = url.split("/")[-1:]
            path = DataSourceABCImpl.create_file_name_for_multi_fetch_expand_params(source, "_".join(url_name))
            self.logger.info(f" count {i + 1}")
            if self.check_multi_metadata_before_fetch(url=url, headers=source.headers,
                                                      params=source.params, path=path):
                path = http_handler.call(uri=url, destination_path=path, stream=source.stream,
                                         headers=source.headers, params=source.params,
                                         file_extension=source.response_type)
            paths.append(path)
        return paths

    def multi_fetch(self) -> list[str]:
        source = self.data_source_config.source
        multi_fetch = source.multi_fetch
        paths: list[str] = []

        if source.fetch in (FetchTypeEnum.HTTP.value or FetchTypeEnum.HTTPS.value):
            if multi_fetch.enable:
                if not SourceMultiFetchStrategy.has_value(multi_fetch.strategy):
                    self.logger.error(f"Not valid fetch type: {multi_fetch.strategy}")
                    # raise ValueError(f"Invalid fetch type: {multi_fetch.strategy}")
                    return paths
                else:
                    if multi_fetch.strategy == SourceMultiFetchStrategy.EXPAND_PARAMS.value:
                        params = multi_fetch.expand or {}
                        constant_param = multi_fetch.params or {}
                        http_handler = HttpHandler()
                        keys = list(params.keys())
                        values = list(params.values())
                        for combo in product(*values):
                            call_params = dict(zip(keys, combo))
                            param = {**constant_param, **call_params}
                            path = DataSourceABCImpl.create_file_name_for_multi_fetch_expand_params(source, param)
                            if self.check_multi_metadata_before_fetch(url=source.url, headers=source.headers,
                                                                      params=param, path=path):
                                path = http_handler.call(uri=source.url, destination_path=path, stream=source.stream,
                                                         headers=source.headers, params=param,
                                                         file_extension=source.response_type)
                            else:
                                path = f"{path}"
                            #     read from the file
                            paths.append(path)
                        # return paths
                    elif multi_fetch.strategy == SourceMultiFetchStrategy.URL_TEMPLATE.value:
                        template_values = multi_fetch.template_params
                        http_handler = HttpHandler()
                        keys = list(template_values.keys())
                        values = list(template_values.values())
                        length = len(values[0])

                        for i in range(length):
                            params_dict = {key: values[j][i] for j, key in enumerate(keys)}
                            try:
                                url = multi_fetch.url_template.format(**params_dict)
                            except Exception as e:
                                self.logger.error(f"URL template and template urls specified not correct {e} ")
                            path = DataSourceABCImpl.create_file_name_for_multi_fetch_expand_params(source, params_dict)
                            paths.append(path)

                            if self.check_multi_metadata_before_fetch(url=url, headers=source.headers,
                                                                      params=source.params, path=path):
                                http_handler.call(uri=url, destination_path=path, stream=source.stream,
                                                  headers=source.headers, params=source.params,
                                                  file_extension=source.response_type)

                    elif multi_fetch.strategy == SourceMultiFetchStrategy.EXPLICIT_URL_LIST.value:
                        if isinstance(multi_fetch.urls, list):
                            paths = self.process_multi_fetch_expand_list(source, multi_fetch.urls)

                        elif isinstance(multi_fetch.urls, SourceInputDTO):

                            file_handler = FileHandler(multi_fetch.urls.input)
                            print(f"{multi_fetch.urls.input.split("/")[-1]}")
                            urls = file_handler.read_local_file(f"{multi_fetch.urls.input.split("/")[-1]}")
                            paths = self.process_multi_fetch_expand_list(source, urls)

        elif source.fetch in FetchTypeEnum.LOCAL.value:
            if multi_fetch.enable:
                if not SourceMultiFetchStrategy.has_value(multi_fetch.strategy):
                    self.logger.error(f"Not valid fetch type: {multi_fetch.strategy}")
                    raise ValueError(f"Invalid fetch type: {multi_fetch.strategy}")
                else:
                    if multi_fetch.strategy == SourceMultiFetchStrategy.EXPAND_PARAMS.value:
                        # TODO: DO we need for normal source multi fetch as the file reading has no attributes
                        paths.append(source.file_path)
                    elif multi_fetch.strategy == SourceMultiFetchStrategy.URL_TEMPLATE.value:
                        template_values = multi_fetch.template_params
                        keys = list(template_values.keys())
                        values = list(template_values.values())
                        length = len(values[0])

                        for i in range(length):
                            params_dict = {key: values[j][i] for j, key in enumerate(keys)}
                            path = multi_fetch.url_template.format(**params_dict)
                            paths.append(path)

                    elif multi_fetch.strategy == SourceMultiFetchStrategy.EXPLICIT_URL_LIST.value:
                        if isinstance(multi_fetch.urls, list):

                            for url in multi_fetch.urls:
                                paths.append(url)

        else:
            self.logger.error(f"Not valid multi fetch type strategy: {multi_fetch.strategy}")
        return paths

    def read_file_content(self, path):
        return NotImplemented

    def read_files(self, path: Path | str) -> list[dict]:
        result = []
        try:
            path = Path(path)
            file_handler = FileHandler(path.parent)
            res = file_handler.read_local_file(path.name, self.read_file_content)
            if isinstance(res, list):
                result.extend(res)
            elif isinstance(res, dict):
                result.append(res)
            elif isinstance(res, str):
                result.append(res)
            else:
                self.logger.error(
                    f"File {path} not readable or the format specifies by read_file_content not correct")
        except Exception as e:
            self.logger.error(f"Error occurred while reading the files {e}")

        return result

    def is_new_data_available_in_multi_fetch(self, source, url, path, headers, params) -> bool:
        return (not source.check_metadata.enable) and self.check_multi_metadata_before_fetch(url=url,
                                                                                             headers=headers,
                                                                                             params=params,
                                                                                             path=path)

    def source(self, source: SourceDTO) -> List[Any] | None:
        source = self.data_source_config.source
        if source is None:
            return None

        if not FetchTypeEnum.has_value(source.fetch):
            self.logger.error(f"Not valid fetch type: {source.fetch}")
            self.logger.error(
                f"Invalid fetch type '{source.fetch}'. Expected one of: {[e.value for e in FetchTypeEnum]}")
            return None
        if not SourceFetchModeEnum.has_value(source.mode):
            self.logger.error(f"Not valid fetch type: {source.fetch}")
            self.logger.error(
                f"Invalid fetch type '{source.mode}'. Expected one of: {[e.value for e in SourceFetchModeEnum]}")
            return None
        else:
            if source.mode == SourceFetchModeEnum.SINGLE.value:
                if source.check_metadata.enable:
                    return self.fetch()
            elif source.mode == SourceFetchModeEnum.MULTI.value:
                return self.multi_fetch()

        return None

    def is_metadata_for_single_fetch_changed(self) -> bool:

        source = self.data_source_config.source
        current_metadata = HttpHandler().call_remote_metadata(uri=source.url, headers=source.headers,
                                                              params=source.params)
        # read a file from last meta output
        file_handler = FileHandler(source.destination)
        name = source.destination.split("/")[-1].split(".")
        old_metadata = file_handler.read_metadata('.'.join(name[:-1]))
        if self.is_metadata_changed(old_metadata, current_metadata, source.check_metadata.keys):
            self.logger.info("New UPDATES available for Metadata checks. Fetching new DATA ...... ")
            return True
        else:
            self.logger.warning(
                f"No new data found for metadata before fetch check. Hence skipping the rest of processing steps ")
            return False

    def is_metadata_changed(self, old_metadata, current_metadata, keys: list[str]) -> bool:
        if old_metadata is None or current_metadata is None:
            # Metadata changed
            return True
        else:
            for key in keys:
                if old_metadata.get(key) != current_metadata.get(key):
                    return True
        return False

    def source_filter(self, data: list[Any]) -> List[Any]:
        """Default filter: returns data unchanged.
            data : the fetch data which needs to be processed after the fetch request
        """
        return data

    def start_execution(self):
        self.logger.info(f"Executing starting for datasource {self.data_source_config.name}")
        self.start_timer = time.perf_counter()

    def extract(self):
        paths = self.source(self.data_source_config.source)
        self.logger.info(f"Total number of paths found {len(paths)}")

        return paths

    @staticmethod
    def is_file_available(path: list) -> bool:
        if path is None or len(path) == 0:
            return False
        return True

    def transform(self, path):
        result = self.read_files(path)
        self.logger.info(f"result contains currently {len(result)}")

        # self.source_result = result
        # print(self.source_result)
        self.pre_filter_processing(result)
        # 1.1 filter from the results in case needs to be filtered
        result = self.source_filter(result)

        # 1.2 post processing filter
        self.post_filter_processing(result)
        return result

    def load(self, data):
        db_storage = self.data_source_config.storage
        try:
            if not db_storage.persistent:
                self.logger.warning(
                    f"data source {self.data_source_name} persistent is set to false. Hence it won't be saved to the database ")

            else:
                if self.db is not None:
                    self.logger.warning("found new data hence continuing with db upsert")
                    self.pre_database_processing()
                    self.db.bulk_insert(self.raw_staging_table, self.raw_staging_schema
                                        , data, True)


        except Exception as e:
            self.logger.error(f"Error occurred while loading the file into Database: {e}")

    def process_file(self, path: str):
        """
        One-file ETL unit
        """

        thread = threading.current_thread()
        thread_id = threading.get_ident()
        start = time.monotonic()
        self.logger.info(
            f"[THREAD START] name={thread.name} id={thread_id} file={path}"
        )

        self.logger.info(f"Processing file {path}")

        try:
            t0 = time.monotonic()
            transformed_data = self.transform(path)
            self.logger.info(
                f"[THREAD TRANSFORM DONE] name={thread.name} "
                f"rows={len(transformed_data) if transformed_data else 0} "
                f"time={time.monotonic() - t0:.2f}s"
            )

            if not transformed_data:
                self.logger.info(
                    f"[THREAD SKIP] name={thread.name} no data"
                )
                return

            t1 = time.monotonic()
            self.load(transformed_data)
            self.logger.info(
                f"[THREAD LOAD DONE] name={thread.name} "
                f"time={time.monotonic() - t1:.2f}s"
            )

        except Exception as e:
            self.logger.error(
                f"[THREAD ERROR] name={thread.name} file={path}"
            )
            raise
        finally:
            self.logger.info(
                f"[THREAD END] name={thread.name} "
                f"total_time={time.monotonic() - start:.2f}s"
            )

        # transformed_data = self.transform(path)
        #
        # if not transformed_data:
        #     self.logger.info(f"No data after transform for {path}")
        #     return
        #
        # self.load(transformed_data)

    def run(self):
        self.start_execution()

        try:
            # 1 Extract
            paths = self.extract()
            # # Testing scalability with small dataset from elevation  # TODO: Remove later
            # paths = ['tmp/elevation_zips/data_elevation_DGM1_368_5808.zip_2025-12-18T16-15-40.zip',
            #           'tmp/elevation_zips/data_elevation_DGM1_370_5806.zip_2025-12-18T16-15-41.zip']

            if not DataSourceABCImpl.is_file_available(paths):
                return self.run_job_response("No files available")
            # Create tables if not exist

            self.create_data_tables()
            max_workers = min(1, os.cpu_count() * 2)
            self.logger.critical(f"Starting with {max_workers} workers")
            with ThreadPoolExecutor(
                    max_workers=max_workers,
                    thread_name_prefix="ETLWorker"
            ) as executor:
                futures = [
                    executor.submit(self.process_file, path)
                    for path in paths
                ]

                for future in as_completed(futures):
                    # surfaces exceptions immediately
                    future.result()

            # for i, path in enumerate(paths):
            #     self.logger.info(f"Reading file {i + 1} -> {path}")
            #     transformed_data = self.transform(path)
            #
            #     if self.check_before_update():
            #         self.load(transformed_data)
            #
            #     else:
            #         self.logger.warning(f"No new data available for {self.data_source_config.name}")
            #         return self.run_job_response(f"No new data available for {self.data_source_config.name}")
            # add indexes for the newly formed table for faster inserts and transactions

            self.post_database_processing()

            res = self.sync_raw_to_staging()

            self.execute_on_staging()

            self.sync_staging_to_enrichment()
            self.execute_on_enrichment()

            self.map_to_base()
            # delete fully if the staging sync is successful
            self.clean_raw_staging_table(not res.get("success"))
            self.recreate_table_indexes()
        except Exception as e:
            self.logger.error(f"Error occurred in run {e}")

        return self.run_job_response("Job finished Successfully !!!")

    def sync_staging_to_enrichment(self):
        self.db.sync_staging_to_enrichment(self.data_source_config.storage.staging.table_schema,
                                           self.data_source_config.storage.staging.table_name,
                                           self.data_source_config.storage.enrichment.table_schema,
                                           self.data_source_config.storage.enrichment.table_name
                                           )

    def sync_raw_to_staging(self)  :
        return self.db.sync_source_to_target_table(self.raw_staging_schema, self.raw_staging_table
                                            , self.data_source_config.storage.staging.table_schema,
                                            self.data_source_config.storage.staging.table_name)


    def clean_raw_staging_table(self, backup: bool):
        self.db.drop_table(self.raw_staging_table, self.raw_staging_schema, backup, True, True)

    def recreate_table_indexes(self):
        if self.db is not None and self.data_source_config.storage.persistent:
            if self.data_source_config.storage.enrichment.table_name:
                self.db.create_indexes(self.data_source_config.storage.enrichment.table_name,
                                       self.data_source_config.storage.enrichment.table_schema)
            if self.data_source_config.storage.staging.table_name:
                self.db.create_indexes(self.data_source_config.storage.staging.table_name,
                                       self.data_source_config.storage.staging.table_schema)
            if self.data_source_config.mapping.table_name and self.data_source_config.mapping.enable:
                self.db.create_indexes(self.data_source_config.mapping.table_name,
                                       self.data_source_config.mapping.table_schema)

    def post_filter_processing_save_data(self, conf, data):
        file_handler = FileHandler(conf.destination)
        file_handler.save_data(conf.destination, data, True)

    def post_filter_processing(self, data):
        if self.data_source_config.post_filter_processing is not None and self.data_source_config.post_filter_processing.save:
            conf = self.data_source_config.post_filter_processing
            if conf is not None and conf.save:
                self.post_filter_processing_save_data(conf,data)

    def run_job_response(self, message: str):
        end_timer = time.perf_counter()
        duration = end_timer - self.start_timer
        formatted_duration = format_duration(duration)

        self.logger.info(
            f"Finished run for {self.data_source_config.name} in {formatted_duration} seconds -> message: {message}"
        )

        return {"message": message, "duration": formatted_duration}

    def execute_query(self, table_key: str, query: str | None, params= None):
        if query is not None:
            # self.logger.info(f"calling the query for {table_key} -->, {query}")
            self.db.call_sql(query, params)
        else:
            if table_key.lower() == "mapping":
                self.logger.info(
                    "No mapping Query given. Please write a postgresql query in the respective mapper class. Implement "
                    "func map_to_link_db_query")

    def map_to_links(self):
        query = self.mapping_db_query()
        self.execute_query("Mapping", query)

    def mapping_db_query(self) -> None | str:
        sql_query = None
        return sql_query

    def execute_on_staging(self):
        query = self.staging_db_query()
        self.execute_query("Staging", query)

    def staging_db_query(self) -> None | str:
        sql_query = None
        return sql_query

    def execute_on_enrichment(self):
        query = self.enrichment_db_query()
        self.execute_query("Enrichment", query)

    def enrichment_db_query(self) -> None | str:
        sql_query = None
        return sql_query

    def pre_filter_processing(self, data):
        pass

    def pre_database_processing(self):
        pass

    def post_database_processing(self):

        pass

    def map_to_base(self):
        if self.data_source_config.mapping.enable:
            try:
                if self.db is not None:
                    self.logger.info(f"Mapping started on Mapping Table.....")
                    total_ways_count = self.base_graph.get_base_graph_row_counts()
                    mapped_ways_count = self.db.get_table_count(self.data_source_config.mapping.table_name,
                                                                self.data_source_config.mapping.table_schema)
                    if mapped_ways_count != total_ways_count:
                        self.map_to_links()
                    else:
                        self.logger.info(f"Skipping mapping as all ways geometry mapped....")
            except Exception as e:
                self.logger.error(f"Error occurred during base table update {e}")

    def create_job(self):
        self.logger.info(f"Job creation started for {self.job_configuration.name}")

        trigger_conf = self.job_configuration.trigger.type

        TRIGGER_MAP = {
            "interval": IntervalTrigger,
            "date": DateTrigger,
            "cron": CronTrigger,
            "calendar_interval": CalendarIntervalTrigger,
            "run_once": DateTrigger
        }

        trigger_cls = TRIGGER_MAP[trigger_conf.name]
        if trigger_conf.name == "run_once":
            trigger = trigger_cls(run_date=datetime.now())
        else:
            if trigger_conf.start_date is not None:
                if trigger_conf.name != "date":
                    trigger = trigger_cls(**trigger_conf.config, start_date=trigger_conf.start_date)
                else:
                    trigger = trigger_cls(run_date=trigger_conf.start_date)
            else:
                trigger = trigger_cls(**trigger_conf.config)

        job_conf = {
            "func": self.run,
            "trigger": trigger,
            "name": self.job_configuration.name,
            "replace_existing": self.job_configuration.replace_existing,
            "executor": "process" if self.job_configuration.executor is not None else "default"
        }
        self.scheduler.add_job(job_conf, self.job_configuration.id or self.data_source_name)
