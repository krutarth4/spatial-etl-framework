import importlib
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Union, Optional, Mapping, Any, List

from dacite import from_dict, Config

from log_manager.logger_manager import LoggerManager


@dataclass
class TriggerTypeDTO:
    name: str
    start_date: Optional[datetime]
    end_date: Optional[datetime]
    config: Optional[Mapping[str, Any]]


@dataclass
class JobTriggerDTO:
    type: TriggerTypeDTO


@dataclass
class JobConfigurationDTO:
    name: str
    id: str
    executor: Optional[str]
    trigger: JobTriggerDTO
    replace_existing: bool
    coalesce: bool
    max_instances: int
    next_run_time: str


class SourceFetchModeEnum(str, Enum):
    SINGLE = "single"
    MULTI = "multi"

    @classmethod
    def has_value(cls, value):
        return value in cls._value2member_map_


class SourceMultiFetchStrategy(str, Enum):
    EXPAND_PARAMS = "expand_params"
    URL_TEMPLATE = "url_template"
    EXPLICIT_URL_LIST = "explicit_url_list"

    @classmethod
    def has_value(cls, value):
        return value in cls._value2member_map_


@dataclass
class StorageDTO:
    table_name: Optional[str]
    table_schema: Optional[str]
    table_class: Optional[str]
    persistent: bool
    expires_after: Optional[str]


@dataclass
class BaseDataMappingSourceDTO:
    table_name: str
    table_schema: str
    column_name: str
    column_type: str


@dataclass
class MappingDTO:
    joins_on: str
    enable: bool
    base_table: BaseDataMappingSourceDTO
    # mapping: Mapping[str, Any]


@dataclass
class CheckMetadataDTO:
    enable: bool
    keys: Optional[List[str]]


@dataclass
class SourceInputDTO:
    path: Optional[Union[str, Path]]
    data: Optional[list[Any]]


@dataclass
class SourceInptuDTO:
    input: Union[str, Path]
    type: Optional[str]


@dataclass
class SourceMultiFetchDTO:
    enable: bool
    strategy: Union[SourceMultiFetchStrategy, str]
    params: Optional[Mapping[str, Any]]
    expand: Optional[Mapping[str, Any]]  # for looping over the params
    url_template: Optional[str]
    template_params: Optional[Mapping[str, Any]]
    urls: Optional[Union[list[str]]] | Optional[SourceInptuDTO]


@dataclass
class SourceDTO:
    # input: Optional[str]
    check_metadata: CheckMetadataDTO
    url: Optional[str]
    file_path: Optional[Union[str, Path]]
    fetch: str
    stream: Optional[bool]
    save_local: Optional[bool]
    destination: Optional[Union[str, Path]]
    response_type: Optional[str]
    headers: Optional[Mapping[str, Any]]
    params: Optional[Mapping[str, Any]]
    mode: str | SourceFetchModeEnum
    multi_fetch: Optional[SourceMultiFetchDTO]


#     ----------for multi fetch
#      expand:
#       dwd_station_id: ["00399", "00403", "00410"]
#
#     # Strategy 2: URL template (optional)
#     url_template: "https://example.com/{station}/details"
#     template_params:
#       station: ["A", "B", "C"]
#
#     # Strategy 3: explicit URLs
#     urls:
#       - "https://example.com/page/1"
#       - "https://example.com/page/2"


@dataclass()
class PostFilterDTO:
    save: bool
    destination: Union[str, Path]
    # type: str


@dataclass
class DataSourceDTO:
    name: str
    description: str
    enable: bool
    table_name: str
    class_name: str
    data_type: str
    check_before_update: bool
    source: SourceDTO
    pre_filter_processing: Optional[PostFilterDTO]
    post_filter_processing: Optional[PostFilterDTO]
    pre_database_processing: Optional[PostFilterDTO]
    pro_database_processing: Optional[PostFilterDTO]
    cleanup_processing: Optional[PostFilterDTO]
    mapping: MappingDTO
    storage: StorageDTO
    job: JobConfigurationDTO


class DataSourceMapper:
    _prefix_path = "data_mappers"

    def __init__(self, sources, db_instance, scheduler_core):
        self.logger = LoggerManager(type(self).__name__).get_logger()
        self.db_instance = db_instance
        self.scheduler_core = scheduler_core
        self.data_sources = sources
        self.logger.info(f"Found {len(self.data_sources)} data sources")
        self.data_sources = self.check_enable_data_sources()
        self.logger.info(f"Enable Found {len(self.data_sources)} data sources")
        self.run_data_source_mapper()

    def check_enable_data_sources(self):
        try:
            result = []
            for source in self.data_sources:
                if isinstance(source, dict):
                    data = from_dict(DataSourceDTO, data=source, config=Config(cast=[dict]))
                    if data.enable:
                        result.append(data)
            return result
        except Exception as e:
            self.logger.error(f"Error loading data sources for {source.get("name")} {e}", e)

    def run_data_source_mapper(self):
        for source in self.data_sources:
            data = source
            class_name = data.class_name

            try:
                module_path = f"{self._prefix_path}.{class_name}Mapper"
                module = importlib.import_module(module_path)
                mapper_class = getattr(module, f"{class_name[0].upper() + class_name[1:]}Mapper")
                #  TODO directly link the table class also present in the mapper class
                # classes = [
                #     member
                #     for name, member in inspect.getmembers(module, inspect.isclass)
                #     if member.__module__ == module.__name__ # only classes defined in this module
                #     and issubclass(member,Base)
                # ]
                # print(classes)

                instance_data_source = mapper_class(data, self.db_instance, self.scheduler_core)
                self.logger.info(f"execution finished for the {mapper_class.__name__}")
                # instance_data_source.run()
            except Exception as e:
                self.logger.error(f"Error loading data source {class_name} :{e}", e)


if __name__ == "__main__":
    dsm = DataSourceMapper("weather", None, None)
