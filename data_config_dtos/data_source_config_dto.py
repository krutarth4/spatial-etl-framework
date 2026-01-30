from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Union, Optional, Mapping, Any, List


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


class MappingStrategyDTO(str, Enum):
    EXPAND_PARAMS = "expand_params"
    URL_TEMPLATE = "url_template"
    EXPLICIT_URL_LIST = "explicit_url_list"

    @classmethod
    def has_value(cls, value):
        return value in cls._value2member_map_


@dataclass
class StagingConfDTO:
    table_name: str
    table_schema: str
    table_class: str
    persistent: Optional[bool] = False


@dataclass
class EnrichmentConfDTO:
    table_name: str
    table_schema: str
    table_class: str
    persistent: Optional[bool] = False


@dataclass
class StorageDTO:
    enrichment: EnrichmentConfDTO
    persistent: bool
    staging: StagingConfDTO
    expires_after: Optional[str]
    force_create: Optional[bool] = False


@dataclass
class BaseGraphDTO:
    table_name: str
    table_schema: str
    force_generate: Optional[bool]


@dataclass
class BaseDataMappingSourceDTO:
    table_name: str
    table_schema: str
    column_name: str
    column_type: str


@dataclass
class MappingDTO:
    joins_on: str
    strategy: Optional[MappingStrategyDTO]
    table_name: str
    table_schema: str
    enable: bool
    base_table: BaseDataMappingSourceDTO


@dataclass
class CheckMetadataDTO:
    enable: bool
    keys: Optional[List[str]]


@dataclass
class SourceInputDTO:
    path: Optional[Union[str, Path]]
    data: Optional[list[Any]]


@dataclass
class SourceInputDTO:
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
    urls: Optional[Union[list[str]]] | Optional[SourceInputDTO]


@dataclass
class SourceDTO:
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


@dataclass
class PostFilterDTO:
    save: bool
    destination: Union[str, Path]


@dataclass
class DataSourceDTO:
    name: str
    description: str
    enable: bool
    class_name: str
    data_type: str
    source: SourceDTO
    pre_filter_processing: Optional[PostFilterDTO]
    post_filter_processing: Optional[PostFilterDTO]
    pre_database_processing: Optional[PostFilterDTO]
    pro_database_processing: Optional[PostFilterDTO]
    cleanup_processing: Optional[PostFilterDTO]
    mapping: MappingDTO
    storage: StorageDTO
    job: JobConfigurationDTO


@dataclass
class GraphConfDTO:
    tool: str
    schema: str
    table_name: str
    enable: bool
    osm_file_path: str
    cmd: Optional[List[str | Any]]
    env: Mapping[str, str]
    datasource: List[DataSourceDTO]


@dataclass
class MetadataConfDTO:
    description: Optional[str]
    table_schema: str
