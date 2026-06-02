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
    CUSTOM = "custom"
    SQL_TEMPLATE = "sql_template"
    NONE = "none"
    NEAREST_NEIGHBOUR = "nearest_neighbour"
    WITHIN_DISTANCE = "within_distance"
    INTERSECTION = "intersection"
    KNN = "knn"
    NEAREST_STATION = "nearest_station"
    MAPPER_SQL = "mapper_sql"
    NEAREST_K = "nearest_k"
    K_NEAREST = "k_nearest"
    KNN_MULTIPLE = "knn_multiple"
    AGGREGATE_WITHIN_DISTANCE = "aggregate_within_distance"
    BUFFER_AGGREGATE = "buffer_aggregate"
    AGGREGATE_BUFFER = "aggregate_buffer"
    ATTRIBUTE_JOIN = "attribute_join"
    ID_JOIN = "id_join"
    KEY_JOIN = "key_join"

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
    enrichment: Optional[EnrichmentConfDTO]
    persistent: bool
    staging: Optional[StagingConfDTO]
    expires_after: Optional[str] = None
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
class MappingStrategyLinkDTO:
    mapping_column: Optional[str] = None
    base_column: Optional[str] = None
    basis: Optional[str] = None


@dataclass
class MappingStrategyConfigDTO:
    type: Optional[str] = None
    description: Optional[str] = None
    link_on: Optional[MappingStrategyLinkDTO] = None


@dataclass
class MappingDTO:
    joins_on: Optional[str]
    strategy: Optional[Union[MappingStrategyDTO, str, MappingStrategyConfigDTO]]
    table_name: Optional[str]
    table_schema: Optional[str]
    enable: bool
    base_table: BaseDataMappingSourceDTO
    config: Optional[Mapping[str, Any]] = None
    # When true, the mapping step runs only for ways present in the
    # ways_base_changes diff table instead of full-rescanning ways_base.
    incremental: bool = False


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
    fetch_workers: Optional[int] = 8
    request_timeout: Optional[int] = 120
    retry_attempts: Optional[int] = 3
    retry_backoff: Optional[float] = 1.0
    inter_request_delay: Optional[float] = 0.0
    fail_fast: Optional[bool] = False


@dataclass
class ReaderConfigDTO:
    engine: Optional[str] = None       # geopandas engine: "pyogrio" or "fiona"
    target_crs: Optional[int] = None   # EPSG code to reproject spatial data to, e.g. 25833


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
    reader: Optional[ReaderConfigDTO] = None


@dataclass
class HookConfigDTO:
    save: bool = False
    destination: Optional[Union[str, Path]] = None


@dataclass
class DataSourceDTO:
    name: str
    description: str
    enable: bool
    class_name: str
    data_type: str
    source: SourceDTO
    before_filter_hook: Optional[HookConfigDTO]
    after_filter_hook: Optional[HookConfigDTO]
    before_load_hook: Optional[HookConfigDTO]
    after_load_hook: Optional[HookConfigDTO]
    cleanup_hook: Optional[HookConfigDTO]
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
    coupled: Optional[Union[str, bool]] = None
    coupled_task_key: Optional[str] = None
    coupled_poll_seconds: Optional[float] = 5.0
    coupled_timeout_seconds: Optional[float] = None
    communication: Optional[Mapping[str, Any]] = None


@dataclass
class MetadataConfDTO:
    description: Optional[str]
    table_schema: str
