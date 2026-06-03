"""DataSourceABCImpl — thin combiner that composes all pipeline mixins.

The 1797-line monolith has been split into focused mixin classes under
main_core/mixins/.  This module now only:
  1. Re-exports the public enums that other code may import from here
  2. Defines the combiner class that inherits from all mixins

Every mapper subclass still does exactly:
    from main_core.data_source_abc_impl import DataSourceABCImpl
    class MyMapper(DataSourceABCImpl): ...

The MRO (left-to-right mixin order) ensures mapper overrides are always
found first and that all abstract methods from DataSourceABC are satisfied.

Mixin responsibilities:
  StateMixin               — __init__, run-state reset, health-check predicates
  FetchMixin               — HTTP / local fetch, multi-fetch, metadata checks
  FileReadTransformMixin   — read_file_content, format auto-detection, transform
  TableManagementMixin     — create/clone/index ETL tables
  EtlOperationsMixin       — staging → enrichment → mapping operations
  RunPipelineMixin         — run, execute, thread-pool file processing, create_job
  MetadataTrackingMixin    — run-lifecycle metadata persistence
  TimingReportingMixin     — stage timing and ASCII/CSV reports
  LifecycleHooksMixin      — overridable hooks (load, filters, before/after file)
  DataSourceABC            — abstract contracts (unchanged)
"""

# Re-export enums so existing imports like:
#   from main_core.data_source_abc_impl import FetchTypeEnum
# continue to work without change.
from main_core.mixins.fetch_mixin import FetchTypeEnum  # noqa: F401
from main_core.mixins.run_pipeline_mixin import TriggerTypeEnum  # noqa: F401

from main_core.data_source_abc import DataSourceABC
from main_core.mixins.etl_operations_mixin import EtlOperationsMixin
from main_core.mixins.fetch_mixin import FetchMixin
from main_core.mixins.file_read_transform_mixin import FileReadTransformMixin
from main_core.mixins.lifecycle_hooks_mixin import LifecycleHooksMixin
from main_core.mixins.metadata_tracking_mixin import MetadataTrackingMixin
from main_core.mixins.run_pipeline_mixin import RunPipelineMixin
from main_core.mixins.state_mixin import StateMixin
from main_core.mixins.table_management_mixin import TableManagementMixin
from main_core.mixins.timing_reporting_mixin import TimingReportingMixin


class DataSourceABCImpl(
    StateMixin,
    FetchMixin,
    FileReadTransformMixin,
    TableManagementMixin,
    EtlOperationsMixin,
    RunPipelineMixin,
    MetadataTrackingMixin,
    TimingReportingMixin,
    LifecycleHooksMixin,
    DataSourceABC,
):
    """Composed datasource implementation.

    Inherits all behaviour from the mixins above.  Mapper subclasses extend
    this class and override only the methods they need (read_file_content,
    source_filter, pre_filter_processing, post_database_processing, load,
    mapping_db_query, enrichment_db_query, staging_db_query, etc.).

    See main_core/mixins/ for full documentation of each method.
    """
