"""EtlOperationsMixin — thin combiner over the focused ETL stage mixins.

The original ~500-line mixin has been split into one mixin per ETL stage plus a
shared query/batching helper.  EtlOperationsMixin now only composes them so that
existing imports keep working unchanged:

    from main_core.mixins.etl_operations_mixin import EtlOperationsMixin

Stage responsibilities:
  EtlStagingMixin      — staging_db_query, execute_on_staging, sync_raw_to_staging
  EtlEnrichmentMixin   — enrichment_db_query, execute_on_enrichment, operators, sync
  EtlMappingMixin      — map_to_base (full-rescan / incremental), strategy builder
  EtlQueryMixin        — execute_query, batching helpers (shared by the above)

Each stage mixin is individually @safe_class-decorated, so the wrapping behaviour
is identical to the previous single-class form.
"""
from main_core.mixins.etl_enrichment_mixin import EtlEnrichmentMixin
from main_core.mixins.etl_mapping_mixin import EtlMappingMixin
from main_core.mixins.etl_query_mixin import EtlQueryMixin
from main_core.mixins.etl_staging_mixin import EtlStagingMixin


class EtlOperationsMixin(
    EtlStagingMixin,
    EtlEnrichmentMixin,
    EtlMappingMixin,
    EtlQueryMixin,
):
    """Implements the staging → enrichment → mapping ETL operations."""
