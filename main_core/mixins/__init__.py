"""Mixin classes that together compose DataSourceABCImpl.

Each mixin owns one cohesive slice of the pipeline:

  TimingReportingMixin     — stage timing accumulators and CSV / ASCII reports
  MetadataTrackingMixin    — run-lifecycle metadata (start / finish / paths)
  LifecycleHooksMixin      — overridable hooks (load, filters, before/after file)
  StateMixin               — __init__, run-state reset, health-check predicates
  TableManagementMixin     — create / clone / index ETL tables
  FetchMixin               — HTTP / local fetch, multi-fetch, metadata comparison
  FileReadTransformMixin   — read_file_content, format auto-detection, transform
  EtlOperationsMixin       — sync, execute_on_staging/enrichment, map_to_base
  RunPipelineMixin         — run, execute, thread-pool file processing, create_job
"""
