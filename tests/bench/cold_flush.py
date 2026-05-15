"""
Flush all DB tables associated with a datasource before a cold benchmark run.

Reads table coordinates from the DataSourceDTO (staging, enrichment, mapping) and
from the mapper instance's raw_staging attributes (set by create_staging_tables).
Uses TRUNCATE … CASCADE so foreign-key dependents are also cleared.
Silent on missing tables — the first-ever cold run may find tables that don't exist yet.
"""
from __future__ import annotations


def flush_tables(db, dto, mapper_instance=None) -> list[str]:
    """
    TRUNCATE all staging / enrichment / mapping / raw_staging tables for dto.

    Args:
        db:              DbInstance — live PostGIS connection
        dto:             DataSourceDTO — carries table names
        mapper_instance: optional; if provided, raw_staging coords are read from it

    Returns:
        List of "schema.table" strings that were successfully truncated.
    """
    targets: list[tuple[str, str]] = []

    storage = dto.storage
    if storage and storage.staging:
        targets.append((storage.staging.table_schema, storage.staging.table_name))
    if storage and storage.enrichment:
        targets.append((storage.enrichment.table_schema, storage.enrichment.table_name))

    mapping = getattr(dto, "mapping", None)
    if mapping and getattr(mapping, "enable", False) and getattr(mapping, "table_name", None):
        schema = getattr(mapping, "table_schema", None) or (
            storage.staging.table_schema if storage and storage.staging else None
        )
        if schema:
            targets.append((schema, mapping.table_name))

    if mapper_instance is not None:
        raw_schema = getattr(mapper_instance, "raw_staging_schema", None)
        raw_table = getattr(mapper_instance, "raw_staging_table", None)
        if raw_schema and raw_table:
            targets.append((raw_schema, raw_table))

    truncated: list[str] = []
    for schema, table in targets:
        if not schema or not table:
            continue
        try:
            db.call_sql(f'TRUNCATE TABLE "{schema}"."{table}" CASCADE')
            truncated.append(f"{schema}.{table}")
        except Exception:
            pass
    return truncated
