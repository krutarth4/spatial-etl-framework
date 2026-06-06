"""Lightweight adapters/value-objects used by the debug mapper service.

Extracted verbatim from core/debug_mapper_service.py.
"""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from uuid import UUID
from types import SimpleNamespace

from sqlalchemy import select
from sqlalchemy.sql import text

from database.db_instance import DbInstance
from metadata.data_source_metadata_repository import DataSourceMetadataRepository


class _StorageRef:
    """Duck-typed storage reference for mapping SQL builder."""
    def __init__(self, d: dict | None):
        self.table_name = (d or {}).get("table_name")
        self.table_schema = (d or {}).get("table_schema")

class _StorageConf:
    """Duck-typed storage configuration."""
    def __init__(self, storage: dict):
        enr = storage.get("enrichment")
        stg = storage.get("staging")
        self.enrichment = _StorageRef(enr) if enr else None
        self.staging = _StorageRef(stg) if stg else None

class _MappingConf:
    """Duck-typed mapping configuration."""
    def __init__(self, mapping: dict):
        base = mapping.get("base_table") or {}
        self.table_name = mapping.get("table_name")
        self.table_schema = mapping.get("table_schema")
        self.joins_on = mapping.get("joins_on")
        self.strategy = mapping.get("strategy")
        self.config = mapping.get("config") or {}
        self.base_table = _StorageRef(base)

class _DataSourceConfigAdapter:
    """Wraps raw datasource dict to provide interface expected by mapping SQL strategies."""
    def __init__(self, ds: dict):
        self.data_source_name = ds.get("name")
        self.data_source_config = SimpleNamespace(
            mapping=_MappingConf(ds.get("mapping") or {}),
            storage=_StorageConf(ds.get("storage") or {}),
        )

    def get_mapping_strategy_type(self) -> str | None:
        """Extract strategy type from mapping config."""
        strategy = self.data_source_config.mapping.strategy
        if strategy is None:
            return None
        if isinstance(strategy, str):
            return strategy
        if isinstance(strategy, dict):
            return str(strategy.get("type") or strategy.get("name") or "")
        return None

    def get_mapping_strategy_link_fields(self) -> dict:
        """Extract link field configuration."""
        mapping_conf = self.data_source_config.mapping
        joins_on = mapping_conf.joins_on
        strategy = mapping_conf.strategy
        link_on = strategy.get("link_on") if isinstance(strategy, dict) else None
        mapping_column = (link_on or {}).get("mapping_column") if isinstance(link_on, dict) else None
        base_column = (link_on or {}).get("base_column") if isinstance(link_on, dict) else None
        basis = (link_on or {}).get("basis") if isinstance(link_on, dict) else None
        return {
            "mapping_column": str(mapping_column) if mapping_column else (str(joins_on) if joins_on else None),
            "base_column": str(base_column) if base_column else None,
            "basis": str(basis) if basis else None,
        }

    def get_mapping_config(self) -> dict:
        """Get mapping config dict."""
        config = self.data_source_config.mapping.config
        return config if isinstance(config, dict) else {}

    def _get_insert_spec(self):
        """Extract insert specification from mapping config."""
        from main_core.mapping_sql_builder import MappingInsertSpec

        insert_conf = self.get_mapping_config().get("insert")
        if not isinstance(insert_conf, dict):
            return None

        columns = insert_conf.get("columns") or []
        conflict_columns = insert_conf.get("conflict_columns")
        update_columns = insert_conf.get("update_columns")

        return MappingInsertSpec(
            columns=[str(column) for column in columns],
            conflict_columns=[str(column) for column in conflict_columns] if conflict_columns else None,
            update_columns=[str(column) for column in update_columns] if update_columns else None,
        )

class TableTarget:
    STAGING = "staging"
    ENRICHMENT = "enrichment"
    MAPPING = "mapping"

    @classmethod
    def values(cls) -> set[str]:
        return {cls.STAGING, cls.ENRICHMENT, cls.MAPPING}
