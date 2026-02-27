from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from log_manager.logger_manager import LoggerManager

if TYPE_CHECKING:
    from main_core.data_source_abc_impl import DataSourceABCImpl


class MappingStrategy(Protocol):
    name: str

    def execute(self, datasource: "DataSourceABCImpl") -> None:
        ...


@dataclass
class MappingStrategyContext:
    datasource: "DataSourceABCImpl"


class NoopMappingStrategy:
    name = "none"

    def execute(self, datasource: "DataSourceABCImpl") -> None:
        datasource.logger.info("Mapping strategy 'none': skipping mapping step")


class MapperSqlMethodStrategy:
    """
    Default legacy-compatible strategy: reuse mapper-defined `mapping_db_query()`.
    """

    name = "mapper_sql"

    def execute(self, datasource: "DataSourceABCImpl") -> None:
        datasource.map_to_links()


class SqlTemplateMappingStrategy:
    name = "sql_template"

    def execute(self, datasource: "DataSourceABCImpl") -> None:
        mapping_conf = getattr(datasource.data_source_config, "mapping", None)
        config = getattr(mapping_conf, "config", None) or {}
        sql = config.get("sql")
        if not sql:
            raise ValueError(
                f"Mapping strategy 'sql_template' requires mapping.config.sql "
                f"for datasource {datasource.data_source_name}"
            )

        sql = self._render_sql(sql, datasource)
        datasource.execute_query("Mapping", sql)

    def _render_sql(self, sql: str, datasource: "DataSourceABCImpl") -> str:
        """
        Best-effort placeholder formatting. If SQL contains no placeholders or uses
        literal braces for other reasons, it is returned unchanged on format errors.
        """
        mapping = datasource.data_source_config.mapping
        storage = datasource.data_source_config.storage
        base = mapping.base_table
        link_fields = datasource.get_mapping_strategy_link_fields()
        strategy_type = datasource.get_mapping_strategy_type()
        values = {
            "datasource_name": datasource.data_source_name,
            "mapping_table": mapping.table_name,
            "mapping_schema": mapping.table_schema,
            "staging_table": storage.staging.table_name,
            "staging_schema": storage.staging.table_schema,
            "enrichment_table": storage.enrichment.table_name,
            "enrichment_schema": storage.enrichment.table_schema,
            "base_table": base.table_name,
            "base_schema": base.table_schema,
            "joins_on": mapping.joins_on,
            "strategy_type": strategy_type,
            "link_mapping_column": link_fields.get("mapping_column"),
            "link_base_column": link_fields.get("base_column"),
            "link_basis": link_fields.get("basis"),
        }
        try:
            return sql.format(**values)
        except Exception:
            return sql


class MappingStrategyRegistry:
    def __init__(self):
        self.logger = LoggerManager(type(self).__name__)
        self._strategies: dict[str, MappingStrategy] = {}
        self.register(NoopMappingStrategy())
        self.register(MapperSqlMethodStrategy())
        self.register(SqlTemplateMappingStrategy())

    def register(self, strategy: MappingStrategy) -> None:
        self._strategies[str(strategy.name).lower()] = strategy

    def get(self, name: str | None) -> MappingStrategy | None:
        if not name:
            return None
        return self._strategies.get(str(name).lower())

    def resolve(self, datasource: "DataSourceABCImpl") -> MappingStrategy:
        # Mapper can provide a custom strategy object directly.
        custom_strategy = datasource.get_custom_mapping_strategy()
        if custom_strategy is not None:
            return custom_strategy

        # Config-driven strategy name.
        strategy_name = datasource.get_mapping_strategy_name()
        strategy = self.get(strategy_name)
        if strategy is not None:
            return strategy

        if strategy_name:
            datasource.logger.warning(
                f"Unknown mapping strategy '{strategy_name}' for datasource "
                f"{datasource.data_source_name}. Falling back to legacy mapper SQL."
            )

        # Backward compatible default.
        return self._strategies["mapper_sql"]


mapping_strategy_registry = MappingStrategyRegistry()
