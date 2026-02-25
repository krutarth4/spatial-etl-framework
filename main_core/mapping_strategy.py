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


class MappingStrategyRegistry:
    def __init__(self):
        self.logger = LoggerManager(type(self).__name__)
        self._strategies: dict[str, MappingStrategy] = {}
        self.register(NoopMappingStrategy())
        self.register(MapperSqlMethodStrategy())

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
