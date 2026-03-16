from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from data_config_dtos.data_source_config_dto import MappingDTO
    from main_core.data_source_abc_impl import DataSourceABCImpl


class MappingSelectSqlStrategy(Protocol):
    name: str

    def build_select(self, datasource: "DataSourceABCImpl") -> str:
        ...


@dataclass
class MappingInsertSpec:
    columns: list[str]
    conflict_columns: list[str] | None = None
    update_columns: list[str] | None = None


class MappingInsertBuilder:
    def build_insert(self, mapping: "MappingDTO", select_sql: str, spec: MappingInsertSpec) -> str:
        if not spec.columns:
            raise ValueError("Mapping insert spec requires at least one column")

        cleaned_select_sql = select_sql.strip().rstrip(";")
        columns_sql = ", ".join(spec.columns)

        print(columns_sql)
        sql = (
            f"INSERT INTO {mapping.table_schema}.{mapping.table_name} ({columns_sql})\n"
            f"{cleaned_select_sql}"
        )

        if not spec.conflict_columns:
            return f"{sql};"

        update_columns = spec.update_columns or [
            column for column in spec.columns if column not in spec.conflict_columns
        ]
        if not update_columns:
            return f"{sql}\nON CONFLICT ({', '.join(spec.conflict_columns)}) DO NOTHING;"

        update_sql = ",\n                ".join(
            f"{column} = EXCLUDED.{column}" for column in update_columns
        )
        return (
            f"{sql}\n"
            f"ON CONFLICT ({', '.join(spec.conflict_columns)})\n"
            f"DO UPDATE SET\n"
            f"                {update_sql};"
        )


class NearestStationMappingSelectStrategy:
    name = "nearest_station"

    def build_select(self, datasource: "DataSourceABCImpl") -> str:
        base = datasource.data_source_config.mapping.base_table
        enrichment = datasource.data_source_config.storage.enrichment

        return f"""
SELECT
    w.id AS way_id,
    s.dwd_station_id AS dwd_station_id,
    ST_Distance(
        w.geometry::geography,
        s.point::geography
    ) AS distance,
    MOD(
        (DEGREES(
          ST_Azimuth(
            ST_StartPoint(w.geometry),
            ST_EndPoint(w.geometry)
          )
        ) + 360)::NUMERIC,
        360
      ) AS bearing_degree
FROM {base.table_schema}.{base.table_name} w
JOIN LATERAL (
    SELECT
        en.uid,
        en.dwd_station_id,
        en.point
    FROM {enrichment.table_schema}.{enrichment.table_name} en
    ORDER BY
        ST_Distance(
            w.geometry::geography,
            en.point::geography
        )
    LIMIT 1
) s ON TRUE
"""


class MappingSelectSqlStrategyRegistry:
    def __init__(self):
        self._strategies: dict[str, MappingSelectSqlStrategy] = {}
        self.register(NearestStationMappingSelectStrategy())

    def register(self, strategy: MappingSelectSqlStrategy) -> None:
        self._strategies[str(strategy.name).lower()] = strategy

    def get(self, name: str | None) -> MappingSelectSqlStrategy | None:
        if not name:
            return None
        return self._strategies.get(str(name).lower())


mapping_select_sql_strategy_registry = MappingSelectSqlStrategyRegistry()
