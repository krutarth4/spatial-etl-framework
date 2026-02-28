import math
from pathlib import Path

import pandas as pd
from sqlalchemy import Column, Integer, Float, String, BigInteger, DateTime, func, Text, UniqueConstraint, Index

from database_tables.staging_table import StagingTable
from main_core.data_source_abc_impl import DataSourceABCImpl

try:
    import geopandas as gpd
except Exception:
    gpd = None


class PleasantStagingTable(StagingTable):
    __tablename__ = "pleasant_staging"

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    connection_id = Column(String, nullable=False, index=True)
    interval_start = Column(BigInteger, nullable=True)
    interval_end = Column(BigInteger, nullable=True)

    avg_temporal_mean_speed = Column(Float, nullable=True)
    avg_spatial_mean_speed = Column(Float, nullable=True)
    avg_naive_mean_speed = Column(Float, nullable=True)
    avg_speed_performance_index = Column(Float, nullable=True)
    sample_count = Column(Integer, nullable=True)

    lane_id = Column(String, nullable=True, index=True)
    edge_id = Column(String, nullable=True, index=True)
    geometry = Column(Text, nullable=True)
    join_status = Column(String, nullable=False, default="matched")

    created_at = Column(DateTime(timezone=True), default=func.now())
    __table_args__ = (
        UniqueConstraint("connection_id", "interval_start", "interval_end"),
        Index(
            None,
            "geometry",
            postgresql_using="gist"
        )
    )


class PleasantBicyclingMapper(DataSourceABCImpl):
    @staticmethod
    def _normalize_connection_id(value) -> str | None:
        if value is None:
            return None
        normalized = str(value).strip()
        if not normalized:
            return None
        if normalized.startswith(":"):
            normalized = normalized[1:]
        return normalized.replace("_", "#")

    @staticmethod
    def _safe_int(value):
        if value is None:
            return None
        if isinstance(value, float) and math.isnan(value):
            return None
        return int(value)

    @staticmethod
    def _safe_float(value):
        if value is None:
            return None
        if isinstance(value, float) and math.isnan(value):
            return None
        return float(value)

    def _read_lanes_dataframe(self, lanes_path: Path) -> pd.DataFrame:
        if gpd is not None:
            lanes = gpd.read_parquet(lanes_path)
            lanes["geometry_wkt"] = lanes.geometry.apply(
                lambda geom: geom.wkt if geom is not None else None
            )
            lanes = lanes.drop(columns=["geometry"], errors="ignore")
            return pd.DataFrame(lanes)

        lanes = pd.read_parquet(lanes_path)
        if "geometry" in lanes.columns:
            lanes["geometry_wkt"] = lanes["geometry"].astype(str)
        else:
            lanes["geometry_wkt"] = None
        return lanes

    @staticmethod
    def _first_not_null(series: pd.Series):
        for value in series:
            if value is None:
                continue
            if isinstance(value, float) and math.isnan(value):
                continue
            return value
        return None

    @staticmethod
    def _deduplicate_conflict_keys(records: list[dict]) -> list[dict]:
        deduped = {}
        for row in records:
            key = (row.get("connection_id"), row.get("interval_start"), row.get("interval_end"))
            prev = deduped.get(key)
            if prev is None:
                deduped[key] = row
                continue
            for field in ("lane_id", "edge_id", "geometry"):
                if prev.get(field) in (None, "") and row.get(field) not in (None, ""):
                    prev[field] = row[field]
        return list(deduped.values())

    def read_file_content(self, path: str) -> list[dict]:
        agg_df = pd.read_parquet(path)

        agg_required = [
            "connectionID",
            "intervalStart",
            "intervalEnd",
            "avgTemporalMeanSpeed",
            "avgSpatialMeanSpeed",
            "avgNaiveMeanSpeed",
            "avgSpeedPerformanceIndex",
            "sampleCount",
        ]
        missing = [c for c in agg_required if c not in agg_df.columns]
        if missing:
            raise ValueError(
                f"Missing required columns in pleasant parquet: {missing}"
            )

        agg_df = agg_df[agg_required].copy()
        agg_df["connection_id_norm"] = agg_df["connectionID"].apply(self._normalize_connection_id)

        lanes_path = Path(path).parent / "berlin_lanes.parquet"
        lanes_df = self._read_lanes_dataframe(lanes_path)
        lanes_required = ["lane_id", "edge_id", "geometry_wkt"]
        missing_lanes = [c for c in lanes_required if c not in lanes_df.columns]
        if missing_lanes:
            raise ValueError(
                f"Missing required columns in berlin lanes parquet: {missing_lanes}"
            )

        lanes_df = lanes_df[lanes_required].copy()
        lanes_df["edge_id_norm"] = lanes_df["edge_id"].apply(self._normalize_connection_id)
        lanes_df = (
            lanes_df.groupby("edge_id_norm", as_index=False)
            .agg(
                lane_id=("lane_id", self._first_not_null),
                edge_id=("edge_id", self._first_not_null),
                geometry_wkt=("geometry_wkt", self._first_not_null),
            )
        )

        merged = agg_df.merge(
            lanes_df,
            left_on="connection_id_norm",
            right_on="edge_id_norm",
            how="left",
            indicator=True,
        )

        status_map = {
            "both": "matched",
            "left_only": "metrics_only",
            "right_only": "lanes_only",
        }

        records = []
        for row in merged.to_dict(orient="records"):
            connection_id = row.get("connectionID")

            cleaned = {
                "connection_id": connection_id,
                "interval_start": self._safe_int(row.get("intervalStart")),
                "interval_end": self._safe_int(row.get("intervalEnd")),
                "avg_temporal_mean_speed": self._safe_float(row.get("avgTemporalMeanSpeed")),
                "avg_spatial_mean_speed": self._safe_float(row.get("avgSpatialMeanSpeed")),
                "avg_naive_mean_speed": self._safe_float(row.get("avgNaiveMeanSpeed")),
                "avg_speed_performance_index": self._safe_float(row.get("avgSpeedPerformanceIndex")),
                "sample_count": self._safe_int(row.get("sampleCount")),
                "lane_id": row.get("lane_id"),
                "edge_id": row.get("edge_id"),
                "geometry": row.get("geometry_wkt"),
                "join_status": status_map.get(row.get("_merge"), "unknown"),
            }
            records.append(cleaned)

        records = self._deduplicate_conflict_keys(records)
        self.logger.info(
            f"Read {len(records)} pleasant rows after left join "
            f"(metrics={len(agg_df)}, lanes={len(lanes_df)})"
        )
        return records
