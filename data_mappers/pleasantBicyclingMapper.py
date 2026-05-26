import math
from pathlib import Path

from geoalchemy2 import Geometry
import pandas as pd
from pyproj import Transformer
from shapely import wkt as shapely_wkt
from shapely.ops import transform as shp_transform
from sqlalchemy import Column, Integer, Float, String, BigInteger, DateTime, func, Text, UniqueConstraint, Index, \
    ForeignKey

from core.globalconstants import GlobalConstants
from database_tables.enrichment_table import EnrichmentTable
from database_tables.mapping_table import MappingTable
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
    geometry_25833 = Column(Geometry("Linestring", srid=25833), nullable=True)
    join_status = Column(String, nullable=False, default="matched")

    created_at = Column(DateTime(timezone=True), default=func.now())
    __table_args__ = (
        UniqueConstraint("connection_id", "interval_start", "interval_end"),
        Index(None, "geometry_25833", postgresql_using="gist"),

    )


class PleasantMappingTable(MappingTable):
    __tablename__ = "pleasant_mapping"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    connection_id = Column(String, nullable=True, index=True)
    distance_m = Column(Float, nullable=True)


class PleasantEnrichmentTable(EnrichmentTable):
    """
    Hourly aggregation of pleasant bicycling metrics per road segment (connection_id).

    Each row represents one hour of the day (0–23) for one connection_id, with the
    four speed metrics averaged across the four 15-minute source slots that fall
    within that hour.  Geometry is kept so the KNN mapping stage can still use this
    table as its spatial source — it sees ~1 M rows instead of the original 4 M,
    making every KNN batch ~4× faster.
    """

    __tablename__ = "pleasant_enrichment"

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    connection_id = Column(String, nullable=False, index=True)
    # hour of day: 0 = 00:00–01:00, 1 = 01:00–02:00, …, 23 = 23:00–24:00
    hour = Column(Integer, nullable=False)

    # Averaged across the (up to 4) 15-minute source slots in this hour
    avg_temporal_mean_speed = Column(Float, nullable=True)
    avg_spatial_mean_speed = Column(Float, nullable=True)
    avg_naive_mean_speed = Column(Float, nullable=True)
    avg_speed_performance_index = Column(Float, nullable=True)
    # Total sample count summed across the slots in this hour
    sample_count = Column(Integer, nullable=True)

    lane_id = Column(String, nullable=True, index=True)
    edge_id = Column(String, nullable=True, index=True)
    # Geometry retained for KNN mapping; identical for all 24 rows of a connection
    geometry_25833 = Column(Geometry("GEOMETRY", srid=25833), nullable=True)
    join_status = Column(String, nullable=False, default="matched")

    created_at = Column(DateTime(timezone=True), default=func.now())
    __table_args__ = (
        UniqueConstraint("connection_id", "hour"),
        Index(None, "geometry_25833", postgresql_using="gist"),
    )


class PleasantBicyclingMapper(DataSourceABCImpl):
    _to_25833 = Transformer.from_crs(4326, 25833, always_xy=True).transform

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

    def _to_ewkt_25833(self, geom_wkt: str | None) -> str | None:
        if not geom_wkt:
            return None
        try:
            geom = shapely_wkt.loads(geom_wkt)
            geom_25833 = shp_transform(self._to_25833, geom)
            return f"SRID=25833;{geom_25833.wkt}"
        except Exception:
            return None

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
                "geometry_25833": self._to_ewkt_25833(row.get("geometry_wkt")),
                "join_status": status_map.get(row.get("_merge"), "unknown"),
            }
            records.append(cleaned)

        records = self._deduplicate_conflict_keys(records)
        self.logger.info(
            f"Read {len(records)} pleasant rows after left join "
            f"(metrics={len(agg_df)}, lanes={len(lanes_df)})"
        )
        return records

    # ------------------------------------------------------------------
    # Mapping: LEFT JOIN LATERAL so every way_base row is present
    # ------------------------------------------------------------------

    def mapping_db_query(self) -> str:
        """
        Insert one row per way_base into pleasant_mapping regardless of whether
        a matching connection exists within 20 m.  Ways that have no nearby
        pleasant-bicycling connection get connection_id = NULL and distance_m = NULL;
        the MV and the Java scorer both treat NULL as "no data".

        The enrichment table holds 24 rows per connection_id (one per hour of the
        day) — all with the same geometry_25833.  A LATERAL sub-query with LIMIT 1
        ordered by KNN distance (<->) efficiently picks one representative row of the
        nearest connection without redundant DISTINCT ON overhead.

        ON CONFLICT (way_id) DO UPDATE lets the mapping run again (e.g. after a
        schema reset) without duplicating rows.
        """
        mapping       = self.data_source_config.mapping
        storage       = self.data_source_config.storage
        base          = mapping.base_table

        base_schema   = base.table_schema
        base_table    = base.table_name
        enrich_schema = storage.enrichment.table_schema
        enrich_table  = storage.enrichment.table_name
        map_schema    = mapping.table_schema
        map_table     = mapping.table_name

        return f"""
            INSERT INTO "{map_schema}"."{map_table}" (way_id, connection_id, distance_m)
            SELECT
                b.id                                              AS way_id,
                e.connection_id                                   AS connection_id,
                ST_Distance(b.geometry_25833, e.geometry_25833)   AS distance_m
            FROM "{base_schema}"."{base_table}" b
            LEFT JOIN LATERAL (
                SELECT e2.connection_id, e2.geometry_25833
                FROM "{enrich_schema}"."{enrich_table}" e2
                WHERE ST_DWithin(b.geometry_25833, e2.geometry_25833, 20)
                ORDER BY b.geometry_25833 <-> e2.geometry_25833
                LIMIT 1
            ) e ON TRUE
            ON CONFLICT (way_id)
            DO UPDATE SET
                connection_id = EXCLUDED.connection_id,
                distance_m    = EXCLUDED.distance_m
        """

    # ------------------------------------------------------------------
    # Enrichment: aggregate 15-min staging slots → hourly averages
    # ------------------------------------------------------------------

    def sync_staging_to_enrichment(self):
        """
        Skip the default 4 M-row verbatim copy.
        execute_on_enrichment() populates enrichment directly from staging
        with hourly aggregates (~1 M rows), so the generic sync is not needed.
        """
        self.logger.info(
            "[pleasant_bicycling] Skipping default staging→enrichment sync; "
            "hourly aggregation will run in execute_on_enrichment."
        )

    def execute_on_enrichment(self):
        """
        Aggregate the 96 × 15-minute rows in pleasant_staging into 24 hourly
        rows per connection_id and write them to pleasant_enrichment.

        Slot duration in the source data is 900 000 000 000 nanoseconds (15 min).
        One hour = 4 slots → 3 600 000 000 000 ns.

        Each metric is averaged across the slots that fall in the hour; sample_count
        is summed.  Geometry (geometry_25833) is the same for all slots of a
        connection_id so MAX() is used as a safe pick.
        """
        staging_schema = self.data_source_config.storage.staging.table_schema
        staging_table  = self.data_source_config.storage.staging.table_name
        enrich_schema  = self.data_source_config.storage.enrichment.table_schema
        enrich_table   = self.data_source_config.storage.enrichment.table_name

        ns_per_hour = 3_600_000_000_000  # 1 hour in nanoseconds

        truncate_sql = f'TRUNCATE TABLE "{enrich_schema}"."{enrich_table}"'

        insert_sql = f"""
            INSERT INTO "{enrich_schema}"."{enrich_table}" (
                connection_id,
                hour,
                avg_temporal_mean_speed,
                avg_spatial_mean_speed,
                avg_naive_mean_speed,
                avg_speed_performance_index,
                sample_count,
                lane_id,
                edge_id,
                geometry_25833,
                join_status
            )
            SELECT
                connection_id,
                (interval_start / {ns_per_hour})::int          AS hour,
                AVG(avg_temporal_mean_speed)                    AS avg_temporal_mean_speed,
                AVG(avg_spatial_mean_speed)                     AS avg_spatial_mean_speed,
                AVG(avg_naive_mean_speed)                       AS avg_naive_mean_speed,
                AVG(avg_speed_performance_index)                AS avg_speed_performance_index,
                SUM(sample_count)                               AS sample_count,
                MAX(lane_id)                                    AS lane_id,
                MAX(edge_id)                                    AS edge_id,
                MAX(geometry_25833)                             AS geometry_25833,
                MAX(join_status)                                AS join_status
            FROM "{staging_schema}"."{staging_table}"
            WHERE connection_id IS NOT NULL
              AND interval_start IS NOT NULL
            GROUP BY
                connection_id,
                (interval_start / {ns_per_hour})::int
            ON CONFLICT (connection_id, hour)
            DO UPDATE SET
                avg_temporal_mean_speed   = EXCLUDED.avg_temporal_mean_speed,
                avg_spatial_mean_speed    = EXCLUDED.avg_spatial_mean_speed,
                avg_naive_mean_speed      = EXCLUDED.avg_naive_mean_speed,
                avg_speed_performance_index = EXCLUDED.avg_speed_performance_index,
                sample_count              = EXCLUDED.sample_count,
                lane_id                   = EXCLUDED.lane_id,
                edge_id                   = EXCLUDED.edge_id,
                geometry_25833            = EXCLUDED.geometry_25833,
                join_status               = EXCLUDED.join_status
        """

        self.logger.info(
            f"[pleasant_bicycling] Truncating enrichment table "
            f"{enrich_schema}.{enrich_table} before hourly aggregation."
        )
        self.db.call_sql(truncate_sql)

        self.logger.info(
            f"[pleasant_bicycling] Aggregating staging 15-min slots → "
            f"hourly averages in {enrich_schema}.{enrich_table}."
        )
        self.db.call_sql(insert_sql)

        self.logger.info(
            "[pleasant_bicycling] Hourly enrichment aggregation complete."
        )
