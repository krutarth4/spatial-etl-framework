-- Per-way elevation stats by sampling the averaged elevation raster
-- (elevation_enrichment.rast, EPSG:25833) along each way's geometry_25833.
--
-- For every way we walk the line at a fixed spacing (step_m), read the raster
-- value at each sample point, then derive ascent/descent/slope from the
-- consecutive elevation differences.
--
-- Every way in ways_base gets exactly one row. Ways with no raster coverage
-- (outside the loaded tiles, or sampling only nodata) default to all-zero with
-- sample_count = 0, which is the "no elevation data" sentinel.
--
-- Placeholders are filled by the sql_template mapping strategy:
--   {base_schema}.{base_table}              -> ways_base (alias w, geometry_25833 LINESTRING)
--   {enrichment_schema}.{enrichment_table}  -> elevation_enrichment (raster)
--   {mapping_schema}.{mapping_table}        -> elevation_mapping
INSERT INTO {mapping_schema}.{mapping_table}
    (way_id, total_ascent, total_descent, max_slope, avg_slope, sample_count)
WITH params AS (
    SELECT 10.0::float8 AS step_m            -- sample spacing in metres
),
ways AS (
    -- All ways in the batch — no geometry filter here so every way reaches the
    -- final LEFT JOIN and gets a row (defaulted when it has no usable geometry).
    SELECT
        w.id                       AS way_id,
        w.geometry_25833           AS geom,
        ST_Length(w.geometry_25833) AS len
    FROM {base_schema}.{base_table} w
),
samples AS (
    SELECT
        wy.way_id,
        gs.i,
        ST_LineInterpolatePoint(
            wy.geom,
            LEAST(1.0, (gs.i * p.step_m) / wy.len)
        ) AS pt
    FROM ways wy
    CROSS JOIN params p
    CROSS JOIN LATERAL generate_series(
        0,
        GREATEST(1, CEIL(wy.len / p.step_m)::int)
    ) AS gs(i)
    -- Only sample ways that can actually be walked; others fall through to the
    -- LEFT JOIN as no-data defaults.
    WHERE wy.geom IS NOT NULL
      AND wy.len > 0
),
elev AS (
    SELECT
        s.way_id,
        s.i,
        ST_Value(r.rast, s.pt) AS z
    FROM samples s
    JOIN {enrichment_schema}.{enrichment_table} r
      -- && on the convex-hull geometry hits idx_..._rast (GiST); without it the
      -- planner seq-scans every tile per sample point. ST_Intersects refines.
      ON ST_ConvexHull(r.rast) && s.pt
     AND ST_Intersects(r.rast, s.pt)
),
diffs AS (
    SELECT
        way_id,
        z - LAG(z) OVER (PARTITION BY way_id ORDER BY i) AS dz
    FROM elev
    WHERE z IS NOT NULL
      AND z <> -9999
),
agg AS (
    SELECT
        way_id,
        COALESCE(SUM(dz) FILTER (WHERE dz > 0), 0)  AS total_ascent,
        COALESCE(-SUM(dz) FILTER (WHERE dz < 0), 0) AS total_descent,
        COALESCE(MAX(ABS(dz)), 0)                   AS max_step_rise,
        COUNT(dz)                                   AS sample_count
    FROM diffs
    GROUP BY way_id
)
SELECT
    wy.way_id,
    COALESCE(a.total_ascent, 0)  AS total_ascent,
    COALESCE(a.total_descent, 0) AS total_descent,
    CASE WHEN p.step_m > 0 THEN COALESCE(a.max_step_rise, 0) / p.step_m ELSE 0 END AS max_slope,
    CASE WHEN wy.len   > 0 THEN (COALESCE(a.total_ascent, 0) + COALESCE(a.total_descent, 0)) / wy.len ELSE 0 END AS avg_slope,
    COALESCE(a.sample_count, 0)  AS sample_count
FROM ways wy
CROSS JOIN params p
LEFT JOIN agg a ON a.way_id = wy.way_id
ON CONFLICT (way_id) DO UPDATE SET
    total_ascent  = EXCLUDED.total_ascent,
    total_descent = EXCLUDED.total_descent,
    max_slope     = EXCLUDED.max_slope,
    avg_slope     = EXCLUDED.avg_slope,
    sample_count  = EXCLUDED.sample_count;
