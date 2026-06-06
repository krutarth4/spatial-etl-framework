-- Per-way elevation stats by sampling the averaged elevation raster
-- (elevation_enrichment.rast, EPSG:25833) along each way's geometry_25833.
--
-- For every way we walk the line at a fixed spacing (step_m), read the raster
-- value at each sample point, then derive ascent/descent/slope from the
-- consecutive elevation differences.
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
    SELECT
        w.id                       AS way_id,
        w.geometry_25833           AS geom,
        ST_Length(w.geometry_25833) AS len
    FROM {base_schema}.{base_table} w
    WHERE w.geometry_25833 IS NOT NULL
      AND ST_Length(w.geometry_25833) > 0
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
),
elev AS (
    SELECT
        s.way_id,
        s.i,
        ST_Value(r.rast, s.pt) AS z
    FROM samples s
    JOIN {enrichment_schema}.{enrichment_table} r
      ON ST_Intersects(r.rast, s.pt)
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
    a.way_id,
    a.total_ascent,
    a.total_descent,
    CASE WHEN p.step_m > 0 THEN a.max_step_rise / p.step_m ELSE 0 END        AS max_slope,
    CASE WHEN wy.len   > 0 THEN (a.total_ascent + a.total_descent) / wy.len ELSE 0 END AS avg_slope,
    a.sample_count
FROM agg a
JOIN ways wy ON wy.way_id = a.way_id
CROSS JOIN params p
ON CONFLICT (way_id) DO UPDATE SET
    total_ascent  = EXCLUDED.total_ascent,
    total_descent = EXCLUDED.total_descent,
    max_slope     = EXCLUDED.max_slope,
    avg_slope     = EXCLUDED.avg_slope,
    sample_count  = EXCLUDED.sample_count;
