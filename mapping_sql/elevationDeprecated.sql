WITH sampled_points AS (
    SELECT
        w.id AS way_id,
        dp.path[1] AS path_idx,
        dp.geom AS geom_25833
    FROM {base_schema}.{base_table} w
    CROSS JOIN LATERAL ST_DumpPoints(
        ST_Segmentize(
            ST_Transform(w.geometry, 25833),
            100.0
        )
    ) AS dp
),
points_with_elevation AS (
    SELECT
        sp.way_id,
        sp.path_idx,
        sp.geom_25833,
        z_lookup.z
    FROM sampled_points sp
    LEFT JOIN LATERAL (
        SELECT z
        FROM (
            SELECT ST_Value(r.rast, sp.geom_25833) AS z
            FROM {enrichment_schema}.{enrichment_table} r
            WHERE ST_Intersects(r.footprint_4326, ST_Transform(sp.geom_25833, 4326))
        ) sampled_z
        WHERE sampled_z.z IS NOT NULL
        LIMIT 1
    ) AS z_lookup ON TRUE
)
INSERT INTO {mapping_schema}.{mapping_table} (way_id, elevation_profile)
SELECT
    p.way_id AS way_id,
    jsonb_agg(
        jsonb_build_object(
            'x', ST_X(ST_Transform(p.geom_25833, 4326)),
            'y', ST_Y(ST_Transform(p.geom_25833, 4326)),
            'z', p.z
        )
        ORDER BY p.path_idx
    ) AS elevation_profile
FROM points_with_elevation p
WHERE p.z IS NOT NULL
GROUP BY p.way_id
ON CONFLICT (way_id)
DO UPDATE SET
    elevation_profile = EXCLUDED.elevation_profile;
