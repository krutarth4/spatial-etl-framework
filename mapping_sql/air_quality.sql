INSERT INTO {mapping_schema}.{mapping_table} (way_id, grid_uid, grid_id, intersection_length_m)
SELECT
    w.id AS way_id,
    lpg.uid AS grid_uid,
    lpg.grid_id,
    ST_Length(
        ST_Intersection(
            COALESCE(w.geometry_25833, ST_Transform(w.geometry, 25833)),
            ST_Expand(lpg.geom_25833, 25)
        )
    ) AS intersection_length_m
FROM {base_schema}.{base_table} w
JOIN LATERAL (
    SELECT DISTINCT ON (e.grid_id)
        e.uid,
        e.grid_id,
        e.geom_25833
    FROM {enrichment_schema}.{enrichment_table} e
    WHERE e.geom_25833 IS NOT NULL
      AND ST_DWithin(COALESCE(w.geometry_25833, ST_Transform(w.geometry, 25833)), e.geom_25833, 36)
    ORDER BY e.grid_id, e.forecast_time DESC NULLS LAST, e.uid DESC
) lpg ON ST_Intersects(COALESCE(w.geometry_25833, ST_Transform(w.geometry, 25833)), ST_Expand(lpg.geom_25833, 25))
WHERE (w.geometry_25833 IS NOT NULL OR w.geometry IS NOT NULL)
  AND ST_Length(ST_Intersection(COALESCE(w.geometry_25833, ST_Transform(w.geometry, 25833)), ST_Expand(lpg.geom_25833, 25))) > 0
ON CONFLICT (way_id, grid_id) DO UPDATE
    SET grid_uid = EXCLUDED.grid_uid,
        intersection_length_m = EXCLUDED.intersection_length_m;
