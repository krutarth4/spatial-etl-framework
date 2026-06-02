INSERT INTO {mapping_schema}.{mapping_table} (way_id, no2, pm10, pm25, total_intersection_length_m)
SELECT
    w.id AS way_id,
    e.no2,
    e.pm10,
    e.pm25,
    ST_Distance(
        COALESCE(w.geometry_25833, ST_Transform(w.geometry, 25833)),
        e.geom_25833
    ) AS total_intersection_length_m
FROM {base_schema}.{base_table} w
JOIN LATERAL (
    SELECT e.no2, e.pm10, e.pm25, e.geom_25833
    FROM {enrichment_schema}.{enrichment_table} e
    WHERE e.geom_25833 IS NOT NULL
      AND e.no2 IS NOT NULL
    ORDER BY e.geom_25833 <-> COALESCE(w.geometry_25833, ST_Transform(w.geometry, 25833))
    LIMIT 1
) e ON TRUE
WHERE w.geometry_25833 IS NOT NULL OR w.geometry IS NOT NULL
ON CONFLICT (way_id) DO UPDATE
    SET no2                         = EXCLUDED.no2,
        pm10                        = EXCLUDED.pm10,
        pm25                        = EXCLUDED.pm25,
        total_intersection_length_m = EXCLUDED.total_intersection_length_m;
