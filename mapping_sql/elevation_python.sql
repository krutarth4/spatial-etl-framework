INSERT INTO {mapping_schema}.{mapping_table}
    (way_id, total_ascent, total_descent, max_slope, avg_slope, sample_count)
SELECT way_id, total_ascent, total_descent, max_slope, avg_slope, sample_count
FROM {staging_schema}.{staging_table}
ON CONFLICT (way_id) DO UPDATE SET
    total_ascent  = EXCLUDED.total_ascent,
    total_descent = EXCLUDED.total_descent,
    max_slope     = EXCLUDED.max_slope,
    avg_slope     = EXCLUDED.avg_slope,
    sample_count  = EXCLUDED.sample_count;
