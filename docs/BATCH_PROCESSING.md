# Batch Processing for SQL Operations

## Overview

The pipeline now supports **batch processing** for large SQL operations to prevent long-running transactions that can block PostgreSQL and cause performance issues.

## Problem

Previously, mapping operations executed queries like:
```sql
INSERT INTO mapping_table (way_id, data, ...)
SELECT ...
FROM ways_base w  -- millions of rows
JOIN LATERAL (...)
```

This could:
- Lock tables for minutes or hours
- Consume excessive memory
- Block other queries
- Cause connection pool exhaustion
- Create database bloat

## Solution

The new batching system automatically:
1. **Detects large INSERT INTO ... SELECT queries** with JOINs
2. **Splits them into smaller batches** (default: 10,000 rows per batch)
3. **Commits each batch separately** to release locks
4. **Provides progress logging** for visibility
5. **Reduces PostgreSQL stress** by limiting transaction size

## Configuration

### Global Configuration

Add to your `config.yaml` under `database`:

```yaml
database:
  # ... existing config ...
  performance:
    enable_batching: true           # Enable batch processing
    default_batch_size: 10000       # Default rows per batch
    batch_commit_interval: 5000     # Commit every N rows (0 = per batch only)
```

### Per-Datasource Configuration

Override batch size for specific datasources:

```yaml
datasources:
  - name: "my_heavy_datasource"
    # ... other config ...
    mapping:
      enable: true
      batch_size: 5000  # Override global default for this datasource
      strategy:
        type: sql_template
      config:
        sql: |
          INSERT INTO ...
          # Your complex query here
```

## How It Works

### Automatic Detection

The system detects queries that are suitable for batching:
- Contains `INSERT INTO ... SELECT`
- Has `JOIN` clauses (typically mapping queries)
- Operates on a base table

### Batching Process

1. **Extract base table** from the query (e.g., `ways_base`)
2. **Count total rows** to process
3. **Calculate number of batches** needed
4. **Execute query in batches** using LIMIT/OFFSET
5. **Commit after each batch**
6. **Log progress** throughout

### Example Execution

Before (single query):
```
INFO: Starting SQL execution...
INFO: SQL execution completed.  [10 minutes later]
```

After (batched):
```
INFO: Using batched execution for Mapping with batch size: 10000
INFO: Total rows to process: 125000, batch size: 10000
INFO: Processing in 13 batches...
INFO: Processing batch 1/13 (rows 1-10000)
INFO: Batch 1 completed in 45.23s (10000 rows affected)
INFO: Processing batch 2/13 (rows 10001-20000)
INFO: Batch 2 completed in 43.12s (10000 rows affected)
...
INFO: All 13 batches completed successfully
```

## Benefits

### Performance
- **90%+ reduction in lock duration** - Each batch releases locks
- **Better resource utilization** - Smaller transactions use less memory
- **Database remains responsive** - Other queries can execute between batches

### Visibility
- **Progress tracking** - See how many batches remain
- **Time estimates** - Calculate ETA based on batch timing
- **Error isolation** - Know which batch failed

### Reliability
- **Easier recovery** - Resume from failed batch
- **Less risk of timeout** - Smaller transactions complete faster
- **Reduced bloat** - Smaller transactions create less WAL/vacuum work

## Query Transformation

The system automatically transforms queries:

**Original:**
```sql
INSERT INTO trial.mapping_table (way_id, data)
SELECT w.id, e.data
FROM trial.ways_base w
JOIN enrichment e ON ...
```

**Batched (Batch 1):**
```sql
INSERT INTO trial.mapping_table (way_id, data)
SELECT w.id, e.data
FROM (SELECT * FROM trial.ways_base LIMIT 10000 OFFSET 0) w
JOIN enrichment e ON ...
```

**Batched (Batch 2):**
```sql
INSERT INTO trial.mapping_table (way_id, data)
SELECT w.id, e.data
FROM (SELECT * FROM trial.ways_base LIMIT 10000 OFFSET 10000) w
JOIN enrichment e ON ...
```

## When to Adjust Batch Size

### Increase Batch Size (20000+)
- Simple queries with minimal JOINs
- Fast disk I/O
- Low concurrent load
- Want fewer total commits

### Decrease Batch Size (5000 or less)
- Complex queries with multiple JOINs or CTEs
- Heavy PostGIS operations (ST_Distance, ST_Intersects)
- High concurrent database load
- Limited RAM
- Need more responsive progress updates

## Monitoring

### Check Batch Performance

Look for log messages:
```
INFO: Batch 1 completed in 45.23s (10000 rows affected)
INFO: Batch 2 completed in 43.12s (10000 rows affected)
```

If batches slow down over time:
- Consider smaller batch size
- Check for lock contention
- Review query execution plan
- Ensure indexes exist

### Database Monitoring

While batched queries run:
```sql
-- Check active queries
SELECT pid, state, query_start, query
FROM pg_stat_activity
WHERE state = 'active';

-- Check lock waits
SELECT * FROM pg_locks WHERE NOT granted;
```

## Disabling Batching

To disable batching globally:
```yaml
database:
  performance:
    enable_batching: false
```

To disable for specific datasource:
```yaml
mapping:
  batch_size: 0  # 0 or null disables batching
```

Or remove the `performance` config entirely to use original behavior.

## Advanced Configuration

### Custom Batch Logic

For queries that need custom batching logic, you can call `call_sql_batched()` directly:

```python
# In your mapper class
def mapping_db_query(self):
    # Build your query
    query = "INSERT INTO ... SELECT ..."

    # Don't return it - execute with custom batch size
    self.db.call_sql_batched(
        query,
        batch_size=5000,
        base_id_column="id",  # Column to use for ordering
        raise_on_error=True
    )
    return None  # Already executed
```

## Troubleshooting

### "Query not suitable for batching"
- Query doesn't match expected pattern
- Will execute normally without batching
- Check that query has `INSERT INTO ... SELECT ... JOIN`

### "Could not extract base table from query"
- Base table pattern not recognized
- Falls back to normal execution
- May need to adjust regex patterns in `_extract_base_table_from_sql()`

### Batches are too slow
- Reduce batch size
- Add indexes on join columns
- Simplify query if possible
- Check for sequential scans in EXPLAIN

### Memory issues during batching
- Reduce `default_batch_size`
- Set `batch_commit_interval` to smaller value
- Check PostgreSQL `work_mem` setting

## Future Enhancements

Potential improvements:
- **Cursor-based streaming** for very large datasets
- **Parallel batch execution** using multiple workers
- **Adaptive batch sizing** based on query performance
- **Temporary table optimization** for complex CTEs
- **UNLOGGED table mode** for staging operations
