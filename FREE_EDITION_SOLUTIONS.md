# Free Edition Task Limit Solutions - Quick Reference

This guide documents the solutions implemented to work around Databricks Free Edition serverless compute task limits.

## Problem Summary

**Issue**: Databricks Free Edition serverless compute has a 5 concurrent task limit, but the original gold layer creation was spawning 8-16 tasks, causing failures.

**Root Cause**: 
- Large fact table joins and writes naturally fan out to multiple parallel tasks
- Delta Lake optimization features can multiply task counts
- Spark's default parallelism exceeds Free Edition limits

## Solutions Implemented

### 1. SQL-Based Monthly Processing (Recommended)

**File**: `src/sql/create_gold_layer_free_edition.sql`

**Key Techniques**:
```sql
-- Single writer per month
INSERT INTO fact_table
SELECT /*+ REPARTITION(1) BROADCAST(dim1) BROADCAST(dim2) */ ...
WHERE created_date >= '2023-01-01' AND created_date < '2023-02-01';

-- Disable auto-optimization
ALTER TABLE fact_table SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'false',
  'delta.autoOptimize.autoCompact' = 'false'
);
```

**Benefits**:
- Each monthly batch uses only 1-2 tasks
- Broadcast hints eliminate shuffle for small dimensions
- Automated looping via SQL scripting
- No Spark configuration changes needed

### 2. Python-Based Monthly Processing (Alternative)

**File**: `src/pipelines/nyc311/nyc311_gold_free_edition.py`

**Key Techniques**:
```python
# Load dimensions once and broadcast
dd = broadcast(spark.table("dim_date"))
ad = broadcast(spark.table("dim_agency"))

# Process each month with single writer
for month in months:
    fact = (silver_monthly
            .join(dd, ..., "left")
            .join(ad, ..., "left")
            .coalesce(1))  # Single writer
    
    fact.write.mode("append").saveAsTable("fact_table")
```

**Benefits**:
- More control over processing logic
- Better error handling and logging
- Progress tracking per month
- Broadcast joins explicitly controlled

### 3. Optional Silver Compaction

**File**: `src/sql/compact_silver_optional.sql`

**Purpose**: If silver table has many small files, compact it monthly to reduce scan tasks in gold layer.

**Usage**:
```sql
-- Creates service_requests_silver_compact
-- One file per month instead of many small files
-- Point gold layer at compacted table for better performance
```

## Configuration Changes

### databricks.yml Updates

```yaml
# Uses the Free Edition compatible SQL script
- task_key: "gold_layer"
  sql_task:
    file:
      path: ${workspace.file_path}/src/sql/create_gold_layer_free_edition.sql
      source: WORKSPACE
    warehouse_id: ${var.warehouse_id}

# Alternative: Python notebook approach (commented out)
# - task_key: "gold_layer_python"
#   notebook_task:
#     notebook_path: "./src/pipelines/nyc311/nyc311_gold_free_edition.py"
```

## Performance Characteristics

### Task Count Comparison

| Approach | Tasks per Month | Total Tasks | Free Edition Compatible |
|----------|-----------------|-------------|------------------------|
| Original | 8-16 | 100+ | ❌ No |
| Monthly SQL | 1-2 | 10-20 | ✅ Yes |
| Monthly Python | 1-2 | 10-20 | ✅ Yes |

### File Organization

| Approach | Files Generated | Scan Performance |
|----------|-----------------|------------------|
| Original | Many small files | Poor |
| Monthly Processing | ~1 file per month | Excellent |
| With Compaction | Optimized layout | Best |

## Usage Instructions

### Option 1: SQL Approach (Default)
1. Deploy with `databricks bundle deploy`
2. Run with `databricks bundle run nyc311_pipeline`
3. SQL script automatically processes all months

### Option 2: Python Approach
1. Comment out SQL task in `databricks.yml`
2. Uncomment Python notebook task
3. Deploy and run as normal

### Option 3: Manual Monthly Processing
1. Run `create_gold_layer_free_edition.sql` to set up dimensions
2. Use `create_fact_table_monthly.sql` to load specific months
3. Customize month ranges as needed

## Monitoring

### Success Indicators
- Task count stays under 5 per job run
- One file generated per month in fact table
- Processing completes without timeout errors

### Verification Queries
```sql
-- Check file count (should ≈ number of months)
DESCRIBE DETAIL gold.nyc311.fact_service_requests;

-- Verify monthly distribution
SELECT date_format(created_date, 'yyyy-MM') as month, COUNT(*)
FROM gold.nyc311.fact_service_requests
GROUP BY month ORDER BY month;
```

## Troubleshooting

### Still Getting Task Limit Errors?
1. Check if auto-optimization is disabled on all tables
2. Verify REPARTITION(1) hints are being applied
3. Consider further reducing batch size (weekly instead of monthly)
4. Check for other concurrent jobs consuming task quota

### Performance Issues?
1. Run optional silver compaction first
2. Use `ANALYZE TABLE` on dimensions after creation
3. Consider Z-ordering on frequently filtered columns (after initial load)

## Future Improvements

When moving to paid Databricks:
1. Remove REPARTITION(1) constraints
2. Enable auto-optimization features
3. Use larger batch sizes for better throughput
4. Add concurrent processing for different data streams
