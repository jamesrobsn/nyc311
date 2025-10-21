# NYC 311 Gold Layer SQL Scripts

This directory contains SQL scripts for creating the gold layer star schema directly via SQL Warehouse using Databricks SQL file tasks.

## Files

### Pipeline-Integrated Scripts
- **`create_gold_layer_free_edition.sql`** - Free Edition compatible master script with monthly processing
- **`create_fact_table_monthly.sql`** - Standalone monthly fact table loading script

### Alternative/Optional Scripts  
- **`create_gold_layer_complete.sql`** - Original single-batch approach (may hit task limits)
- **`compact_silver_optional.sql`** - Optional silver table compaction for better performance
- **`create_dimension_tables.sql`** - Creates dimension tables only
- **`create_fact_table.sql`** - Creates fact table only  
- **`create_aggregate_tables.sql`** - Creates aggregate tables and Power BI view

## Free Edition Approach

The main approach uses monthly processing to work around Free Edition serverless task limits:

### How It Works
1. **Creates Dimensions First** - Small tables that don't trigger task limits
2. **Creates Empty Fact Table** - Proper schema with optimizations disabled  
3. **Processes Monthly Batches** - Loops through months using automated scripting
4. **Single Writer Per Month** - `REPARTITION(1)` hint forces one writer task
5. **Broadcast Joins** - Small dimensions are broadcast to avoid shuffles

### Key Benefits
- **No Configuration Changes** - Works without modifying Spark/cluster settings
- **Stays Under Task Limits** - Each monthly batch uses only 1-2 tasks
- **Preserves Data Quality** - Same star schema and business logic
- **Better File Organization** - One file per month for optimal scanning

### Parameters Used
- `:bronze_catalog` - Source bronze catalog name
- `:silver_catalog` - Source silver catalog name  
- `:gold_catalog` - Target gold catalog name
- `:schema_name` - Schema name for all objects
- `:environment` - Environment identifier (dev/prod)

### Manual (SQL Warehouse)
1. Open Databricks SQL workspace
2. Create a new query
3. Copy/paste content from any of the SQL scripts
4. Update variables at the top:
   ```sql
   SET VAR.silver_catalog = 'silver';
   SET VAR.gold_catalog = 'gold';
   SET VAR.schema_name = 'nyc311';
   SET VAR.environment = 'dev';
   ```
5. Execute the script

## Tables Created

### Dimension Tables
- `dim_date` - Date dimension with fiscal year, seasons, weekends
- `dim_agency` - Agency dimension with types and emergency service flags
- `dim_location` - Location dimension with regions and coordinate flags
- `dim_complaint_type` - Complaint type dimension with categories and priority scores

### Fact Table
- `fact_service_requests` - Main fact table with all metrics and foreign keys

### Views
- `vw_service_requests_powerbi` - Denormalized view for Power BI consumption

## Benefits of SQL Approach
- No Spark task limitations (bypasses Free Edition 5 concurrent task limit)
- Faster execution on SQL warehouses
- Simpler debugging and monitoring
- Automatic query optimization
- No memory management issues
