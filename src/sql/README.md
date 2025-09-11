# NYC 311 Gold Layer SQL Scripts

This directory contains SQL scripts for creating the gold layer star schema directly via SQL Warehouse using Databricks SQL file tasks.

## Files

### Pipeline-Integrated Scripts
- **`create_gold_layer_complete.sql`** - Master script executed by Databricks bundle SQL task

### Standalone SQL Scripts (For Manual Execution)  
- **`create_dimension_tables.sql`** - Creates dimension tables only
- **`create_fact_table.sql`** - Creates fact table only  
- **`create_aggregate_tables.sql`** - Creates aggregate tables and Power BI view

## Pipeline Integration

The gold layer is automatically created as part of the Databricks pipeline using SQL file tasks:

### How It Works
1. **Databricks Bundle** deploys the SQL file to workspace  
2. **SQL Task** executes the script on SQL Warehouse with job parameters
3. **Named Parameters** (`:gold_catalog`, `:schema_name`, etc.) are automatically provided
4. **Direct Execution** bypasses Spark task limits on Free Edition accounts

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
