-- NYC 311 Silver Layer Compaction - Optional Script
-- Run this to compact silver tables if they have many small files
-- This can improve gold layer performance by reducing scan tasks

-- =============================================================================
-- PARAMETERS: Automatically provided by Databricks job
-- =============================================================================
-- :silver_catalog - Silver catalog name  
-- :schema_name - Schema name for NYC 311 data

USE CATALOG IDENTIFIER(:silver_catalog);
USE SCHEMA IDENTIFIER(:schema_name);

-- =============================================================================
-- Check Current Silver Table Details
-- =============================================================================

SELECT 'Checking silver table file structure...' as status;

DESCRIBE DETAIL IDENTIFIER(CONCAT(:silver_catalog, '.', :schema_name, '.service_requests_silver'));

-- =============================================================================
-- Create Compacted Silver Table (Month by Month)
-- =============================================================================

SELECT 'Creating compacted silver table...' as status;

-- Create empty compacted table with same schema
CREATE OR REPLACE TABLE IDENTIFIER(CONCAT(:silver_catalog, '.', :schema_name, '.service_requests_silver_compact'))
AS SELECT * FROM (
  SELECT /*+ REPARTITION(1) */ *
  FROM   IDENTIFIER(CONCAT(:silver_catalog, '.', :schema_name, '.service_requests_silver'))
  WHERE  1=0
);

-- Get month boundaries from silver data
WITH bounds AS (
  SELECT date_trunc('month', min(created_date)) AS min_m,
         date_trunc('month', max(created_date)) AS max_m
  FROM   IDENTIFIER(CONCAT(:silver_catalog, '.', :schema_name, '.service_requests_silver'))
),
months AS (
  SELECT explode(sequence(min_m, max_m, interval 1 month)) AS month_start
  FROM bounds
)
SELECT 'Months to compact:' as info, date_format(month_start, 'yyyy-MM') as month_label
FROM months 
ORDER BY month_start;

-- =============================================================================
-- Monthly Compaction Loop
-- =============================================================================

DECLARE month_cursor CURSOR FOR 
    WITH bounds AS (
      SELECT date_trunc('month', min(created_date)) AS min_m,
             date_trunc('month', max(created_date)) AS max_m
      FROM   IDENTIFIER(CONCAT(:silver_catalog, '.', :schema_name, '.service_requests_silver'))
    ),
    months AS (
      SELECT explode(sequence(min_m, max_m, interval 1 month)) AS month_start
      FROM bounds
    )
    SELECT 
        month_start,
        add_months(month_start, 1) AS month_end,
        date_format(month_start, 'yyyy-MM') AS month_label
    FROM months 
    ORDER BY month_start;

FOR month_row IN month_cursor DO
    
    SELECT CONCAT('Compacting month: ', month_row.month_label) as status;
    
    -- Insert month data with single partition
    INSERT INTO IDENTIFIER(CONCAT(:silver_catalog, '.', :schema_name, '.service_requests_silver_compact'))
    SELECT /*+ REPARTITION(1) */ *
    FROM   IDENTIFIER(CONCAT(:silver_catalog, '.', :schema_name, '.service_requests_silver'))
    WHERE  created_date >= month_row.month_start
      AND  created_date <  month_row.month_end;
    
    SELECT CONCAT('Completed month: ', month_row.month_label, ' - Records: ', ROW_COUNT()) as result;

END FOR;

-- =============================================================================
-- Verification
-- =============================================================================

SELECT 'Compaction complete!' as message;

-- Compare original vs compacted
SELECT 'Original table:' as table_type, COUNT(*) as record_count
FROM IDENTIFIER(CONCAT(:silver_catalog, '.', :schema_name, '.service_requests_silver'))
UNION ALL
SELECT 'Compacted table:' as table_type, COUNT(*) as record_count  
FROM IDENTIFIER(CONCAT(:silver_catalog, '.', :schema_name, '.service_requests_silver_compact'));

-- Check file counts
SELECT 'File count comparison:' as info;
DESCRIBE DETAIL IDENTIFIER(CONCAT(:silver_catalog, '.', :schema_name, '.service_requests_silver'));
DESCRIBE DETAIL IDENTIFIER(CONCAT(:silver_catalog, '.', :schema_name, '.service_requests_silver_compact'));

-- =============================================================================
-- Usage Instructions
-- =============================================================================

SELECT 'Compaction completed successfully!' as message;
SELECT 'Update your gold layer scripts to use service_requests_silver_compact instead of service_requests_silver' as instruction;
