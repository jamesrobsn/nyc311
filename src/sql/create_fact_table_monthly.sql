-- NYC 311 Gold Layer - Monthly Fact Table Loading
-- This script loads fact data month by month to avoid Free Edition task limits
-- Run this after create_gold_layer_free_edition.sql

-- =============================================================================
-- PARAMETERS: Automatically provided by Databricks job
-- =============================================================================
-- :silver_catalog - Silver catalog name  
-- :gold_catalog - Gold catalog name
-- :schema_name - Schema name for NYC 311 data
-- :environment - Environment (dev/prod)

USE CATALOG IDENTIFIER(:gold_catalog);
USE SCHEMA IDENTIFIER(:schema_name);

-- =============================================================================
-- Get Month List for Processing
-- =============================================================================

-- This shows the months we need to process
SELECT 'Available months to process:' as info;
SELECT month_label, month_start, month_end 
FROM IDENTIFIER(CONCAT(:gold_catalog, '.', :schema_name, '.v_month_boundaries'))
ORDER BY month_start;

-- =============================================================================
-- Month-by-Month Insert Template
-- =============================================================================
-- Copy and modify the INSERT statement below for each month
-- Replace :month_start with actual date like '2023-01-01'

-- Example for January 2023 (modify dates as needed):
/*
INSERT INTO IDENTIFIER(CONCAT(:gold_catalog, '.', :schema_name, '.fact_service_requests'))
SELECT /*+ REPARTITION(1) BROADCAST(d) BROADCAST(a) BROADCAST(l) BROADCAST(c) */
    s.unique_key                                           AS service_request_key,
    COALESCE(d.date_key, -1)                              AS date_key,
    COALESCE(a.agency_key, -1)                            AS agency_key,
    COALESCE(l.location_key, -1)                          AS location_key,
    COALESCE(c.complaint_key, -1)                         AS complaint_key,
    s.created_date, 
    s.closed_date, 
    s.due_date,
    s.response_time_hours, 
    s.response_time_category,
    s.is_weekend,
    CASE WHEN s.status = 'CLOSED' THEN 1 ELSE 0 END       AS is_closed,
    CASE WHEN s.closed_date IS NOT NULL THEN 1 ELSE 0 END AS is_resolved,
    CASE WHEN s.response_time_hours <= 24 THEN 1 ELSE 0 END AS resolved_same_day,
    CASE WHEN s.response_time_hours <= 168 THEN 1 ELSE 0 END AS resolved_within_week,
    s.status, 
    s.open_data_channel_type, 
    s.resolution_description,
    s.silver_processed_ts,
    current_timestamp()                                   AS gold_processed_ts,
    :environment                                          AS environment
FROM   IDENTIFIER(CONCAT(:silver_catalog, '.', :schema_name, '.service_requests_silver')) s
LEFT JOIN IDENTIFIER(CONCAT(:gold_catalog, '.', :schema_name, '.dim_date'))           d ON CAST(s.created_date AS DATE) = d.date
LEFT JOIN IDENTIFIER(CONCAT(:gold_catalog, '.', :schema_name, '.dim_agency'))         a ON s.agency = a.agency
LEFT JOIN IDENTIFIER(CONCAT(:gold_catalog, '.', :schema_name, '.dim_location'))       l ON s.borough = l.borough
LEFT JOIN IDENTIFIER(CONCAT(:gold_catalog, '.', :schema_name, '.dim_complaint_type')) c ON s.complaint_type = c.complaint_type
WHERE  s.created_date >= '2023-01-01'
  AND  s.created_date <  '2023-02-01';
*/

-- =============================================================================
-- Automated Loop Version (Databricks SQL Scripting)
-- =============================================================================

DECLARE month_cursor CURSOR FOR 
    SELECT month_start, month_end, month_label
    FROM IDENTIFIER(CONCAT(:gold_catalog, '.', :schema_name, '.v_month_boundaries'))
    ORDER BY month_start;

FOR month_row IN month_cursor DO
    
    SELECT CONCAT('Processing month: ', month_row.month_label) as status;
    
    INSERT INTO IDENTIFIER(CONCAT(:gold_catalog, '.', :schema_name, '.fact_service_requests'))
    SELECT /*+ REPARTITION(1) BROADCAST(d) BROADCAST(a) BROADCAST(l) BROADCAST(c) */
        s.unique_key                                           AS service_request_key,
        COALESCE(d.date_key, -1)                              AS date_key,
        COALESCE(a.agency_key, -1)                            AS agency_key,
        COALESCE(l.location_key, -1)                          AS location_key,
        COALESCE(c.complaint_key, -1)                         AS complaint_key,
        s.created_date, 
        s.closed_date, 
        s.due_date,
        s.response_time_hours, 
        s.response_time_category,
        s.is_weekend,
        CASE WHEN s.status = 'CLOSED' THEN 1 ELSE 0 END       AS is_closed,
        CASE WHEN s.closed_date IS NOT NULL THEN 1 ELSE 0 END AS is_resolved,
        CASE WHEN s.response_time_hours <= 24 THEN 1 ELSE 0 END AS resolved_same_day,
        CASE WHEN s.response_time_hours <= 168 THEN 1 ELSE 0 END AS resolved_within_week,
        s.status, 
        s.open_data_channel_type, 
        s.resolution_description,
        s.silver_processed_ts,
        current_timestamp()                                   AS gold_processed_ts,
        :environment                                          AS environment
    FROM   IDENTIFIER(CONCAT(:silver_catalog, '.', :schema_name, '.service_requests_silver')) s
    LEFT JOIN IDENTIFIER(CONCAT(:gold_catalog, '.', :schema_name, '.dim_date'))           d ON CAST(s.created_date AS DATE) = d.date
    LEFT JOIN IDENTIFIER(CONCAT(:gold_catalog, '.', :schema_name, '.dim_agency'))         a ON s.agency = a.agency
    LEFT JOIN IDENTIFIER(CONCAT(:gold_catalog, '.', :schema_name, '.dim_location'))       l ON s.borough = l.borough
    LEFT JOIN IDENTIFIER(CONCAT(:gold_catalog, '.', :schema_name, '.dim_complaint_type')) c ON s.complaint_type = c.complaint_type
    WHERE  s.created_date >= month_row.month_start
      AND  s.created_date <  month_row.month_end;
    
    SELECT CONCAT('Completed month: ', month_row.month_label, ' - Records inserted: ', ROW_COUNT()) as result;

END FOR;

-- =============================================================================
-- Final Verification
-- =============================================================================

SELECT 'Monthly fact table loading complete!' as message;

SELECT 
    'Final verification' as check_type,
    COUNT(*) as total_records,
    MIN(created_date) as earliest_date,
    MAX(created_date) as latest_date,
    COUNT(DISTINCT date_format(created_date, 'yyyy-MM')) as months_processed
FROM IDENTIFIER(CONCAT(:gold_catalog, '.', :schema_name, '.fact_service_requests'));

-- Check file count (should be roughly equal to number of months)
DESCRIBE DETAIL IDENTIFIER(CONCAT(:gold_catalog, '.', :schema_name, '.fact_service_requests'));
