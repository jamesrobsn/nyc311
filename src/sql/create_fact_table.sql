-- NYC 311 Gold Layer - Fact Table Creation
-- SQL Warehouse Script
-- Run this directly in a SQL warehouse to avoid Spark task limitations

-- =============================================================================
-- Configuration Variables (Update these as needed)
-- =============================================================================

-- Set your catalog and schema names here
SET VAR.silver_catalog = 'silver';
SET VAR.gold_catalog = 'gold';
SET VAR.schema_name = 'nyc311';
SET VAR.environment = 'dev';

-- =============================================================================
-- Create Gold Catalog and Schema (if they don't exist)
-- =============================================================================

CREATE CATALOG IF NOT EXISTS ${VAR.gold_catalog};
USE CATALOG ${VAR.gold_catalog};
CREATE SCHEMA IF NOT EXISTS ${VAR.schema_name};
USE SCHEMA ${VAR.schema_name};

-- =============================================================================
-- Verify Dimension Tables Exist
-- =============================================================================

-- Check that required dimension tables exist
SELECT 'Checking dimension tables...' as status;

SELECT 
    'dim_date' as table_name,
    COUNT(*) as record_count
FROM ${VAR.gold_catalog}.${VAR.schema_name}.dim_date

UNION ALL

SELECT 
    'dim_agency' as table_name,
    COUNT(*) as record_count
FROM ${VAR.gold_catalog}.${VAR.schema_name}.dim_agency

UNION ALL

SELECT 
    'dim_location' as table_name,
    COUNT(*) as record_count
FROM ${VAR.gold_catalog}.${VAR.schema_name}.dim_location

UNION ALL

SELECT 
    'dim_complaint_type' as table_name,
    COUNT(*) as record_count
FROM ${VAR.gold_catalog}.${VAR.schema_name}.dim_complaint_type;

-- =============================================================================
-- Create Fact Table
-- =============================================================================

SELECT 'Creating fact table...' as status;

CREATE OR REPLACE TABLE ${VAR.gold_catalog}.${VAR.schema_name}.fact_service_requests
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'false',
    'delta.autoOptimize.autoCompact' = 'false'
)
AS
SELECT 
    -- Business keys
    s.unique_key as service_request_key,
    
    -- Foreign keys to dimensions
    COALESCE(d.date_key, -1) as date_key,
    COALESCE(a.agency_key, -1) as agency_key,
    COALESCE(l.location_key, -1) as location_key,
    COALESCE(c.complaint_key, -1) as complaint_key,
    
    -- Timestamps
    s.created_date,
    s.closed_date,
    s.due_date,
    
    -- Measures and metrics
    s.response_time_hours,
    s.response_time_category,
    
    -- Flags and indicators
    s.is_weekend,
    CASE WHEN s.status = 'CLOSED' THEN 1 ELSE 0 END as is_closed,
    CASE WHEN s.closed_date IS NOT NULL THEN 1 ELSE 0 END as is_resolved,
    CASE WHEN s.response_time_hours <= 24 THEN 1 ELSE 0 END as resolved_same_day,
    CASE WHEN s.response_time_hours <= 168 THEN 1 ELSE 0 END as resolved_within_week,
    
    -- Additional attributes
    s.status,
    s.open_data_channel_type,
    s.resolution_description,
    
    -- Metadata
    s.silver_processed_ts,
    current_timestamp() as gold_processed_ts,
    '${VAR.environment}' as environment

FROM ${VAR.silver_catalog}.${VAR.schema_name}.service_requests_silver s

-- Join with dimensions using LEFT JOINs
LEFT JOIN ${VAR.gold_catalog}.${VAR.schema_name}.dim_date d 
    ON CAST(s.created_date AS DATE) = d.date

LEFT JOIN ${VAR.gold_catalog}.${VAR.schema_name}.dim_agency a 
    ON s.agency = a.agency

LEFT JOIN ${VAR.gold_catalog}.${VAR.schema_name}.dim_location l 
    ON s.borough = l.borough

LEFT JOIN ${VAR.gold_catalog}.${VAR.schema_name}.dim_complaint_type c 
    ON s.complaint_type = c.complaint_type;

-- =============================================================================
-- Verify Fact Table Creation
-- =============================================================================

SELECT 'Fact table creation completed!' as status;

SELECT 
    'fact_service_requests' as table_name,
    COUNT(*) as record_count,
    MIN(created_date) as earliest_date,
    MAX(created_date) as latest_date,
    COUNT(DISTINCT agency_key) as unique_agencies,
    COUNT(DISTINCT location_key) as unique_locations,
    COUNT(DISTINCT complaint_key) as unique_complaint_types,
    SUM(is_closed) as closed_requests,
    ROUND(AVG(response_time_hours), 2) as avg_response_hours
FROM ${VAR.gold_catalog}.${VAR.schema_name}.fact_service_requests;

-- =============================================================================
-- Data Quality Checks
-- =============================================================================

SELECT 'Running data quality checks...' as status;

-- Check for missing dimension keys
SELECT 
    'Missing Dimension Keys' as check_type,
    SUM(CASE WHEN date_key = -1 THEN 1 ELSE 0 END) as missing_date_keys,
    SUM(CASE WHEN agency_key = -1 THEN 1 ELSE 0 END) as missing_agency_keys,
    SUM(CASE WHEN location_key = -1 THEN 1 ELSE 0 END) as missing_location_keys,
    SUM(CASE WHEN complaint_key = -1 THEN 1 ELSE 0 END) as missing_complaint_keys
FROM ${VAR.gold_catalog}.${VAR.schema_name}.fact_service_requests;

-- Check for data distribution
SELECT 
    'Data Distribution' as check_type,
    COUNT(*) as total_records,
    COUNT(DISTINCT service_request_key) as unique_service_requests,
    COUNT(CASE WHEN is_closed = 1 THEN 1 END) as closed_count,
    COUNT(CASE WHEN resolved_same_day = 1 THEN 1 END) as same_day_count,
    COUNT(CASE WHEN resolved_within_week = 1 THEN 1 END) as within_week_count
FROM ${VAR.gold_catalog}.${VAR.schema_name}.fact_service_requests;

-- =============================================================================
-- Success Message
-- =============================================================================

SELECT 
    'SUCCESS: Fact table created successfully!' as message,
    current_timestamp() as completion_time,
    '${VAR.gold_catalog}.${VAR.schema_name}.fact_service_requests' as table_name;
