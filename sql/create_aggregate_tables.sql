-- NYC 311 Gold Layer - Aggregate Tables Creation
-- SQL Warehouse Script
-- Run this after creating dimension and fact tables

-- =============================================================================
-- Configuration Variables (Update these as needed)
-- =============================================================================

-- Set your catalog and schema names here
SET VAR.gold_catalog = 'gold';
SET VAR.schema_name = 'nyc311';

-- =============================================================================
-- Use Gold Catalog and Schema
-- =============================================================================

USE CATALOG ${VAR.gold_catalog};
USE SCHEMA ${VAR.schema_name};

-- =============================================================================
-- Create Daily Summary Aggregate
-- =============================================================================

SELECT 'Creating daily summary aggregate...' as status;

CREATE OR REPLACE TABLE ${VAR.gold_catalog}.${VAR.schema_name}.agg_daily_summary
USING DELTA
AS
SELECT 
    f.date_key,
    d.date as report_date,
    d.year,
    d.month,
    d.day_name,
    l.borough,
    a.agency,
    COUNT(*) as total_requests,
    SUM(f.is_closed) as closed_requests,
    AVG(f.response_time_hours) as avg_response_time_hours,
    SUM(f.resolved_same_day) as same_day_resolutions,
    COUNT(DISTINCT c.complaint_type) as unique_complaint_types,
    SUM(CASE WHEN d.is_weekend THEN 1 ELSE 0 END) as weekend_requests,
    ROUND((SUM(f.is_closed) * 100.0) / COUNT(*), 2) as closure_rate,
    ROUND((SUM(f.resolved_same_day) * 100.0) / COUNT(*), 2) as same_day_rate
FROM ${VAR.gold_catalog}.${VAR.schema_name}.fact_service_requests f
LEFT JOIN ${VAR.gold_catalog}.${VAR.schema_name}.dim_date d ON f.date_key = d.date_key
LEFT JOIN ${VAR.gold_catalog}.${VAR.schema_name}.dim_agency a ON f.agency_key = a.agency_key
LEFT JOIN ${VAR.gold_catalog}.${VAR.schema_name}.dim_location l ON f.location_key = l.location_key
LEFT JOIN ${VAR.gold_catalog}.${VAR.schema_name}.dim_complaint_type c ON f.complaint_key = c.complaint_key
GROUP BY 
    f.date_key, d.date, d.year, d.month, d.day_name, l.borough, a.agency;

-- =============================================================================
-- Create Monthly Agency Summary
-- =============================================================================

SELECT 'Creating monthly agency summary...' as status;

CREATE OR REPLACE TABLE ${VAR.gold_catalog}.${VAR.schema_name}.agg_monthly_agency_summary
USING DELTA
AS
SELECT 
    d.year,
    d.month,
    d.month_name,
    a.agency,
    a.agency_name,
    COUNT(*) as total_requests,
    SUM(f.is_closed) as closed_requests,
    AVG(f.response_time_hours) as avg_response_time_hours,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY f.response_time_hours) as median_response_time_hours,
    MIN(f.response_time_hours) as min_response_time_hours,
    MAX(f.response_time_hours) as max_response_time_hours,
    COUNT(DISTINCT c.complaint_type) as unique_complaint_types,
    ROUND((SUM(f.is_closed) * 100.0) / COUNT(*), 2) as closure_rate,
    CONCAT(CAST(d.year AS STRING), '-', LPAD(CAST(d.month AS STRING), 2, '0')) as year_month
FROM ${VAR.gold_catalog}.${VAR.schema_name}.fact_service_requests f
LEFT JOIN ${VAR.gold_catalog}.${VAR.schema_name}.dim_date d ON f.date_key = d.date_key
LEFT JOIN ${VAR.gold_catalog}.${VAR.schema_name}.dim_agency a ON f.agency_key = a.agency_key
LEFT JOIN ${VAR.gold_catalog}.${VAR.schema_name}.dim_complaint_type c ON f.complaint_key = c.complaint_key
GROUP BY 
    d.year, d.month, d.month_name, a.agency, a.agency_name;

-- =============================================================================
-- Create Complaint Performance Summary
-- =============================================================================

SELECT 'Creating complaint performance summary...' as status;

CREATE OR REPLACE TABLE ${VAR.gold_catalog}.${VAR.schema_name}.agg_complaint_performance
USING DELTA
AS
SELECT 
    c.complaint_type,
    c.complaint_priority,
    COUNT(*) as total_requests,
    SUM(f.is_closed) as closed_requests,
    AVG(f.response_time_hours) as avg_response_time_hours,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY f.response_time_hours) as median_response_time_hours,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY f.response_time_hours) as p95_response_time_hours,
    SUM(f.resolved_same_day) as same_day_resolutions,
    SUM(f.resolved_within_week) as week_resolutions,
    ROUND((SUM(f.is_closed) * 100.0) / COUNT(*), 2) as closure_rate,
    ROUND((SUM(f.resolved_same_day) * 100.0) / COUNT(*), 2) as same_day_rate,
    ROUND((SUM(f.resolved_within_week) * 100.0) / COUNT(*), 2) as week_resolution_rate
FROM ${VAR.gold_catalog}.${VAR.schema_name}.fact_service_requests f
LEFT JOIN ${VAR.gold_catalog}.${VAR.schema_name}.dim_complaint_type c ON f.complaint_key = c.complaint_key
GROUP BY 
    c.complaint_type, c.complaint_priority;

-- =============================================================================
-- Create Borough Comparison
-- =============================================================================

SELECT 'Creating borough comparison...' as status;

CREATE OR REPLACE TABLE ${VAR.gold_catalog}.${VAR.schema_name}.agg_borough_comparison
USING DELTA
AS
SELECT 
    l.borough,
    COUNT(*) as total_requests,
    SUM(f.is_closed) as closed_requests,
    AVG(f.response_time_hours) as avg_response_time_hours,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY f.response_time_hours) as median_response_time_hours,
    COUNT(DISTINCT a.agency) as agencies_involved,
    COUNT(DISTINCT c.complaint_type) as unique_complaint_types,
    SUM(f.resolved_same_day) as same_day_resolutions,
    ROUND((SUM(f.is_closed) * 100.0) / COUNT(*), 2) as closure_rate,
    ROUND((SUM(f.resolved_same_day) * 100.0) / COUNT(*), 2) as same_day_rate
FROM ${VAR.gold_catalog}.${VAR.schema_name}.fact_service_requests f
LEFT JOIN ${VAR.gold_catalog}.${VAR.schema_name}.dim_agency a ON f.agency_key = a.agency_key
LEFT JOIN ${VAR.gold_catalog}.${VAR.schema_name}.dim_location l ON f.location_key = l.location_key
LEFT JOIN ${VAR.gold_catalog}.${VAR.schema_name}.dim_complaint_type c ON f.complaint_key = c.complaint_key
GROUP BY 
    l.borough;

-- =============================================================================
-- Create Power BI View
-- =============================================================================

SELECT 'Creating Power BI view...' as status;

CREATE OR REPLACE VIEW ${VAR.gold_catalog}.${VAR.schema_name}.vw_service_requests_powerbi AS
SELECT 
    f.service_request_key,
    d.date,
    d.year,
    d.quarter,
    d.month,
    d.month_name,
    d.day_name,
    d.is_weekend,
    d.season,
    a.agency,
    a.agency_name,
    a.agency_type,
    a.is_emergency_service,
    l.borough,
    l.region,
    l.incident_zip,
    l.community_board,
    c.complaint_type,
    c.complaint_category,
    c.complaint_priority,
    c.priority_score,
    f.created_date,
    f.closed_date,
    f.response_time_hours,
    f.response_time_category,
    f.is_closed,
    f.is_resolved,
    f.resolved_same_day,
    f.resolved_within_week,
    f.status,
    f.open_data_channel_type
FROM ${VAR.gold_catalog}.${VAR.schema_name}.fact_service_requests f
LEFT JOIN ${VAR.gold_catalog}.${VAR.schema_name}.dim_date d ON f.date_key = d.date_key
LEFT JOIN ${VAR.gold_catalog}.${VAR.schema_name}.dim_agency a ON f.agency_key = a.agency_key
LEFT JOIN ${VAR.gold_catalog}.${VAR.schema_name}.dim_location l ON f.location_key = l.location_key
LEFT JOIN ${VAR.gold_catalog}.${VAR.schema_name}.dim_complaint_type c ON f.complaint_key = c.complaint_key;

-- =============================================================================
-- Verify All Aggregate Tables
-- =============================================================================

SELECT 'Aggregate tables creation completed!' as status;

SELECT 'agg_daily_summary' as table_name, COUNT(*) as record_count FROM ${VAR.gold_catalog}.${VAR.schema_name}.agg_daily_summary
UNION ALL
SELECT 'agg_monthly_agency_summary' as table_name, COUNT(*) as record_count FROM ${VAR.gold_catalog}.${VAR.schema_name}.agg_monthly_agency_summary
UNION ALL
SELECT 'agg_complaint_performance' as table_name, COUNT(*) as record_count FROM ${VAR.gold_catalog}.${VAR.schema_name}.agg_complaint_performance
UNION ALL
SELECT 'agg_borough_comparison' as table_name, COUNT(*) as record_count FROM ${VAR.gold_catalog}.${VAR.schema_name}.agg_borough_comparison;

-- =============================================================================
-- Sample Data from Aggregate Tables
-- =============================================================================

SELECT 'Sample from daily summary:' as info;
SELECT * FROM ${VAR.gold_catalog}.${VAR.schema_name}.agg_daily_summary 
ORDER BY report_date DESC, total_requests DESC 
LIMIT 10;

SELECT 'Sample from borough comparison:' as info;
SELECT * FROM ${VAR.gold_catalog}.${VAR.schema_name}.agg_borough_comparison 
ORDER BY total_requests DESC;

-- =============================================================================
-- Success Message
-- =============================================================================

SELECT 
    'SUCCESS: All aggregate tables and Power BI view created successfully!' as message,
    current_timestamp() as completion_time,
    'vw_service_requests_powerbi' as powerbi_view;
