-- NYC 311 Gold Layer - Complete SQL Warehouse Setup
-- Master Script - Run this via Databricks bundle SQL task
-- Requires: silver.nyc311.service_requests_silver table to exist

-- =============================================================================
-- PARAMETERS: Automatically provided by Databricks job
-- =============================================================================
-- :bronze_catalog - Bronze catalog name
-- :silver_catalog - Silver catalog name  
-- :gold_catalog - Gold catalog name
-- :schema_name - Schema name for NYC 311 data
-- :environment - Environment (dev/prod)

-- =============================================================================
-- Step 1: Create Gold Catalog and Schema
-- =============================================================================

CREATE CATALOG IF NOT EXISTS IDENTIFIER(:gold_catalog);
USE CATALOG IDENTIFIER(:gold_catalog);
CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:schema_name);
USE SCHEMA IDENTIFIER(:schema_name);

SELECT 'Step 1: Gold catalog and schema ready' as status;

-- =============================================================================
-- Step 2: Verify Silver Data Exists
-- =============================================================================

SELECT 'Step 2: Verifying silver data...' as status;

SELECT 
    'Silver data check' as check_type,
    COUNT(*) as record_count,
    MIN(created_date) as earliest_date,
    MAX(created_date) as latest_date
FROM IDENTIFIER(CONCAT(:silver_catalog, '.', :schema_name, '.service_requests_silver'));

-- =============================================================================
-- Step 3: Create Dimension Tables
-- =============================================================================

SELECT 'Step 3: Creating dimension tables...' as status;

-- Date Dimension
CREATE OR REPLACE TABLE IDENTIFIER(CONCAT(:gold_catalog, '.', :schema_name, '.dim_date'))
USING DELTA
AS
SELECT DISTINCT
    CAST(date_format(created_date, 'yyyyMMdd') AS INT) as date_key,
    CAST(created_date AS DATE) as date,
    YEAR(created_date) as year,
    QUARTER(created_date) as quarter,
    MONTH(created_date) as month,
    date_format(created_date, 'MMMM') as month_name,
    DAYOFMONTH(created_date) as day,
    DAYOFWEEK(created_date) as day_of_week,
    date_format(created_date, 'EEEE') as day_name,
    WEEKOFYEAR(created_date) as week_of_year,
    DAYOFWEEK(created_date) IN (1, 7) as is_weekend,
    NOT DAYOFWEEK(created_date) IN (1, 7) as is_weekday,
    CASE 
        WHEN MONTH(created_date) >= 7 THEN YEAR(created_date) + 1 
        ELSE YEAR(created_date) 
    END as fiscal_year,
    CASE 
        WHEN MONTH(created_date) IN (12, 1, 2) THEN 'Winter'
        WHEN MONTH(created_date) IN (3, 4, 5) THEN 'Spring'
        WHEN MONTH(created_date) IN (6, 7, 8) THEN 'Summer'
        WHEN MONTH(created_date) IN (9, 10, 11) THEN 'Fall'
    END as season
FROM IDENTIFIER(CONCAT(:silver_catalog, '.', :schema_name, '.service_requests_silver'))
WHERE created_date IS NOT NULL;

-- Agency Dimension
CREATE OR REPLACE TABLE IDENTIFIER(CONCAT(:gold_catalog, '.', :schema_name, '.dim_agency'))
USING DELTA
AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY agency) as agency_key,
    agency,
    agency_name,
    CASE 
        WHEN agency IN ('NYPD', 'FDNY', 'EMS') THEN 'Emergency Services'
        WHEN agency IN ('DOT', 'DEP', 'DSNY') THEN 'Infrastructure'
        WHEN agency IN ('HPD', 'DOB') THEN 'Housing & Buildings'
        WHEN agency IN ('DOHMH', 'ACS') THEN 'Health & Social Services'
        WHEN agency IN ('DPR', 'DOE') THEN 'Parks & Education'
        ELSE 'Other'
    END as agency_type,
    agency IN ('NYPD', 'FDNY', 'EMS') as is_emergency_service
FROM (
    SELECT DISTINCT agency, agency_name
    FROM IDENTIFIER(CONCAT(:silver_catalog, '.', :schema_name, '.service_requests_silver'))
    WHERE agency IS NOT NULL
);

-- Location Dimension
CREATE OR REPLACE TABLE IDENTIFIER(CONCAT(:gold_catalog, '.', :schema_name, '.dim_location'))
USING DELTA
AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY borough, incident_zip) as location_key,
    borough,
    incident_zip,
    community_board,
    latitude,
    longitude,
    CASE 
        WHEN borough = 'Manhattan' THEN 'Manhattan'
        WHEN borough IN ('Brooklyn', 'Queens') THEN 'Western Long Island'
        WHEN borough = 'Bronx' THEN 'Bronx'
        WHEN borough = 'Staten Island' THEN 'Staten Island'
        ELSE 'Unknown'
    END as region,
    (latitude IS NOT NULL AND longitude IS NOT NULL) as has_coordinates
FROM (
    SELECT DISTINCT borough, incident_zip, community_board, latitude, longitude
    FROM IDENTIFIER(CONCAT(:silver_catalog, '.', :schema_name, '.service_requests_silver'))
    WHERE borough IS NOT NULL
);

-- Complaint Type Dimension
CREATE OR REPLACE TABLE IDENTIFIER(CONCAT(:gold_catalog, '.', :schema_name, '.dim_complaint_type'))
USING DELTA
AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY complaint_type) as complaint_key,
    complaint_type,
    descriptor,
    complaint_priority,
    CASE 
        WHEN complaint_type LIKE '%Noise%' THEN 'Noise'
        WHEN complaint_type LIKE '%Water%' THEN 'Water/Sewer'
        WHEN complaint_type LIKE '%Heat%' THEN 'Housing'
        WHEN complaint_type LIKE '%Street%' THEN 'Transportation'
        WHEN complaint_type LIKE '%Parking%' THEN 'Parking'
        WHEN complaint_type LIKE '%Animal%' THEN 'Animal Services'
        WHEN complaint_type LIKE '%Homeless%' THEN 'Social Services'
        ELSE 'Other'
    END as complaint_category,
    CASE 
        WHEN complaint_priority = 'Critical' THEN 4
        WHEN complaint_priority = 'High' THEN 3
        WHEN complaint_priority = 'Medium' THEN 2
        WHEN complaint_priority = 'Low' THEN 1
        ELSE 0
    END as priority_score
FROM (
    SELECT DISTINCT complaint_type, descriptor, complaint_priority
    FROM IDENTIFIER(CONCAT(:silver_catalog, '.', :schema_name, '.service_requests_silver'))
    WHERE complaint_type IS NOT NULL
);

-- =============================================================================
-- Step 4: Create Fact Table
-- =============================================================================

SELECT 'Step 4: Creating fact table...' as status;

CREATE OR REPLACE TABLE IDENTIFIER(CONCAT(:gold_catalog, '.', :schema_name, '.fact_service_requests'))
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
    :environment as environment

FROM IDENTIFIER(CONCAT(:silver_catalog, '.', :schema_name, '.service_requests_silver')) s

-- Join with dimensions using LEFT JOINs
LEFT JOIN IDENTIFIER(CONCAT(:gold_catalog, '.', :schema_name, '.dim_date')) d 
    ON CAST(s.created_date AS DATE) = d.date

LEFT JOIN IDENTIFIER(CONCAT(:gold_catalog, '.', :schema_name, '.dim_agency')) a 
    ON s.agency = a.agency

LEFT JOIN IDENTIFIER(CONCAT(:gold_catalog, '.', :schema_name, '.dim_location')) l 
    ON s.borough = l.borough

LEFT JOIN IDENTIFIER(CONCAT(:gold_catalog, '.', :schema_name, '.dim_complaint_type')) c 
    ON s.complaint_type = c.complaint_type;

-- =============================================================================
-- Step 5: Create Power BI View
-- =============================================================================

SELECT 'Step 5: Creating Power BI view...' as status;

CREATE OR REPLACE VIEW IDENTIFIER(CONCAT(:gold_catalog, '.', :schema_name, '.vw_service_requests_powerbi')) AS
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
FROM IDENTIFIER(CONCAT(:gold_catalog, '.', :schema_name, '.fact_service_requests')) f
LEFT JOIN IDENTIFIER(CONCAT(:gold_catalog, '.', :schema_name, '.dim_date')) d ON f.date_key = d.date_key
LEFT JOIN IDENTIFIER(CONCAT(:gold_catalog, '.', :schema_name, '.dim_agency')) a ON f.agency_key = a.agency_key
LEFT JOIN IDENTIFIER(CONCAT(:gold_catalog, '.', :schema_name, '.dim_location')) l ON f.location_key = l.location_key
LEFT JOIN IDENTIFIER(CONCAT(:gold_catalog, '.', :schema_name, '.dim_complaint_type')) c ON f.complaint_key = c.complaint_key;

-- =============================================================================
-- Step 6: Final Verification
-- =============================================================================

SELECT 'Step 6: Final verification...' as status;

SELECT 
    'NYC 311 Gold Layer Complete!' as message,
    current_timestamp() as completion_time,
    :environment as environment,
    (SELECT COUNT(*) FROM IDENTIFIER(CONCAT(:gold_catalog, '.', :schema_name, '.fact_service_requests'))) as fact_records,
    (SELECT COUNT(*) FROM IDENTIFIER(CONCAT(:gold_catalog, '.', :schema_name, '.dim_date'))) as date_records,
    (SELECT COUNT(*) FROM IDENTIFIER(CONCAT(:gold_catalog, '.', :schema_name, '.dim_agency'))) as agency_records,
    (SELECT COUNT(*) FROM IDENTIFIER(CONCAT(:gold_catalog, '.', :schema_name, '.dim_location'))) as location_records,
    (SELECT COUNT(*) FROM IDENTIFIER(CONCAT(:gold_catalog, '.', :schema_name, '.dim_complaint_type'))) as complaint_records;
