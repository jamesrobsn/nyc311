-- NYC 311 Gold Layer - Free Edition Compatible Approach
-- Master Script - Processes data in monthly slices to avoid task limits
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
-- Step 2: Create Dimension Tables (These are small, no task limit issues)
-- =============================================================================

SELECT 'Step 2: Creating dimension tables...' as status;

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
-- Step 3: Create Empty Fact Table with Proper Schema
-- =============================================================================

SELECT 'Step 3: Creating empty fact table...' as status;

-- Create an empty table with the final schema
CREATE OR REPLACE TABLE IDENTIFIER(CONCAT(:gold_catalog, '.', :schema_name, '.fact_service_requests'))
AS SELECT * FROM (
  SELECT
    CAST(NULL AS string)    AS service_request_key,
    CAST(NULL AS int)       AS date_key,
    CAST(NULL AS int)       AS agency_key,
    CAST(NULL AS int)       AS location_key,
    CAST(NULL AS int)       AS complaint_key,
    CAST(NULL AS timestamp) AS created_date,
    CAST(NULL AS timestamp) AS closed_date,
    CAST(NULL AS timestamp) AS due_date,
    CAST(NULL AS double)    AS response_time_hours,
    CAST(NULL AS string)    AS response_time_category,
    CAST(NULL AS boolean)   AS is_weekend,
    CAST(NULL AS int)       AS is_closed,
    CAST(NULL AS int)       AS is_resolved,
    CAST(NULL AS int)       AS resolved_same_day,
    CAST(NULL AS int)       AS resolved_within_week,
    CAST(NULL AS string)    AS status,
    CAST(NULL AS string)    AS open_data_channel_type,
    CAST(NULL AS string)    AS resolution_description,
    CAST(NULL AS timestamp) AS silver_processed_ts,
    CAST(NULL AS timestamp) AS gold_processed_ts,
    CAST(NULL AS string)    AS environment
) WHERE 1=0;

-- Set table properties to avoid optimization issues on Free Edition
ALTER TABLE IDENTIFIER(CONCAT(:gold_catalog, '.', :schema_name, '.fact_service_requests'))
SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'false',
  'delta.autoOptimize.autoCompact'   = 'false'
);

-- =============================================================================
-- Step 4: Get Month Boundaries for Processing
-- =============================================================================

SELECT 'Step 4: Computing month boundaries...' as status;

-- Create a view with month boundaries for processing
CREATE OR REPLACE VIEW IDENTIFIER(CONCAT(:gold_catalog, '.', :schema_name, '.v_month_boundaries')) AS
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

-- Show the months that will be processed
SELECT 'Months to process:' as info, month_label
FROM IDENTIFIER(CONCAT(:gold_catalog, '.', :schema_name, '.v_month_boundaries'))
ORDER BY month_start;

-- =============================================================================
-- Step 5: Process Data Month by Month (Manual Implementation)
-- =============================================================================

SELECT 'Step 5: Ready for month-by-month processing...' as status;
SELECT 'Use the companion script create_fact_table_monthly.sql to load data in batches' as instruction;

-- =============================================================================
-- Step 6: Create Power BI View
-- =============================================================================

SELECT 'Step 6: Creating Power BI view...' as status;

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
-- Final Status
-- =============================================================================

SELECT 'NYC 311 Gold Layer Setup Complete!' as message;
SELECT 'Dimensions created, fact table structure ready' as status;
SELECT 'Use create_fact_table_monthly.sql to populate fact table in batches' as next_step;
