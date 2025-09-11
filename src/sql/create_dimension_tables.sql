-- NYC 311 Gold Layer - Dimension Tables Creation
-- SQL Warehouse Script
-- Run this directly in a SQL warehouse to create all dimension tables

-- =============================================================================
-- Configuration Variables (Update these as needed)
-- =============================================================================

-- Set your catalog and schema names here
SET VAR.silver_catalog = 'silver';
SET VAR.gold_catalog = 'gold';
SET VAR.schema_name = 'nyc311';

-- =============================================================================
-- Create Gold Catalog and Schema
-- =============================================================================

CREATE CATALOG IF NOT EXISTS ${VAR.gold_catalog};
USE CATALOG ${VAR.gold_catalog};
CREATE SCHEMA IF NOT EXISTS ${VAR.schema_name};
USE SCHEMA ${VAR.schema_name};

-- =============================================================================
-- Create Date Dimension
-- =============================================================================

SELECT 'Creating date dimension...' as status;

CREATE OR REPLACE TABLE ${VAR.gold_catalog}.${VAR.schema_name}.dim_date
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
FROM ${VAR.silver_catalog}.${VAR.schema_name}.service_requests_silver
WHERE created_date IS NOT NULL;

-- =============================================================================
-- Create Agency Dimension
-- =============================================================================

SELECT 'Creating agency dimension...' as status;

CREATE OR REPLACE TABLE ${VAR.gold_catalog}.${VAR.schema_name}.dim_agency
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
    SELECT DISTINCT
        agency,
        agency_name
    FROM ${VAR.silver_catalog}.${VAR.schema_name}.service_requests_silver
    WHERE agency IS NOT NULL
);

-- =============================================================================
-- Create Location Dimension
-- =============================================================================

SELECT 'Creating location dimension...' as status;

CREATE OR REPLACE TABLE ${VAR.gold_catalog}.${VAR.schema_name}.dim_location
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
    SELECT DISTINCT
        borough,
        incident_zip,
        community_board,
        latitude,
        longitude
    FROM ${VAR.silver_catalog}.${VAR.schema_name}.service_requests_silver
    WHERE borough IS NOT NULL
);

-- =============================================================================
-- Create Complaint Type Dimension
-- =============================================================================

SELECT 'Creating complaint type dimension...' as status;

CREATE OR REPLACE TABLE ${VAR.gold_catalog}.${VAR.schema_name}.dim_complaint_type
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
    SELECT DISTINCT
        complaint_type,
        descriptor,
        complaint_priority
    FROM ${VAR.silver_catalog}.${VAR.schema_name}.service_requests_silver
    WHERE complaint_type IS NOT NULL
);

-- =============================================================================
-- Verify All Dimension Tables
-- =============================================================================

SELECT 'Dimension tables creation completed!' as status;

SELECT 'dim_date' as table_name, COUNT(*) as record_count FROM ${VAR.gold_catalog}.${VAR.schema_name}.dim_date
UNION ALL
SELECT 'dim_agency' as table_name, COUNT(*) as record_count FROM ${VAR.gold_catalog}.${VAR.schema_name}.dim_agency
UNION ALL
SELECT 'dim_location' as table_name, COUNT(*) as record_count FROM ${VAR.gold_catalog}.${VAR.schema_name}.dim_location
UNION ALL
SELECT 'dim_complaint_type' as table_name, COUNT(*) as record_count FROM ${VAR.gold_catalog}.${VAR.schema_name}.dim_complaint_type;

-- =============================================================================
-- Sample Data from Each Dimension
-- =============================================================================

SELECT 'Sample from dim_date:' as info;
SELECT * FROM ${VAR.gold_catalog}.${VAR.schema_name}.dim_date LIMIT 5;

SELECT 'Sample from dim_agency:' as info;
SELECT * FROM ${VAR.gold_catalog}.${VAR.schema_name}.dim_agency LIMIT 5;

SELECT 'Sample from dim_location:' as info;
SELECT * FROM ${VAR.gold_catalog}.${VAR.schema_name}.dim_location LIMIT 5;

SELECT 'Sample from dim_complaint_type:' as info;
SELECT * FROM ${VAR.gold_catalog}.${VAR.schema_name}.dim_complaint_type LIMIT 5;

-- =============================================================================
-- Success Message
-- =============================================================================

SELECT 
    'SUCCESS: All dimension tables created successfully!' as message,
    current_timestamp() as completion_time;
