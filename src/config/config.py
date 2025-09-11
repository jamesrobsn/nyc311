"""
Shared configuration for NYC 311 Data Pipeline
"""

# Data source configuration
NYC_311_CONFIG = {
    "base_url": "https://data.cityofnewyork.us/resource/erm2-nwe9.json",
    "batch_size": 50000,  # Optimized for serverless compute
    "timeout": 300,
    "rate_limit_delay": 1,
    "max_retries": 3
}

# Catalog and schema configuration
CATALOGS = {
    "bronze": "bronze",
    "silver": "silver",
    "gold": "gold"
}

SCHEMA_NAME = "nyc311"

# Table names without nyc311 prefix since schema provides context
TABLES = {
    "bronze": {
        "service_requests": "service_requests"
    },
    "silver": {
        "service_requests": "service_requests_silver"
    },
    "gold": {
        "dim_date": "dim_date",
        "dim_agency": "dim_agency",
        "dim_location": "dim_location", 
        "dim_complaint_type": "dim_complaint_type",
        "fact_service_requests": "fact_service_requests",
        "agg_daily_summary": "agg_daily_summary",
        "agg_monthly_agency_summary": "agg_monthly_agency_summary",
        "agg_complaint_performance": "agg_complaint_performance",
        "agg_borough_comparison": "agg_borough_comparison",
        "powerbi_view": "vw_service_requests_powerbi"
    }
}

# Data quality thresholds
QUALITY_THRESHOLDS = {
    "min_records_bronze": 5000,  # Higher threshold for serverless
    "max_null_rate_unique_key": 0.01,  # 1%
    "max_null_rate_created_date": 0.05,  # 5%
    "valid_coord_min_rate": 0.70,  # 70%
    "max_response_time_hours": 8760  # 1 year
}

# Borough standardization mapping
BOROUGH_MAPPING = {
    "MANHATTAN": "Manhattan",
    "Manhattan": "Manhattan", 
    "NEW YORK": "Manhattan",
    "BROOKLYN": "Brooklyn",
    "Brooklyn": "Brooklyn",
    "KINGS": "Brooklyn",
    "QUEENS": "Queens",
    "Queens": "Queens",
    "BRONX": "Bronx",
    "Bronx": "Bronx",
    "THE BRONX": "Bronx",
    "STATEN ISLAND": "Staten Island",
    "Staten Island": "Staten Island",
    "RICHMOND": "Staten Island"
}

# Complaint priority mapping
COMPLAINT_PRIORITY_MAPPING = {
    'Water System': 'High',
    'HEATING': 'High', 
    'Emergency Response Team (ERT)': 'Critical',
    'Street Condition': 'Medium',
    'Noise - Residential': 'Low',
    'Blocked Driveway': 'Low',
    'Illegal Parking': 'Low'
}

# NYC coordinate bounds for validation
NYC_BOUNDS = {
    "lat_min": 40.4,
    "lat_max": 40.9,
    "lon_min": -74.3,
    "lon_max": -73.7
}
