# NYC 311 Data Pipeline

This directory contains the production NYC 311 data pipeline implementation and examples for future pipeline development.

## Current Implementation

### NYC 311 Service Requests Pipeline
- **Status**: ✅ Production Ready
- **Source**: NYC 311 Socrata API
- **Structure**: Bronze → Silver → Gold (Python notebooks)
- **Files**:
  - `nyc311/nyc311_bronze_ingest.py` - Raw data ingestion from NYC 311 API
  - `nyc311/nyc311_silver_transform.py` - Data cleaning and standardization
  - `nyc311/nyc311_gold_star_schema.py` - Star schema for analytics
- **Integration**: Connected to Power BI dashboard

## Future Pipeline Examples

### 1. NYC Taxi Trip Data
- **Source**: NYC TLC Trip Record Data
- **Structure**: Similar bronze/silver/gold pattern
- **Files**:
  - `nyc_taxi_bronze_ingest.py`
  - `nyc_taxi_silver_transform.py` 
  - `nyc_taxi_gold_analytics.py`

### 2. NYC Weather Data
- **Source**: NOAA Weather API
- **Structure**: Time-series optimized
- **Files**:
  - `nyc_weather_bronze_ingest.py`
  - `nyc_weather_silver_transform.py`
  - `nyc_weather_gold_timeseries.py`

### 3. NYC Real Estate Data
- **Source**: NYC Department of Finance
- **Structure**: Property dimension focused
- **Files**:
  - `nyc_realestate_bronze_ingest.py`
  - `nyc_realestate_silver_transform.py`
  - `nyc_realestate_gold_market_analysis.py`

## Pipeline Naming Convention

All pipeline files should follow the pattern:
`{data_source}_{layer}_{purpose}.py`

Examples:
- `nyc311_bronze_ingest.py`
- `nyc_taxi_silver_transform.py`
- `nyc_weather_gold_timeseries.py`

This ensures clear identification and prevents naming conflicts when multiple teams work on different data sources.
