# Future Pipeline: NYC Taxi Data

This folder structure demonstrates how to add additional data pipelines to the same repository.

## Suggested Future Pipelines

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
