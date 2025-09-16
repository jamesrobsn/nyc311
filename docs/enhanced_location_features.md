# Enhanced Location Features for NYC 311 Data Model

## Overview

Your NYC 311 data model has been enhanced with detailed location data and geographic analysis capabilities. This document outlines the new features and how to use them for spatial analysis and mapping.

## New Location Fields

### Bronze Layer (Raw Data)
- `latitude` / `longitude`: Direct coordinate fields from the API
- `x_coordinate_state_plane` / `y_coordinate_state_plane`: New York State Plane coordinates  
- `location`: JSON field with additional coordinate data

### Silver Layer (Processed Data)

#### Enhanced Coordinate Processing
- `latitude_final` / `longitude_final`: Consolidated coordinates (prefers direct lat/lon, falls back to JSON)
- `has_coordinates`: Boolean flag indicating if valid coordinates exist
- `coordinates_quality`: Data quality assessment ("Valid NYC", "Invalid NYC", "Missing")
- `location_precision`: Precision level ("High", "Medium", "Low")

#### Geographic Enrichment
- `lat_grid` / `lng_grid`: Rounded coordinates for spatial aggregation (0.01 degree grid ≈ 1km)
- `grid_cell`: Combined grid identifier for spatial analysis
- `neighborhood_zone`: Approximate neighborhood classification based on coordinates

#### Spatial Grid System
The model creates a grid system dividing NYC into approximately 1km² cells:
- **Upper Manhattan**: Above 40.75°N
- **Midtown Manhattan**: 40.71°N - 40.75°N
- **Lower Manhattan**: 40.68°N - 40.71°N  
- **Brooklyn North/South**: Split at 40.65°N
- **Queens North/South**: Split at 40.72°N / 40.65°N
- **Bronx North/South**: Split at 40.82°N
- **Staten Island**: Below 40.65°N

### Gold Layer (Analytics-Ready)

#### Enhanced Location Dimension
The `dim_location` table now includes:
- `location_key`: Composite key based on borough, neighborhood, ZIP, and grid cell
- `borough` / `neighborhood_zone` / `incident_zip`: Geographic hierarchy
- `grid_cell` / `lat_grid` / `lng_grid`: Spatial grid references
- `region`: Higher-level geographic grouping
- `zone_type`: Area classification (Residential, Commercial, Financial, Mixed)
- `coordinates_quality` / `location_precision`: Data quality indicators
- `has_precise_location`: Boolean for filtering high-quality data

#### Enhanced Fact Table
The `fact_service_requests` table now includes:
- All coordinate fields for direct mapping
- Grid cell references for spatial aggregation
- `has_valid_coordinates`: Flag for filtering mappable records
- Neighborhood zone for mid-level geographic analysis

## New Aggregate Tables

### 1. Geographic Summary (`agg_geographic_summary`)
Summarizes requests by borough and neighborhood zone:
- Total requests per geographic area
- Response time metrics by location
- Closure rates by neighborhood
- Coordinate coverage statistics
- Average coordinates for area centroids
- Request density per grid cell

### 2. Grid Cell Heatmap (`agg_grid_heatmap`)
Optimized for heat map visualizations:
- Request counts per 1km grid cell
- Response time averages per cell
- Closure rates per cell
- Request density categories (High/Medium/Low/Very Low)
- Unique complaint types and agencies per cell

## Use Cases

### 1. Heat Map Visualizations
```sql
SELECT lat_grid, lng_grid, total_requests, avg_response_time_hours
FROM gold.nyc311.agg_grid_heatmap
WHERE total_requests >= 10
```

### 2. Neighborhood Analysis
```sql
SELECT neighborhood_zone, total_requests, closure_rate, 
       avg_latitude, avg_longitude
FROM gold.nyc311.agg_geographic_summary
ORDER BY total_requests DESC
```

### 3. High-Quality Coordinate Filtering
```sql
SELECT service_request_key, latitude, longitude, complaint_type
FROM gold.nyc311.fact_service_requests
WHERE has_valid_coordinates = 1
  AND coordinates_quality = 'Valid NYC'
```

### 4. Spatial Clustering Analysis
```sql
SELECT grid_cell, COUNT(*) as requests, 
       AVG(response_time_hours) as avg_response_time
FROM gold.nyc311.fact_service_requests
WHERE has_valid_coordinates = 1
GROUP BY grid_cell
HAVING COUNT(*) >= 50
ORDER BY requests DESC
```

## Power BI Integration

### Map Visualizations
1. **Scatter Plot Maps**: Use `latitude`/`longitude` from fact table
2. **Filled Maps**: Use borough/neighborhood from location dimension  
3. **Heat Maps**: Use grid cell data from `agg_grid_heatmap`

### Geographic Filters
- Borough → Neighborhood → Grid Cell hierarchy
- Coordinate quality filters for data reliability
- Request density categories for focus areas

### Calculated Fields
```dax
Coordinate Coverage % = 
    SUM(fact_service_requests[has_valid_coordinates]) / 
    COUNT(fact_service_requests[service_request_key]) * 100

Request Density = 
    SUM(agg_geographic_summary[total_requests]) / 
    SUM(agg_geographic_summary[unique_grid_cells])
```

## Data Quality Considerations

### Coordinate Validation
- Coordinates are validated to be within NYC boundaries (40.4-41.0°N, -74.3 to -73.7°W)
- Invalid coordinates are flagged but retained for analysis
- Multiple coordinate sources are consolidated with quality indicators

### Coverage Statistics
Track coordinate availability in your dashboards:
- Overall coordinate coverage percentage
- Coverage by borough/agency/complaint type
- Data quality trends over time

### Spatial Accuracy
- High precision: Direct lat/lon fields from API
- Medium precision: Parsed from location JSON
- Low precision: Missing coordinates

## Performance Optimizations

### Indexing
Tables are optimized with Z-ordering on spatial columns:
- Fact table: Z-ordered by (date_key, agency_key, location_key)
- Geographic summary: Z-ordered by (borough, neighborhood_zone)
- Grid heatmap: Z-ordered by (lat_grid, lng_grid)

### Partitioning
For large datasets, consider partitioning by:
- Borough for regional analysis
- Grid cell clusters for spatial queries
- Date ranges for temporal-spatial analysis

## Future Enhancements

Potential additions for advanced spatial analysis:
1. **Census Tract Integration**: Map to NYC census boundaries
2. **Distance Calculations**: Calculate distances to key facilities
3. **Demographic Enrichment**: Add population and socioeconomic data
4. **Transportation Analysis**: Proximity to subway stations/bus routes
5. **Real Estate Integration**: Property values and land use data

## Sample Queries

### Top 10 Busiest Grid Cells
```sql
SELECT TOP 10 grid_cell, lat_grid, lng_grid, total_requests, 
       closure_rate, request_density_category
FROM gold.nyc311.agg_grid_heatmap
ORDER BY total_requests DESC
```

### Response Time by Neighborhood
```sql
SELECT neighborhood_zone, AVG(avg_response_time_hours) as avg_response_time,
       SUM(total_requests) as total_requests
FROM gold.nyc311.agg_geographic_summary
GROUP BY neighborhood_zone
ORDER BY avg_response_time DESC
```

### Coordinate Quality Distribution
```sql
SELECT coordinates_quality, COUNT(*) as request_count,
       COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() as percentage
FROM gold.nyc311.fact_service_requests
GROUP BY coordinates_quality
```

This enhanced location model provides comprehensive spatial analysis capabilities while maintaining performance for large-scale analytics and visualization.
