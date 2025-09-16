# Power BI Integration Guide

This guide explains how to connect Power BI to the NYC 311 data pipeline and create a production-ready star schema model.

## Prerequisites

- Power BI Desktop or Power BI Service
- Access to Databricks workspace
- Deployed NYC 311 pipeline with gold layer tables
- Databricks connector for Power BI

## Connection Setup

1. **Get SQL Endpoint Details:**
   - Go to Databricks workspace → SQL → SQL Warehouses
   - Note the server hostname and HTTP path

2. **Connect from Power BI Desktop:**
   - Open Power BI Desktop
   - Get Data → More → Databricks
   - Enter server hostname and HTTP path
   - Use Azure Active Directory or Personal Access Token authentication

3. **Select Gold Layer Tables:**
   ```
   Catalog: gold
   Schema: nyc311
   ```

## Data Model Setup

### Tables to Import

**Fact Table:**
- `fact_service_requests` → Rename to "Fact Service Requests"

**Dimension Tables:**
- `dim_date` → Rename to "Date" (mark as Date table on [date])
- `dim_agency` → Rename to "Agency"
- `dim_location` → Rename to "Location"
- `dim_complaint_type` → Rename to "Complaint"

**Aggregate Tables (for fast visuals):**
- `agg_daily_summary` (date grain)
- `agg_geographic_summary` (neighborhood_zone)
- `agg_grid_heatmap` (grid_cell / lat_grid / lng_grid)
- `agg_complaint_performance`
- `agg_borough_comparison`

### Relationships (Single, Many-to-One)

Configure these relationships to filter from dimensions to fact:

```
fact_service_requests[date_key] → dim_date[date_key]
fact_service_requests[agency_key] → dim_agency[agency_key]
fact_service_requests[location_key] → dim_location[location_key]
fact_service_requests[complaint_key] → dim_complaint_type[complaint_key]
```

### Storage Strategy (Composite Model)

**Import Mode:** All `agg_*` tables (small & fast for KPIs)
**DirectQuery:** `fact_service_requests` (for drill-downs and detail pages)

**Aggregations Setup:**
- Map measures like `Sum(agg_daily_summary[total_requests])` to detail measure `[Total Requests]`
- Key aggregations on `date_key`, `borough`, `neighborhood_zone`
- Power BI will hit Import tables for speed, fall back to DirectQuery when needed

## DAX Measures

Create a dedicated "Measures" table and add these production-ready DAX measures:

### Base Counters

```dax
Total Requests = 
COUNTROWS ( fact_service_requests )

Closed Requests = 
SUM ( fact_service_requests[is_closed] )

Open Requests = 
[Total Requests] - [Closed Requests]

Requests with Coordinates = 
SUM ( fact_service_requests[has_valid_coordinates] )

Distinct Complaint Types = 
DISTINCTCOUNT ( fact_service_requests[complaint_key] )

Distinct Agencies = 
DISTINCTCOUNT ( fact_service_requests[agency_key] )
```

### Rates & KPIs

```dax
Closure Rate % = 
DIVIDE ( [Closed Requests], [Total Requests] )

Coordinate Coverage % = 
DIVIDE ( [Requests with Coordinates], [Total Requests] )

Avg Response Time (hrs) = 
AVERAGE ( fact_service_requests[response_time_hours] )

Median Response Time (hrs) = 
MEDIAN ( fact_service_requests[response_time_hours] )

P95 Response Time (hrs) = 
PERCENTILEX.INC (
    VALUES ( fact_service_requests[service_request_key] ),
    fact_service_requests[response_time_hours],
    0.95
)

Same Day Resolutions = 
SUM ( fact_service_requests[resolved_same_day] )

Same Day Rate % = 
DIVIDE ( [Same Day Resolutions], [Total Requests] )

Week Resolutions = 
SUM ( fact_service_requests[resolved_within_week] )

Week Resolution Rate % = 
DIVIDE ( [Week Resolutions], [Total Requests] )

Weekend Requests = 
SUM ( fact_service_requests[is_weekend] )

Weekend Share % = 
DIVIDE ( [Weekend Requests], [Total Requests] )
```

### Date Intelligence (uses dim_date[date])

```dax
Requests MTD = 
CALCULATE ( [Total Requests], DATESMTD ( dim_date[date] ) )

Requests YTD = 
CALCULATE ( [Total Requests], DATESYTD ( dim_date[date] ) )

Requests LY = 
CALCULATE ( [Total Requests], DATEADD ( dim_date[date], -1, YEAR ) )

YoY Δ (Requests) = 
[Total Requests] - [Requests LY]

YoY % (Requests) = 
DIVIDE ( [YoY Δ (Requests)], [Requests LY] )

7-Day Avg Requests = 
AVERAGEX (
    DATESINPERIOD ( dim_date[date], MAX ( dim_date[date] ), -7, DAY ),
    [Total Requests]
)
```

### SLA & Aging Measures

```dax
-- For calculated columns on fact_service_requests (if using Import)
Age (days) = 
DATEDIFF (
    fact_service_requests[created_date],
    COALESCE ( fact_service_requests[closed_date], TODAY() ),
    DAY
)

Age Bucket = 
VAR d = fact_service_requests[Age (days)]
RETURN
SWITCH ( TRUE(),
    d <= 1, "0–1d",
    d <= 7, "2–7d", 
    d <= 30, "8–30d",
    d <= 90, "31–90d",
    "90d+"
)

-- SLA compliance with parameter (create What-if parameter "SLA Hours")
Within SLA (by parameter) % = 
VAR hrs = SELECTEDVALUE ( 'SLA Hours'[SLA Hours Value], 24 )
VAR met = 
    CALCULATE (
        COUNTROWS ( fact_service_requests ),
        fact_service_requests[response_time_hours] <= hrs
    )
RETURN DIVIDE ( met, [Total Requests] )
```

### Freshness/Admin Measures

```dax
Last Gold Processed (UTC) = 
MAX ( fact_service_requests[gold_processed_ts] )

Last Silver Processed (UTC) = 
MAX ( fact_service_requests[silver_processed_ts] )
```

## Best Practices

### Performance Optimization
1. **Use composite model** - Import aggregates, DirectQuery for details
2. **Implement aggregations** - Map summary tables to detail measures
3. **Optimize DAX** - Avoid complex calculated columns in DirectQuery
4. **Filter at source** - Use table filters when possible

### Data Freshness
- **Monitor pipeline timestamps** using admin measures
- **Set up alerts** for data freshness issues
- **Schedule refresh** after gold layer completion (around 3:00 AM)

### Dashboard Design
- **Start with aggregates** for overview KPIs
- **Drill-through** to fact table for details
- **Use geographic features** for spatial analysis
- **Implement time intelligence** for trending

## Troubleshooting

### Common Issues

**Connection Timeout:**
- Use smaller date ranges initially
- Implement proper aggregations
- Consider DirectQuery for large fact table

**Slow Performance:**
- Check aggregation mappings
- Optimize DAX measures
- Use Import mode for dimensions

**Data Freshness:**
- Verify pipeline completion before refresh
- Check admin measures for last processed timestamps
- Set up refresh notifications

This production-ready Power BI model provides comprehensive analytics capabilities for NYC 311 data with optimal performance and flexibility.
