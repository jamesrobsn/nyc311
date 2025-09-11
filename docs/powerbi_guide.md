# Power BI Integration Guide

This guide explains how to connect Power BI to the NYC 311 data pipeline and create compelling visualizations.

## Prerequisites

- Power BI Desktop or Power BI Service
- Access to Databricks workspace
- Deployed NYC 311 pipeline with gold layer tables
- Databricks SQL connector for Power BI

## Connection Setup

### Option 1: Direct SQL Endpoint Connection

1. **Get SQL Endpoint Details:**
   - Go to Databricks workspace → SQL → SQL Warehouses
   - Note the server hostname and HTTP path

2. **Connect from Power BI Desktop:**
   - Open Power BI Desktop
   - Get Data → More → Databricks
   - Enter server hostname and HTTP path
   - Use Azure Active Directory or Personal Access Token authentication

3. **Select the Gold Layer View:**
   ```
   Catalog: gold
   Schema: nyc311
   Table: vw_service_requests_powerbi
   ```

### Option 2: ODBC Connection

1. **Install Databricks ODBC Driver**
2. **Configure DSN with connection details**
3. **Connect via Power BI ODBC connector**

## Data Model Setup

### Primary View
Use the denormalized view for easy reporting:
```sql
gold.nyc311.vw_service_requests_powerbi
```

This view includes all necessary fields pre-joined for performance.

### Key Fields for Analysis

**Time Dimensions:**
- `date` - Service request creation date
- `year`, `month`, `quarter` - Time hierarchies
- `day_name` - Day of week
- `season` - Seasonal analysis
- `is_weekend` - Weekend flag

**Geographic Dimensions:**
- `borough` - NYC borough
- `region` - Regional grouping
- `incident_zip` - ZIP code
- `community_board` - Community board

**Service Dimensions:**
- `agency` - Responsible agency
- `agency_name` - Full agency name
- `agency_type` - Agency categorization
- `complaint_type` - Type of complaint
- `complaint_category` - Grouped categories
- `complaint_priority` - Priority level

**Measures:**
- `response_time_hours` - Time to resolution
- `is_closed` - Closure status
- `is_resolved` - Resolution status
- `resolved_same_day` - Same-day resolution flag
- `resolved_within_week` - Week resolution flag

## Data Model Relationships

If using multiple tables instead of the view:

```
dim_date[date_key] → fact_service_requests[date_key]
dim_agency[agency_key] → fact_service_requests[agency_key]
dim_location[location_key] → fact_service_requests[location_key]
dim_complaint_type[complaint_key] → fact_service_requests[complaint_key]
```

## Key Measures (DAX)

### Basic Counts
```dax
Total Requests = COUNTROWS(vw_service_requests_powerbi)

Open Requests = 
CALCULATE(
    COUNTROWS(vw_service_requests_powerbi),
    vw_service_requests_powerbi[is_closed] = 0
)

Closed Requests = 
CALCULATE(
    COUNTROWS(vw_service_requests_powerbi),
    vw_service_requests_powerbi[is_closed] = 1
)
```

### Performance Metrics
```dax
Closure Rate = 
DIVIDE(
    [Closed Requests],
    [Total Requests],
    0
) * 100

Average Response Time = 
AVERAGE(vw_service_requests_powerbi[response_time_hours])

Same Day Resolution Rate = 
DIVIDE(
    CALCULATE(
        COUNTROWS(vw_service_requests_powerbi),
        vw_service_requests_powerbi[resolved_same_day] = 1
    ),
    [Total Requests],
    0
) * 100

Week Resolution Rate = 
DIVIDE(
    CALCULATE(
        COUNTROWS(vw_service_requests_powerbi),
        vw_service_requests_powerbi[resolved_within_week] = 1
    ),
    [Total Requests],
    0
) * 100
```

### Time Intelligence
```dax
Requests MTD = 
CALCULATE(
    [Total Requests],
    DATESMTD(vw_service_requests_powerbi[date])
)

Requests YTD = 
CALCULATE(
    [Total Requests],
    DATESYTD(vw_service_requests_powerbi[date])
)

Previous Month Requests = 
CALCULATE(
    [Total Requests],
    PREVIOUSMONTH(vw_service_requests_powerbi[date])
)

MoM Growth = 
DIVIDE(
    [Total Requests] - [Previous Month Requests],
    [Previous Month Requests],
    0
) * 100
```

### Advanced Metrics
```dax
Emergency Response Rate = 
DIVIDE(
    CALCULATE(
        COUNTROWS(vw_service_requests_powerbi),
        vw_service_requests_powerbi[is_emergency_service] = TRUE(),
        vw_service_requests_powerbi[resolved_same_day] = 1
    ),
    CALCULATE(
        COUNTROWS(vw_service_requests_powerbi),
        vw_service_requests_powerbi[is_emergency_service] = TRUE()
    ),
    0
) * 100

Weekend Request Percentage = 
DIVIDE(
    CALCULATE(
        COUNTROWS(vw_service_requests_powerbi),
        vw_service_requests_powerbi[is_weekend] = TRUE()
    ),
    [Total Requests],
    0
) * 100
```

## Report Templates

### Executive Dashboard

**KPI Cards:**
- Total Requests (current month)
- Closure Rate
- Average Response Time
- Same Day Resolution Rate

**Charts:**
1. **Trend Line**: Requests over time (monthly)
2. **Bar Chart**: Top 10 complaint types
3. **Map**: Requests by borough with closure rates
4. **Donut Chart**: Requests by agency type

### Operational Dashboard

**Performance Metrics:**
- Response time distribution
- SLA compliance by agency
- Backlog analysis
- Resolution trends

**Charts:**
1. **Table**: Agency performance metrics
2. **Heatmap**: Requests by day of week and hour
3. **Scatter Plot**: Response time vs complaint priority
4. **Waterfall**: Request status flow

### Geographic Analysis

**Maps and Spatial Analysis:**
- Borough comparison
- ZIP code hotspots
- Community board analysis
- Seasonal patterns by location

**Charts:**
1. **Filled Map**: Requests by borough
2. **Shape Map**: Community board analysis
3. **Table**: Geographic performance metrics
4. **Line Chart**: Seasonal trends by borough

## Visualization Best Practices

### Color Schemes
- **Red**: High priority, poor performance, delays
- **Yellow/Orange**: Medium priority, warning levels
- **Green**: Good performance, on-time resolution
- **Blue**: Information, neutral metrics

### Chart Selection
- **KPI Cards**: Key metrics and targets
- **Line Charts**: Trends over time
- **Bar Charts**: Category comparisons
- **Maps**: Geographic analysis
- **Tables**: Detailed breakdowns
- **Scatter Plots**: Correlation analysis

### Interactivity
- **Slicers**: Date ranges, boroughs, agencies
- **Cross-filtering**: Click to filter across visuals
- **Drill-through**: Detail pages for deep dives
- **Bookmarks**: Saved views and stories

## Performance Optimization

### Power BI Optimization
1. **Use Import Mode** for better performance
2. **Implement incremental refresh** for large datasets
3. **Create aggregations** for summary views
4. **Optimize DAX queries** to reduce load times

### Data Source Optimization
1. **Use the denormalized view** instead of multiple tables
2. **Filter data** at source when possible
3. **Schedule refresh** during off-peak hours
4. **Monitor Databricks SQL warehouse** performance

## Sample Visualizations

### Key Performance Indicators
```
┌─────────────────┬─────────────────┬─────────────────┐
│  Total Requests │   Closure Rate  │ Avg Response    │
│     45,234      │      87.5%      │   72.3 hours    │
└─────────────────┴─────────────────┴─────────────────┘
```

### Agency Performance Table
```
Agency    | Requests | Closed | Closure Rate | Avg Response
----------|----------|--------|--------------|-------------
NYPD      | 12,543   | 11,234 | 89.6%        | 48.2 hrs
DOT       | 8,765    | 7,543  | 86.1%        | 96.7 hrs
DEP       | 6,432    | 5,876  | 91.4%        | 124.5 hrs
```

### Monthly Trend
```
Requests
    ↑
15K |     ●
    |   ●   ●
10K | ●       ●
    |           ●
 5K |             ●
    └─────────────────→
    Jan Feb Mar Apr May Jun
```

## Refresh Strategy

### Scheduled Refresh
1. **Daily Refresh**: 6:00 AM (after gold layer processing)
2. **Incremental Refresh**: Last 30 days of data
3. **Full Refresh**: Weekly on Sundays

### Real-time Options
- **DirectQuery**: For real-time dashboards (with performance trade-offs)
- **Composite Model**: Mix of import and DirectQuery
- **Streaming**: For real-time KPIs (requires additional setup)

## Sharing and Security

### Row-level Security (RLS)
```dax
-- Example: Restrict by borough
Borough Security = 
IF(
    USERPRINCIPALNAME() = "manhattan.analyst@company.com",
    vw_service_requests_powerbi[borough] = "Manhattan",
    TRUE()
)
```

### Workspace Sharing
1. **Create workspace** for NYC 311 reports
2. **Add users** with appropriate roles
3. **Publish reports** to workspace
4. **Set up usage monitoring**

## Troubleshooting

### Common Issues

**1. Connection Timeout**
- Increase timeout settings
- Use smaller date ranges
- Optimize SQL warehouse size

**2. Memory Errors**
- Switch to DirectQuery mode
- Implement data aggregations
- Filter data at source

**3. Slow Refresh**
- Use incremental refresh
- Optimize data model
- Schedule during off-peak hours

### Performance Monitoring

Monitor:
- Refresh duration
- Query performance
- User adoption
- Report usage

## Advanced Features

### Custom Connectors
Build custom connectors for:
- Real-time data streaming
- Custom aggregations
- Specialized calculations

### AI and Machine Learning
Leverage Power BI AI features:
- **Anomaly Detection**: Identify unusual patterns
- **Forecasting**: Predict future request volumes
- **Key Influencers**: Understand what drives performance
- **Q&A**: Natural language queries

### Embedded Analytics
Embed reports in:
- SharePoint pages
- Custom applications
- Public websites
- Mobile apps

This Power BI integration provides comprehensive analytics capabilities for the NYC 311 data, enabling data-driven decision making and operational insights.
