# Data Architecture Documentation

## Overview

The NYC 311 Service Requests Data Pipeline implements a modern **medallion architecture** (Bronze → Silver → Gold) using Databricks and Delta Lake. This architecture provides a robust, scalable foundation for analytics and machine learning workloads.

## Architecture Principles

### 1. Medallion Architecture
- **Bronze Layer**: Raw data with minimal transformation
- **Silver Layer**: Cleaned and validated data
- **Gold Layer**: Business-ready aggregations and star schema

### 2. Data Quality by Design
- Schema validation at each layer
- Data quality metrics and monitoring
- Automated data profiling and cleansing

### 3. Performance Optimization
- Delta Lake for ACID transactions
- Table optimization and Z-ordering
- Partition strategies for query performance

### 4. Scalability and Flexibility
- Modular notebook design
- Configurable batch sizes and schedules
- Environment-specific configurations

## Detailed Layer Architecture

### Bronze Layer (Raw Data Lake)

**Purpose**: Store raw data with full fidelity and lineage

**Characteristics**:
- All fields stored as strings for maximum flexibility
- No data transformation or cleaning
- Complete audit trail with ingestion timestamps
- Schema evolution support
- Error handling and retry logic

**Schema Strategy**:
```python
# All fields as StringType for flexibility
bronze_schema = StructType([
    StructField("unique_key", StringType(), True),
    StructField("created_date", StringType(), True),
    StructField("agency", StringType(), True),
    # ... all other fields as strings
])
```

**Data Sources**:
- NYC 311 Socrata API (primary)
- Future sources can be easily added

**Quality Assurance**:
- API response validation
- Record count monitoring
- Duplicate detection
- Source system metadata tracking

### Silver Layer (Curated Data)

**Purpose**: Provide clean, validated, business-ready data

**Transformations**:
1. **Data Type Conversion**:
   - Timestamps: ISO format to Spark timestamp
   - Coordinates: String to double with validation
   - Categorical: Standardized values

2. **Data Cleaning**:
   - Borough name standardization
   - ZIP code validation
   - Text field normalization (trim, case)

3. **Data Enhancement**:
   - Derived time dimensions (hour, day of week, season)
   - Business rules (complaint priority, response categories)
   - Geographic validation (NYC bounds checking)

4. **Data Quality**:
   - Null rate calculation
   - Coordinate validation
   - Response time reasonableness checks

**Schema Evolution**:
- Automated schema merging
- Backward compatibility maintenance
- Change detection and alerting

### Gold Layer (Business Layer)

**Purpose**: Optimized for analytics and reporting

**Star Schema Design**:

```
                    dim_date
                   (365 rows)
                       |
                   date_key
                       |
    dim_agency -----> fact_service_requests <----- dim_location
   (20 rows)            (millions of rows)          (500 rows)
                           |
                    complaint_key
                           |
                 dim_complaint_type
                    (300 rows)
```

**Dimension Tables**:

1. **dim_date**: Comprehensive date dimension
   - Fiscal year calculations
   - Holiday flags
   - Business day indicators
   - Seasonal groupings

2. **dim_agency**: Agency master data
   - Agency categorization (Emergency, Infrastructure, etc.)
   - Emergency service flags
   - Contact information (future)

3. **dim_location**: Geographic hierarchy
   - Borough → ZIP → Community Board
   - Coordinate validation flags
   - Regional groupings

4. **dim_complaint_type**: Service taxonomy
   - Complaint categorization
   - Priority scoring
   - SLA definitions (future)

**Fact Table**:
- **fact_service_requests**: Core business events
  - Foreign keys to all dimensions
  - Additive measures (counts, durations)
  - Calculated flags and indicators

**Aggregate Tables**:
Pre-calculated summaries for performance:
- Daily summaries by borough and agency
- Monthly agency performance metrics
- Complaint type performance analysis
- Borough comparison metrics

## Data Flow Architecture

### Ingestion Flow
```
NYC 311 API → Bronze Delta Tables → Silver Delta Tables → Gold Star Schema
     ↓              ↓                    ↓                     ↓
Raw JSON      String Schema      Cleaned Types        Star Schema
```

### Processing Pipeline
1. **Extract**: API calls with pagination and rate limiting
2. **Load**: Raw data to bronze with metadata
3. **Transform**: Type conversion and cleaning to silver
4. **Model**: Star schema creation in gold
5. **Optimize**: Table optimization and indexing

### Error Handling
- **Graceful Degradation**: Pipeline continues on non-critical errors
- **Retry Logic**: Exponential backoff for API failures
- **Dead Letter Queue**: Failed records isolated for investigation
- **Monitoring**: Comprehensive logging and alerting

## Technical Implementation

### Delta Lake Features Used

1. **ACID Transactions**: Consistent reads and writes
2. **Schema Evolution**: Automatic schema merging
3. **Time Travel**: Historical data access and rollback
4. **Optimization**: OPTIMIZE and Z-ORDER for performance
5. **Change Data Feed**: Track data changes (future enhancement)

### Performance Optimization

1. **Partitioning Strategy**:
   ```sql
   -- Bronze: Partitioned by ingestion date
   PARTITIONED BY (DATE(ingest_ts))
   
   -- Silver: Partitioned by created date
   PARTITIONED BY (DATE(created_date))
   
   -- Gold: Partitioned by date_key
   PARTITIONED BY (date_key)
   ```

2. **Z-Ordering**:
   ```sql
   -- Fact table: Optimize for common query patterns
   ZORDER BY (date_key, agency_key, location_key)
   
   -- Aggregates: Optimize for filtering
   ZORDER BY (report_date, borough, agency)
   ```

3. **Caching Strategy**:
   - Frequently accessed dimensions cached
   - Recent partitions cached automatically
   - Query result caching enabled

### Security and Governance

1. **Unity Catalog Integration**:
   - Centralized metadata management
   - Fine-grained access control
   - Data lineage tracking

2. **Access Control**:
   ```sql
   -- Example table-level permissions
   GRANT SELECT ON gold.nyc311.vw_service_requests_powerbi 
   TO `analytics-team`
   
   -- Column-level security for sensitive data
   GRANT SELECT(unique_key, complaint_type, borough) 
   ON silver.nyc311.nyc311_service_requests_silver 
   TO `public-analysts`
   ```

3. **Data Classification**:
   - Public data: Open for analysis
   - Internal data: Requires authentication
   - Sensitive data: Restricted access (future PII handling)

## Scalability Considerations

### Horizontal Scaling
- **Cluster Auto-scaling**: Dynamic worker allocation
- **Multi-cluster Loading**: Parallel processing capabilities
- **Partition Pruning**: Efficient query execution

### Vertical Scaling
- **Memory-optimized Clusters**: For large transformations
- **Photon Engine**: Accelerated query performance
- **Delta Cache**: SSD-based caching layer

### Cost Optimization
- **Spot Instances**: Cost-effective for batch processing
- **Job Clusters**: Ephemeral clusters for specific tasks
- **Automatic Termination**: Idle cluster shutdown

## Monitoring and Observability

### Data Quality Monitoring
```python
quality_metrics = {
    "null_rate_unique_key": null_count / total_records,
    "valid_coordinate_rate": valid_coords / total_records,
    "duplicate_rate": duplicates / total_records,
    "avg_response_time": avg_response_hours
}
```

### Performance Monitoring
- Query execution metrics
- Cluster utilization tracking
- Job success/failure rates
- Data freshness indicators

### Alerting Strategy
- **Critical**: Job failures, data quality degradation
- **Warning**: Performance degradation, unusual patterns
- **Info**: Successful job completion, metrics summary

## Future Enhancements

### Machine Learning Integration
1. **Predictive Analytics**:
   - Request volume forecasting
   - Response time prediction
   - Resource allocation optimization

2. **Anomaly Detection**:
   - Unusual complaint patterns
   - Performance degradation alerts
   - Data quality anomalies

### Real-time Processing
1. **Streaming Ingestion**:
   - Real-time API consumption
   - Event-driven processing
   - Near real-time dashboards

2. **Change Data Capture**:
   - Track data lineage
   - Audit trail maintenance
   - Impact analysis

### Advanced Analytics
1. **Geographic Analysis**:
   - Spatial clustering
   - Service area optimization
   - Response time mapping

2. **Sentiment Analysis**:
   - Complaint text analysis
   - Satisfaction scoring
   - Topic modeling

## Best Practices Demonstrated

### Data Engineering
- **Separation of Concerns**: Clear layer responsibilities
- **Idempotent Processing**: Rerunnable pipelines
- **Schema Management**: Evolution without breaking changes
- **Error Handling**: Comprehensive error management

### DevOps
- **Infrastructure as Code**: Databricks Asset Bundles
- **Environment Management**: Dev/prod separation
- **CI/CD Integration**: Automated testing and deployment
- **Version Control**: Complete pipeline versioning

### Data Governance
- **Data Lineage**: Complete tracking from source to consumption
- **Data Quality**: Automated validation and monitoring
- **Access Control**: Role-based security model
- **Documentation**: Comprehensive technical documentation

This architecture provides a solid foundation for enterprise data analytics while maintaining flexibility for future enhancements and scaling requirements.
