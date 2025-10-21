# NYC 311 Service Requests Data Pipeline

A comprehensive Databricks Asset Bundle for processing NYC 311 service request data through bronze, silver, and gold layers with a star schema optimized for analytics and Power BI reporting.

## Architecture Overview

This project implements a modern data lakehouse architecture using Databricks with three layers:

- **Bronze Layer**: Raw data ingestion from NYC 311 API (Python notebook)
- **Silver Layer**: Cleaned and standardized data with data quality improvements (Python notebook)
- **Gold Layer**: Star schema with dimensions and fact tables (Python notebook)

### Data Flow

```
NYC 311 API → Bronze Tables → Silver Tables → Gold Star Schema → Power BI
              (Notebook)      (Notebook)       (SQL Warehouse)
```

## Project Structure

```
nyc311/
├── 📋 databricks.yml                    # Main DAB configuration
├── 🚀 deploy.sh                         # Automated deployment script  
├── 📖 README.md                         # This comprehensive guide
├── 📊 src/
│   ├──  pipelines/                    # Pipeline notebook code
│   │   ├── 📄 README.md                 # Pipeline organization guide
│   │   ├── 🏢 nyc311/                   # NYC 311 pipeline
│   │   │   ├── nyc311_bronze_ingest.py    # Raw data ingestion
│   │   │   ├── nyc311_silver_transform.py # Data cleaning & transformation  
│   │   │   └── nyc311_gold_star_schema.py # Star schema creation
│   │   └── 📂 future_examples/          # Examples for additional pipelines
│   │       └── nyc_taxi_bronze_ingest.py # Future taxi pipeline example
│   └── 📁 sql/                          # Gold layer SQL scripts (examples)
│       ├── 📄 README.md                 # SQL scripts documentation
│       ├── create_gold_layer_complete.sql # Master gold layer script
│       ├── create_dimension_tables.sql  # Individual dimension tables
│       ├── create_fact_table.sql        # Fact table creation
│       └── create_aggregate_tables.sql  # Additional aggregation tables
├──  docs/
│   ├── deployment.md                    # Detailed deployment guide
│   └── powerbi_guide.md                 # Power BI integration guide
├── 📦 requirements.txt                  # Python dependencies
├── 🔧 .env.template                     # Environment variables template
└── 🚫 .gitignore                        # Git ignore rules
```

## Features

### Bronze Layer
- **Raw Data Ingestion**: Pulls data from NYC 311 Socrata API
- **Robust Schema**: All fields stored as strings for maximum flexibility
- **Error Handling**: Retry logic and rate limiting for API calls
- **Incremental Loading**: Support for both full and incremental loads
- **Data Lineage**: Metadata columns for tracking ingestion

### Silver Layer
- **Data Cleaning**: Standardization of text fields and categorical data
- **Type Conversion**: Proper data types for timestamps, coordinates, and numeric fields
- **Data Quality**: Validation and cleansing of geographic coordinates
- **Derived Columns**: Business logic for response times, seasons, priorities
- **Borough Standardization**: Consistent borough names across the dataset

### Gold Layer (Star Schema)
- **Fact Table**: `fact_service_requests` with measures and foreign keys
- **Dimensions**:
  - `dim_date`: Comprehensive date dimension with fiscal year, seasons
  - `dim_agency`: Agency information with categorization
  - `dim_location`: Geographic hierarchy (borough, zip, community board)
  - `dim_complaint_type`: Complaint categories with priority scores
- **Aggregates**: Pre-calculated summaries for faster reporting
- **Power BI View**: Denormalized view for easy Power BI consumption

### Star Schema Design

```
                    dim_date
                       |
                   date_key
                       |
    dim_agency -----> fact_service_requests <----- dim_location
   agency_key            |                        location_key
                    complaint_key
                         |
                 dim_complaint_type
```

## Quick Start

### Prerequisites

1. **Databricks Workspace**: Access to a Databricks workspace
2. **Databricks CLI**: Install the new Databricks CLI (v0.205.0+)
3. **Authentication**: Configure CLI authentication with profiles

### Deployment

1. **Install Databricks CLI**:
   ```bash
   # Download and install the new Databricks CLI
   curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
   
   # Verify installation (should show v0.205.0+)
   databricks -v
   ```

2. **Clone and Navigate**:
   ```bash
   git clone <repository-url>
   cd nyc311
   ```

3. **Configure Authentication**:
   ```bash
   # Authenticate and create a profile
   databricks auth login
   # Follow browser flow and save as profile (e.g., DEFAULT)
   
   # Verify authentication
   databricks auth profiles
   ```

4. **Deploy to Development**:
   ```bash
   ./deploy.sh dev
   ```

5. **Deploy to Production**:
   ```bash
   ./deploy.sh prod
   ```

### Manual Deployment

If you prefer manual deployment:

```bash
# Validate the bundle
databricks bundle validate

# Deploy to dev environment
databricks bundle deploy --target dev

# Run the pipeline (all three tasks will execute in sequence)
databricks bundle run nyc311_pipeline --target dev
```

### Additional examples

For more Databricks Asset Bundle examples and reference patterns, see the Databricks bundle-examples knowledge base:

[Databricks bundle-examples — knowledge_base](https://github.com/databricks/bundle-examples/blob/main/knowledge_base)

This contains example bundles and patterns you can reference when customizing or extending this project.

## Configuration

### Serverless Compute Optimizations

This project is optimized for Databricks serverless compute with:

- **Separate Catalogs**: Bronze, Silver, and Gold catalogs for proper data governance
- **Serverless Runtime**: Uses serverless compute with Photon engine for optimal performance
- **Enhanced Processing**: Higher batch sizes (50K+ records) for efficient data processing
- **Modern Features**: Latest Spark runtime with advanced optimization capabilities

### Catalogs and Schemas

- **Bronze**: `bronze.nyc311` - Raw data from NYC 311 API
- **Silver**: `silver.nyc311` - Cleaned and validated data
- **Gold**: `gold.nyc311` - Analytics-ready star schema

### Scheduled Jobs

The pipeline is organized as a single job with three sequential tasks:

- **NYC 311 Pipeline**: Daily at 2:00 AM (runs all three tasks in sequence)
  1. **Bronze Task**: Data ingestion from NYC 311 API
  2. **Silver Task**: Data transformation and cleaning (depends on Bronze)
  3. **Gold Task**: Star schema creation (depends on Silver)

### Customization

Customization options are available in the individual notebook files:
- API endpoints and batch sizes (bronze notebook)
- Data quality thresholds (silver notebook)
- Borough mappings (silver notebook)
- Star schema structure (gold notebook)

## Data Quality

The pipeline includes comprehensive data quality checks:

- **Null Rate Monitoring**: Tracks null percentages for key fields
- **Coordinate Validation**: Ensures coordinates are within NYC bounds
- **Duplicate Detection**: Identifies duplicate unique keys
- **Response Time Validation**: Flags unrealistic response times

## Power BI Integration

The gold layer includes a denormalized view specifically for Power BI:

```sql
SELECT * FROM gold.nyc311.vw_service_requests_powerbi
```

This view combines all dimension attributes with fact measures for easy reporting.

### Key Metrics Available

- **Volume Metrics**: Request counts by time, location, agency
- **Performance Metrics**: Response times, closure rates, SLA compliance
- **Operational Metrics**: Same-day resolution rates, backlog analysis
- **Geographic Analysis**: Borough comparisons, community board analysis

## Tables Created

### Bronze Layer
- `bronze.nyc311.service_requests`

### Silver Layer
- `silver.nyc311.service_requests_silver`

### Gold Layer
- `gold.nyc311.dim_date`
- `gold.nyc311.dim_agency`
- `gold.nyc311.dim_location`
- `gold.nyc311.dim_complaint_type`
- `gold.nyc311.fact_service_requests`
- `gold.nyc311.agg_daily_summary`
- `gold.nyc311.agg_monthly_agency_summary`
- `gold.nyc311.agg_complaint_performance`
- `gold.nyc311.agg_borough_comparison`
- `gold.nyc311.vw_service_requests_powerbi` (view)

## Monitoring and Maintenance

### Pipeline Execution

The pipeline consists of a single job with three sequential tasks:
1. **Bronze Ingest** (Notebook): Ingests data from NYC 311 API to bronze tables
2. **Silver Transform** (Notebook): Cleans and transforms data to silver tables  
3. **Gold Layer** (SQL Warehouse): Creates star schema using SQL file execution

- Tasks automatically execute in dependency order (Bronze → Silver → Gold)
- If any task fails, subsequent tasks will not execute
- Gold layer runs on SQL Warehouse for optimal performance and to avoid Spark task limits
- Monitor the entire pipeline through a single job run

### SQL Warehouse Benefits

The gold layer uses SQL file execution on SQL Warehouse instead of notebooks for:
- **Better Performance**: Native SQL execution optimized for analytics workloads
- **Resource Efficiency**: Avoids Spark task limits on Free Edition accounts
- **Simpler Maintenance**: Pure SQL scripts are easier to debug and modify
- **Parameter Support**: Job parameters automatically passed to SQL scripts

### Free Edition Compatibility

The pipeline includes special optimizations for Databricks Free Edition serverless compute:
- **Month-by-Month Processing**: Processes data in monthly batches to avoid 5 concurrent task limit
- **Single Writer Strategy**: Uses `REPARTITION(1)` and `coalesce(1)` to force single writer per batch
- **Broadcast Joins**: Small dimension tables are broadcast to avoid shuffle operations
- **No Spark Configs**: Avoids cluster configuration changes not allowed on Free Edition
- **Optimized Hints**: SQL hints like `/*+ BROADCAST(table) */` reduce task counts

### Optimization

Tables are automatically optimized with:
- **OPTIMIZE** commands for better query performance
- **Z-ORDER** on frequently queried columns
- **Auto-compaction** in production environment

### Monitoring

Monitor the pipeline through:
- Single Databricks job run history for the entire pipeline
- Individual task status within the job run
- Data quality metrics logged in each task
- Table statistics and row counts

## Troubleshooting

### Common Issues

1. **API Rate Limiting**: Adjust `rate_limit_delay` in config
2. **Memory Issues**: Increase cluster size or reduce batch size
3. **Schema Evolution**: Use `mergeSchema` option for schema changes

### Debug Mode

For development, limit data volume by setting environment to "dev" which processes fewer batches.

## Databricks Free Edition Optimizations

This pipeline includes several specific optimizations to work within Databricks Free Edition serverless compute constraints:

### Task Limit Workarounds

**Sequential Fact Table Creation:**
- Gold layer fact table creation is broken into multiple sequential steps to avoid exceeding the 5 concurrent task limit
- Uses intermediate temporary tables: `temp_silver_base` → `temp_silver_with_agency` → `temp_silver_with_location` → final fact table
- Each step uses `coalesce(1)` to force single partition writes, minimizing task count
- Temporary tables are cleaned up after fact table creation to free resources

**Broadcast Join Strategy:**
- Small dimension tables (< 1000 records) use broadcast joins with `F.broadcast()` hints
- Prevents shuffle operations that would create additional tasks
- Location dimension uses conditional broadcast based on record count

### Resource Optimization

**Hash-based Surrogate Keys:**
- Uses `F.hash()` function for dimension keys instead of monotonically increasing IDs
- Avoids expensive `monotonically_increasing_id()` operations that can exceed task limits
- Hash keys are deterministic and consistent across runs

**Shuffle Partition Reduction:**
- Sets `spark.sql.shuffle.partitions` to 4 (down from default 200)
- Minimizes task count for small datasets in Free Edition environment
- Balances performance with task limit constraints

**Aggregation Simplification:**
- Skips complex monthly agency summary aggregation (too resource intensive)
- Geographic summary uses fact table directly without additional joins
- Pre-calculated aggregates designed for single-pass processing

**Delta Optimization Disabled:**
- Disables `delta.autoOptimize.optimizeWrite` and `delta.autoOptimize.autoCompact` in dev
- Prevents automatic background tasks that could exceed limits
- Table optimization only runs in production environment

### Performance vs. Limits Trade-offs

**Single Writer Strategy:**
- All writes use `coalesce(1)` to ensure single task execution
- Trades parallel write performance for task limit compliance
- Acceptable for moderate data volumes in Free Edition

**Simplified Star Schema:**
- Location dimension simplified to borough-level only (vs. full geographic hierarchy)
- Reduces join complexity and task count in fact table creation
- Geographic details preserved in fact table for Power BI mapping

These optimizations demonstrate how to adapt enterprise data patterns for Databricks Free Edition constraints while maintaining data quality and analytical capabilities.

## Best Practices Demonstrated

- **Medallion Architecture**: Bronze → Silver → Gold pattern
- **Single Job Pipeline**: One job with sequential task dependencies
- **Task Dependencies**: Proper dependency management ensures data consistency
- **Star Schema Design**: Optimized for analytical workloads
- **Data Quality Framework**: Comprehensive validation and monitoring
- **Infrastructure as Code**: Everything defined in databricks.yml
- **Environment Management**: Separate dev/prod configurations
- **Error Handling**: Robust retry logic and graceful failures
- **Performance Optimization**: Delta table optimization and Z-ordering

## Contributing

This project demonstrates enterprise-grade data engineering practices suitable for:
- Data engineering teams
- Analytics organizations
- Companies evaluating Databricks
- Power BI implementation projects

## License

This project is intended as a demonstration and learning resource for Databricks best practices.

## Python Environment Setup

You can create a Python environment for local development using either Conda or venv:

**Using Conda:**
```bash
conda create -n nyc311 python=3.9
conda activate nyc311
```

**Using venv (standard library):**
```bash
python3 -m venv .venv
source .venv/bin/activate
```

After activating your environment, install dependencies:
```bash
pip install -r requirements.txt
```