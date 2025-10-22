# NYC 311 Service Requests Data Pipeline

A Databricks Asset Bundle for processing NYC 311 service request data through bronze, silver, and gold layers with a star schema optimized for analytics and Power BI reporting.

## Table of Contents

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Getting Started](#getting-started)
- [Project Structure](#project-structure)
- [Architecture](#architecture)
- [Configuration](#configuration)
- [Tables Created](#tables-created)
- [Power BI Integration](#power-bi-integration)
- [Monitoring and Maintenance](#monitoring-and-maintenance)
- [Troubleshooting](#troubleshooting)
- [Best Practices](#best-practices)

## Overview

This project implements a medallion architecture data pipeline using Databricks Asset Bundles:

```
NYC 311 API → Bronze (raw) → Silver (clean) → Gold (analytics) → Power BI
```

**Bronze Layer** - Incremental ingestion from NYC 311 Socrata API with retry logic and rate limiting

**Silver Layer** - Type conversions, data quality validation, geographic standardization, and derived metrics

**Gold Layer** - Star schema with fact table (`fact_service_requests`), dimensions (`dim_date`, `dim_agency`, `dim_location`, `dim_complaint_type`), and pre-calculated aggregates

## Quick Start

### Prerequisites

- Databricks workspace (Free Edition or higher)
- Databricks CLI v0.205.0+
- Git

### Setup & Deploy

**1. Install CLI and Clone:**
```bash
# Install Databricks CLI
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh

# Verify installation
databricks --version

# Clone and navigate
git clone <repository-url>
cd nyc311
```

**2. Authenticate:**
```bash
databricks auth login
databricks auth profiles  # Verify
```

**3. Validate:**
```bash
export BUNDLE_VAR_notification_email=your-email@example.com
databricks bundle validate --var notification_email=${BUNDLE_VAR_notification_email}
```

**4. Deploy:**
```bash
./deploy.sh dev   # Development environment
./deploy.sh prod  # Production environment
```

**5. Monitor:**
Navigate to **Workflows** in Databricks UI to view "NYC 311 Data Pipeline" and monitor execution.

### Optional: Local Python Environment

For local development, choose either conda or venv:

**Using Conda:**
```bash
conda create -n nyc311 python=3.11
conda activate nyc311
pip install -r requirements.txt
```

**Using venv:**
```bash
python3 -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## Project Structure

```
nyc311/
├── databricks.yml                       # Main DAB configuration
├── deploy.sh                            # Automated deployment script
├── README.md                            # This guide
├── requirements.txt                     # Python dependencies
├── src/
│   ├── pipelines/
│   │   ├── README.md                    # Pipeline organization guide
│   │   └── nyc311/                      # NYC 311 pipeline
│   │       ├── nyc311_bronze_ingest.py    # Raw data ingestion
│   │       ├── nyc311_silver_transform.py # Data cleaning & transformation
│   │       └── nyc311_gold_star_schema.py # Star schema creation
│   └── sql/                             # Gold layer SQL scripts (reference examples)
│       ├── README.md                    # SQL scripts documentation
│       ├── create_gold_layer_complete.sql
│       ├── create_dimension_tables.sql
│       ├── create_fact_table.sql
│       └── create_aggregate_tables.sql
└── docs/
    ├── deployment.md                    # Detailed deployment guide
    └── powerbi_guide.md                 # Power BI integration guide
```

## Architecture

**Medallion Pattern:** Bronze (raw) → Silver (clean) → Gold (analytics)

**Star Schema:**
```
                    dim_date
                       |
    dim_agency -----> fact_service_requests <----- dim_location
                       |
                 dim_complaint_type
```

**Pipeline:** Single job with three sequential tasks. Each task depends on the previous one, ensuring data consistency.


## Configuration

**Catalogs:** `bronze.nyc311` (raw) → `silver.nyc311` (clean) → `gold.nyc311` (analytics)

**Environments:**
- **Dev**: 500K batch size, manual runs, 1-hour timeout
- **Prod**: 5M batch size, daily at 11 PM UTC, 2-hour timeout

**Customization:** Edit notebook parameters in `databricks.yml` or modify notebooks directly:
- `nyc311_bronze_ingest.py` - API settings, batch sizes, retry logic
- `nyc311_silver_transform.py` - Data quality rules, transformations
- `nyc311_gold_star_schema.py` - Star schema structure, aggregations

## Tables Created

**Bronze:** `service_requests` - Raw API data

**Silver:** `service_requests_silver` - Cleaned and validated

**Gold Dimensions:** `dim_date`, `dim_agency`, `dim_location`, `dim_complaint_type`

**Gold Fact:** `fact_service_requests` - Core service request metrics

**Gold Aggregates:** `agg_daily_summary`, `agg_geographic_summary`, `agg_grid_heatmap`, `agg_complaint_performance`, `agg_borough_comparison`

## Power BI Integration

**Connect:** Power BI Desktop → Get Data → Databricks → Enter workspace URL and SQL warehouse HTTP path

**Tables:** Import from `gold.nyc311` schema
- Use `fact_service_requests` + dimensions for detailed analysis
- Use aggregate tables (`agg_*`) for faster dashboards

**Available Metrics:** Request volumes, response times, closure rates, geographic analysis, agency performance

See `docs/powerbi_guide.md` for detailed setup.

## Monitoring

**Pipeline Status:** Databricks UI → Workflows → "NYC 311 Data Pipeline"

**Data Quality:** Each layer logs validation metrics (duplicates, nulls, coordinate validation, row counts)

**Optimization:** In production only, tables are automatically optimized with OPTIMIZE and Z-ORDER commands on key columns

**Maintenance:** Monitor job history, review task logs for errors, check table statistics with `DESCRIBE DETAIL <table>`

## Troubleshooting

**Authentication Failed:**
```bash
databricks auth login
databricks auth profiles  # Verify
```

**API Rate Limiting (429 errors):** Automatic retry logic included. Reduce `batch_size` parameter or get NYC 311 API token for higher limits.

**Memory/Resource Errors:** Reduce batch size or use dev environment for smaller data volumes.

**Schema Changes:** Enable `option("mergeSchema", "true")` in write operations.

**Debug Mode:** Use `./deploy.sh dev` for smaller batches and faster iteration.

See `docs/` for detailed guides or [Databricks documentation](https://docs.databricks.com/dev-tools/bundles/).

## Free Edition Optimizations

The pipeline is optimized for Databricks Free Edition's 5 concurrent task limit:

- **Sequential processing**: Gold layer uses temp tables to break complex joins into steps
- **Single writers**: `coalesce(1)` forces single partition writes
- **Broadcast joins**: Small dimensions use `F.broadcast()` to avoid shuffles
- **Reduced partitions**: `spark.sql.shuffle.partitions` set to 4 (vs default 200)
- **Hash keys**: Uses `F.hash()` instead of expensive `monotonically_increasing_id()`

These trade parallel performance for task limit compliance while maintaining data quality.

## Best Practices Demonstrated

- **Medallion Architecture** - Bronze/Silver/Gold layering with clear separation of concerns
- **Infrastructure as Code** - Complete pipeline in `databricks.yml` for version control
- **Star Schema** - Dimension/fact model optimized for analytics
- **Data Quality** - Validation at each layer with comprehensive logging
- **Error Handling** - Retry logic, graceful failures, and informative messages
- **Environment Management** - Separate dev/prod configs with safe deployments

## Resources

**Project Docs:** `docs/deployment.md`, `docs/powerbi_guide.md`, `src/pipelines/README.md`

**External:** [Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/) | [NYC 311 API](https://dev.socrata.com/foundry/data.cityofnewyork.us/erm2-nwe9) | [Bundle Examples](https://github.com/databricks/bundle-examples/blob/main/knowledge_base)

## License

Demonstration and learning resource for Databricks best practices.