# NYC 311 Data Pipeline - AI Coding Instructions

**Databricks Asset Bundle (DAB)** processing NYC 311 service request data through medallion architecture (bronze → silver → gold) for Power BI analytics.

## Critical Constraint: Databricks Free Edition

**All code must work within Free Edition's 5 concurrent task limit.** This is the number one architectural driver.

### Free Edition Optimizations (Non-Negotiable)
```python
# ALWAYS set shuffle partitions to 4 (not default 200)
spark.conf.set("spark.sql.shuffle.partitions", "4")

# Force single-writer pattern to minimize tasks
df.coalesce(1).write.format("delta").mode("append").saveAsTable(table)

# Broadcast small dimensions (< 1K records) to avoid shuffles
from pyspark.sql.functions import broadcast
fact.join(broadcast(small_dim), "key")
```

### Sequential Join Pattern (Gold Layer)
The gold layer fact table creation uses **temp tables with sequential joins** to avoid exceeding task limits:
1. Create `temp_silver_base` with inline date_key
2. Join agency → write `temp_silver_with_agency`
3. Join location → write `temp_silver_with_location`  
4. Join complaint → write final fact table
5. Drop temp tables

**Why:** Single large multi-join operation triggers ~10-15 tasks; sequential approach uses 1-2 tasks per step.

## Repository Structure

- **Notebooks:** `src/pipelines/nyc311/*.py` are Databricks notebooks (`.py` format with `# MAGIC` comments)
- **Config:** `databricks.yml` defines DAB with job, tasks, environment-specific settings
- **Deployment:** `deploy.sh` automates validation + deployment via Databricks CLI
- **Tests:** `tests/test_*.py` are standalone Python (no PySpark/Databricks deps required)

## Architecture Overview

**Bronze** (`nyc311_bronze_ingest.py`): Incremental ingestion from NYC 311 Socrata API
- Uses `:updated_at` watermark with 6-hour overlap window
- State table tracks last processed timestamp
- Exponential backoff for 429 rate limits (reads `Retry-After` header)
- All fields stored as strings

**Silver** (`nyc311_silver_transform.py`): Type conversions and standardization
- Column suffixes: `_ts` (timestamps), `_num` (coordinates), `_final` (consolidated)
- Borough standardization maps variants → canonical names
- Grid cells (0.01° ≈ 1km) for spatial analysis
- Z-ORDER by `(borough, complaint_type, created_year, created_month)`

**Gold** (`nyc311_gold_star_schema.py`): Star schema with sequential processing
- Hash-based surrogate keys (not `monotonically_increasing_id` - too expensive)
- Date keys created inline via `date_format()` (not joined)
- Monthly agency summary **skipped** (too resource-intensive for Free Edition)

## Critical Workflows

### Authentication (Must Use Profiles)
```bash
# Profile-based auth (REQUIRED - env vars don't work reliably)
databricks auth login  # Browser flow, save as profile

# Verify
databricks auth profiles
databricks workspace list
```
**Never** use legacy `databricks-cli` package or `DATABRICKS_TOKEN` env vars with DAB.

### Deployment
```bash
./deploy.sh          # dev (default)
./deploy.sh prod     # production

# What it does:
# 1. Checks CLI v0.205.0+
# 2. Validates profile auth
# 3. Runs: databricks bundle validate
# 4. Deploys: databricks bundle deploy --target <env>
# 5. Optionally runs pipeline
```

### Notebook Widget Parameters
Every notebook MUST define widgets at top:
```python
dbutils.widgets.text("bronze_catalog", "bronze", "Bronze Catalog Name")
dbutils.widgets.text("schema_name", "nyc311", "Schema Name")
dbutils.widgets.text("environment", "dev", "Environment")

# Get values
bronze_catalog = dbutils.widgets.get("bronze_catalog")
```
**Why:** DAB job parameters flow down to tasks; widgets make notebooks runnable standalone or via workflow.

## Project-Specific Conventions

### Catalog Pattern (Always Three Catalogs)
```python
bronze_catalog = "bronze"  # Raw data
silver_catalog = "silver"  # Cleaned
gold_catalog = "gold"      # Analytics

# Full reference (never hardcode catalog/schema)
table_ref = f"{catalog}.{schema}.{table_name}"
```

### Column Naming
- **Bronze:** Original API names (`created_date`, `unique_key`)
- **Silver:** Type suffixes (`created_date_ts`, `latitude_num`, `borough_final`)
- **Gold:** Keys (`date_key`, `agency_key`, `service_request_key`)

### Secrets (Free Edition May Not Have Scopes)
```python
try:
    token = dbutils.secrets.get(scope="nyc311", key="app_token")
except:
    token = None  # Graceful degradation
    print("Warning: No app token (rate limits apply)")
```

## Coding Standards

**Simplicity First:**
- Choose the simplest solution; avoid premature abstraction
- Write modular code with single responsibility
- No over-engineering, speculative features, or clever-but-opaque code

**PySpark Specifics:**
- Use DataFrame API (not RDD)
- Avoid UDFs when built-in functions suffice
- Filter early, prune columns, cache reused DataFrames

**Error Handling:**
- Raise specific exceptions with helpful messages
- Use `print()` for notebook output (visible in Databricks UI)
- Never log secrets or PII

**Testing:**
- Minimal unit tests (happy path + one edge case)
- Tests in `tests/` with `test_` prefix
- Standalone Python (mock external APIs, no Databricks deps)

## Common Pitfalls

1. **Don't** use default shuffle partitions (200) - always set to 4
2. **Don't** use `databricks-cli` package - use new CLI with `databricks bundle`
3. **Don't** hardcode catalog/schema names - use f-strings with variables
4. **Don't** assume secrets exist - Free Edition often lacks secret scopes
5. **Don't** collect large datasets to driver - use distributed processing
6. **Don't** skip widget definitions - notebooks must be workflow-compatible

