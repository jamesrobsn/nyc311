# Databricks notebook source
# MAGIC %md
# MAGIC # NYC 311 Bronze Layer Data Ingestion
# MAGIC 
# MAGIC This notebook ingests raw NYC 311 service request data from the Socrata API into the bronze layer.
# MAGIC The bronze layer stores raw data with minimal transformation for data lineage and reprocessing capabilities.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("bronze_catalog", "bronze", "Bronze Catalog Name")
dbutils.widgets.text("schema_name", "nyc311", "Schema Name")
dbutils.widgets.text("environment", "dev", "Environment")
dbutils.widgets.text("batch_size", "50000", "Batch Size for API requests")

bronze_catalog = dbutils.widgets.get("bronze_catalog")
schema_name = dbutils.widgets.get("schema_name")
environment = dbutils.widgets.get("environment")
batch_size = int(dbutils.widgets.get("batch_size"))

print(f"Bronze Catalog: {bronze_catalog}")
print(f"Schema: {schema_name}")
print(f"Environment: {environment}")
print(f"Batch Size: {batch_size}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports and Configuration

# COMMAND ----------

import time
import requests
from datetime import datetime, timedelta, timezone
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import json

# Initialize Spark session - this is provided by Databricks but needs to be explicitly referenced
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# NYC 311 API configuration
NYC_311_BASE_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
try:
    APP_TOKEN = dbutils.secrets.get(scope="nyc311", key="app_token")  # Optional but recommended
except:
    APP_TOKEN = None  # Gracefully handle missing secrets for free accounts

# Incremental processing configuration
OVERLAP_HOURS = 6  # overlap window to avoid missing records
MAX_ROWS_PER_RUN = 500000 if environment == "prod" else 50000  # safety limit
HISTORY_FLOOR_DAYS = 30  # only process records from last N days

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Definition
# MAGIC 
# MAGIC The bronze table schema is now created automatically using Delta Lake's schema evolution capabilities.
# MAGIC The table will be created with proper data types during the first write operation.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def soql_floating_ts(dt: datetime) -> str:
    """
    Return a 'floating' timestamp string that Socrata accepts widely: YYYY-MM-DDTHH:MM:SS
    We still compute in UTC, but do not append 'Z' because this dataset treats timestamps as floating.
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S")

def get_json_query(url: str, query: str, headers: dict, timeout: int = 60,
                   max_retries: int = 5, backoff: float = 1.5):
    """
    GET using $query=..., with retries and 429 Retry-After handling.
    Prints server error body on failure to ease debugging.
    """
    attempt = 0
    while True:
        try:
            r = requests.get(url, params={"$query": query}, headers=headers, timeout=timeout)
            if r.status_code == 429:
                retry_after = r.headers.get("Retry-After")
                sleep_s = float(retry_after) if retry_after else backoff ** attempt
                print(f"[rate limit] 429; sleeping {sleep_s:.1f}s (attempt {attempt+1}/{max_retries})")
                time.sleep(sleep_s)
                attempt += 1
                if attempt > max_retries:
                    r.raise_for_status()
                continue
            if r.status_code >= 400:
                try:
                    print("[error body]", r.text[:2000])
                except Exception:
                    pass
                r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            attempt += 1
            if attempt > max_retries:
                raise
            sleep_s = backoff ** (attempt - 1)
            print(f"[warn] HTTP error: {e}. Retry {attempt}/{max_retries} in {sleep_s:.1f}s")
            time.sleep(sleep_s)

def setup_watermark_table():
    """Set up state tracking table for incremental processing"""
    state_table = f"{bronze_catalog}.{schema_name}._state_311"
    
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {state_table} (updated_at_watermark TIMESTAMP)
    USING DELTA
    """)
    
    # Get last watermark
    last_watermark = spark.table(state_table).agg(F.max("updated_at_watermark")).first()[0]
    if last_watermark is None:
        # First run: start from recent history
        last_watermark = datetime.now(timezone.utc) - timedelta(days=3)
        print(f"First run - starting from {last_watermark}")
    else:
        print(f"Incremental run - last watermark: {last_watermark}")
    
    return state_table, last_watermark

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Database Structure

# COMMAND ----------

# Create catalog and schema if they don't exist
try:
    print(f"Creating catalog: {bronze_catalog}")
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {bronze_catalog}")
    print(f"✓ Catalog {bronze_catalog} created/verified")
    
    print(f"Using catalog: {bronze_catalog}")
    spark.sql(f"USE CATALOG {bronze_catalog}")
    
    print(f"Creating schema: {schema_name}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
    print(f"✓ Schema {schema_name} created/verified")
    
    print(f"Using schema: {schema_name}")
    spark.sql(f"USE SCHEMA {schema_name}")
    
    # Verify we can access the schema
    current_catalog = spark.sql("SELECT current_catalog()").collect()[0][0]
    current_schema = spark.sql("SELECT current_schema()").collect()[0][0]
    print(f"✓ Current catalog: {current_catalog}")
    print(f"✓ Current schema: {current_schema}")
    
except Exception as e:
    print(f"❌ Error creating database structure: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Ingestion

# COMMAND ----------

# Set up watermark tracking for incremental processing
state_table, last_watermark = setup_watermark_table()

# Determine table name and check if exists
table_name = "service_requests"
full_table_name = f"{bronze_catalog}.{schema_name}.{table_name}"

# Create table if it doesn't exist
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {full_table_name} (
    unique_key BIGINT,
    created_date TIMESTAMP,
    closed_date TIMESTAMP,
    agency STRING,
    agency_name STRING,
    complaint_type STRING,
    descriptor STRING,
    location_type STRING,
    incident_zip STRING,
    incident_address STRING,
    street_name STRING,
    cross_street_1 STRING,
    cross_street_2 STRING,
    intersection_street_1 STRING,
    intersection_street_2 STRING,
    address_type STRING,
    city STRING,
    landmark STRING,
    facility_type STRING,
    status STRING,
    due_date TIMESTAMP,
    resolution_description STRING,
    resolution_action_updated_date TIMESTAMP,
    community_board STRING,
    bbl STRING,
    borough STRING,
    x_coordinate_state_plane STRING,
    y_coordinate_state_plane STRING,
    open_data_channel_type STRING,
    park_facility_name STRING,
    park_borough STRING,
    vehicle_type STRING,
    taxi_company_borough STRING,
    taxi_pick_up_location STRING,
    bridge_highway_name STRING,
    bridge_highway_segment STRING,
    bridge_highway_direction STRING,
    road_ramp STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    location STRING,
    _updated_at TIMESTAMP,
    ingest_ts TIMESTAMP,
    run_date DATE,
    environment STRING,
    source_system STRING
) USING DELTA
""")

print(f"Table {full_table_name} ready for incremental ingestion")

# COMMAND ----------

# Incremental data fetching using :updated_at watermark
print("Starting incremental data ingestion...")

# Calculate overlap window to avoid gaps
start_ts = last_watermark - timedelta(hours=OVERLAP_HOURS)
where_ts = soql_floating_ts(start_ts)

# Only process records from recent history
created_floor_ts = soql_floating_ts(
    datetime.now(timezone.utc) - timedelta(days=HISTORY_FLOOR_DAYS)
)

print(f"Using :updated_at overlap start: {where_ts}")
print(f"Created floor: {created_floor_ts}")

# Set up headers
headers = {"Accept": "application/json"}
if APP_TOKEN:
    headers["X-App-Token"] = APP_TOKEN

# Initialize pagination
offset = 0
rows_total = 0
dfs = []
ORDER_BY = ":updated_at, :id"
LIMIT = batch_size

# API connection test
try:
    peek_updated = get_json_query(NYC_311_BASE_URL, "SELECT max(:updated_at) AS max_updated", headers, timeout=30)
    print(f"API max(:updated_at): {peek_updated}")
except Exception as e:
    print(f"Could not query API max(:updated_at): {e}")

# COMMAND ----------

# Paging loop with improved error handling
while True:
    # Include :updated_at as a projected column
    soql = (
        "SELECT *, :updated_at AS _updated_at "
        f"WHERE :updated_at >= '{where_ts}' "
        f"  AND created_date >= '{created_floor_ts}' "
        f"ORDER BY {ORDER_BY} "
        f"LIMIT {LIMIT} OFFSET {offset}"
    )
    
    print(f"Fetching batch at offset {offset}")
    batch = get_json_query(NYC_311_BASE_URL, soql, headers)
    
    if not batch:
        print("No more rows; stopping pagination.")
        break
    
    # Create DataFrame with proper typing
    df = spark.createDataFrame(batch)
    
    # Add metadata columns
    df = df.withColumn("ingest_ts", F.current_timestamp()) \
           .withColumn("run_date", F.to_date(F.current_timestamp())) \
           .withColumn("environment", F.lit(environment)) \
           .withColumn("source_system", F.lit("nyc_311_api"))
    
    dfs.append(df)
    rows_total += len(batch)
    offset += LIMIT
    
    print(f"Retrieved {len(batch)} rows; total so far: {rows_total}")
    
    if rows_total >= MAX_ROWS_PER_RUN:
        print(f"Reached MAX_ROWS_PER_RUN={MAX_ROWS_PER_RUN}; stopping early.")
        break

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create DataFrame and Write to Delta

# COMMAND ----------

# Persist & update watermark
if dfs:
    print(f"Processing {len(dfs)} dataframe batches...")
    
    # Union all dataframes
    bronze = dfs[0]
    for extra in dfs[1:]:
        bronze = bronze.unionByName(extra, allowMissingColumns=True)
    
    ingested = bronze.count()
    print(f"Total records to process: {ingested}")
    
    # Ensure proper data types
    if "_updated_at" not in bronze.columns:
        raise RuntimeError("Expected column '_updated_at' not found in batch DataFrame")
    
    bronze = (
        bronze
        .withColumn("_updated_at", F.to_timestamp(F.col("_updated_at")))
        .withColumn("created_date", F.to_timestamp("created_date"))
        .withColumn("closed_date", F.to_timestamp("closed_date"))
        .withColumn("due_date", F.to_timestamp("due_date"))
        .withColumn("resolution_action_updated_date", F.to_timestamp("resolution_action_updated_date"))
        .withColumn("unique_key", F.col("unique_key").cast("bigint"))
        .withColumn("latitude", F.col("latitude").cast("double"))
        .withColumn("longitude", F.col("longitude").cast("double"))
        .withColumn("location", F.col("location").cast("string"))
    )
    
    # Write to Delta table in append mode (incremental)
    print(f"Writing data to {full_table_name} in append mode...")
    
    (bronze
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(full_table_name))
    
    print(f"✓ Data written to {full_table_name}")
    
    # Update watermark to the true max _updated_at from this batch
    max_updated = (
        bronze
          .select(F.col("_updated_at").cast("timestamp").alias("up_ts"))
          .agg(F.max("up_ts").alias("mx"))
          .first()["mx"]
    )
    
    if max_updated:
        spark.sql(f"DELETE FROM {state_table}")
        spark.createDataFrame([(max_updated,)], ["updated_at_watermark"]).write.mode("append").saveAsTable(state_table)
        print(f"✓ Watermark updated to: {max_updated}")
    
    # Display sample data (Databricks-compatible)
    try:
        print("Sample data from table:")
        sample_df = spark.sql(f"SELECT * FROM {full_table_name} ORDER BY _updated_at DESC LIMIT 5")
        try:
            display(sample_df)  # Databricks-specific display function
        except NameError:
            # Fallback for non-Databricks environments
            sample_df.show(5, truncate=False)
    except Exception as e:
        print(f"Could not display sample data: {e}")
    
    # Show basic statistics
    try:
        record_count = spark.sql(f"SELECT COUNT(*) as count FROM {full_table_name}").collect()[0]['count']
        print(f"✓ Total records in table: {record_count}")
        print(f"✓ Records ingested this run: {ingested}")
    except Exception as e:
        print(f"Could not get record count: {e}")
    
else:
    print("❌ No records to process - no data returned from API")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Checks

# COMMAND ----------

# Basic data quality checks - only run if data was successfully written
if dfs and len(dfs) > 0:
    print("=== Data Quality Summary ===")

    # Check for duplicate unique_keys
    try:
        duplicates = spark.sql(f"""
            SELECT COUNT(*) as duplicate_count
            FROM (
                SELECT unique_key, COUNT(*) as cnt
                FROM {full_table_name}
                WHERE unique_key IS NOT NULL
                GROUP BY unique_key
                HAVING COUNT(*) > 1
            )
        """).collect()[0]['duplicate_count']

        print(f"Duplicate unique_keys: {duplicates}")
    except Exception as e:
        print(f"Could not check for duplicates: {e}")

    # Check null rates for key fields
    try:
        null_checks = spark.sql(f"""
            SELECT 
                COUNT(*) as total_records,
                SUM(CASE WHEN unique_key IS NULL THEN 1 ELSE 0 END) as null_unique_key,
                SUM(CASE WHEN created_date IS NULL THEN 1 ELSE 0 END) as null_created_date,
                SUM(CASE WHEN agency IS NULL THEN 1 ELSE 0 END) as null_agency,
                SUM(CASE WHEN complaint_type IS NULL THEN 1 ELSE 0 END) as null_complaint_type,
                SUM(CASE WHEN _updated_at IS NULL THEN 1 ELSE 0 END) as null_updated_at
            FROM {full_table_name}
        """).collect()[0]

        for field, value in null_checks.asDict().items():
            if field != 'total_records':
                percentage = (value / null_checks['total_records']) * 100 if null_checks['total_records'] > 0 else 0
                print(f"{field}: {value} ({percentage:.2f}%)")
    except Exception as e:
        print(f"Could not check null rates: {e}")
        
    # Check watermark advancement
    try:
        current_watermark = spark.table(state_table).agg(F.max("updated_at_watermark")).first()[0]
        print(f"Current watermark: {current_watermark}")
    except Exception as e:
        print(f"Could not check watermark: {e}")
else:
    print("=== No Data to Validate ===")
    print("Data quality checks skipped - no records were processed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestion Complete

# COMMAND ----------

print("=== Bronze Layer Ingestion Complete ===")
print(f"Environment: {environment}")
print(f"Target Table: {full_table_name}")
print(f"Ingestion Timestamp: {datetime.now()}")
if 'ingested' in locals():
    print(f"Records Processed: {ingested}")
    print(f"Total DataFrames: {len(dfs) if 'dfs' in locals() else 0}")
else:
    print("Records Processed: 0 (no data ingested)")
print(f"Max rows per run limit: {MAX_ROWS_PER_RUN}")
print("Incremental processing: ENABLED with watermark tracking")
