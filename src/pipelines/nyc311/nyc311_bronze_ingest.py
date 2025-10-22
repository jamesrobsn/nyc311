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
dbutils.widgets.text("batch_size", "500000", "Batch Size for API requests")

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

# NYC 311 API configuration
NYC_311_BASE_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
try:
    APP_TOKEN = dbutils.secrets.get(scope="nyc311", key="app_token")
except:
    APP_TOKEN = None
    print("Warning: No app token (rate limits apply)")

# Incremental processing configuration
OVERLAP_HOURS = 6
MAX_ROWS_PER_RUN = 5000000 if environment == "prod" else 500000
HISTORY_FLOOR_DAYS = 30

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def soql_floating_ts(dt: datetime) -> str:
    """Convert datetime to Socrata floating timestamp format: YYYY-MM-DDTHH:MM:SS"""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S")

def get_json_query(url: str, query: str, headers: dict, timeout: int = 60,
                   max_retries: int = 5, backoff: float = 1.5):
    """GET using $query=..., with retries and exponential backoff for 429 rate limits"""
    attempt = 0
    while True:
        try:
            r = requests.get(url, params={"$query": query}, headers=headers, timeout=timeout)
            if r.status_code == 429:
                retry_after = r.headers.get("Retry-After")
                sleep_s = float(retry_after) if retry_after else backoff ** attempt
                print(f"Rate limit hit; sleeping {sleep_s:.1f}s (attempt {attempt+1}/{max_retries})")
                time.sleep(sleep_s)
                attempt += 1
                if attempt > max_retries:
                    r.raise_for_status()
                continue
            if r.status_code >= 400:
                print(f"Error response: {r.text[:500]}")
                r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            attempt += 1
            if attempt > max_retries:
                raise
            sleep_s = backoff ** (attempt - 1)
            print(f"HTTP error: {e}. Retry {attempt}/{max_retries} in {sleep_s:.1f}s")
            time.sleep(sleep_s)

def setup_watermark_table():
    """Set up state tracking table for incremental processing"""
    state_table = f"{bronze_catalog}.{schema_name}._state_311"
    
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {state_table} (updated_at_watermark TIMESTAMP)
    USING DELTA
    """)
    
    last_watermark = spark.table(state_table).agg(F.max("updated_at_watermark")).first()[0]
    if last_watermark is None:
        last_watermark = datetime.now(timezone.utc) - timedelta(days=3)
        print(f"First run - starting from {last_watermark}")
    else:
        print(f"Incremental run - last watermark: {last_watermark}")
    
    return state_table, last_watermark

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Database Structure

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {bronze_catalog}")
spark.sql(f"USE CATALOG {bronze_catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
spark.sql(f"USE SCHEMA {schema_name}")
print(f"Using {bronze_catalog}.{schema_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Ingestion

# COMMAND ----------

state_table, last_watermark = setup_watermark_table()

table_name = "service_requests"
full_table_name = f"{bronze_catalog}.{schema_name}.{table_name}"

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

print(f"Table ready: {full_table_name}")

# COMMAND ----------

start_ts = last_watermark - timedelta(hours=OVERLAP_HOURS)
where_ts = soql_floating_ts(start_ts)
created_floor_ts = soql_floating_ts(datetime.now(timezone.utc) - timedelta(days=HISTORY_FLOOR_DAYS))

print(f"Watermark start: {where_ts}")
print(f"Created floor: {created_floor_ts}")

headers = {"Accept": "application/json"}
if APP_TOKEN:
    headers["X-App-Token"] = APP_TOKEN

offset = 0
rows_total = 0
dfs = []
ORDER_BY = ":updated_at, :id"
LIMIT = batch_size

# COMMAND ----------

while True:
    soql = (
        "SELECT *, :updated_at AS _updated_at "
        f"WHERE :updated_at >= '{where_ts}' "
        f"  AND created_date >= '{created_floor_ts}' "
        f"ORDER BY {ORDER_BY} "
        f"LIMIT {LIMIT} OFFSET {offset}"
    )
    
    print(f"Fetching offset {offset}")
    batch = get_json_query(NYC_311_BASE_URL, soql, headers)
    
    if not batch:
        print("No more rows")
        break
    
    df = spark.createDataFrame(batch) \
             .withColumn("ingest_ts", F.current_timestamp()) \
             .withColumn("run_date", F.to_date(F.current_timestamp())) \
             .withColumn("environment", F.lit(environment)) \
             .withColumn("source_system", F.lit("nyc_311_api"))
    
    dfs.append(df)
    rows_total += len(batch)
    offset += LIMIT
    
    print(f"Retrieved {len(batch)} rows; total: {rows_total}")
    
    if rows_total >= MAX_ROWS_PER_RUN:
        print(f"Reached limit: {MAX_ROWS_PER_RUN}")
        break

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Delta

# COMMAND ----------

if dfs:
    print(f"Processing {len(dfs)} batches...")
    
    bronze = dfs[0]
    for extra in dfs[1:]:
        bronze = bronze.unionByName(extra, allowMissingColumns=True)
    
    ingested = bronze.count()
    print(f"Total records: {ingested}")
    
    if "_updated_at" not in bronze.columns:
        raise RuntimeError("Missing required column: _updated_at")
    
    bronze = (
        bronze
        .withColumn("_updated_at", F.to_timestamp("_updated_at"))
        .withColumn("created_date", F.to_timestamp("created_date"))
        .withColumn("closed_date", F.to_timestamp("closed_date"))
        .withColumn("due_date", F.to_timestamp("due_date"))
        .withColumn("resolution_action_updated_date", F.to_timestamp("resolution_action_updated_date"))
        .withColumn("unique_key", F.col("unique_key").cast("bigint"))
        .withColumn("latitude", F.col("latitude").cast("double"))
        .withColumn("longitude", F.col("longitude").cast("double"))
        .withColumn("location", F.col("location").cast("string"))
    )
    
    print(f"Writing to {full_table_name} (MERGE on unique_key)...")
    
    # Get pre-merge count
    count_before = spark.sql(f"SELECT COUNT(*) as count FROM {full_table_name}").first()['count']
    
    # Use Delta merge to handle duplicates (upsert pattern)
    bronze.createOrReplaceTempView("bronze_staging")
    
    merge_result = spark.sql(f"""
    MERGE INTO {full_table_name} AS target
    USING bronze_staging AS source
    ON target.unique_key = source.unique_key
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)
    
    # Get post-merge count
    count_after = spark.sql(f"SELECT COUNT(*) as count FROM {full_table_name}").first()['count']
    records_inserted = count_after - count_before
    records_updated = ingested - records_inserted
    
    if records_inserted == 0 and records_updated > 0:
        print(f"Merged: {records_updated:,} existing records refreshed (no new service requests)")
    elif records_inserted > 0 and records_updated == 0:
        print(f"Merged: {records_inserted:,} new records inserted")
    elif records_inserted > 0 and records_updated > 0:
        print(f"Merged: {records_inserted:,} new records, {records_updated:,} existing records refreshed")
    else:
        print(f"Merged: No changes (0 records)")
    
    max_updated = bronze.agg(F.max("_updated_at")).first()[0]
    if max_updated:
        spark.sql(f"DELETE FROM {state_table}")
        spark.createDataFrame([(max_updated,)], ["updated_at_watermark"]) \
             .write.mode("append").saveAsTable(state_table)
        print(f"Watermark updated: {max_updated}")
    
    try:
        display(spark.sql(f"SELECT * FROM {full_table_name} ORDER BY _updated_at DESC LIMIT 5"))
    except NameError:
        spark.sql(f"SELECT * FROM {full_table_name} ORDER BY _updated_at DESC LIMIT 5").show(5)
    
    print(f"Total records in table: {count_after:,}")
    print(f"Batch summary: {ingested:,} records fetched from API ({records_inserted:,} new, {records_updated:,} existing)")
else:
    print("No data to process")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Checks

# COMMAND ----------

if dfs:
    print("=== Data Quality ===")
    
    duplicates = spark.sql(f"""
        SELECT COUNT(*) as count
        FROM (
            SELECT unique_key, COUNT(*) as cnt
            FROM {full_table_name}
            WHERE unique_key IS NOT NULL
            GROUP BY unique_key
            HAVING COUNT(*) > 1
        )
    """).first()['count']
    print(f"Duplicate unique_keys: {duplicates}")
    
    null_checks = spark.sql(f"""
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN unique_key IS NULL THEN 1 ELSE 0 END) as null_unique_key,
            SUM(CASE WHEN created_date IS NULL THEN 1 ELSE 0 END) as null_created_date,
            SUM(CASE WHEN agency IS NULL THEN 1 ELSE 0 END) as null_agency,
            SUM(CASE WHEN complaint_type IS NULL THEN 1 ELSE 0 END) as null_complaint_type
        FROM {full_table_name}
    """).first()
    
    for field, value in null_checks.asDict().items():
        if field != 'total':
            pct = (value / null_checks['total']) * 100 if null_checks['total'] > 0 else 0
            print(f"{field}: {value} ({pct:.2f}%)")
else:
    print("No data to validate")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Complete

# COMMAND ----------

print("=== Bronze Ingestion Complete ===")
print(f"Table: {full_table_name}")
print(f"Records: {ingested if 'ingested' in locals() else 0}")
print(f"Limit: {MAX_ROWS_PER_RUN}")
