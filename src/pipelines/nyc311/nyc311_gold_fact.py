# Databricks notebook source
# MAGIC %md
# MAGIC # NYC 311 Gold Layer - Fact Table Only
# MAGIC 
# MAGIC This notebook creates only the fact table for the star schema.
# MAGIC Requires dimension tables to be created first.
# MAGIC Optimized for Free Edition serverless with minimal concurrent tasks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("silver_catalog", "silver", "Silver Catalog Name")
dbutils.widgets.text("gold_catalog", "gold", "Gold Catalog Name")
dbutils.widgets.text("schema_name", "nyc311", "Schema Name")
dbutils.widgets.text("environment", "dev", "Environment")

silver_catalog = dbutils.widgets.get("silver_catalog")
gold_catalog = dbutils.widgets.get("gold_catalog")
schema_name = dbutils.widgets.get("schema_name")
environment = dbutils.widgets.get("environment")

print(f"Silver Catalog: {silver_catalog}")
print(f"Gold Catalog: {gold_catalog}")
print(f"Schema: {schema_name}")
print(f"Environment: {environment}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports and Configuration

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime

# Optimize for Free Edition serverless - absolute minimum for fact table
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "1")
spark.conf.set("spark.default.parallelism", "1")

# Crucial: stop Delta from spawning parallel writer tasks
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "false")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "false")

# Optional but helps AQE keep it tiny:
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.initialPartitionNum", "1")

# Use existing catalog and schema
spark.sql(f"USE CATALOG {gold_catalog}")
spark.sql(f"USE SCHEMA {schema_name}")

# Source table from silver layer
source_table = f"{silver_catalog}.{schema_name}.service_requests_silver"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Silver Data

# COMMAND ----------

silver_df = spark.table(source_table)
print(f"Silver records: {silver_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Dimension Tables Exist

# COMMAND ----------

# Check that all dimension tables exist
dimension_tables = [
    f"{gold_catalog}.{schema_name}.dim_date",
    f"{gold_catalog}.{schema_name}.dim_agency",
    f"{gold_catalog}.{schema_name}.dim_location",
    f"{gold_catalog}.{schema_name}.dim_complaint_type"
]

for table in dimension_tables:
    try:
        count = spark.table(table).count()
        print(f"✓ {table}: {count:,} records")
    except Exception as e:
        print(f"✗ {table}: ERROR - {e}")
        raise Exception(f"Dimension table {table} not found. Please run dimension creation notebook first.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Dimension Tables

# COMMAND ----------

# Read dimension tables (select only needed columns for efficiency)
print("Loading dimension tables...")

date_dim_df = spark.table(f"{gold_catalog}.{schema_name}.dim_date").select("date_key", "date")
agency_dim_df = spark.table(f"{gold_catalog}.{schema_name}.dim_agency").select("agency_key", "agency")
location_dim_df = spark.table(f"{gold_catalog}.{schema_name}.dim_location").select("location_key", "borough")
complaint_dim_df = spark.table(f"{gold_catalog}.{schema_name}.dim_complaint_type").select("complaint_key", "complaint_type")

print("✓ Dimension tables loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Fact Table

# COMMAND ----------

print("Preparing silver data for fact table joins...")

# Prepare silver data with only needed columns (reduce shuffle data)
silver_prepared = (
    silver_df
    .withColumn("created_date_only", F.col("created_date").cast("date"))
    .select(
        "unique_key", "created_date", "closed_date", "due_date", "response_time_hours",
        "response_time_category", "is_weekend", "status", "open_data_channel_type",
        "resolution_description", "silver_processed_ts", "agency", "complaint_type", "borough",
        "created_date_only"
    )
)

print("✓ Silver data prepared")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Perform Joins

# COMMAND ----------

print("Starting fact table joins with sequential approach...")

# Create explicit aliases for clean join plans
sp = silver_prepared.alias("sp")
dd = F.broadcast(date_dim_df).alias("dd")
ad = F.broadcast(agency_dim_df).alias("ad")
cd = F.broadcast(complaint_dim_df).alias("cd")
ld = F.broadcast(location_dim_df).alias("ld")

# Sequential joins to minimize tasks - one join at a time
print("Step 1: Joining with date dimension...")
fact_df_step1 = sp.join(dd, F.col("sp.created_date_only") == F.col("dd.date"), "left")

print("Step 2: Joining with agency dimension...")
fact_df_step2 = fact_df_step1.join(ad, F.col("sp.agency") == F.col("ad.agency"), "left")

print("Step 3: Joining with complaint dimension...")
fact_df_step3 = fact_df_step2.join(cd, F.col("sp.complaint_type") == F.col("cd.complaint_type"), "left")

print("Step 4: Joining with location dimension...")
fact_df = fact_df_step3.join(ld, F.col("sp.borough") == F.col("ld.borough"), "left")

print("✓ All fact table joins completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Select Final Fact Table Columns

# COMMAND ----------

print("Selecting fact table columns...")

# Select fact table columns with explicit references
fact_service_requests = fact_df.select(
    # Business keys
    F.col("sp.unique_key").alias("service_request_key"),
    
    # Foreign keys to dimensions
    F.col("dd.date_key").alias("date_key"),
    F.col("ad.agency_key").alias("agency_key"),
    F.col("ld.location_key").alias("location_key"),
    F.col("cd.complaint_key").alias("complaint_key"),
    
    # Timestamps
    F.col("sp.created_date"),
    F.col("sp.closed_date"),
    F.col("sp.due_date"),
    
    # Measures and metrics
    F.col("sp.response_time_hours"),
    F.col("sp.response_time_category"),
    
    # Flags and indicators
    F.col("sp.is_weekend"),
    F.when(F.col("sp.status") == "CLOSED", 1).otherwise(0).alias("is_closed"),
    F.when(F.col("sp.closed_date").isNotNull(), 1).otherwise(0).alias("is_resolved"),
    F.when(F.col("sp.response_time_hours") <= 24, 1).otherwise(0).alias("resolved_same_day"),
    F.when(F.col("sp.response_time_hours") <= 168, 1).otherwise(0).alias("resolved_within_week"),
    
    # Additional attributes
    F.col("sp.status"),
    F.col("sp.open_data_channel_type"),
    F.col("sp.resolution_description"),
    
    # Metadata
    F.col("sp.silver_processed_ts"),
    F.current_timestamp().alias("gold_processed_ts"),
    F.lit(environment).alias("environment")
)

print("✓ Fact table columns selected")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Fact Table

# COMMAND ----------

print("Writing fact table...")

# Debug partitions before final repartition
print("Partitions after joins:", fact_service_requests.rdd.getNumPartitions())

# Force to single partition to minimize tasks for Free Edition
print("Repartitioning to single partition for Free Edition compatibility...")
fact_single_partition = fact_service_requests.repartition(1)

# Debug partitions at final stage
print("Partitions at final:", fact_single_partition.rdd.getNumPartitions())

# Write directly without cache - with optimizeWrite off + 1 partition this is 1 task
print("Writing to Delta table...")
fact_single_partition.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{gold_catalog}.{schema_name}.fact_service_requests")

# Get row count AFTER the table exists (single simple scan)
row_count = spark.table(f"{gold_catalog}.{schema_name}.fact_service_requests").count()

print(f"✓ Fact table written to {gold_catalog}.{schema_name}.fact_service_requests")
print(f"✓ Fact table contains {row_count:,} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Alternative: Two-Step Write (if still seeing >5 tasks)

# COMMAND ----------

# Uncomment this section if the direct write still creates too many tasks
"""
print("Using two-step write approach...")

# Step 1: Write to temporary Parquet location
tmp_path = f"/tmp/{schema_name}_fact_sr_single"
print(f"Writing to temporary location: {tmp_path}")

fact_service_requests.repartition(1).write.mode("overwrite").parquet(tmp_path)

# Step 2: Read from Parquet and write to Delta (more obedient to 1 partition)
print("Reading from temp and writing to Delta...")
spark.read.parquet(tmp_path) \
    .repartition(1) \
    .write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{gold_catalog}.{schema_name}.fact_service_requests")

print("Two-step write completed")
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=== Fact Table Creation Complete ===")
print(f"Environment: {environment}")
print(f"Catalog: {gold_catalog}")
print(f"Schema: {schema_name}")
print(f"Processing Timestamp: {datetime.now()}")
print(f"Fact Table: {gold_catalog}.{schema_name}.fact_service_requests ({row_count:,} records)")
print("\n✓ Fact table created successfully!")
print("Next step: Run the aggregate tables creation notebook")
