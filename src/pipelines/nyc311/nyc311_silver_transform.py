# Databricks notebook source
# MAGIC %md
# MAGIC # NYC 311 Silver Layer Data Transformation
# MAGIC 
# MAGIC This notebook transforms bronze layer data into the silver layer with:
# MAGIC - Data type conversions
# MAGIC - Data cleaning and standardization
# MAGIC - Data quality improvements
# MAGIC - Derived columns

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("bronze_catalog", "bronze", "Bronze Catalog Name")
dbutils.widgets.text("silver_catalog", "silver", "Silver Catalog Name")
dbutils.widgets.text("schema_name", "nyc311", "Schema Name")
dbutils.widgets.text("environment", "dev", "Environment")

bronze_catalog = dbutils.widgets.get("bronze_catalog")
silver_catalog = dbutils.widgets.get("silver_catalog")
schema_name = dbutils.widgets.get("schema_name")
environment = dbutils.widgets.get("environment")

print(f"Bronze Catalog: {bronze_catalog}")
print(f"Silver Catalog: {silver_catalog}")
print(f"Schema: {schema_name}")
print(f"Environment: {environment}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports and Configuration

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime
import json

# Set up silver catalog and schema
spark.sql(f"CREATE CATALOG IF NOT EXISTS {silver_catalog}")
spark.sql(f"USE CATALOG {silver_catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
spark.sql(f"USE SCHEMA {schema_name}")

# Source table from bronze layer
source_table = f"{bronze_catalog}.{schema_name}.service_requests"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Bronze Data

# COMMAND ----------

# Read from bronze layer
bronze_df = spark.table(source_table)

print(f"Bronze records: {bronze_df.count()}")
print("Bronze schema:")
bronze_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Transformations

# COMMAND ----------

# MAGIC %md
# MAGIC ### Date and Time Conversions

# COMMAND ----------

# Convert date strings to proper timestamps
silver_df = bronze_df.withColumn(
    "created_date_ts", 
    F.to_timestamp(F.col("created_date"))
).withColumn(
    "closed_date_ts", 
    F.to_timestamp(F.col("closed_date"))
).withColumn(
    "due_date_ts", 
    F.to_timestamp(F.col("due_date"))
).withColumn(
    "resolution_action_updated_date_ts", 
    F.to_timestamp(F.col("resolution_action_updated_date"))
)

# Extract date parts for easier analysis
silver_df = silver_df \
    .withColumn("created_year", F.year("created_date_ts")) \
    .withColumn("created_month", F.month("created_date_ts")) \
    .withColumn("created_day", F.dayofmonth("created_date_ts")) \
    .withColumn("created_dayofweek", F.dayofweek("created_date_ts")) \
    .withColumn("created_hour", F.hour("created_date_ts")) \
    .withColumn("created_quarter", F.quarter("created_date_ts"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Geographic Data Processing

# COMMAND ----------

# Convert coordinates to proper numeric types
silver_df = silver_df \
    .withColumn("latitude_num", F.col("latitude").cast("double")) \
    .withColumn("longitude_num", F.col("longitude").cast("double")) \
    .withColumn("x_coordinate_num", F.col("x_coordinate_state_plane").cast("double")) \
    .withColumn("y_coordinate_num", F.col("y_coordinate_state_plane").cast("double"))

# Parse location JSON and extract coordinates if available
silver_df = silver_df.withColumn(
    "location_parsed",
    F.when(F.col("location").isNotNull(), F.from_json(F.col("location"), "latitude double, longitude double"))
     .otherwise(F.lit(None))
)

# Create consolidated coordinates (prefer direct lat/lon, fallback to location JSON)
silver_df = silver_df \
    .withColumn(
        "latitude_final",
        F.coalesce(F.col("latitude_num"), F.col("location_parsed.latitude"))
    ) \
    .withColumn(
        "longitude_final", 
        F.coalesce(F.col("longitude_num"), F.col("location_parsed.longitude"))
    )

# Add geographic enrichment based on coordinates
silver_df = silver_df \
    .withColumn("has_coordinates", 
                F.col("latitude_final").isNotNull() & F.col("longitude_final").isNotNull()) \
    .withColumn("coordinates_quality",
                F.when((F.col("latitude_final").between(40.4, 41.0)) & 
                       (F.col("longitude_final").between(-74.3, -73.7)), "Valid NYC")
                 .when(F.col("has_coordinates"), "Invalid NYC")
                 .otherwise("Missing")) \
    .withColumn("location_precision",
                F.when(F.col("latitude_num").isNotNull() & F.col("longitude_num").isNotNull(), "High")
                 .when(F.col("location_parsed.latitude").isNotNull(), "Medium") 
                 .otherwise("Low"))

# Create location grid for spatial analysis (0.01 degree grid ~ 1km in NYC)
silver_df = silver_df \
    .withColumn("lat_grid", F.when(F.col("latitude_final").isNotNull(), 
                                   F.round(F.col("latitude_final") / 0.01) * 0.01)) \
    .withColumn("lng_grid", F.when(F.col("longitude_final").isNotNull(), 
                                   F.round(F.col("longitude_final") / 0.01) * 0.01)) \
    .withColumn("grid_cell", F.when(F.col("lat_grid").isNotNull() & F.col("lng_grid").isNotNull(),
                                    F.concat(F.col("lat_grid").cast("string"), F.lit("_"), F.col("lng_grid").cast("string"))))

# Add neighborhood approximation based on coordinates (simplified NYC zones)
silver_df = silver_df.withColumn("neighborhood_zone",
    F.when((F.col("latitude_final") > 40.75) & (F.col("longitude_final") > -73.98), "Upper Manhattan")
     .when((F.col("latitude_final") > 40.71) & (F.col("longitude_final") > -74.02), "Midtown Manhattan")
     .when((F.col("latitude_final") > 40.68) & (F.col("longitude_final") > -74.02), "Lower Manhattan")
     .when((F.col("latitude_final") > 40.65) & (F.col("longitude_final") < -73.98), "Brooklyn North")
     .when((F.col("latitude_final") > 40.58) & (F.col("longitude_final") < -73.95), "Brooklyn South")
     .when((F.col("latitude_final") > 40.72) & (F.col("longitude_final") < -73.85), "Queens North")
     .when((F.col("latitude_final") > 40.65) & (F.col("longitude_final") < -73.80), "Queens South")
     .when((F.col("latitude_final") > 40.82), "Bronx North")
     .when((F.col("latitude_final") > 40.78), "Bronx South")
     .when((F.col("latitude_final") < 40.65) & (F.col("longitude_final") < -74.10), "Staten Island")
     .otherwise("Unknown Zone"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Cleaning and Standardization

# COMMAND ----------

# Clean and standardize text fields
silver_df = silver_df \
    .withColumn("agency_clean", F.upper(F.trim(F.col("agency")))) \
    .withColumn("complaint_type_clean", F.initcap(F.trim(F.col("complaint_type")))) \
    .withColumn("descriptor_clean", F.initcap(F.trim(F.col("descriptor")))) \
    .withColumn("status_clean", F.upper(F.trim(F.col("status")))) \
    .withColumn("borough_clean", F.initcap(F.trim(F.col("borough"))))

# Standardize borough names
silver_df = silver_df.withColumn(
    "borough_standardized",
    F.when(F.col("borough_clean").isin("MANHATTAN", "Manhattan", "NEW YORK"), "Manhattan")
     .when(F.col("borough_clean").isin("BROOKLYN", "Brooklyn", "KINGS"), "Brooklyn")
     .when(F.col("borough_clean").isin("QUEENS", "Queens"), "Queens")
     .when(F.col("borough_clean").isin("BRONX", "Bronx", "THE BRONX"), "Bronx")
     .when(F.col("borough_clean").isin("STATEN ISLAND", "Staten Island", "RICHMOND"), "Staten Island")
     .otherwise(F.col("borough_clean"))
)

# Clean zip codes (remove invalid ones) and community board
silver_df = silver_df.withColumn(
    "incident_zip_clean",
    F.when(
        F.col("incident_zip").rlike("^[0-9]{5}$"),
        F.col("incident_zip")
    ).otherwise(F.lit(None))
).withColumn(
    "community_board_clean",
    F.regexp_replace(F.trim(F.col("community_board")), "[^0-9A-Za-z ]", "")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Business Logic and Derived Fields

# COMMAND ----------

# Calculate response time in hours
silver_df = silver_df.withColumn(
    "response_time_hours",
    F.when(
        (F.col("created_date_ts").isNotNull()) & (F.col("closed_date_ts").isNotNull()),
        (F.unix_timestamp("closed_date_ts") - F.unix_timestamp("created_date_ts")) / 3600
    ).otherwise(F.lit(None))
)

# Categorize response time
silver_df = silver_df.withColumn(
    "response_time_category",
    F.when(F.col("response_time_hours") <= 24, "Same Day")
     .when(F.col("response_time_hours") <= 168, "Within Week")  # 168 hours = 1 week
     .when(F.col("response_time_hours") <= 720, "Within Month")  # 720 hours = 30 days
     .when(F.col("response_time_hours").isNotNull(), "Over Month")
     .otherwise("Unknown")
)

# Create complaint priority based on type
priority_mapping = {
    'Water System': 'High',
    'HEATING': 'High', 
    'Emergency Response Team (ERT)': 'Critical',
    'Street Condition': 'Medium',
    'Noise - Residential': 'Low',
    'Blocked Driveway': 'Low',
    'Illegal Parking': 'Low'
}

# Build priority expression
priority_expr = F.lit("Medium")  # default
for complaint_type, priority in priority_mapping.items():
    priority_expr = F.when(F.col("complaint_type_clean").contains(complaint_type), priority).otherwise(priority_expr)

silver_df = silver_df.withColumn("complaint_priority", priority_expr)

# Add season based on month
silver_df = silver_df.withColumn(
    "season",
    F.when(F.col("created_month").isin(12, 1, 2), "Winter")
     .when(F.col("created_month").isin(3, 4, 5), "Spring")
     .when(F.col("created_month").isin(6, 7, 8), "Summer")
     .when(F.col("created_month").isin(9, 10, 11), "Fall")
     .otherwise("Unknown")
)

# Is weekend flag
silver_df = silver_df.withColumn(
    "is_weekend",
    F.col("created_dayofweek").isin(1, 7)  # Sunday=1, Saturday=7 in Spark
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Final Silver DataFrame

# COMMAND ----------

# Select final columns for silver layer
silver_final = silver_df.select(
    # Original identifiers
    F.col("unique_key"),
    
    # Cleaned timestamps
    F.col("created_date_ts").alias("created_date"),
    F.col("closed_date_ts").alias("closed_date"),
    F.col("due_date_ts").alias("due_date"),
    F.col("resolution_action_updated_date_ts").alias("resolution_action_updated_date"),
    
    # Date components
    F.col("created_year"),
    F.col("created_month"),
    F.col("created_day"),
    F.col("created_dayofweek"),
    F.col("created_hour"),
    F.col("created_quarter"),
    F.col("season"),
    F.col("is_weekend"),
    
    # Cleaned categorical data
    F.col("agency_clean").alias("agency"),
    F.col("agency_name"),
    F.col("complaint_type_clean").alias("complaint_type"),
    F.col("descriptor_clean").alias("descriptor"),
    F.col("status_clean").alias("status"),
    F.col("complaint_priority"),
    
    # Location data (enhanced)
    F.col("borough_standardized").alias("borough"),
    F.col("incident_zip_clean").alias("incident_zip"),
    F.col("incident_address"),
    F.col("street_name"),
    F.col("city"),
    F.col("community_board_clean").alias("community_board"),
    F.col("latitude_final").alias("latitude"),
    F.col("longitude_final").alias("longitude"),
    F.col("x_coordinate_num").alias("x_coordinate_state_plane"),
    F.col("y_coordinate_num").alias("y_coordinate_state_plane"),
    F.col("has_coordinates"),
    F.col("coordinates_quality"),
    F.col("location_precision"),
    F.col("grid_cell"),
    F.col("lat_grid"),
    F.col("lng_grid"),
    F.col("neighborhood_zone"),
    
    # Other important fields
    F.col("location_type"),
    F.col("cross_street_1"),
    F.col("cross_street_2"),
    F.col("resolution_description"),
    F.col("open_data_channel_type"),
    
    # Derived metrics
    F.col("response_time_hours"),
    F.col("response_time_category"),
    
    # Metadata
    F.col("ingest_ts"),
    F.col("run_date"),
    F.current_timestamp().alias("silver_processed_ts"),
    F.lit(environment).alias("environment")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Checks

# COMMAND ----------

print("=== Silver Layer Data Quality Checks ===")

# Basic counts
total_records = silver_final.count()
print(f"Total records: {total_records}")

# Check for valid timestamps
valid_created_dates = silver_final.filter(F.col("created_date").isNotNull()).count()
print(f"Records with valid created_date: {valid_created_dates} ({(valid_created_dates/total_records)*100:.2f}%)")

# Check geographic data
valid_coords = silver_final.filter(
    (F.col("latitude").isNotNull()) & 
    (F.col("longitude").isNotNull()) &
    (F.col("latitude").between(40.4, 40.9)) &  # Reasonable bounds for NYC
    (F.col("longitude").between(-74.3, -73.7))
).count()
print(f"Records with valid NYC coordinates: {valid_coords} ({(valid_coords/total_records)*100:.2f}%)")

# Borough distribution
print("\nBorough Distribution:")
silver_final.groupBy("borough").count().orderBy(F.desc("count")).show()

# Complaint type distribution (top 10)
print("\nTop 10 Complaint Types:")
silver_final.groupBy("complaint_type").count().orderBy(F.desc("count")).limit(10).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Silver Layer

# COMMAND ----------

# Write to silver table
silver_table_name = "service_requests_silver"
full_silver_table = f"{silver_catalog}.{schema_name}.{silver_table_name}"

silver_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("mergeSchema", "true") \
    .saveAsTable(full_silver_table)

print(f"Silver data written to {full_silver_table}")

# Display sample
print("\nSample Silver Data:")
display(spark.table(full_silver_table).limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimization

# COMMAND ----------

# Optimize the table for better query performance
spark.sql(f"OPTIMIZE {full_silver_table}")

# Add Z-ordering on commonly queried columns
spark.sql(f"""
    OPTIMIZE {full_silver_table}
    ZORDER BY (borough, complaint_type, created_year, created_month)
""")

print("Table optimization complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer Processing Complete

# COMMAND ----------

print("=== Silver Layer Processing Complete ===")
print(f"Environment: {environment}")
print(f"Target Table: {full_silver_table}")
print(f"Processing Timestamp: {datetime.now()}")
print(f"Records Processed: {total_records}")
print(f"Records with Valid Coordinates: {valid_coords}")
print(f"Records with Valid Created Date: {valid_created_dates}")
