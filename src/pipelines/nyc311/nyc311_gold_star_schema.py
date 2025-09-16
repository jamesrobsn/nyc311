# Databricks notebook source
# MAGIC %md
# MAGIC # NYC 311 Gold Layer - Star Schema for Analytics (Free Edition Optimized)
# MAGIC 
# MAGIC This notebook creates the gold layer with a star schema optimized for analytics and Power BI:
# MAGIC - **Fact Table**: Service requests with metrics
# MAGIC - **Dimension Tables**: Agency, Location, Time, Complaint Type
# MAGIC - **Aggregate Tables**: Pre-calculated summaries for faster reporting
# MAGIC 
# MAGIC **Free Edition Optimizations Applied:**
# MAGIC - Removed date dimension join from fact table (creates date_key directly)
# MAGIC - Reduced shuffle partitions to 4 (from default 200)
# MAGIC - Single partition writes using coalesce(1)
# MAGIC - Disabled Delta auto-optimization features
# MAGIC - Broadcast hints for small dimension joins

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
import hashlib

# Optimize for Free Edition serverless - keep shuffles tiny for small datasets
spark.conf.set("spark.sql.shuffle.partitions", "4")  # Reduced to minimize tasks for Free Edition

# Set up catalog and schema
spark.sql(f"CREATE CATALOG IF NOT EXISTS {gold_catalog}")
spark.sql(f"USE CATALOG {gold_catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
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
# MAGIC ## Create Dimension Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### Date Dimension

# COMMAND ----------

# Create a comprehensive date dimension
date_dim = silver_df.select("created_date").distinct() \
    .filter(F.col("created_date").isNotNull()) \
    .withColumn("date", F.col("created_date").cast("date")) \
    .select("date").distinct() \
    .withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int")) \
    .withColumn("year", F.year("date")) \
    .withColumn("quarter", F.quarter("date")) \
    .withColumn("month", F.month("date")) \
    .withColumn("month_name", F.date_format("date", "MMMM")) \
    .withColumn("day", F.dayofmonth("date")) \
    .withColumn("day_of_week", F.dayofweek("date")) \
    .withColumn("day_name", F.date_format("date", "EEEE")) \
    .withColumn("week_of_year", F.weekofyear("date")) \
    .withColumn("is_weekend", F.col("day_of_week").isin(1, 7)) \
    .withColumn("is_weekday", ~F.col("day_of_week").isin(1, 7)) \
    .withColumn("fiscal_year", 
                F.when(F.col("month") >= 7, F.col("year") + 1)
                 .otherwise(F.col("year"))) \
    .withColumn("season",
                F.when(F.col("month").isin(12, 1, 2), "Winter")
                 .when(F.col("month").isin(3, 4, 5), "Spring")
                 .when(F.col("month").isin(6, 7, 8), "Summer")
                 .when(F.col("month").isin(9, 10, 11), "Fall"))

# Write date dimension
date_dim.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{gold_catalog}.{schema_name}.dim_date")

print(f"Date dimension created with {date_dim.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Agency Dimension

# COMMAND ----------

# Create agency dimension with additional attributes (simplified for Free Edition)
agency_dim = silver_df.select(
    "agency",
    "agency_name"
).distinct() \
.filter(F.col("agency").isNotNull()) \
.withColumn("agency_key", F.hash("agency")) \
.withColumn(
    "agency_type",
    F.when(F.col("agency").isin("NYPD", "FDNY", "EMS"), "Emergency Services")
     .when(F.col("agency").isin("DOT", "DEP", "DSNY"), "Infrastructure")
     .when(F.col("agency").isin("HPD", "DOB"), "Housing & Buildings")
     .when(F.col("agency").isin("DOHMH", "ACS"), "Health & Social Services")
     .when(F.col("agency").isin("DPR", "DOE"), "Parks & Education")
     .otherwise("Other")
) \
.withColumn(
    "is_emergency_service",
    F.col("agency").isin("NYPD", "FDNY", "EMS")
)

# Write agency dimension
agency_dim.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{gold_catalog}.{schema_name}.dim_agency")

print(f"Agency dimension created with {agency_dim.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Location Dimension

# COMMAND ----------

# Create simplified location dimension for Free Edition (borough-based only)
location_dim = silver_df.select(
    "borough"
).distinct() \
.filter(F.col("borough").isNotNull()) \
.withColumn("location_key", F.hash("borough")) \
.withColumn(
    "region",
    F.when(F.col("borough") == "Manhattan", "Manhattan")
     .when(F.col("borough").isin("Brooklyn", "Queens"), "Western Long Island")
     .when(F.col("borough") == "Bronx", "Bronx")
     .when(F.col("borough") == "Staten Island", "Staten Island")
     .otherwise("Unknown")
) \
.withColumn(
    "zone_type",
    F.when(F.col("borough") == "Manhattan", "Mixed")
     .when(F.col("borough") == "Brooklyn", "Residential")
     .when(F.col("borough") == "Queens", "Residential") 
     .when(F.col("borough") == "Bronx", "Residential")
     .when(F.col("borough") == "Staten Island", "Residential")
     .otherwise("Mixed")
) \
.select(
    "location_key", 
    "borough", 
    "region",
    "zone_type"
)

# Write simplified location dimension
location_dim.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{gold_catalog}.{schema_name}.dim_location")

print(f"Simplified location dimension created with {location_dim.count()} records (borough-level only for Free Edition)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Complaint Type Dimension

# COMMAND ----------

# Create complaint type dimension (simplified for Free Edition)
complaint_dim = silver_df.select(
    "complaint_type",
    "descriptor",
    "complaint_priority"
).distinct() \
.filter(F.col("complaint_type").isNotNull()) \
.withColumn(
    "complaint_key", 
    F.hash(F.concat_ws("||", F.col("complaint_type"), F.coalesce(F.col("descriptor"), F.lit("NULL")), F.coalesce(F.col("complaint_priority"), F.lit("NULL"))))
) \
.withColumn(
    "complaint_category",
    F.when(F.col("complaint_type").contains("Noise"), "Noise")
     .when(F.col("complaint_type").contains("Water"), "Water/Sewer")
     .when(F.col("complaint_type").contains("Heat"), "Housing")
     .when(F.col("complaint_type").contains("Street"), "Transportation")
     .when(F.col("complaint_type").contains("Parking"), "Parking")
     .when(F.col("complaint_type").contains("Animal"), "Animal Services")
     .when(F.col("complaint_type").contains("Homeless"), "Social Services")
     .otherwise("Other")
) \
.withColumn(
    "priority_score",
    F.when(F.col("complaint_priority") == "Critical", 4)
     .when(F.col("complaint_priority") == "High", 3)
     .when(F.col("complaint_priority") == "Medium", 2)
     .when(F.col("complaint_priority") == "Low", 1)
     .otherwise(0)
)

# Write complaint dimension
complaint_dim.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{gold_catalog}.{schema_name}.dim_complaint_type")

print(f"Complaint type dimension created with {complaint_dim.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Fact Table - Sequential Approach

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Prepare Base Silver Data

# COMMAND ----------

# Step 1: Prepare base silver data with date_key and geographic details
print("Step 1: Preparing base silver data...")
silver_base = (
    silver_df
    .withColumn("created_date_only", F.col("created_date").cast("date"))
    .withColumn("date_key", F.date_format("created_date", "yyyyMMdd").cast("int"))  # Create date_key directly
    .select(
        "unique_key", "created_date", "closed_date", "due_date", "response_time_hours",
        "response_time_category", "is_weekend", "status", "open_data_channel_type",
        "resolution_description", "silver_processed_ts", "agency", "complaint_type", "descriptor", "complaint_priority", "borough",
        # Include geographic fields directly in fact table
        "latitude", "longitude", "x_coordinate_state_plane", "y_coordinate_state_plane",
        "grid_cell", "lat_grid", "lng_grid", "neighborhood_zone", "coordinates_quality", 
        "location_precision", "has_coordinates", "incident_zip",
        "created_date_only", "date_key"
    )
    .coalesce(1)  # Single partition to minimize shuffles
)

# Write intermediate table
silver_base.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{gold_catalog}.{schema_name}.temp_silver_base")

print("✓ Base silver data prepared and written")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Join with Agency Dimension

# COMMAND ----------

# Step 2: Join with Agency dimension
print("Step 2: Joining with agency dimension...")
agency_dim_df = spark.table(f"{gold_catalog}.{schema_name}.dim_agency").select("agency_key", "agency")
silver_base_df = spark.table(f"{gold_catalog}.{schema_name}.temp_silver_base")

silver_with_agency = (
    silver_base_df.alias("sb")
    .join(F.broadcast(agency_dim_df).alias("ad"), 
          F.col("sb.agency") == F.col("ad.agency"), "left")
    .select(
        F.col("sb.*"),
        F.col("ad.agency_key")
    )
    .coalesce(1)
)

# Write intermediate table
silver_with_agency.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{gold_catalog}.{schema_name}.temp_silver_with_agency")

print("✓ Agency join completed and written")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Join with Location Dimension

# COMMAND ----------

# Step 3: Join with Location dimension
print("Step 3: Joining with location dimension...")

# First, let's check the size of our dimension table to ensure broadcast is appropriate
location_dim_df = spark.table(f"{gold_catalog}.{schema_name}.dim_location")
location_count = location_dim_df.count()
print(f"Location dimension has {location_count} records")

# Check for data distribution issues
print("Checking location dimension data distribution...")
location_dim_df.groupBy("borough").count().orderBy("count", ascending=False).show()

# Select only needed columns and ensure no nulls in join key
location_dim_small = location_dim_df.select("location_key", "borough").filter(F.col("borough").isNotNull())

silver_with_agency_df = spark.table(f"{gold_catalog}.{schema_name}.temp_silver_with_agency")

# Check silver data distribution for borough values
print("Checking silver data borough distribution...")
silver_with_agency_df.groupBy("borough").count().orderBy("count", ascending=False).show(10)

# Only broadcast if dimension is small (< 1000 records), otherwise regular join
if location_count < 1000:
    print(f"Using broadcast join for location dimension ({location_count} records)")
    silver_with_location = (
        silver_with_agency_df.alias("swa")
        .join(F.broadcast(location_dim_small).alias("ld"), 
              F.col("swa.borough") == F.col("ld.borough"), "left")
        .select(
            F.col("swa.*"),
            F.col("ld.location_key")
        )
        .coalesce(1)
    )
else:
    print(f"Using regular join for location dimension ({location_count} records)")
    silver_with_location = (
        silver_with_agency_df.alias("swa")
        .join(location_dim_small.alias("ld"), 
              F.col("swa.borough") == F.col("ld.borough"), "left")
        .select(
            F.col("swa.*"),
            F.col("ld.location_key")
        )
        .coalesce(1)
    )

# Write intermediate table
silver_with_location.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{gold_catalog}.{schema_name}.temp_silver_with_location")

print("✓ Location join completed and written")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Join with Complaint Type Dimension

# COMMAND ----------

# Step 4: Join with Complaint Type dimension
print("Step 4: Joining with complaint type dimension...")
complaint_dim_df = spark.table(f"{gold_catalog}.{schema_name}.dim_complaint_type").select("complaint_key", "complaint_type", "descriptor", "complaint_priority")
silver_with_location_df = spark.table(f"{gold_catalog}.{schema_name}.temp_silver_with_location")

fact_df = (
    silver_with_location_df.alias("swl")
    .join(F.broadcast(complaint_dim_df).alias("cd"), 
          (F.col("swl.complaint_type") == F.col("cd.complaint_type")) &
          (F.coalesce(F.col("swl.descriptor"), F.lit("NULL")) == F.coalesce(F.col("cd.descriptor"), F.lit("NULL"))) &
          (F.coalesce(F.col("swl.complaint_priority"), F.lit("NULL")) == F.coalesce(F.col("cd.complaint_priority"), F.lit("NULL"))), "left")
    .select(
        F.col("swl.*"),
        F.col("cd.complaint_key")
    )
)

print("✓ All sequential joins completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5: Create Final Fact Table

# COMMAND ----------

# Step 5: Create final fact table with all dimensions joined
print("Step 5: Creating final fact table...")
fact_service_requests = fact_df.select(
    # Business keys
    F.col("unique_key").alias("service_request_key"),
    
    # Foreign keys to dimensions
    F.col("date_key").alias("date_key"),  # Using directly created date_key
    F.col("agency_key").alias("agency_key"),
    F.col("location_key").alias("location_key"),
    F.col("complaint_key").alias("complaint_key"),
    
    # Timestamps
    F.col("created_date"),
    F.col("closed_date"),
    F.col("due_date"),
    
    # Geographic coordinates (for map visualizations)
    F.col("latitude"),
    F.col("longitude"),
    F.col("x_coordinate_state_plane"),
    F.col("y_coordinate_state_plane"),
    F.col("grid_cell"),
    F.col("lat_grid"),
    F.col("lng_grid"),
    F.col("neighborhood_zone"),
    F.col("coordinates_quality"),
    F.col("location_precision"),
    F.col("has_coordinates"),
    
    # Measures and metrics
    F.col("response_time_hours"),
    F.col("response_time_category"),
    
    # Flags and indicators
    F.col("is_weekend"),
    F.when(F.col("status") == "CLOSED", 1).otherwise(0).alias("is_closed"),
    F.when(F.col("closed_date").isNotNull(), 1).otherwise(0).alias("is_resolved"),
    F.when(F.col("response_time_hours") <= 24, 1).otherwise(0).alias("resolved_same_day"),
    F.when(F.col("response_time_hours") <= 168, 1).otherwise(0).alias("resolved_within_week"),
    F.when(F.col("coordinates_quality") == "Valid NYC", 1).otherwise(0).alias("has_valid_coordinates"),
    
    # Additional attributes
    F.col("status"),
    F.col("open_data_channel_type"),
    F.col("resolution_description"),
    
    # Metadata
    F.col("silver_processed_ts"),
    F.current_timestamp().alias("gold_processed_ts"),
    F.lit(environment).alias("environment")
).coalesce(1)  # Single partition for final write

# Write fact table with minimal partitions for Free Edition
fact_service_requests.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("delta.autoOptimize.optimizeWrite", "false") \
    .option("delta.autoOptimize.autoCompact", "false") \
    .saveAsTable(f"{gold_catalog}.{schema_name}.fact_service_requests")

print(f"✓ Fact table written to {gold_catalog}.{schema_name}.fact_service_requests")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6: Cleanup Temporary Tables

# COMMAND ----------

# Cleanup temporary tables to free up space
print("Cleaning up temporary tables...")
try:
    spark.sql(f"DROP TABLE IF EXISTS {gold_catalog}.{schema_name}.temp_silver_base")
    spark.sql(f"DROP TABLE IF EXISTS {gold_catalog}.{schema_name}.temp_silver_with_agency")
    spark.sql(f"DROP TABLE IF EXISTS {gold_catalog}.{schema_name}.temp_silver_with_location")
    print("✓ Temporary tables cleaned up")
except Exception as e:
    print(f"Note: Could not clean up some temporary tables: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Aggregate Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Fact Table for Aggregates

# COMMAND ----------

# Load fact table and date dimension for aggregates (sequential approach)
print("Loading fact table for aggregates...")
fact_table_df = spark.table(f"{gold_catalog}.{schema_name}.fact_service_requests")
date_dim_df = spark.table(f"{gold_catalog}.{schema_name}.dim_date")

print("✓ Fact table and date dimension loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Daily Summary

# COMMAND ----------

# Daily summary for dashboards (single join to minimize tasks)
print("Creating daily summary aggregate...")
daily_summary = fact_table_df.join(
    date_dim_df.select("date_key", "date", "year", "month", "day_name"), 
    fact_table_df.date_key == date_dim_df.date_key, 
    "left"
).groupBy(
    fact_table_df.date_key,
    F.col("date").alias("report_date"),
    "year",
    "month", 
    "day_name"
).agg(
    F.count("*").alias("total_requests"),
    F.sum(F.col("is_closed").cast("int")).alias("closed_requests"),
    F.avg("response_time_hours").alias("avg_response_time_hours"),
    F.sum(F.col("resolved_same_day").cast("int")).alias("same_day_resolutions"),
    F.sum(F.col("is_weekend").cast("int")).alias("weekend_requests"),
    F.sum(F.col("has_valid_coordinates").cast("int")).alias("requests_with_coordinates")
).withColumn(
    "closure_rate",
    (F.col("closed_requests") / F.col("total_requests")) * 100
).withColumn(
    "same_day_rate", 
    (F.col("same_day_resolutions") / F.col("total_requests")) * 100
).withColumn(
    "coordinate_coverage",
    (F.col("requests_with_coordinates") / F.col("total_requests")) * 100
)

daily_summary.coalesce(2).write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{gold_catalog}.{schema_name}.agg_daily_summary")

print("✓ Daily summary created")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Geographic Summary by Borough and Neighborhood

# COMMAND ----------

# Geographic summary for spatial analysis (simplified for Free Edition - no joins needed)
print("Creating geographic summary aggregate...")
geographic_summary = fact_table_df.groupBy(
    "neighborhood_zone"
).agg(
    F.count("*").alias("total_requests"),
    F.sum(F.col("is_closed").cast("int")).alias("closed_requests"),
    F.avg("response_time_hours").alias("avg_response_time_hours"),
    F.sum(F.col("resolved_same_day").cast("int")).alias("same_day_resolutions"),
    F.sum(F.col("has_valid_coordinates").cast("int")).alias("requests_with_coordinates"),
    F.avg("latitude").alias("avg_latitude"),
    F.avg("longitude").alias("avg_longitude"),
    F.countDistinct("grid_cell").alias("unique_grid_cells"),
    F.count(F.when(F.col("coordinates_quality") == "Valid NYC", 1)).alias("valid_coordinates_count"),
    F.first("neighborhood_zone").alias("zone_name")  # For display purposes
).withColumn(
    "closure_rate",
    (F.col("closed_requests") / F.col("total_requests")) * 100
).withColumn(
    "same_day_rate", 
    (F.col("same_day_resolutions") / F.col("total_requests")) * 100
).withColumn(
    "coordinate_coverage",
    (F.col("requests_with_coordinates") / F.col("total_requests")) * 100
).withColumn(
    "requests_per_grid_cell",
    F.when(F.col("unique_grid_cells") > 0, F.col("total_requests") / F.col("unique_grid_cells")).otherwise(0)
).filter(F.col("neighborhood_zone").isNotNull())

geographic_summary.coalesce(1).write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{gold_catalog}.{schema_name}.agg_geographic_summary")

print("✓ Geographic summary created")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Grid Cell Heatmap Data

# COMMAND ----------

# Grid cell summary for heatmap visualizations
print("Creating grid cell heatmap data...")
grid_summary = fact_table_df.filter(
    (F.col("has_valid_coordinates") == 1) & F.col("grid_cell").isNotNull()
).groupBy(
    "grid_cell",
    "lat_grid",
    "lng_grid",
    "neighborhood_zone"
).agg(
    F.count("*").alias("total_requests"),
    F.sum(F.col("is_closed").cast("int")).alias("closed_requests"),
    F.avg("response_time_hours").alias("avg_response_time_hours"),
    F.countDistinct("complaint_key").alias("unique_complaint_types"),
    F.countDistinct("agency_key").alias("unique_agencies")
).withColumn(
    "closure_rate",
    (F.col("closed_requests") / F.col("total_requests")) * 100
).withColumn(
    "request_density_category",
    F.when(F.col("total_requests") >= 100, "High")
     .when(F.col("total_requests") >= 50, "Medium") 
     .when(F.col("total_requests") >= 10, "Low")
     .otherwise("Very Low")
)

grid_summary.coalesce(1).write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{gold_catalog}.{schema_name}.agg_grid_heatmap")

print("✓ Grid cell heatmap data created")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Monthly Summary by Agency - SKIPPED FOR FREE EDITION

# COMMAND ----------

# Skip monthly agency summary - too complex for Free Edition (double joins + aggregations)
# This can be recreated in Power BI or through simpler queries if needed
print("Skipping monthly agency summary - too resource intensive for Free Edition")
print("✓ Monthly agency summary skipped (can be recreated in Power BI)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Complaint Type Performance

# COMMAND ----------

# Complaint performance (using fact table directly to avoid joins)
print("Creating complaint type performance summary...")
complaint_dim_df = spark.table(f"{gold_catalog}.{schema_name}.dim_complaint_type")

complaint_performance = fact_table_df.join(
    complaint_dim_df.select("complaint_key", "complaint_type", "complaint_priority"),
    fact_table_df.complaint_key == complaint_dim_df.complaint_key,
    "left"
).groupBy(
    "complaint_type",
    "complaint_priority"
).agg(
    F.count("*").alias("total_requests"),
    F.sum("is_closed").alias("closed_requests"),
    F.avg("response_time_hours").alias("avg_response_time_hours"),
    F.expr("percentile_approx(response_time_hours, 0.5)").alias("median_response_time_hours"),
    F.expr("percentile_approx(response_time_hours, 0.95)").alias("p95_response_time_hours"),
    F.sum("resolved_same_day").alias("same_day_resolutions"),
    F.sum("resolved_within_week").alias("week_resolutions")
).withColumn(
    "closure_rate",
    (F.col("closed_requests") / F.col("total_requests")) * 100
).withColumn(
    "same_day_rate",
    (F.col("same_day_resolutions") / F.col("total_requests")) * 100
).withColumn(
    "week_resolution_rate",
    (F.col("week_resolutions") / F.col("total_requests")) * 100
)

complaint_performance.coalesce(2).write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{gold_catalog}.{schema_name}.agg_complaint_performance")

print(f"✓ Complaint performance summary created")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Borough Comparison

# COMMAND ----------

# Borough comparison (using fact table directly)
print("Creating borough comparison summary...")
location_dim_df = spark.table(f"{gold_catalog}.{schema_name}.dim_location")

borough_comparison = fact_table_df.join(
    location_dim_df.select("location_key", "borough"),
    fact_table_df.location_key == location_dim_df.location_key,
    "left"
).groupBy(
    "borough"
).agg(
    F.count("*").alias("total_requests"),
    F.sum("is_closed").alias("closed_requests"),
    F.avg("response_time_hours").alias("avg_response_time_hours"),
    F.expr("percentile_approx(response_time_hours, 0.5)").alias("median_response_time_hours"),
    F.sum("resolved_same_day").alias("same_day_resolutions")
).withColumn(
    "closure_rate",
    (F.col("closed_requests") / F.col("total_requests")) * 100
).withColumn(
    "same_day_rate",
    (F.col("same_day_resolutions") / F.col("total_requests")) * 100
)

borough_comparison.coalesce(1).write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{gold_catalog}.{schema_name}.agg_borough_comparison")

print(f"✓ Borough comparison created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimize Tables

# COMMAND ----------

# Skip table optimization in dev environment to avoid resource contention
if environment == "prod":
    # List of tables to optimize
    tables_to_optimize = [
        f"{gold_catalog}.{schema_name}.dim_date",
        f"{gold_catalog}.{schema_name}.dim_agency", 
        f"{gold_catalog}.{schema_name}.dim_location",
        f"{gold_catalog}.{schema_name}.dim_complaint_type",
        f"{gold_catalog}.{schema_name}.fact_service_requests",
        f"{gold_catalog}.{schema_name}.agg_daily_summary",
        f"{gold_catalog}.{schema_name}.agg_geographic_summary",
        f"{gold_catalog}.{schema_name}.agg_grid_heatmap",
        # Monthly agency summary skipped for Free Edition
        f"{gold_catalog}.{schema_name}.agg_complaint_performance",
        f"{gold_catalog}.{schema_name}.agg_borough_comparison"
    ]

    for table in tables_to_optimize:
        print(f"Optimizing {table}...")
        spark.sql(f"OPTIMIZE {table}")

    # Z-order key tables for better performance
    spark.sql(f"OPTIMIZE {gold_catalog}.{schema_name}.fact_service_requests ZORDER BY (date_key, agency_key, location_key)")
    spark.sql(f"OPTIMIZE {gold_catalog}.{schema_name}.agg_daily_summary ZORDER BY (report_date)")
    spark.sql(f"OPTIMIZE {gold_catalog}.{schema_name}.agg_geographic_summary ZORDER BY (neighborhood_zone)")
    spark.sql(f"OPTIMIZE {gold_catalog}.{schema_name}.agg_grid_heatmap ZORDER BY (lat_grid, lng_grid)")

    print("All tables optimized")
else:
    print("Skipping table optimization in dev environment to conserve resources")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Views for Power BI - SKIPPED FOR FREE EDITION

# COMMAND ----------

# Skip Power BI view creation - complex multi-table joins too resource intensive for Free Edition
# Power BI can connect directly to fact and dimension tables for flexible reporting
print("Skipping Power BI view creation - too resource intensive for Free Edition")
print("✓ Power BI can connect directly to fact_service_requests and dimension tables")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality and Summary

# COMMAND ----------

print("=== Gold Layer Summary ===")

# Get record counts for all tables
tables = [
    ("NYC 311 Date Dimension", f"{gold_catalog}.{schema_name}.dim_date"),
    ("NYC 311 Agency Dimension", f"{gold_catalog}.{schema_name}.dim_agency"),
    ("NYC 311 Location Dimension", f"{gold_catalog}.{schema_name}.dim_location"),
    ("NYC 311 Complaint Type Dimension", f"{gold_catalog}.{schema_name}.dim_complaint_type"),
    ("NYC 311 Fact Service Requests", f"{gold_catalog}.{schema_name}.fact_service_requests"),
    ("NYC 311 Daily Summary", f"{gold_catalog}.{schema_name}.agg_daily_summary"),
    ("NYC 311 Geographic Summary", f"{gold_catalog}.{schema_name}.agg_geographic_summary"),
    ("NYC 311 Grid Heatmap Data", f"{gold_catalog}.{schema_name}.agg_grid_heatmap"),
    # Monthly agency summary skipped for Free Edition
    ("NYC 311 Complaint Performance", f"{gold_catalog}.{schema_name}.agg_complaint_performance"),
    ("NYC 311 Borough Comparison", f"{gold_catalog}.{schema_name}.agg_borough_comparison")
]

for table_name, full_table_name in tables:
    count = spark.table(full_table_name).count()
    print(f"{table_name}: {count:,} records")

# Show sample from fact table instead
print("\nSample from Fact Table:")
display(spark.table(f"{gold_catalog}.{schema_name}.fact_service_requests").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer Processing Complete

# COMMAND ----------

print("=== Gold Layer Processing Complete ===")
print(f"Environment: {environment}")
print(f"Catalog: {gold_catalog}")
print(f"Schema: {schema_name}")
print(f"Processing Timestamp: {datetime.now()}")
print("\nCreated Tables:")
for table_name, _ in tables:
    print(f"  - {table_name}")
print("\nStar schema is ready for analytics and Power BI reporting!")
print("Note: Power BI can connect directly to fact_service_requests and dimension tables")
