# Databricks notebook source
# MAGIC %md
# MAGIC # NYC 311 Gold Layer - Star Schema for Analytics
# MAGIC 
# MAGIC This notebook creates the gold layer with a star schema optimized for analytics and Power BI:
# MAGIC - **Fact Table**: Service requests with metrics
# MAGIC - **Dimension Tables**: Agency, Location, Time, Complaint Type
# MAGIC - **Aggregate Tables**: Pre-calculated summaries for faster reporting

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
spark.conf.set("spark.sql.shuffle.partitions", "8")  # Default 200 is too many for 50k rows

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
    .withColumn("date_key", F.date_format("created_date", "yyyyMMdd").cast("int")) \
    .withColumn("date", F.col("created_date").cast("date")) \
    .withColumn("year", F.year("created_date")) \
    .withColumn("quarter", F.quarter("created_date")) \
    .withColumn("month", F.month("created_date")) \
    .withColumn("month_name", F.date_format("created_date", "MMMM")) \
    .withColumn("day", F.dayofmonth("created_date")) \
    .withColumn("day_of_week", F.dayofweek("created_date")) \
    .withColumn("day_name", F.date_format("created_date", "EEEE")) \
    .withColumn("week_of_year", F.weekofyear("created_date")) \
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

# Create agency dimension with additional attributes
agency_dim = silver_df.select(
    "agency",
    "agency_name"
).distinct() \
.filter(F.col("agency").isNotNull()) \
.withColumn("agency_key", F.monotonically_increasing_id()) \
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

# Create location dimension
location_dim = silver_df.select(
    "borough",
    "incident_zip",
    "community_board",
    "latitude",
    "longitude"
).distinct() \
.filter(F.col("borough").isNotNull()) \
.withColumn("location_key", F.monotonically_increasing_id()) \
.withColumn(
    "region",
    F.when(F.col("borough") == "Manhattan", "Manhattan")
     .when(F.col("borough").isin("Brooklyn", "Queens"), "Western Long Island")
     .when(F.col("borough") == "Bronx", "Bronx")
     .when(F.col("borough") == "Staten Island", "Staten Island")
     .otherwise("Unknown")
) \
.withColumn(
    "has_coordinates",
    (F.col("latitude").isNotNull()) & (F.col("longitude").isNotNull())
)

# Write location dimension
location_dim.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{gold_catalog}.{schema_name}.dim_location")

print(f"Location dimension created with {location_dim.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Complaint Type Dimension

# COMMAND ----------

# Create complaint type dimension
complaint_dim = silver_df.select(
    "complaint_type",
    "descriptor",
    "complaint_priority"
).distinct() \
.filter(F.col("complaint_type").isNotNull()) \
.withColumn("complaint_key", F.monotonically_increasing_id()) \
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
# MAGIC ## Create Fact Table

# COMMAND ----------

# Read dimension tables (select only needed columns for efficiency)
date_dim_df = spark.table(f"{gold_catalog}.{schema_name}.dim_date").select("date_key", "date")
agency_dim_df = spark.table(f"{gold_catalog}.{schema_name}.dim_agency").select("agency_key", "agency")
location_dim_df = spark.table(f"{gold_catalog}.{schema_name}.dim_location").select("location_key", "borough")
complaint_dim_df = spark.table(f"{gold_catalog}.{schema_name}.dim_complaint_type").select("complaint_key", "complaint_type")

print("Dimension tables loaded, starting fact table joins...")

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

# Create explicit aliases for clean join plans
sp = silver_prepared.alias("sp")
dd = F.broadcast(date_dim_df).alias("dd")
ad = F.broadcast(agency_dim_df).alias("ad")
cd = F.broadcast(complaint_dim_df).alias("cd")
ld = F.broadcast(location_dim_df).alias("ld")

# Optimized joins with explicit column references
fact_df = (
    sp
    .join(dd, F.col("sp.created_date_only") == F.col("dd.date"), "left")
    .join(ad, F.col("sp.agency") == F.col("ad.agency"), "left")
    .join(cd, F.col("sp.complaint_type") == F.col("cd.complaint_type"), "left")
    .join(ld, F.col("sp.borough") == F.col("ld.borough"), "left")
)

print("Fact table joins completed")

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

# Write fact table with coalesced partitions for small data
fact_service_requests.coalesce(4).write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{gold_catalog}.{schema_name}.fact_service_requests")

print(f"✓ Fact table written to {gold_catalog}.{schema_name}.fact_service_requests")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Aggregate Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### Daily Summary

# COMMAND ----------

# Daily summary for dashboards
daily_summary = fact_df.groupBy(
    "date_key",
    date_dim_df.date.alias("report_date"),
    date_dim_df.year,
    date_dim_df.month,
    date_dim_df.day_name,
    "borough",
    "agency"
).agg(
    F.count("*").alias("total_requests"),
    F.sum(F.when(F.col("status") == "CLOSED", 1).otherwise(0)).alias("closed_requests"),
    F.avg("response_time_hours").alias("avg_response_time_hours"),
    F.sum(F.when(F.col("response_time_hours") <= 24, 1).otherwise(0)).alias("same_day_resolutions"),
    F.countDistinct("complaint_type").alias("unique_complaint_types"),
    F.count(F.when(F.col("is_weekend"), 1)).alias("weekend_requests")
).withColumn(
    "closure_rate",
    (F.col("closed_requests") / F.col("total_requests")) * 100
).withColumn(
    "same_day_rate", 
    (F.col("same_day_resolutions") / F.col("total_requests")) * 100
)

daily_summary.coalesce(2).write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{gold_catalog}.{schema_name}.agg_daily_summary")

print(f"✓ Daily summary created")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Monthly Summary by Agency

# COMMAND ----------

monthly_agency_summary = fact_df.groupBy(
    date_dim_df.year,
    date_dim_df.month,
    date_dim_df.month_name,
    "agency",
    "agency_name"
).agg(
    F.count("*").alias("total_requests"),
    F.sum(F.when(F.col("status") == "CLOSED", 1).otherwise(0)).alias("closed_requests"),
    F.avg("response_time_hours").alias("avg_response_time_hours"),
    F.expr("percentile_approx(response_time_hours, 0.5)").alias("median_response_time_hours"),
    F.min("response_time_hours").alias("min_response_time_hours"),
    F.max("response_time_hours").alias("max_response_time_hours"),
    F.countDistinct("complaint_type").alias("unique_complaint_types")
).withColumn(
    "closure_rate",
    (F.col("closed_requests") / F.col("total_requests")) * 100
).withColumn(
    "year_month",
    F.concat(F.col("year"), F.lit("-"), F.lpad(F.col("month"), 2, "0"))
)

monthly_agency_summary.coalesce(2).write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{gold_catalog}.{schema_name}.agg_monthly_agency_summary")

print(f"✓ Monthly agency summary created")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Complaint Type Performance

# COMMAND ----------

complaint_performance = fact_df.groupBy(
    "complaint_type",
    "complaint_priority"
).agg(
    F.count("*").alias("total_requests"),
    F.sum(F.when(F.col("status") == "CLOSED", 1).otherwise(0)).alias("closed_requests"),
    F.avg("response_time_hours").alias("avg_response_time_hours"),
    F.expr("percentile_approx(response_time_hours, 0.5)").alias("median_response_time_hours"),
    F.expr("percentile_approx(response_time_hours, 0.95)").alias("p95_response_time_hours"),
    F.sum(F.when(F.col("response_time_hours") <= 24, 1).otherwise(0)).alias("same_day_resolutions"),
    F.sum(F.when(F.col("response_time_hours") <= 168, 1).otherwise(0)).alias("week_resolutions")
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

borough_comparison = fact_df.groupBy(
    "borough"
).agg(
    F.count("*").alias("total_requests"),
    F.sum(F.when(F.col("status") == "CLOSED", 1).otherwise(0)).alias("closed_requests"),
    F.avg("response_time_hours").alias("avg_response_time_hours"),
    F.expr("percentile_approx(response_time_hours, 0.5)").alias("median_response_time_hours"),
    F.countDistinct("agency").alias("agencies_involved"),
    F.countDistinct("complaint_type").alias("unique_complaint_types"),
    F.sum(F.when(F.col("response_time_hours") <= 24, 1).otherwise(0)).alias("same_day_resolutions")
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
        f"{gold_catalog}.{schema_name}.agg_monthly_agency_summary",
        f"{gold_catalog}.{schema_name}.agg_complaint_performance",
        f"{gold_catalog}.{schema_name}.agg_borough_comparison"
    ]

    for table in tables_to_optimize:
        print(f"Optimizing {table}...")
        spark.sql(f"OPTIMIZE {table}")

    # Z-order key tables for better performance
    spark.sql(f"OPTIMIZE {gold_catalog}.{schema_name}.fact_service_requests ZORDER BY (date_key, agency_key, location_key)")
    spark.sql(f"OPTIMIZE {gold_catalog}.{schema_name}.agg_daily_summary ZORDER BY (report_date, borough, agency)")

    print("All tables optimized")
else:
    print("Skipping table optimization in dev environment to conserve resources")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Views for Power BI

# COMMAND ----------

# Create a denormalized view for easy Power BI consumption
spark.sql(f"""
CREATE OR REPLACE VIEW {gold_catalog}.{schema_name}.vw_service_requests_powerbi AS
SELECT 
    f.service_request_key,
    d.date,
    d.year,
    d.quarter,
    d.month,
    d.month_name,
    d.day_name,
    d.is_weekend,
    d.season,
    a.agency,
    a.agency_name,
    a.agency_type,
    a.is_emergency_service,
    l.borough,
    l.region,
    l.incident_zip,
    l.community_board,
    c.complaint_type,
    c.complaint_category,
    c.complaint_priority,
    c.priority_score,
    f.created_date,
    f.closed_date,
    f.response_time_hours,
    f.response_time_category,
    f.is_closed,
    f.is_resolved,
    f.resolved_same_day,
    f.resolved_within_week,
    f.status,
    f.open_data_channel_type
FROM {gold_catalog}.{schema_name}.fact_service_requests f
LEFT JOIN {gold_catalog}.{schema_name}.dim_date d ON f.date_key = d.date_key
LEFT JOIN {gold_catalog}.{schema_name}.dim_agency a ON f.agency_key = a.agency_key
LEFT JOIN {gold_catalog}.{schema_name}.dim_location l ON f.location_key = l.location_key
LEFT JOIN {gold_catalog}.{schema_name}.dim_complaint_type c ON f.complaint_key = c.complaint_key
""")

print("Power BI view created")

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
    ("NYC 311 Monthly Agency Summary", f"{gold_catalog}.{schema_name}.agg_monthly_agency_summary"),
    ("NYC 311 Complaint Performance", f"{gold_catalog}.{schema_name}.agg_complaint_performance"),
    ("NYC 311 Borough Comparison", f"{gold_catalog}.{schema_name}.agg_borough_comparison")
]

for table_name, full_table_name in tables:
    count = spark.table(full_table_name).count()
    print(f"{table_name}: {count:,} records")

# Show sample from Power BI view
print("\nSample from Power BI View:")
display(spark.sql(f"SELECT * FROM {gold_catalog}.{schema_name}.vw_service_requests_powerbi LIMIT 5"))

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
print(f"\nPower BI View: {gold_catalog}.{schema_name}.vw_service_requests_powerbi")
print("\nStar schema is ready for analytics and Power BI reporting!")
