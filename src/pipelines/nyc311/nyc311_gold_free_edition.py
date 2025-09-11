# Databricks notebook source
# MAGIC %md
# MAGIC # NYC 311 Gold Layer - Free Edition Compatible (Python)
# MAGIC 
# MAGIC This notebook creates the gold layer using month-by-month processing to avoid Free Edition task limits.
# MAGIC 
# MAGIC **Key Features:**
# MAGIC - No Spark configuration changes (works on Free Edition)
# MAGIC - Month-by-month processing to keep task counts low
# MAGIC - Broadcast hints for dimension joins
# MAGIC - Single writer per batch using coalesce(1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

# Get parameters from job
bronze_catalog = dbutils.widgets.get("bronze_catalog")
silver_catalog = dbutils.widgets.get("silver_catalog") 
gold_catalog = dbutils.widgets.get("gold_catalog")
schema_name = dbutils.widgets.get("schema_name")
environment = dbutils.widgets.get("environment")

print(f"Processing parameters:")
print(f"  Bronze Catalog: {bronze_catalog}")
print(f"  Silver Catalog: {silver_catalog}")
print(f"  Gold Catalog: {gold_catalog}")
print(f"  Schema: {schema_name}")
print(f"  Environment: {environment}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Imports

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast, lit, when, coalesce, current_timestamp
from pyspark.sql.types import *
import time

# Set catalog and schema context
spark.sql(f"USE CATALOG {gold_catalog}")
spark.sql(f"USE SCHEMA {schema_name}")

print("✅ Catalog and schema context set")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Dimension Tables

# COMMAND ----------

print("🔄 Creating dimension tables...")

# Load silver data once for dimension creation
silver_df = spark.table(f"{silver_catalog}.{schema_name}.service_requests_silver")

# Date Dimension
print("  Creating dim_date...")
date_dim = (silver_df
    .filter(F.col("created_date").isNotNull())
    .select(F.col("created_date"))
    .distinct()
    .select(
        F.date_format("created_date", "yyyyMMdd").cast("int").alias("date_key"),
        F.col("created_date").cast("date").alias("date"),
        F.year("created_date").alias("year"),
        F.quarter("created_date").alias("quarter"),
        F.month("created_date").alias("month"),
        F.date_format("created_date", "MMMM").alias("month_name"),
        F.dayofmonth("created_date").alias("day"),
        F.dayofweek("created_date").alias("day_of_week"),
        F.date_format("created_date", "EEEE").alias("day_name"),
        F.weekofyear("created_date").alias("week_of_year"),
        F.col("day_of_week").isin(1, 7).alias("is_weekend"),
        (~F.col("day_of_week").isin(1, 7)).alias("is_weekday"),
        F.when(F.month("created_date") >= 7, F.year("created_date") + 1)
         .otherwise(F.year("created_date")).alias("fiscal_year"),
        F.when(F.month("created_date").isin(12, 1, 2), "Winter")
         .when(F.month("created_date").isin(3, 4, 5), "Spring")
         .when(F.month("created_date").isin(6, 7, 8), "Summer")
         .when(F.month("created_date").isin(9, 10, 11), "Fall")
         .alias("season")
    )
)

date_dim.write.format("delta").mode("overwrite").saveAsTable(f"{gold_catalog}.{schema_name}.dim_date")

# Agency Dimension  
print("  Creating dim_agency...")
agency_dim = (silver_df
    .filter(F.col("agency").isNotNull())
    .select("agency", "agency_name")
    .distinct()
    .withColumn("agency_key", F.row_number().over(F.Window.orderBy("agency")))
    .withColumn("agency_type", 
        F.when(F.col("agency").isin("NYPD", "FDNY", "EMS"), "Emergency Services")
         .when(F.col("agency").isin("DOT", "DEP", "DSNY"), "Infrastructure")
         .when(F.col("agency").isin("HPD", "DOB"), "Housing & Buildings")
         .when(F.col("agency").isin("DOHMH", "ACS"), "Health & Social Services")
         .when(F.col("agency").isin("DPR", "DOE"), "Parks & Education")
         .otherwise("Other"))
    .withColumn("is_emergency_service", F.col("agency").isin("NYPD", "FDNY", "EMS"))
)

agency_dim.write.format("delta").mode("overwrite").saveAsTable(f"{gold_catalog}.{schema_name}.dim_agency")

# Location Dimension
print("  Creating dim_location...")
location_dim = (silver_df
    .filter(F.col("borough").isNotNull())
    .select("borough", "incident_zip", "community_board", "latitude", "longitude")
    .distinct()
    .withColumn("location_key", F.row_number().over(F.Window.orderBy("borough", "incident_zip")))
    .withColumn("region",
        F.when(F.col("borough") == "Manhattan", "Manhattan")
         .when(F.col("borough").isin("Brooklyn", "Queens"), "Western Long Island")
         .when(F.col("borough") == "Bronx", "Bronx")
         .when(F.col("borough") == "Staten Island", "Staten Island")
         .otherwise("Unknown"))
    .withColumn("has_coordinates", F.col("latitude").isNotNull() & F.col("longitude").isNotNull())
)

location_dim.write.format("delta").mode("overwrite").saveAsTable(f"{gold_catalog}.{schema_name}.dim_location")

# Complaint Type Dimension
print("  Creating dim_complaint_type...")
complaint_dim = (silver_df
    .filter(F.col("complaint_type").isNotNull())
    .select("complaint_type", "descriptor", "complaint_priority")
    .distinct()
    .withColumn("complaint_key", F.row_number().over(F.Window.orderBy("complaint_type")))
    .withColumn("complaint_category",
        F.when(F.col("complaint_type").contains("Noise"), "Noise")
         .when(F.col("complaint_type").contains("Water"), "Water/Sewer")
         .when(F.col("complaint_type").contains("Heat"), "Housing")
         .when(F.col("complaint_type").contains("Street"), "Transportation")
         .when(F.col("complaint_type").contains("Parking"), "Parking")
         .when(F.col("complaint_type").contains("Animal"), "Animal Services")
         .when(F.col("complaint_type").contains("Homeless"), "Social Services")
         .otherwise("Other"))
    .withColumn("priority_score",
        F.when(F.col("complaint_priority") == "Critical", 4)
         .when(F.col("complaint_priority") == "High", 3)
         .when(F.col("complaint_priority") == "Medium", 2)
         .when(F.col("complaint_priority") == "Low", 1)
         .otherwise(0))
)

complaint_dim.write.format("delta").mode("overwrite").saveAsTable(f"{gold_catalog}.{schema_name}.dim_complaint_type")

print("✅ All dimension tables created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Empty Fact Table

# COMMAND ----------

print("🔄 Creating empty fact table with proper schema...")

# Create empty fact table with the correct schema
fact_schema = StructType([
    StructField("service_request_key", StringType(), True),
    StructField("date_key", IntegerType(), True),
    StructField("agency_key", IntegerType(), True),
    StructField("location_key", IntegerType(), True),
    StructField("complaint_key", IntegerType(), True),
    StructField("created_date", TimestampType(), True),
    StructField("closed_date", TimestampType(), True),
    StructField("due_date", TimestampType(), True),
    StructField("response_time_hours", DoubleType(), True),
    StructField("response_time_category", StringType(), True),
    StructField("is_weekend", BooleanType(), True),
    StructField("is_closed", IntegerType(), True),
    StructField("is_resolved", IntegerType(), True),
    StructField("resolved_same_day", IntegerType(), True),
    StructField("resolved_within_week", IntegerType(), True),
    StructField("status", StringType(), True),
    StructField("open_data_channel_type", StringType(), True),
    StructField("resolution_description", StringType(), True),
    StructField("silver_processed_ts", TimestampType(), True),
    StructField("gold_processed_ts", TimestampType(), True),
    StructField("environment", StringType(), True)
])

# Create empty DataFrame and save as table
empty_fact = spark.createDataFrame([], fact_schema)
empty_fact.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{gold_catalog}.{schema_name}.fact_service_requests")

# Set table properties to avoid optimization issues
spark.sql(f"""
    ALTER TABLE {gold_catalog}.{schema_name}.fact_service_requests
    SET TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'false',
        'delta.autoOptimize.autoCompact' = 'false'
    )
""")

print("✅ Empty fact table created with optimizations disabled")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Get Month Boundaries

# COMMAND ----------

print("🔄 Computing month boundaries for processing...")

# Get distinct months from silver data
months_df = (silver_df
    .select(F.date_trunc("month", "created_date").alias("month_start"))
    .distinct()
    .orderBy("month_start")
)

months_list = [row.month_start for row in months_df.collect()]

print(f"Found {len(months_list)} months to process:")
for month in months_list:
    print(f"  - {month.strftime('%Y-%m')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Load Dimension DataFrames Once (with Broadcast)

# COMMAND ----------

print("🔄 Loading dimension tables for joins...")

# Load dimensions once and broadcast them (they're small)
dd = broadcast(spark.table(f"{gold_catalog}.{schema_name}.dim_date").select("date_key", "date"))
ad = broadcast(spark.table(f"{gold_catalog}.{schema_name}.dim_agency").select("agency_key", "agency"))
ld = broadcast(spark.table(f"{gold_catalog}.{schema_name}.dim_location").select("location_key", "borough"))
cd = broadcast(spark.table(f"{gold_catalog}.{schema_name}.dim_complaint_type").select("complaint_key", "complaint_type"))

print("✅ Dimension tables loaded and broadcast")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Process Fact Data Month by Month

# COMMAND ----------

print("🔄 Processing fact data month by month...")

table_name = f"{gold_catalog}.{schema_name}.fact_service_requests"
processed_months = 0

for i, month_start in enumerate(months_list):
    # Calculate month end
    month_end = month_start.replace(month=month_start.month + 1) if month_start.month < 12 else month_start.replace(year=month_start.year + 1, month=1)
    month_label = month_start.strftime('%Y-%m')
    
    print(f"  Processing month {i+1}/{len(months_list)}: {month_label}")
    start_time = time.time()
    
    # Filter silver data for this month
    s = (silver_df
         .filter((F.col("created_date") >= month_start) & (F.col("created_date") < month_end))
         .withColumn("created_date_only", F.col("created_date").cast("date"))
         .select("unique_key", "created_date", "closed_date", "due_date", "response_time_hours",
                 "response_time_category", "is_weekend", "status", "open_data_channel_type",
                 "resolution_description", "silver_processed_ts", "agency", "complaint_type", "borough", "created_date_only")
    )
    
    # Join with dimensions and create fact record
    fact = (s.join(dd, s["created_date_only"] == dd["date"], "left")
             .join(ad, s["agency"] == ad["agency"], "left")
             .join(cd, s["complaint_type"] == cd["complaint_type"], "left")
             .join(ld, s["borough"] == ld["borough"], "left")
             .select(
                 F.col("unique_key").alias("service_request_key"),
                 coalesce(F.col("date_key"), lit(-1)).alias("date_key"),
                 coalesce(F.col("agency_key"), lit(-1)).alias("agency_key"),
                 coalesce(F.col("location_key"), lit(-1)).alias("location_key"),
                 coalesce(F.col("complaint_key"), lit(-1)).alias("complaint_key"),
                 F.col("created_date"), F.col("closed_date"), F.col("due_date"),
                 F.col("response_time_hours"), F.col("response_time_category"),
                 F.col("is_weekend"),
                 when(F.col("status") == "CLOSED", 1).otherwise(0).alias("is_closed"),
                 when(F.col("closed_date").isNotNull(), 1).otherwise(0).alias("is_resolved"),
                 when(F.col("response_time_hours") <= 24, 1).otherwise(0).alias("resolved_same_day"),
                 when(F.col("response_time_hours") <= 168, 1).otherwise(0).alias("resolved_within_week"),
                 F.col("status"), F.col("open_data_channel_type"), F.col("resolution_description"),
                 F.col("silver_processed_ts"),
                 current_timestamp().alias("gold_processed_ts"),
                 lit(environment).alias("environment")
             )
             .coalesce(1))  # Single writer task per month
    
    # Write the data (append mode after first month)
    mode = "append"
    (fact.write.format("delta").mode(mode).saveAsTable(table_name))
    
    elapsed = time.time() - start_time
    processed_months += 1
    print(f"    ✅ Completed {month_label} in {elapsed:.1f}s")

print(f"✅ All {processed_months} months processed successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Create Power BI View

# COMMAND ----------

print("🔄 Creating Power BI view...")

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

print("✅ Power BI view created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Final Verification

# COMMAND ----------

print("🔄 Running final verification...")

# Get fact table statistics
fact_stats = spark.sql(f"""
    SELECT 
        COUNT(*) as total_records,
        MIN(created_date) as earliest_date,
        MAX(created_date) as latest_date,
        COUNT(DISTINCT date_format(created_date, 'yyyy-MM')) as months_processed,
        environment
    FROM {gold_catalog}.{schema_name}.fact_service_requests
    GROUP BY environment
""").collect()

for row in fact_stats:
    print(f"📊 Final Statistics:")
    print(f"  Environment: {row.environment}")
    print(f"  Total Records: {row.total_records:,}")
    print(f"  Date Range: {row.earliest_date} to {row.latest_date}")
    print(f"  Months Processed: {row.months_processed}")

# Check file count
detail_info = spark.sql(f"DESCRIBE DETAIL {gold_catalog}.{schema_name}.fact_service_requests").collect()
for row in detail_info:
    print(f"  Number of Files: {row.numFiles}")
    print(f"  Table Size: {row.sizeInBytes / (1024*1024*1024):.2f} GB")

print("\n🎉 NYC 311 Gold Layer creation completed successfully!")
print("💡 Monthly processing approach avoided Free Edition task limits")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC ✅ **Completed Successfully:**
# MAGIC - Created all dimension tables without task limit issues
# MAGIC - Processed fact data month-by-month with single writer per batch
# MAGIC - Used broadcast hints for dimension joins
# MAGIC - Disabled auto-optimization to avoid task multiplication
# MAGIC - Created Power BI view for analytics consumption
# MAGIC 
# MAGIC 🔧 **Optimizations Applied:**
# MAGIC - `coalesce(1)` for single writer per month
# MAGIC - `broadcast()` for small dimension tables  
# MAGIC - Monthly batching to limit scan tasks
# MAGIC - No Spark configuration changes (Free Edition compatible)
# MAGIC 
# MAGIC 📈 **Ready for Analytics:**
# MAGIC - Star schema with fact and dimension tables
# MAGIC - Power BI view with all necessary joins
# MAGIC - Optimized for analytical queries
