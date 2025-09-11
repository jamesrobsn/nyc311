"""
Utility functions for NYC 311 Data Pipeline
"""

import time
import requests
import json
from typing import List, Dict, Any, Optional
from pyspark.sql import functions as F
from pyspark.sql.types import *


def create_bronze_schema() -> StructType:
    """
    Create the bronze layer schema for NYC 311 data
    All fields are strings for maximum flexibility in bronze layer
    """
    return StructType([
        StructField("unique_key", StringType(), True),
        StructField("created_date", StringType(), True),
        StructField("closed_date", StringType(), True),
        StructField("agency", StringType(), True),
        StructField("agency_name", StringType(), True),
        StructField("complaint_type", StringType(), True),
        StructField("descriptor", StringType(), True),
        StructField("location_type", StringType(), True),
        StructField("incident_zip", StringType(), True),
        StructField("incident_address", StringType(), True),
        StructField("street_name", StringType(), True),
        StructField("cross_street_1", StringType(), True),
        StructField("cross_street_2", StringType(), True),
        StructField("intersection_street_1", StringType(), True),
        StructField("intersection_street_2", StringType(), True),
        StructField("address_type", StringType(), True),
        StructField("city", StringType(), True),
        StructField("landmark", StringType(), True),
        StructField("facility_type", StringType(), True),
        StructField("status", StringType(), True),
        StructField("due_date", StringType(), True),
        StructField("resolution_description", StringType(), True),
        StructField("resolution_action_updated_date", StringType(), True),
        StructField("community_board", StringType(), True),
        StructField("bbl", StringType(), True),
        StructField("borough", StringType(), True),
        StructField("x_coordinate_state_plane", StringType(), True),
        StructField("y_coordinate_state_plane", StringType(), True),
        StructField("open_data_channel_type", StringType(), True),
        StructField("park_facility_name", StringType(), True),
        StructField("park_borough", StringType(), True),
        StructField("vehicle_type", StringType(), True),
        StructField("taxi_company_borough", StringType(), True),
        StructField("taxi_pick_up_location", StringType(), True),
        StructField("bridge_highway_name", StringType(), True),
        StructField("bridge_highway_segment", StringType(), True),
        StructField("bridge_highway_direction", StringType(), True),
        StructField("road_ramp", StringType(), True),
        StructField("latitude", StringType(), True),
        StructField("longitude", StringType(), True),
        StructField("location", StringType(), True)
    ])


def fetch_nyc311_data(base_url: str, offset: int = 0, limit: int = 50000, 
                      app_token: Optional[str] = None, timeout: int = 300) -> List[Dict[str, Any]]:
    """
    Fetch NYC 311 data from the Socrata API with pagination support
    
    Args:
        base_url: Base URL for the NYC 311 API
        offset: Starting record offset
        limit: Number of records to fetch
        app_token: Optional Socrata app token for rate limiting
        timeout: Request timeout in seconds
        
    Returns:
        List of records from the API
        
    Raises:
        requests.RequestException: If API request fails
    """
    params = {
        "$limit": limit,
        "$offset": offset,
        "$order": "created_date DESC"
    }
    
    headers = {}
    if app_token:
        headers["X-App-Token"] = app_token
    
    try:
        response = requests.get(base_url, params=params, headers=headers, timeout=timeout)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        raise


def clean_record_for_bronze(record: Dict[str, Any], schema: StructType) -> Dict[str, Any]:
    """
    Clean and normalize a single record for bronze layer
    
    Args:
        record: Raw record from API
        schema: Bronze schema to conform to
        
    Returns:
        Cleaned record ready for bronze layer
    """
    cleaned = {}
    
    # Handle location field specially - convert to JSON string
    if 'location' in record and isinstance(record['location'], dict):
        cleaned['location'] = json.dumps(record['location'])
    elif 'location' in record:
        cleaned['location'] = str(record['location'])
    else:
        cleaned['location'] = None
    
    # Process all other fields
    for field in schema.fields:
        field_name = field.name
        if field_name == 'location':
            continue  # Already handled above
        
        if field_name in record:
            value = record[field_name]
            # Convert to string, handle None values
            cleaned[field_name] = str(value) if value is not None else None
        else:
            cleaned[field_name] = None
    
    return cleaned


def standardize_borough(borough_value: str, mapping: Dict[str, str]) -> str:
    """
    Standardize borough names using provided mapping
    
    Args:
        borough_value: Raw borough value
        mapping: Borough name mapping dictionary
        
    Returns:
        Standardized borough name
    """
    if not borough_value:
        return None
    
    cleaned = borough_value.strip().upper()
    return mapping.get(cleaned, borough_value)


def validate_coordinates(lat: float, lon: float, bounds: Dict[str, float]) -> bool:
    """
    Validate if coordinates are within NYC bounds
    
    Args:
        lat: Latitude value
        lon: Longitude value  
        bounds: Dictionary with lat/lon min/max bounds
        
    Returns:
        True if coordinates are valid for NYC
    """
    if not all([lat, lon]):
        return False
        
    return (bounds["lat_min"] <= lat <= bounds["lat_max"] and 
            bounds["lon_min"] <= lon <= bounds["lon_max"])


def calculate_data_quality_metrics(df, total_records: int) -> Dict[str, Any]:
    """
    Calculate data quality metrics for a DataFrame
    
    Args:
        df: Spark DataFrame to analyze
        total_records: Total number of records
        
    Returns:
        Dictionary of data quality metrics
    """
    metrics = {}
    
    # Null rates for key fields
    key_fields = ["unique_key", "created_date", "agency", "complaint_type"]
    
    for field in key_fields:
        if field in df.columns:
            null_count = df.filter(F.col(field).isNull()).count()
            metrics[f"null_rate_{field}"] = null_count / total_records if total_records > 0 else 0
    
    # Valid coordinate rate
    if "latitude" in df.columns and "longitude" in df.columns:
        valid_coords = df.filter(
            (F.col("latitude").isNotNull()) & 
            (F.col("longitude").isNotNull()) &
            (F.col("latitude").between(40.4, 40.9)) &
            (F.col("longitude").between(-74.3, -73.7))
        ).count()
        metrics["valid_coordinate_rate"] = valid_coords / total_records if total_records > 0 else 0
    
    return metrics


def create_table_if_not_exists(spark, catalog: str, schema: str, table: str, df=None):
    """
    Create catalog, schema, and table if they don't exist
    
    Args:
        spark: Spark session
        catalog: Catalog name
        schema: Schema name  
        table: Table name
        df: Optional DataFrame to infer schema from
    """
    # Create catalog and schema
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    spark.sql(f"USE CATALOG {catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    spark.sql(f"USE SCHEMA {schema}")
    
    # Check if table exists
    full_table_name = f"{catalog}.{schema}.{table}"
    try:
        spark.table(full_table_name)
        print(f"Table {full_table_name} already exists")
    except Exception:
        print(f"Table {full_table_name} does not exist")
        if df is not None:
            print(f"Creating table {full_table_name} from DataFrame schema")


def optimize_table(spark, catalog: str, schema: str, table: str, zorder_cols: List[str] = None):
    """
    Optimize a Delta table with optional Z-ordering
    
    Args:
        spark: Spark session
        catalog: Catalog name
        schema: Schema name
        table: Table name
        zorder_cols: Optional list of columns for Z-ordering
    """
    full_table_name = f"{catalog}.{schema}.{table}"
    
    print(f"Optimizing table {full_table_name}...")
    spark.sql(f"OPTIMIZE {full_table_name}")
    
    if zorder_cols:
        zorder_clause = ", ".join(zorder_cols)
        print(f"Applying Z-ORDER on columns: {zorder_clause}")
        spark.sql(f"OPTIMIZE {full_table_name} ZORDER BY ({zorder_clause})")
    
    print(f"Optimization complete for {full_table_name}")


def get_latest_timestamp(spark, catalog: str, schema: str, table: str, 
                        timestamp_col: str = "created_date") -> Optional[str]:
    """
    Get the latest timestamp from a table for incremental processing
    
    Args:
        spark: Spark session
        catalog: Catalog name
        schema: Schema name
        table: Table name
        timestamp_col: Name of timestamp column
        
    Returns:
        Latest timestamp as string or None if table is empty/doesn't exist
    """
    full_table_name = f"{catalog}.{schema}.{table}"
    
    try:
        result = spark.sql(f"""
            SELECT MAX({timestamp_col}) as max_timestamp 
            FROM {full_table_name}
            WHERE {timestamp_col} IS NOT NULL
        """).collect()
        
        if result and result[0]['max_timestamp']:
            return str(result[0]['max_timestamp'])
        else:
            return None
            
    except Exception as e:
        print(f"Error getting latest timestamp from {full_table_name}: {e}")
        return None


def log_pipeline_metrics(stage: str, environment: str, metrics: Dict[str, Any]):
    """
    Log pipeline metrics (placeholder for actual logging implementation)
    
    Args:
        stage: Pipeline stage (bronze, silver, gold)
        environment: Environment (dev, prod)
        metrics: Dictionary of metrics to log
    """
    print(f"=== {stage.upper()} STAGE METRICS ({environment}) ===")
    for metric, value in metrics.items():
        if isinstance(value, float):
            print(f"{metric}: {value:.4f}")
        else:
            print(f"{metric}: {value}")
    print("=" * 50)
