#!/usr/bin/env python3
"""
Simplified test for NYC 311 bronze ingest logic
This is a standalone script to test the core functionality without Databricks dependencies
"""

import time
import requests
import json
from datetime import datetime
from typing import List, Dict, Any

# NYC 311 API configuration
NYC_311_BASE_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
APP_TOKEN = None  # No token for testing

def fetch_all_nyc311_data(limit=10, app_token=None, max_batches=2):
    """
    Fetch all pages of NYC 311 data from the Socrata API.
    Stops after max_batches to avoid excessive calls in dev/test.
    Implements robust error handling and retries.
    """
    all_records = []
    offset = 0
    retry_count = 3
    backoff_factor = 1.5
    
    for batch_num in range(max_batches):
        params = {"$limit": limit, "$offset": offset, "$order": "created_date DESC"}
        headers = {"X-App-Token": app_token} if app_token else {}
        
        print(f"Fetching batch {batch_num + 1}/{max_batches}, offset: {offset}")
        print(f"Request URL: {NYC_311_BASE_URL}")
        print(f"Request params: {params}")
        
        # Implement retry logic
        for attempt in range(retry_count):
            try:
                print(f"  Attempt {attempt + 1}/{retry_count}")
                response = requests.get(NYC_311_BASE_URL, params=params, headers=headers, timeout=60)
                
                print(f"  Response status: {response.status_code}")
                print(f"  Response content type: {response.headers.get('Content-Type')}")
                print(f"  Response length: {len(response.content)} bytes")
                
                # Check for successful response
                response.raise_for_status()
                
                # Try to parse the JSON response
                try:
                    batch_data = response.json()
                    
                    if not isinstance(batch_data, list):
                        print(f"  Warning: Expected list but got {type(batch_data)}")
                        print(f"  First 200 chars: {str(batch_data)[:200]}")
                        if isinstance(batch_data, dict) and "value" in batch_data:
                            # Handle OData format like Northwind
                            batch_data = batch_data.get("value", [])
                            print(f"  Extracted {len(batch_data)} records from 'value' property")
                    
                    print(f"  Retrieved {len(batch_data)} records")
                    
                    # Process the data
                    if batch_data and len(batch_data) > 0:
                        all_records.extend(batch_data)
                        
                        # Check if we've reached the end
                        if len(batch_data) < limit:
                            print("  Reached end of data (last page)")
                            break
                            
                        # Move to next batch
                        offset += limit
                        print(f"  Moving to next batch, new offset: {offset}")
                        time.sleep(1)  # Basic rate limiting
                        break  # Success, exit retry loop
                    else:
                        print("  No records in response, might be end of data")
                        break  # No more data, exit retry loop
                        
                except ValueError as e:
                    print(f"  Error parsing JSON: {e}")
                    print(f"  First 500 chars of response: {response.text[:500]}")
                    if attempt == retry_count - 1:
                        return all_records  # Return what we have so far
                    
            except requests.exceptions.RequestException as e:
                print(f"  Request error on attempt {attempt + 1}: {e}")
                if attempt == retry_count - 1:
                    print(f"  Failed after {retry_count} attempts, stopping.")
                    return all_records  # Return what we have so far
                
            # Retry with backoff
            sleep_time = backoff_factor ** attempt
            print(f"  Retrying in {sleep_time:.1f} seconds...")
            time.sleep(sleep_time)
        
        # If we got no data or hit the end, break the loop
        if len(all_records) == 0 or (batch_num > 0 and len(batch_data) < limit):
            print("  No more data to fetch, stopping.")
            break
            
    print(f"Total records fetched: {len(all_records)}")
    return all_records

def create_bronze_schema():
    """Mock schema definition for testing"""
    return ["unique_key", "created_date", "closed_date", "agency", "agency_name", "complaint_type",
            "descriptor", "location_type", "incident_zip", "incident_address", "street_name",
            "cross_street_1", "cross_street_2", "intersection_street_1", "intersection_street_2",
            "address_type", "city", "landmark", "facility_type", "status", "due_date",
            "resolution_description", "resolution_action_updated_date", "community_board", "bbl",
            "borough", "x_coordinate_state_plane", "y_coordinate_state_plane", "open_data_channel_type",
            "park_facility_name", "park_borough", "vehicle_type", "taxi_company_borough",
            "taxi_pick_up_location", "bridge_highway_name", "bridge_highway_segment", 
            "bridge_highway_direction", "road_ramp", "latitude", "longitude", "location"]

def normalize_records(raw_records, schema_fields):
    """Normalize records to match schema fields"""
    if not raw_records:
        print("WARNING: No raw records to normalize")
        return []
    
    print(f"Starting record normalization for {len(raw_records)} records...")
    
    all_records = []
    for record in raw_records:
        # Create base record with all fields initialized to None
        normalized = {field: None for field in schema_fields}
        
        # Update with values from the record, converting to strings
        for k, v in record.items():
            if k in schema_fields:
                normalized[k] = str(v) if v is not None else None
        
        # Special handling for location field
        if 'location' in record and isinstance(record['location'], dict):
            normalized['location'] = json.dumps(record['location'])
        
        all_records.append(normalized)
    
    print(f"Normalized {len(all_records)} records")
    
    # Verify first record fields
    if all_records:
        missing_fields = [f for f in schema_fields if f not in all_records[0]]
        if missing_fields:
            print(f"WARNING: Missing fields in normalized record: {missing_fields}")
        else:
            print("✓ All schema fields are present in normalized records")
            
    return all_records

if __name__ == "__main__":
    print("=== NYC 311 Bronze Ingest Test ===")
    print(f"Current time: {datetime.now()}")
    
    # 1. Fetch data
    print("\n--- Step 1: Fetching data ---")
    raw_records = fetch_all_nyc311_data(limit=10, max_batches=2)
    
    # 2. Define bronze schema
    print("\n--- Step 2: Define schema ---")
    schema_fields = create_bronze_schema()
    print(f"Schema has {len(schema_fields)} fields")
    
    # 3. Normalize records
    print("\n--- Step 3: Normalize records ---")
    normalized_records = normalize_records(raw_records, schema_fields)
    
    if normalized_records:
        print("\n--- Verification ---")
        print(f"First record sample (truncated):")
        first_record_sample = {k: v for k, v in list(normalized_records[0].items())[:10]}
        print(json.dumps(first_record_sample, indent=2))
        print("...")
        
        print(f"\n✓ TEST PASSED: Successfully processed {len(normalized_records)} records")
    else:
        print("\n✖ TEST FAILED: Could not process records from NYC 311 API")
