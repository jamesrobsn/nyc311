#!/usr/bin/env python3
"""
Test script for NYC 311 API connectivity
This is a standalone script to test the API fetch functionality without Databricks dependencies
"""

import time
import requests
import json
from datetime import datetime

# NYC 311 API configuration
NYC_311_BASE_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
APP_TOKEN = None  # No token for testing

def test_fetch_nyc311_data(limit=10, retry_count=3, backoff_factor=1.5):
    """
    Test fetching NYC 311 data from the Socrata API.
    Uses robust error handling and retries.
    """
    params = {"$limit": limit, "$order": "created_date DESC"}
    headers = {"X-App-Token": APP_TOKEN} if APP_TOKEN else {}
    
    print(f"Testing API connection to: {NYC_311_BASE_URL}")
    print(f"Request params: {params}")
    print(f"Request headers: {headers}")
    
    # Implement retry logic
    for attempt in range(retry_count):
        try:
            print(f"\nAttempt {attempt + 1}/{retry_count}")
            
            start_time = datetime.now()
            print(f"Start time: {start_time}")
            
            response = requests.get(NYC_311_BASE_URL, params=params, headers=headers, timeout=60)
            
            end_time = datetime.now()
            print(f"End time: {end_time}")
            print(f"Duration: {(end_time - start_time).total_seconds():.2f} seconds")
            
            print(f"Response status: {response.status_code}")
            print(f"Response content type: {response.headers.get('Content-Type')}")
            print(f"Response content length: {len(response.content)} bytes")
            
            # Check if we got a valid response
            response.raise_for_status()
            
            # Try to parse as JSON
            data = response.json()
            
            # Check the data structure
            if isinstance(data, list):
                print(f"✓ Successfully retrieved {len(data)} records as list")
                if data:
                    print("\nFirst record sample:")
                    print(json.dumps(data[0], indent=2))
                return data
            else:
                print(f"⚠️ Got data but not as expected list format. Type: {type(data)}")
                print("Data sample:")
                print(json.dumps(data)[:500] + "..." if len(json.dumps(data)) > 500 else json.dumps(data))
                return data
                
        except requests.exceptions.RequestException as e:
            print(f"⚠️ Request error on attempt {attempt + 1}: {e}")
            if attempt == retry_count - 1:
                print(f"✖ Failed after {retry_count} attempts")
                return None
                
            sleep_time = backoff_factor ** attempt
            print(f"Retrying in {sleep_time:.1f} seconds...")
            time.sleep(sleep_time)
        except ValueError as e:
            print(f"⚠️ JSON parsing error: {e}")
            print(f"Raw response content (first 500 chars):")
            print(response.text[:500] + "..." if len(response.text) > 500 else response.text)
            return None

if __name__ == "__main__":
    print("=== NYC 311 API Connectivity Test ===")
    
    # Test with a small limit
    records = test_fetch_nyc311_data(limit=10)
    
    if records:
        print(f"\n✓ TEST PASSED: Successfully fetched {len(records)} records")
    else:
        print("\n✖ TEST FAILED: Could not fetch records from NYC 311 API")
