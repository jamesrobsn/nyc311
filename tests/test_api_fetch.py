#!/usr/bin/env python3
"""
Test NYC 311 API connectivity.

Standalone test script without Databricks dependencies.
Tests API fetch functionality with retry logic and error handling.
"""

import json
import time
from datetime import datetime

import requests

# NYC 311 API configuration
NYC_311_BASE_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
APP_TOKEN = None  # No token - graceful degradation for testing


def test_fetch_nyc311_data(limit=10, retry_count=3, backoff_factor=1.5):
    """
    Fetch NYC 311 data from Socrata API with retry logic.
    
    Args:
        limit: Number of records to fetch
        retry_count: Maximum retry attempts
        backoff_factor: Exponential backoff multiplier
        
    Returns:
        List of records or None on failure
    """
    params = {"$limit": limit, "$order": "created_date DESC"}
    headers = {"X-App-Token": APP_TOKEN} if APP_TOKEN else {}
    
    print(f"Testing API connection: {NYC_311_BASE_URL}")
    print(f"Params: {params}")
    
    for attempt in range(retry_count):
        try:
            print(f"\nAttempt {attempt + 1}/{retry_count}")
            start_time = datetime.now()
            
            response = requests.get(
                NYC_311_BASE_URL,
                params=params,
                headers=headers,
                timeout=60
            )
            
            duration = (datetime.now() - start_time).total_seconds()
            print(f"Duration: {duration:.2f}s | Status: {response.status_code}")
            
            response.raise_for_status()
            data = response.json()
            
            if not isinstance(data, list):
                print(f"Unexpected data type: {type(data)}")
                return data
            
            print(f"Retrieved {len(data)} records")
            if data:
                print("\nSample record:")
                print(json.dumps(data[0], indent=2))
            return data
                
        except requests.exceptions.HTTPError as e:
            print(f"HTTP error: {e}")
            if response.status_code == 429:  # Rate limit
                retry_after = int(response.headers.get("Retry-After", backoff_factor ** attempt))
                print(f"Rate limited. Retrying in {retry_after}s...")
                time.sleep(retry_after)
            elif attempt == retry_count - 1:
                print(f"Failed after {retry_count} attempts")
                return None
            else:
                sleep_time = backoff_factor ** attempt
                print(f"Retrying in {sleep_time:.1f}s...")
                time.sleep(sleep_time)
                
        except requests.exceptions.RequestException as e:
            print(f"Request error: {e}")
            if attempt == retry_count - 1:
                print(f"Failed after {retry_count} attempts")
                return None
            sleep_time = backoff_factor ** attempt
            print(f"Retrying in {sleep_time:.1f}s...")
            time.sleep(sleep_time)
            
        except (ValueError, json.JSONDecodeError) as e:
            print(f"JSON parsing error: {e}")
            print(f"Response preview: {response.text[:500]}")
            return None
    
    return None

if __name__ == "__main__":
    print("=== NYC 311 API Connectivity Test ===\n")
    
    records = test_fetch_nyc311_data(limit=10)
    
    if records:
        print(f"\nTEST PASSED: {len(records)} records fetched successfully")
    else:
        print("\nTEST FAILED: Could not fetch records")
