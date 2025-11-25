"""
ETL Module for Texas Sales Tax Permits.
Fetches recent active sales tax permits from Texas Comptroller Open Data.
"""

import requests
import json
from datetime import datetime, timedelta
from typing import List, Dict, Any
from config import SOCRATA_APP_TOKEN

# Texas Comptroller Active Sales Tax Permit Holders
# New Dataset ID as of Nov 2025: 3kx8-uryv
# https://data.texas.gov/dataset/All-Permitted-Sales-Tax-Locations-and-Local-Sales-/3kx8-uryv
DATASET_ID = "3kx8-uryv"
BASE_URL = f"https://data.texas.gov/resource/{DATASET_ID}.json"

# NAICS Codes for Bars and Restaurants
# 722410: Drinking Places (Alcoholic Beverages)
# 722511: Full-Service Restaurants
# 722513: Limited-Service Restaurants
# 722514: Cafeterias, Grill Buffets, and Buffets
# 722515: Snack and Nonalcoholic Beverage Bars
TARGET_NAICS = [
    "722410",
    "722511",
    "722513",
    "722514",
    "722515"
]

# Target Counties for DFW (using Comptroller County Codes)
# Dallas: 057
# Tarrant: 220
# Collin: 043
# Denton: 061
TARGET_COUNTIES = [
    "057",
    "220",
    "043",
    "061"
]

def fetch_sales_tax_permits_since(days_ago: int = 7) -> List[Dict[str, Any]]:
    """
    Fetch sales tax permits issued in the last N days.
    """
    cutoff_date = (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d")
    
    # Construct SOQL query
    # New Schema Mapping:
    # - permit_date (was outlet_permit_issue_date)
    # - naics (was naics_code)
    # - loc_county (was outlet_county)
    
    naics_filter = " OR ".join([f"naics='{code}'" for code in TARGET_NAICS])
    county_filter = " OR ".join([f"loc_county='{code}'" for code in TARGET_COUNTIES])
    
    where_clause = f"permit_date >= '{cutoff_date}' AND ({naics_filter}) AND ({county_filter})"
    
    params = {
        "$where": where_clause,
        "$order": "permit_date DESC",
        "$limit": 2000
    }
    
    headers = {}
    if SOCRATA_APP_TOKEN:
        headers["X-App-Token"] = SOCRATA_APP_TOKEN
        
    print(f"Fetching Sales Tax permits since {cutoff_date}...")
    
    try:
        response = requests.get(BASE_URL, params=params, headers=headers)
        response.raise_for_status()
        data = response.json()
        print(f"Found {len(data)} Sales Tax permits.")
        return data
    except Exception as e:
        print(f"Error fetching Sales Tax data: {e}")
        return []

def to_source_events(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Convert raw sales tax records to source_events format.
    """
    events = []
    
    for r in records:
        # Skip if no taxpayer name or location name
        if not r.get("tp_name") or not r.get("loc_name"):
            continue
            
        # Create a unique ID
        # tp_number + loc_number is unique
        source_id = f"{r.get('tp_number')}-{r.get('loc_number')}"
        
        # Determine event type
        # We treat "permit issued" as the event
        event_date = r.get("permit_date", "").split("T")[0]
        
        # Construct address
        # New schema splits address into number and text
        addr_num = r.get("address_number", "")
        addr_text = r.get("address_text", "")
        address = f"{addr_num} {addr_text}".strip()
        
        city = r.get("loc_city", "")
        
        # Store full record in payload
        payload = r.copy()
        
        events.append({
            "source_system": "SALES_TAX",
            "source_record_id": source_id,
            "event_type": "permit_issued",
            "event_date": event_date,
            "raw_name": r.get("loc_name"),
            "raw_address": address,
            "city": city,
            "url": f"https://data.texas.gov/dataset/All-Permitted-Sales-Tax-Locations-and-Local-Sales-/{DATASET_ID}",
            "payload_json": json.dumps(payload)
        })
        
    return events
