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
# https://data.texas.gov/dataset/Active-Sales-Tax-Permit-Holders/9e32-2272
DATASET_ID = "9e32-2272"
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

# Target Counties for DFW
TARGET_COUNTIES = [
    "DALLAS",
    "TARRANT",
    "COLLIN",
    "DENTON"
]

def fetch_sales_tax_permits_since(days_ago: int = 7) -> List[Dict[str, Any]]:
    """
    Fetch sales tax permits issued in the last N days.
    """
    cutoff_date = (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d")
    
    # Construct SOQL query
    # We want:
    # - Recent permit issue dates
    # - In our target counties
    # - In our target NAICS codes
    
    naics_filter = " OR ".join([f"naics_code='{code}'" for code in TARGET_NAICS])
    county_filter = " OR ".join([f"outlet_county='{county}'" for county in TARGET_COUNTIES])
    
    where_clause = f"outlet_permit_issue_date >= '{cutoff_date}' AND ({naics_filter}) AND ({county_filter})"
    
    params = {
        "$where": where_clause,
        "$order": "outlet_permit_issue_date DESC",
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
        # Skip if no taxpayer name or outlet name
        if not r.get("taxpayer_name") or not r.get("outlet_name"):
            continue
            
        # Create a unique ID
        # taxpayer_number + outlet_number is usually unique
        source_id = f"{r.get('taxpayer_number')}-{r.get('outlet_number')}"
        
        # Determine event type
        # We treat "permit issued" as the event
        event_date = r.get("outlet_permit_issue_date", "").split("T")[0]
        
        # Construct address
        address = r.get("outlet_street", "")
        city = r.get("outlet_city", "")
        
        # Store NAICS info in payload for later use
        payload = r.copy()
        
        events.append({
            "source_system": "SALES_TAX",
            "source_record_id": source_id,
            "event_type": "permit_issued",
            "event_date": event_date,
            "raw_name": r.get("outlet_name"),
            "raw_address": address,
            "city": city,
            "url": "https://data.texas.gov/dataset/Active-Sales-Tax-Permit-Holders/9e32-2272",
            "payload_json": json.dumps(payload)
        })
        
    return events
