"""
Fort Worth Certificate of Occupancy data extractor.
Uses ArcGIS Feature Service API.
"""

import requests
import json
import os
from datetime import datetime, timedelta
from config import FORTWORTH_CO_ENDPOINT, FORTWORTH_ARCGIS_TOKEN

def fetch_fortworth_cos_since(since_date: str) -> list[dict]:
    """
    Fetch Fort Worth CO records from ArcGIS API.
    
    Args:
        since_date: ISO date string 'YYYY-MM-DD'.
    """
    if not FORTWORTH_CO_ENDPOINT:
        print("[Fort Worth CO] Endpoint not configured.")
        return []

    print(f"Fetching Fort Worth COs since {since_date}...")
    
    # Convert since_date to timestamp (ms)
    # Note: ArcGIS queries often use timestamps
    try:
        dt = datetime.strptime(since_date, "%Y-%m-%d")
        timestamp = int(dt.timestamp() * 1000)
    except ValueError:
        print(f"[Fort Worth CO] Invalid date format: {since_date}")
        return []

    # Query parameters
    # CODate is the field for issue date
    where_clause = f"CODate >= {timestamp}"
    
    params = {
        "where": where_clause,
        "outFields": "*",
        "f": "json",
        "orderByFields": "CODate DESC",
        "resultRecordCount": 2000
    }
    
    headers = {}
    if FORTWORTH_ARCGIS_TOKEN:
        headers["X-Esri-Authorization"] = f"Bearer {FORTWORTH_ARCGIS_TOKEN}"

    try:
        response = requests.get(f"{FORTWORTH_CO_ENDPOINT}/query", params=params, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        features = data.get("features", [])
        print(f"Found {len(features)} Fort Worth CO records.")
        
        # Extract attributes from features
        return [f.get("attributes", {}) for f in features]
        
    except Exception as e:
        print(f"[Fort Worth CO] Error fetching data: {e}")
        return []


def to_source_events(rows: list[dict]) -> list[dict]:
    """
    Map raw ArcGIS attributes to normalized source_events dicts.
    """
    events = []

    for row in rows:
        # Skip if no occupant name
        if not row.get("Occupant"):
            continue

        # Convert timestamp to YYYY-MM-DD
        co_date_ms = row.get("CODate")
        if co_date_ms:
            event_date = datetime.fromtimestamp(co_date_ms / 1000).strftime("%Y-%m-%d")
        else:
            event_date = ""

        # Construct address
        address = row.get("Location") or row.get("AddressLine1") or ""
        
        # Build event dict
        event = {
            "source_system": "FORTWORTH_CO",
            "source_record_id": row.get("PermitID", ""),
            "event_type": "co_issued",
            "event_date": event_date,
            "raw_name": row.get("Occupant", ""),
            "raw_address": address,
            "city": row.get("City", "Fort Worth"),
            "url": "https://fortworth.maps.arcgis.com/apps/opsdashboard/index.html#/32e2d966453942efb6e51240c5f590ff",
            "payload_json": json.dumps(row)
        }

        events.append(event)

    return events
