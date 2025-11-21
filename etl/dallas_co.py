"""
Dallas Certificate of Occupancy data extractor.
Fetches new COs for restaurant/bar-like uses.
"""

import json
import requests
from datetime import datetime
from config import DALLAS_CO_ENDPOINT, SOCRATA_APP_TOKEN


def fetch_dallas_cos_since(since_iso: str) -> list[dict]:
    """
    Fetch Dallas COs issued since since_iso with restaurant/bar-like uses.

    Args:
        since_iso: ISO 8601 datetime string like 'YYYY-MM-DDT00:00:00.000'

    Returns:
        List of raw Dallas CO records (dicts)
    """
    # Filter for restaurant/bar-like occupancies
    # Using case-insensitive search on 'occupancy' field
    use_filters = [
        "upper(occupancy) like '%RESTAURANT%'",
        "upper(occupancy) like '%BAR%'",
        "upper(occupancy) like '%LOUNGE%'",
        "upper(occupancy) like '%TAVERN%'",
        "upper(occupancy) like '%CAFE%'",
        "upper(occupancy) like '%BREWERY%'",
    ]
    use_clause = " OR ".join(use_filters)

    where_clause = f"date_issued >= '{since_iso}' AND ({use_clause})"

    params = {
        "$where": where_clause,
        "$limit": 5000,
        "$order": "date_issued DESC"
    }

    headers = {}
    if SOCRATA_APP_TOKEN:
        headers["X-App-Token"] = SOCRATA_APP_TOKEN

    try:
        response = requests.get(DALLAS_CO_ENDPOINT, params=params, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()
        print(f"[Dallas CO] Fetched {len(data)} records since {since_iso}")
        return data
    except requests.exceptions.RequestException as e:
        print(f"[Dallas CO] Error fetching data: {e}")
        return []


def to_source_events(rows: list[dict]) -> list[dict]:
    """
    Map raw Dallas CO rows to normalized source_events dicts.

    Each event dict contains:
    - source_system: "DALLAS_CO"
    - source_record_id: co (certificate number)
    - event_type: "co_issued"
    - event_date: YYYY-MM-DD
    - raw_name: business_name
    - raw_address: address
    - city: "Dallas"
    - url: dataset URL
    - payload_json: JSON dump of original row
    """
    events = []

    for row in rows:
        # Extract date from date_issued (format: YYYY-MM-DDT...)
        date_issued = row.get("date_issued", "")
        if date_issued:
            try:
                dt = datetime.fromisoformat(date_issued.replace("Z", "+00:00"))
                event_date = dt.date().isoformat()
            except (ValueError, AttributeError):
                event_date = date_issued[:10]
        else:
            event_date = None

        event = {
            "source_system": "DALLAS_CO",
            "source_record_id": row.get("co", ""),
            "event_type": "co_issued",
            "event_date": event_date,
            "raw_name": row.get("business_name", ""),
            "raw_address": row.get("address", ""),
            "city": "Dallas",
            "url": "https://www.dallasopendata.com/Services/Building-Inspection-Certificates-Of-Occupancy/9qet-qt9e",
            "payload_json": json.dumps(row)
        }

        events.append(event)

    return events
