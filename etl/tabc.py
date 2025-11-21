"""
TABC (Texas Alcoholic Beverage Commission) data extractor.
Fetches new liquor licenses issued in DFW counties.
"""

import json
import requests
from datetime import datetime
from config import TABC_ENDPOINT, SOCRATA_APP_TOKEN, TARGET_COUNTIES


def fetch_tabc_licenses_since(since_iso: str) -> list[dict]:
    """
    Fetch TABC license records whose original_issue_date >= since_iso
    and whose county is in TARGET_COUNTIES.

    Args:
        since_iso: ISO 8601 datetime string like 'YYYY-MM-DDT00:00:00.000'

    Returns:
        List of raw TABC license records (dicts)
    """
    # Build the $where clause for counties and date
    county_filter = " OR ".join([f"upper(county) = '{c}'" for c in TARGET_COUNTIES])
    where_clause = f"original_issue_date >= '{since_iso}' AND ({county_filter})"

    params = {
        "$where": where_clause,
        "$limit": 10000,  # Adjust if needed
        "$order": "original_issue_date DESC"
    }

    headers = {}
    if SOCRATA_APP_TOKEN:
        headers["X-App-Token"] = SOCRATA_APP_TOKEN

    try:
        response = requests.get(TABC_ENDPOINT, params=params, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()
        print(f"[TABC] Fetched {len(data)} records since {since_iso}")
        return data
    except requests.exceptions.RequestException as e:
        print(f"[TABC] Error fetching data: {e}")
        return []


def to_source_events(rows: list[dict]) -> list[dict]:
    """
    Map raw TABC rows to normalized source_events dicts.

    Each event dict contains:
    - source_system: "TABC"
    - source_record_id: license_id
    - event_type: "license_issued"
    - event_date: YYYY-MM-DD
    - raw_name: trade_name
    - raw_address: address
    - city: city
    - url: None (or could construct search URL)
    - payload_json: JSON dump of original row
    """
    events = []

    for row in rows:
        # Extract date from original_issue_date (format: YYYY-MM-DDT...)
        original_issue_date = row.get("original_issue_date", "")
        if original_issue_date:
            # Parse and truncate to YYYY-MM-DD
            try:
                dt = datetime.fromisoformat(original_issue_date.replace("Z", "+00:00"))
                event_date = dt.date().isoformat()
            except (ValueError, AttributeError):
                event_date = original_issue_date[:10]  # Fallback to first 10 chars
        else:
            event_date = None

        # Build address from components
        address_parts = [
            row.get("address", ""),
            row.get("address_2", "")
        ]
        raw_address = " ".join([p.strip() for p in address_parts if p])

        event = {
            "source_system": "TABC",
            "source_record_id": row.get("license_id", ""),
            "event_type": "license_issued",
            "event_date": event_date,
            "raw_name": row.get("trade_name", ""),
            "raw_address": raw_address,
            "city": row.get("city", ""),
            "url": None,  # Could add a search URL to TABC portal if desired
            "payload_json": json.dumps(row)
        }

        events.append(event)

    return events
