"""
Arlington Building Permit data extractor.
Fetches issued permits from Arlington Open Data (ArcGIS Hub).
Filters for restaurant/bar-related permits.

Data includes 3-year view of ALL permits (residential, commercial, sign, fence, pool, CO).
Updated daily.
"""

import json
import requests
from datetime import datetime

from config import ARLINGTON_ARCGIS_URL

# Keywords to filter for restaurant/bar-related permits
PERMIT_KEYWORDS = [
    "restaurant", "bar", "lounge", "tavern", "pub",
    "kitchen", "hood", "grease trap", "walk-in",
    "tenant finish", "build-out", "commercial alteration",
    "food service", "cooking", "brewery", "brewpub",
    "cafe", "grill"
]


def _matches_keywords(text: str) -> bool:
    """Check if text contains any of the permit keywords."""
    if not text:
        return False
    text_lower = text.lower()
    return any(keyword in text_lower for keyword in PERMIT_KEYWORDS)


def fetch_arlington_permits_since(since_date: str) -> list[dict]:
    """
    Fetch Arlington building permit records from ArcGIS FeatureServer.

    Args:
        since_date: ISO date string 'YYYY-MM-DD'

    Returns:
        List of permit records filtered for restaurant/bar keywords
    """
    if not ARLINGTON_ARCGIS_URL:
        print("[Arlington] ArcGIS URL not configured.")
        return []

    print(f"[Arlington] Fetching permits since {since_date}...")

    # Convert since_date to timestamp (ms) for ArcGIS query
    try:
        dt = datetime.strptime(since_date, "%Y-%m-%d")
        timestamp = int(dt.timestamp() * 1000)
    except ValueError:
        print(f"[Arlington] Invalid date format: {since_date}")
        return []

    # Common date field names for ArcGIS permit data
    # Try multiple field names
    date_fields = ["IssueDate", "IssuedDate", "PermitDate", "ISSUE_DATE", "Issue_Date"]

    params = {
        "outFields": "*",
        "f": "json",
        "resultRecordCount": 5000
    }

    try:
        data = []
        for date_field in date_fields:
            params["where"] = f"{date_field} >= {timestamp}"
            params["orderByFields"] = f"{date_field} DESC"

            response = requests.get(
                f"{ARLINGTON_ARCGIS_URL}/query",
                params=params,
                timeout=60
            )

            if response.status_code == 200:
                result = response.json()
                if "features" in result and result["features"]:
                    data = result["features"]
                    print(f"[Arlington] Using date field: {date_field}")
                    break
                elif "error" in result:
                    # Field doesn't exist, try next
                    continue
            else:
                continue

        if not data:
            # Fallback: fetch all and filter locally
            params_fallback = {
                "where": "1=1",
                "outFields": "*",
                "f": "json",
                "resultRecordCount": 5000
            }
            response = requests.get(
                f"{ARLINGTON_ARCGIS_URL}/query",
                params=params_fallback,
                timeout=60
            )
            response.raise_for_status()
            result = response.json()
            data = result.get("features", [])

        # Extract attributes from features
        records = [f.get("attributes", {}) for f in data]
        print(f"[Arlington] Fetched {len(records)} total records")

        # Filter for restaurant/bar-related permits
        filtered_records = []
        for record in records:
            # Check multiple description/type fields
            description = (
                record.get("Description") or
                record.get("DESCRIPTION") or
                record.get("WorkDescription") or
                record.get("Work_Description") or
                record.get("ProjectDescription") or
                ""
            )

            permit_type = (
                record.get("PermitType") or
                record.get("PERMIT_TYPE") or
                record.get("Permit_Type") or
                record.get("Type") or
                record.get("WorkType") or
                ""
            )

            occupancy = (
                record.get("Occupancy") or
                record.get("OccupancyType") or
                record.get("UseType") or
                record.get("Use_Type") or
                ""
            )

            combined_text = f"{description} {permit_type} {occupancy}"

            if _matches_keywords(combined_text):
                filtered_records.append(record)

        print(f"[Arlington] Found {len(filtered_records)} restaurant/bar-related permits")
        return filtered_records

    except requests.exceptions.RequestException as e:
        print(f"[Arlington] Error fetching data: {e}")
        return []
    except Exception as e:
        print(f"[Arlington] Error processing response: {e}")
        return []


def to_source_events(rows: list[dict]) -> list[dict]:
    """
    Map raw Arlington permit attributes to normalized source_events dicts.
    """
    events = []

    for row in rows:
        # Extract permit number
        permit_number = (
            row.get("PermitNumber") or
            row.get("PERMIT_NUMBER") or
            row.get("Permit_Number") or
            row.get("PermitNo") or
            row.get("PermitID") or
            row.get("OBJECTID") or
            ""
        )

        # Convert timestamp to date
        date_ms = (
            row.get("IssueDate") or
            row.get("IssuedDate") or
            row.get("PermitDate") or
            row.get("ISSUE_DATE") or
            None
        )

        event_date = None
        if date_ms:
            try:
                event_date = datetime.fromtimestamp(date_ms / 1000).strftime("%Y-%m-%d")
            except (ValueError, TypeError, OSError):
                pass

        # Extract business/project name
        raw_name = (
            row.get("Applicant") or
            row.get("ApplicantName") or
            row.get("Owner") or
            row.get("OwnerName") or
            row.get("Contractor") or
            row.get("ContractorName") or
            row.get("ProjectName") or
            row.get("Description") or
            ""
        )

        # Build address
        raw_address = (
            row.get("Address") or
            row.get("SiteAddress") or
            row.get("Site_Address") or
            row.get("Location") or
            row.get("FullAddress") or
            ""
        )

        if not raw_address:
            # Try to construct address from components
            street_num = row.get("StreetNumber") or row.get("Street_Number") or ""
            street_name = row.get("StreetName") or row.get("Street_Name") or ""
            if street_num or street_name:
                raw_address = f"{street_num} {street_name}".strip()

        event = {
            "source_system": "ARLINGTON_PERMIT",
            "source_record_id": str(permit_number),
            "event_type": "permit_issued",
            "event_date": event_date,
            "raw_name": raw_name,
            "raw_address": raw_address,
            "city": "Arlington",
            "url": "https://opendata.arlingtontx.gov/datasets/arlingtontx::issued-permits",
            "payload_json": json.dumps(row)
        }

        events.append(event)

    return events
