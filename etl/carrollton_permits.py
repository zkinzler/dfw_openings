"""
Carrollton Building Permit data extractor.
Uses CityView Portal / ArcGIS Feature Service API.
"""

import json
import requests
from datetime import datetime
from config import CARROLLTON_ARCGIS_URL

# Keywords to filter for restaurant/bar-related permits
PERMIT_KEYWORDS = [
    "restaurant", "bar", "lounge", "tavern", "pub",
    "kitchen", "hood", "grease trap", "walk-in",
    "tenant finish", "build-out", "commercial alteration",
    "food service", "cooking", "brewery", "brewpub"
]


def _matches_keywords(text: str) -> bool:
    """Check if text contains any of the permit keywords."""
    if not text:
        return False
    text_lower = text.lower()
    return any(keyword in text_lower for keyword in PERMIT_KEYWORDS)


def fetch_carrollton_permits_since(since_date: str) -> list[dict]:
    """
    Fetch Carrollton building permit records from ArcGIS API.

    Args:
        since_date: ISO date string 'YYYY-MM-DD'

    Returns:
        List of permit records filtered for restaurant/bar keywords
    """
    if not CARROLLTON_ARCGIS_URL:
        print("[Carrollton] ArcGIS URL not configured.")
        return []

    print(f"[Carrollton] Fetching permits since {since_date}...")

    # Convert since_date to timestamp (ms) for ArcGIS query
    try:
        dt = datetime.strptime(since_date, "%Y-%m-%d")
        timestamp_ms = int(dt.timestamp() * 1000)
    except ValueError:
        print(f"[Carrollton] Invalid date format: {since_date}")
        return []

    # Query for permits issued since the date
    # Common ArcGIS field names for issue date
    date_fields = ["IssuedDate", "IssueDate", "Issue_Date", "ISSUED_DATE", "PermitDate"]

    params = {
        "outFields": "*",
        "f": "json",
        "resultRecordCount": 2000
    }

    records = []

    for date_field in date_fields:
        params["where"] = f"{date_field} >= {timestamp_ms}"

        try:
            response = requests.get(
                f"{CARROLLTON_ARCGIS_URL}/query",
                params=params,
                timeout=30
            )

            if response.status_code == 400:
                # Field doesn't exist, try next
                continue

            response.raise_for_status()
            data = response.json()

            # Check for error in response
            if "error" in data:
                continue

            features = data.get("features", [])

            if features:
                # Extract attributes and filter
                for feature in features:
                    attrs = feature.get("attributes", {})

                    # Check keywords in description/permit type
                    description = (
                        attrs.get("Description") or
                        attrs.get("DESCRIPTION") or
                        attrs.get("WorkDescription") or
                        attrs.get("Work_Description") or
                        attrs.get("ProjectDescription") or
                        ""
                    )

                    permit_type = (
                        attrs.get("PermitType") or
                        attrs.get("PERMIT_TYPE") or
                        attrs.get("Permit_Type") or
                        attrs.get("Type") or
                        ""
                    )

                    combined_text = f"{description} {permit_type}"

                    if _matches_keywords(combined_text):
                        records.append(attrs)

                print(f"[Carrollton] Found {len(records)} restaurant/bar-related permits")
                return records

        except requests.exceptions.RequestException as e:
            print(f"[Carrollton] Request error: {e}")
            continue
        except Exception as e:
            print(f"[Carrollton] Error: {e}")
            continue

    # Fallback: try without date filter and filter in Python
    params["where"] = "1=1"  # Get all records
    try:
        response = requests.get(
            f"{CARROLLTON_ARCGIS_URL}/query",
            params=params,
            timeout=30
        )
        response.raise_for_status()
        data = response.json()

        features = data.get("features", [])
        print(f"[Carrollton] Retrieved {len(features)} total permits, filtering...")

        for feature in features:
            attrs = feature.get("attributes", {})

            # Try to find and filter by date
            date_val = None
            for field in date_fields:
                if field in attrs and attrs[field]:
                    date_val = attrs[field]
                    break

            if date_val and isinstance(date_val, (int, float)):
                if date_val < timestamp_ms:
                    continue

            # Check keywords
            description = (
                attrs.get("Description") or
                attrs.get("DESCRIPTION") or
                attrs.get("WorkDescription") or
                ""
            )
            permit_type = (
                attrs.get("PermitType") or
                attrs.get("PERMIT_TYPE") or
                attrs.get("Type") or
                ""
            )
            combined_text = f"{description} {permit_type}"

            if _matches_keywords(combined_text):
                records.append(attrs)

        print(f"[Carrollton] Found {len(records)} restaurant/bar-related permits")

    except Exception as e:
        print(f"[Carrollton] Error fetching data: {e}")

    return records


def to_source_events(rows: list[dict]) -> list[dict]:
    """
    Map raw Carrollton permit attributes to normalized source_events dicts.
    """
    events = []

    for row in rows:
        # Extract permit number
        permit_number = (
            row.get("PermitNumber") or
            row.get("PERMIT_NUMBER") or
            row.get("Permit_Number") or
            row.get("PermitNo") or
            row.get("OBJECTID") or
            ""
        )

        # Extract date (timestamp in ms or date string)
        date_val = (
            row.get("IssuedDate") or
            row.get("IssueDate") or
            row.get("Issue_Date") or
            row.get("ISSUED_DATE") or
            row.get("PermitDate") or
            None
        )

        event_date = None
        if date_val:
            if isinstance(date_val, (int, float)):
                # Timestamp in milliseconds
                event_date = datetime.fromtimestamp(date_val / 1000).strftime("%Y-%m-%d")
            elif isinstance(date_val, str):
                event_date = date_val[:10]

        # Extract business/project name
        raw_name = (
            row.get("BusinessName") or
            row.get("Business_Name") or
            row.get("Applicant") or
            row.get("Owner") or
            row.get("OwnerName") or
            row.get("ContractorName") or
            row.get("Description") or
            ""
        )

        # Extract address
        raw_address = (
            row.get("Address") or
            row.get("ADDRESS") or
            row.get("SiteAddress") or
            row.get("Site_Address") or
            row.get("Location") or
            row.get("FullAddress") or
            ""
        )

        event = {
            "source_system": "CARROLLTON_PERMIT",
            "source_record_id": str(permit_number),
            "event_type": "permit_issued",
            "event_date": event_date,
            "raw_name": raw_name,
            "raw_address": raw_address,
            "city": "Carrollton",
            "url": "https://cityserve.cityofcarrollton.com/CityViewPortal/Permit/Locator",
            "payload_json": json.dumps(row, default=str)
        }

        events.append(event)

    return events
