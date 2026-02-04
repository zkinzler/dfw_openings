"""
Mesquite Building Permit data extractor.
Uses EnerGov REST API to fetch building permits.
"""

import json
import requests
from datetime import datetime
from config import MESQUITE_ENERGOV_URL

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


def fetch_mesquite_permits_since(since_date: str) -> list[dict]:
    """
    Fetch Mesquite building permit records from EnerGov API.

    Args:
        since_date: ISO date string 'YYYY-MM-DD'

    Returns:
        List of permit records filtered for restaurant/bar keywords
    """
    if not MESQUITE_ENERGOV_URL:
        print("[Mesquite] EnerGov URL not configured.")
        return []

    print(f"[Mesquite] Fetching permits since {since_date}...")

    try:
        since_dt = datetime.strptime(since_date, "%Y-%m-%d")
    except ValueError:
        print(f"[Mesquite] Invalid date format: {since_date}")
        return []

    # EnerGov API typically has a search endpoint
    # Common patterns: /api/permits, /api/cap/search, /EnerGovProdSelfService/api
    search_endpoints = [
        f"{MESQUITE_ENERGOV_URL}/api/cap/search",
        f"{MESQUITE_ENERGOV_URL}/api/permits/search",
        f"{MESQUITE_ENERGOV_URL}/api/permits",
        f"{MESQUITE_ENERGOV_URL}/selfservice/api/cap/search"
    ]

    # Request payload for EnerGov search
    payload = {
        "IssueDateFrom": since_date,
        "IssueDateTo": datetime.now().strftime("%Y-%m-%d"),
        "ModuleType": "Permit",
        "PageNumber": 1,
        "PageSize": 1000
    }

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

    records = []

    for endpoint in search_endpoints:
        try:
            # Try POST request (common for EnerGov)
            response = requests.post(
                endpoint,
                json=payload,
                headers=headers,
                timeout=30
            )

            if response.status_code == 404:
                continue

            response.raise_for_status()
            data = response.json()

            # Handle different response structures
            if isinstance(data, list):
                raw_records = data
            elif isinstance(data, dict):
                raw_records = (
                    data.get("Result") or
                    data.get("Results") or
                    data.get("Data") or
                    data.get("data") or
                    data.get("Permits") or
                    data.get("permits") or
                    []
                )
            else:
                continue

            # Filter for restaurant/bar keywords
            for record in raw_records:
                description = (
                    record.get("Description") or
                    record.get("WorkDescription") or
                    record.get("ProjectName") or
                    record.get("PermitDescription") or
                    ""
                )

                permit_type = (
                    record.get("PermitType") or
                    record.get("Type") or
                    record.get("PermitTypeName") or
                    ""
                )

                combined_text = f"{description} {permit_type}"

                if _matches_keywords(combined_text):
                    records.append(record)

            print(f"[Mesquite] Found {len(records)} restaurant/bar-related permits")
            return records

        except requests.exceptions.RequestException:
            continue
        except Exception:
            continue

    # Try GET request as fallback
    for endpoint in search_endpoints:
        try:
            params = {
                "issueDateFrom": since_date,
                "issueDateTo": datetime.now().strftime("%Y-%m-%d"),
                "pageSize": 1000
            }

            response = requests.get(endpoint, params=params, headers=headers, timeout=30)

            if response.status_code == 404:
                continue

            response.raise_for_status()
            data = response.json()

            if isinstance(data, list):
                raw_records = data
            elif isinstance(data, dict):
                raw_records = (
                    data.get("Result") or
                    data.get("Results") or
                    data.get("Data") or
                    []
                )
            else:
                continue

            for record in raw_records:
                description = (
                    record.get("Description") or
                    record.get("WorkDescription") or
                    ""
                )
                permit_type = (
                    record.get("PermitType") or
                    record.get("Type") or
                    ""
                )
                combined_text = f"{description} {permit_type}"

                if _matches_keywords(combined_text):
                    records.append(record)

            print(f"[Mesquite] Found {len(records)} restaurant/bar-related permits")
            return records

        except requests.exceptions.RequestException:
            continue
        except Exception:
            continue

    print("[Mesquite] Could not connect to EnerGov API. Check endpoint configuration.")
    return []


def to_source_events(rows: list[dict]) -> list[dict]:
    """
    Map raw Mesquite permit rows to normalized source_events dicts.
    """
    events = []

    for row in rows:
        # Extract permit number
        permit_number = (
            row.get("PermitNumber") or
            row.get("PermitNo") or
            row.get("CAPId") or
            row.get("Id") or
            row.get("RecordNumber") or
            ""
        )

        # Extract date
        date_str = (
            row.get("IssueDate") or
            row.get("IssuedDate") or
            row.get("ApplicationDate") or
            row.get("OpenedDate") or
            ""
        )

        event_date = None
        if date_str:
            # Handle ISO format or other formats
            for fmt in ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%m/%d/%Y"]:
                try:
                    if "T" in str(date_str):
                        event_date = date_str.split("T")[0]
                    else:
                        event_date = datetime.strptime(str(date_str)[:10], "%Y-%m-%d").strftime("%Y-%m-%d")
                    break
                except (ValueError, TypeError):
                    continue

        # Extract business/project name
        raw_name = (
            row.get("ProjectName") or
            row.get("BusinessName") or
            row.get("Applicant") or
            row.get("ApplicantName") or
            row.get("OwnerName") or
            row.get("Description") or
            ""
        )

        # Extract address
        raw_address = (
            row.get("Address") or
            row.get("SiteAddress") or
            row.get("Location") or
            row.get("FullAddress") or
            row.get("AddressLine1") or
            ""
        )

        event = {
            "source_system": "MESQUITE_PERMIT",
            "source_record_id": str(permit_number),
            "event_type": "permit_issued",
            "event_date": event_date,
            "raw_name": raw_name,
            "raw_address": raw_address,
            "city": "Mesquite",
            "url": "https://energov.cityofmesquite.com/",
            "payload_json": json.dumps(row, default=str)
        }

        events.append(event)

    return events
