"""
McKinney Building Permit data extractor.
Uses EnerGov REST API to fetch building permits.
"""

import json
import requests
from datetime import datetime
from config import MCKINNEY_ENERGOV_URL

# Keywords to filter for restaurant/bar-related permits
PERMIT_KEYWORDS = [
    # Business types
    "restaurant", "bar", "lounge", "tavern", "pub", "brewery", "brewpub",
    "cafe", "grill", "bistro", "eatery", "food service",
    # Equipment/systems (early signals)
    "kitchen", "hood", "grease trap", "grease interceptor", "walk-in",
    "fire suppression", "fire hood", "ansul", "exhaust hood", "cooking",
    # Permit types
    "tenant finish", "finish-out", "finish out", "build-out", "build out",
    "commercial alteration", "commercial remodel"
]


def _matches_keywords(text: str) -> bool:
    """Check if text contains any of the permit keywords."""
    if not text:
        return False
    text_lower = text.lower()
    return any(keyword in text_lower for keyword in PERMIT_KEYWORDS)


def fetch_mckinney_permits_since(since_date: str) -> list[dict]:
    """
    Fetch McKinney building permit records from EnerGov API.

    Args:
        since_date: ISO date string 'YYYY-MM-DD'

    Returns:
        List of permit records filtered for restaurant/bar keywords
    """
    if not MCKINNEY_ENERGOV_URL:
        print("[McKinney] EnerGov URL not configured.")
        return []

    print(f"[McKinney] Fetching permits since {since_date}...")

    try:
        since_dt = datetime.strptime(since_date, "%Y-%m-%d")
    except ValueError:
        print(f"[McKinney] Invalid date format: {since_date}")
        return []

    # EnerGov API endpoints to try
    base_url = MCKINNEY_ENERGOV_URL.rstrip('/')
    search_endpoints = [
        f"{base_url}/api/cap/search",
        f"{base_url}/api/permits/search",
        f"{base_url}/api/permits",
        f"{base_url}/api/energov/search/search",
        f"{base_url}/api/cap"
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
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
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

            if response.status_code == 200:
                try:
                    data = response.json()
                except json.JSONDecodeError:
                    continue

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
                        data.get("Items") or
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
                        record.get("RecordType") or
                        ""
                    )

                    combined_text = f"{description} {permit_type}"

                    if _matches_keywords(combined_text):
                        records.append(record)

                if raw_records:  # Found working endpoint
                    print(f"[McKinney] Found {len(records)} restaurant/bar-related permits")
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

            if response.status_code == 200:
                try:
                    data = response.json()
                except json.JSONDecodeError:
                    continue

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

                if raw_records:
                    print(f"[McKinney] Found {len(records)} restaurant/bar-related permits")
                    return records

        except requests.exceptions.RequestException:
            continue
        except Exception:
            continue

    print("[McKinney] Could not connect to EnerGov API. Check endpoint configuration.")
    return []


def to_source_events(rows: list[dict]) -> list[dict]:
    """
    Map raw McKinney permit rows to normalized source_events dicts.
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
            try:
                if "T" in str(date_str):
                    event_date = str(date_str).split("T")[0]
                else:
                    event_date = str(date_str)[:10]
            except (ValueError, TypeError):
                pass

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
            "source_system": "MCKINNEY_PERMIT",
            "source_record_id": str(permit_number),
            "event_type": "permit_issued",
            "event_date": event_date,
            "raw_name": raw_name,
            "raw_address": raw_address,
            "city": "McKinney",
            "url": "https://egov.mckinneytexas.org/EnerGov_Prod/SelfService",
            "payload_json": json.dumps(row, default=str)
        }

        events.append(event)

    return events
