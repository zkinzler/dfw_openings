"""
Dallas Building Permit data extractor.
Fetches building permits from Dallas OpenData (Socrata API).
Filters for restaurant/bar-related permits.
"""

import json
import requests
from datetime import datetime
from config import DALLAS_PERMITS_ENDPOINT, SOCRATA_APP_TOKEN

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


def fetch_dallas_permits_since(since_iso: str) -> list[dict]:
    """
    Fetch Dallas building permit records from Socrata API.

    Args:
        since_iso: ISO 8601 datetime string like 'YYYY-MM-DDT00:00:00.000'
                   or ISO date string 'YYYY-MM-DD'

    Returns:
        List of raw permit records filtered for restaurant/bar keywords
    """
    if not DALLAS_PERMITS_ENDPOINT:
        print("[Dallas Permits] Endpoint not configured.")
        return []

    print(f"[Dallas Permits] Fetching permits since {since_iso}...")

    # Ensure proper ISO format for Socrata
    if len(since_iso) == 10:  # YYYY-MM-DD
        since_iso = f"{since_iso}T00:00:00.000"

    # Build the query - common date fields: issue_date, permit_issue_date, issued_date
    # Try multiple possible date field names
    date_fields = ["issue_date", "permit_issue_date", "issued_date", "permit_date"]

    params = {
        "$limit": 10000,
        "$order": "issue_date DESC"
    }

    headers = {}
    if SOCRATA_APP_TOKEN:
        headers["X-App-Token"] = SOCRATA_APP_TOKEN

    try:
        # Try with different date field names
        data = []
        for date_field in date_fields:
            params["$where"] = f"{date_field} >= '{since_iso}'"
            params["$order"] = f"{date_field} DESC"

            response = requests.get(DALLAS_PERMITS_ENDPOINT, params=params, headers=headers, timeout=30)

            if response.status_code == 200:
                data = response.json()
                if data:
                    print(f"[Dallas Permits] Using date field: {date_field}")
                    break
            elif response.status_code == 400:
                # Field doesn't exist, try next
                continue
            else:
                response.raise_for_status()

        if not data:
            # Fallback: fetch without date filter and filter locally
            params_fallback = {
                "$limit": 10000
            }
            response = requests.get(DALLAS_PERMITS_ENDPOINT, params=params_fallback, headers=headers, timeout=30)
            response.raise_for_status()
            data = response.json()

        print(f"[Dallas Permits] Fetched {len(data)} total records")

        # Filter for restaurant/bar-related permits
        filtered_records = []
        for record in data:
            # Check multiple description/type fields
            description = (
                record.get("description") or
                record.get("work_description") or
                record.get("project_description") or
                record.get("permit_description") or
                ""
            )

            permit_type = (
                record.get("permit_type") or
                record.get("work_type") or
                record.get("type") or
                record.get("permit_class") or
                ""
            )

            occupancy = (
                record.get("occupancy") or
                record.get("occupancy_type") or
                record.get("use_type") or
                ""
            )

            combined_text = f"{description} {permit_type} {occupancy}"

            if _matches_keywords(combined_text):
                filtered_records.append(record)

        print(f"[Dallas Permits] Found {len(filtered_records)} restaurant/bar-related permits")
        return filtered_records

    except requests.exceptions.RequestException as e:
        print(f"[Dallas Permits] Error fetching data: {e}")
        return []


def to_source_events(rows: list[dict]) -> list[dict]:
    """
    Map raw Dallas permit rows to normalized source_events dicts.
    """
    events = []

    for row in rows:
        # Extract permit number
        permit_number = (
            row.get("permit_number") or
            row.get("permit_no") or
            row.get("permit_id") or
            row.get("case_number") or
            ""
        )

        # Extract date from various possible fields
        date_str = (
            row.get("issued_date") or
            row.get("issue_date") or
            row.get("permit_issue_date") or
            row.get("permit_date") or
            ""
        )

        event_date = None
        if date_str:
            # Handle multiple date formats
            try:
                if "T" in date_str:
                    # ISO format: YYYY-MM-DDT...
                    dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                    event_date = dt.date().isoformat()
                elif "/" in date_str:
                    # MM/DD/YY or MM/DD/YYYY format
                    for fmt in ["%m/%d/%y", "%m/%d/%Y"]:
                        try:
                            dt = datetime.strptime(date_str.strip(), fmt)
                            event_date = dt.strftime("%Y-%m-%d")
                            break
                        except ValueError:
                            continue
                else:
                    event_date = date_str[:10]  # First 10 chars (YYYY-MM-DD)
            except (ValueError, AttributeError):
                pass

        # Extract business/project name - use contractor or work_description
        raw_name = (
            row.get("contractor") or
            row.get("applicant_name") or
            row.get("owner_name") or
            row.get("contractor_name") or
            row.get("business_name") or
            row.get("project_name") or
            row.get("work_description") or
            ""
        )

        # Clean up contractor field (often has full address embedded)
        if raw_name and " " in raw_name:
            # Take just the company name part (before address)
            parts = raw_name.split(" ")
            # Find where address starts (typically a number)
            for i, part in enumerate(parts):
                if part.isdigit() and i > 0:
                    raw_name = " ".join(parts[:i])
                    break

        # Build address
        raw_address = (
            row.get("street_address") or
            row.get("address") or
            ""
        )

        if not raw_address:
            # Try alternate address construction
            street_num = row.get("street_number") or ""
            street_name = row.get("street_name") or ""
            if street_num or street_name:
                raw_address = f"{street_num} {street_name}".strip()

        # Extract city (default to Dallas)
        city = row.get("city") or "Dallas"

        event = {
            "source_system": "DALLAS_PERMIT",
            "source_record_id": permit_number,
            "event_type": "permit_issued",
            "event_date": event_date,
            "raw_name": raw_name,
            "raw_address": raw_address,
            "city": city,
            "url": "https://www.dallasopendata.com/resource/e7gq-4sah",
            "payload_json": json.dumps(row)
        }

        events.append(event)

    return events
