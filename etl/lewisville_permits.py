"""
Lewisville Building Permit data extractor.
Calls the Lewisville API to get dynamic CSV links, downloads and parses them.
Filters for restaurant/bar-related permits.
"""

import json
import requests
import csv
import io
from datetime import datetime
from config import LEWISVILLE_CSV_API

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


def _get_csv_links_from_api() -> list[str]:
    """
    Call the Lewisville API to get current CSV download links.

    Returns:
        List of CSV URLs
    """
    if not LEWISVILLE_CSV_API:
        return []

    try:
        # Some city APIs have SSL cert issues - try with verification first, then without
        try:
            response = requests.get(LEWISVILLE_CSV_API, timeout=30)
        except requests.exceptions.SSLError:
            response = requests.get(LEWISVILLE_CSV_API, timeout=30, verify=False)
        response.raise_for_status()
        data = response.json()

        # API returns links - extract CSV URLs
        # Format varies, handle common patterns
        csv_links = []

        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    # Look for URL/link fields
                    url = (
                        item.get("url") or
                        item.get("URL") or
                        item.get("link") or
                        item.get("Link") or
                        item.get("csvUrl") or
                        item.get("downloadUrl") or
                        ""
                    )
                    if url and ".csv" in url.lower():
                        csv_links.append(url)
                elif isinstance(item, str) and ".csv" in item.lower():
                    csv_links.append(item)
        elif isinstance(data, dict):
            # Check for results/data array
            items = data.get("results") or data.get("data") or data.get("links") or []
            for item in items:
                if isinstance(item, dict):
                    url = item.get("url") or item.get("link") or ""
                    if url and ".csv" in url.lower():
                        csv_links.append(url)
                elif isinstance(item, str) and ".csv" in item.lower():
                    csv_links.append(item)

        return csv_links

    except requests.exceptions.RequestException as e:
        print(f"[Lewisville] Error calling API: {e}")
        return []
    except (json.JSONDecodeError, KeyError) as e:
        print(f"[Lewisville] Error parsing API response: {e}")
        return []


def _parse_csv_from_url(csv_url: str, since_dt: datetime) -> list[dict]:
    """
    Download and parse a CSV file, filtering by date and keywords.

    Args:
        csv_url: URL to CSV file
        since_dt: Minimum date for records

    Returns:
        List of matching permit records
    """
    try:
        # Some city APIs have SSL cert issues - try with verification first, then without
        try:
            response = requests.get(csv_url, timeout=60)
        except requests.exceptions.SSLError:
            response = requests.get(csv_url, timeout=60, verify=False)
        response.raise_for_status()

        # Parse CSV content
        csv_content = response.content.decode("utf-8-sig")  # Handle BOM
        reader = csv.DictReader(io.StringIO(csv_content))

        records = []
        for row in reader:
            # Try common date field names
            date_str = (
                row.get("Issue Date") or
                row.get("IssueDate") or
                row.get("Issued Date") or
                row.get("IssuedDate") or
                row.get("Date") or
                row.get("ISSUE_DATE") or
                row.get("PermitDate") or
                ""
            )

            # Parse date and filter by since_date
            if date_str:
                try:
                    # Try multiple date formats
                    row_dt = None
                    for fmt in ["%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%Y/%m/%d"]:
                        try:
                            row_dt = datetime.strptime(date_str.strip(), fmt)
                            break
                        except ValueError:
                            continue

                    if row_dt is None:
                        continue  # Skip if no format matched

                    if row_dt < since_dt:
                        continue
                except (ValueError, AttributeError):
                    continue

            # Check if permit matches keywords in description or work type
            description = (
                row.get("Description") or
                row.get("Work Description") or
                row.get("WorkDescription") or
                row.get("DESCRIPTION") or
                row.get("Project Description") or
                row.get("PermitDescription") or
                ""
            )

            permit_type = (
                row.get("Permit Type") or
                row.get("PermitType") or
                row.get("Type") or
                row.get("PERMIT_TYPE") or
                row.get("Work Type") or
                ""
            )

            combined_text = f"{description} {permit_type}"

            if _matches_keywords(combined_text):
                records.append(row)

        return records

    except requests.exceptions.RequestException as e:
        print(f"[Lewisville] Error fetching CSV {csv_url}: {e}")
        return []
    except Exception as e:
        print(f"[Lewisville] Error parsing CSV: {e}")
        return []


def fetch_lewisville_permits_since(since_date: str) -> list[dict]:
    """
    Fetch Lewisville building permit records by calling the API to get
    CSV links dynamically, then downloading and parsing them.

    Args:
        since_date: ISO date string 'YYYY-MM-DD'

    Returns:
        List of permit records filtered for restaurant/bar keywords
    """
    if not LEWISVILLE_CSV_API:
        print("[Lewisville] CSV API not configured.")
        return []

    print(f"[Lewisville] Fetching permits since {since_date}...")

    try:
        since_dt = datetime.strptime(since_date, "%Y-%m-%d")
    except ValueError:
        print(f"[Lewisville] Invalid date format: {since_date}")
        return []

    # Step 1: Get CSV links from API
    csv_links = _get_csv_links_from_api()

    if not csv_links:
        print("[Lewisville] No CSV links found from API")
        return []

    print(f"[Lewisville] Found {len(csv_links)} CSV links from API")

    # Step 2: Download and parse each CSV
    all_records = []
    for csv_url in csv_links:
        records = _parse_csv_from_url(csv_url, since_dt)
        all_records.extend(records)

    # Deduplicate by permit number if available
    seen_permits = set()
    unique_records = []
    for record in all_records:
        permit_id = (
            record.get("Permit Number") or
            record.get("PermitNumber") or
            record.get("Permit No") or
            record.get("PERMIT_NUMBER") or
            record.get("Permit #") or
            ""
        )
        if permit_id:
            if permit_id not in seen_permits:
                seen_permits.add(permit_id)
                unique_records.append(record)
        else:
            unique_records.append(record)

    print(f"[Lewisville] Found {len(unique_records)} restaurant/bar-related permits")
    return unique_records


def to_source_events(rows: list[dict]) -> list[dict]:
    """
    Map raw Lewisville permit rows to normalized source_events dicts.
    """
    events = []

    for row in rows:
        # Extract permit number
        permit_number = (
            row.get("Permit Number") or
            row.get("PermitNumber") or
            row.get("Permit No") or
            row.get("PERMIT_NUMBER") or
            row.get("Permit #") or
            ""
        )

        # Extract date
        date_str = (
            row.get("Issue Date") or
            row.get("IssueDate") or
            row.get("Issued Date") or
            row.get("IssuedDate") or
            row.get("Date") or
            row.get("ISSUE_DATE") or
            ""
        )

        event_date = None
        if date_str:
            for fmt in ["%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%Y/%m/%d"]:
                try:
                    event_date = datetime.strptime(date_str.strip(), fmt).strftime("%Y-%m-%d")
                    break
                except ValueError:
                    continue

        # Extract business/project name
        raw_name = (
            row.get("Business Name") or
            row.get("BusinessName") or
            row.get("Applicant") or
            row.get("Owner") or
            row.get("Contractor") or
            row.get("Project Name") or
            row.get("Description") or
            ""
        )

        # Extract address
        raw_address = (
            row.get("Address") or
            row.get("Site Address") or
            row.get("SiteAddress") or
            row.get("Location") or
            row.get("PROJECT_ADDRESS") or
            ""
        )

        event = {
            "source_system": "LEWISVILLE_PERMIT",
            "source_record_id": permit_number,
            "event_type": "permit_issued",
            "event_date": event_date,
            "raw_name": raw_name,
            "raw_address": raw_address,
            "city": "Lewisville",
            "url": "https://www.cityoflewisville.com/for-business/building-services/building-permit-reports",
            "payload_json": json.dumps(row)
        }

        events.append(event)

    return events
