"""
Fort Worth Building Permit data extractor.
Scrapes the Accela Citizen Access portal for building and fire permits.

Portal: https://aca-prod.accela.com/CFW/
"""

import json
import requests
import re
from datetime import datetime
from bs4 import BeautifulSoup
from config import FORTWORTH_ACCELA_URL

# Keywords to filter for restaurant/bar-related permits
PERMIT_KEYWORDS = [
    # Business types
    "restaurant", "bar", "lounge", "tavern", "pub", "brewery", "brewpub",
    "cafe", "grill", "bistro", "eatery", "food service", "food establishment",
    # Equipment/systems (early signals)
    "kitchen", "hood", "grease trap", "grease interceptor", "walk-in",
    "fire suppression", "fire hood", "ansul", "exhaust hood", "cooking",
    "type i hood", "type 1 hood",
    # Permit types
    "tenant finish", "finish-out", "finish out", "build-out", "build out",
    "commercial alteration", "commercial remodel", "commercial interior",
    "occupancy"
]


def _matches_keywords(text: str) -> bool:
    """Check if text contains any of the permit keywords."""
    if not text:
        return False
    text_lower = text.lower()
    return any(keyword in text_lower for keyword in PERMIT_KEYWORDS)


def _parse_accela_results(soup: BeautifulSoup) -> list[dict]:
    """Parse permit results from Accela search results page."""
    records = []

    # Find tables that contain permit data (look for permit number patterns)
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if len(rows) < 3:
            continue

        # Check if this table has permit-like data
        table_text = table.get_text()
        if not any(pattern in table_text for pattern in ["BLD", "FIR", "PO2", "COM", "UFC"]):
            continue

        # Parse rows - skip header rows
        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 4:
                continue

            cell_texts = [c.get_text(strip=True) for c in cells]

            # Skip header/navigation rows
            if any(skip in " ".join(cell_texts).lower() for skip in ["showing", "download", "date", "record number"]):
                continue

            # Try to identify the structure by looking for date pattern
            record = {}
            for i, text in enumerate(cell_texts):
                # Date pattern (MM/DD/YYYY)
                if re.match(r'\d{2}/\d{2}/\d{4}', text):
                    record["date"] = text
                # Permit number pattern (letters + numbers)
                elif re.match(r'^[A-Z]{2,4}[\d\-]+', text):
                    record["permit_number"] = text
                # Address pattern (has street type)
                elif any(st in text.upper() for st in [" ST", " AVE", " BLVD", " DR", " RD", " LN", " WAY", " PKWY"]):
                    record["address"] = text
                # Status (common statuses)
                elif text in ["Plan Review", "Pending", "Issued", "Approved", "In Review", "Awaiting Client Reply", "Active"]:
                    record["status"] = text
                # Everything else could be description or project name
                elif len(text) > 3 and "permit_number" in record and "description" not in record:
                    record["description"] = text
                elif len(text) > 3 and "description" in record and "project_name" not in record:
                    record["project_name"] = text

            # Only add if we have at least permit number
            if record.get("permit_number"):
                records.append(record)

    return records


def fetch_fortworth_permits_since(since_date: str) -> list[dict]:
    """
    Fetch Fort Worth building/fire permit records from Accela portal.

    Args:
        since_date: ISO date string 'YYYY-MM-DD'

    Returns:
        List of permit records filtered for restaurant/bar keywords
    """
    if not FORTWORTH_ACCELA_URL:
        print("[Fort Worth Permits] Accela URL not configured.")
        return []

    print(f"[Fort Worth Permits] Fetching permits since {since_date}...")

    try:
        since_dt = datetime.strptime(since_date, "%Y-%m-%d")
        since_formatted = since_dt.strftime("%m/%d/%Y")
        end_formatted = datetime.now().strftime("%m/%d/%Y")
    except ValueError:
        print(f"[Fort Worth Permits] Invalid date format: {since_date}")
        return []

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    })

    all_records = []

    # Search for each keyword
    search_keywords = ["restaurant", "bar", "lounge", "hood", "kitchen", "tenant finish", "food"]

    for keyword in search_keywords:
        try:
            # Use global search with date filters
            search_url = f"{FORTWORTH_ACCELA_URL}/Cap/GlobalSearchResults.aspx"
            params = {
                "QueryText": keyword,
                "FilterByStartDate": since_formatted,
                "FilterByEndDate": end_formatted
            }

            response = session.get(search_url, params=params, timeout=30)

            if response.status_code != 200:
                continue

            soup = BeautifulSoup(response.text, "lxml")
            records = _parse_accela_results(soup)

            # Filter for restaurant/bar keywords in the full record
            for record in records:
                combined_text = " ".join(str(v) for v in record.values())
                if _matches_keywords(combined_text):
                    record["_search_keyword"] = keyword
                    all_records.append(record)

        except requests.exceptions.RequestException as e:
            print(f"[Fort Worth Permits] Request error for '{keyword}': {e}")
            continue
        except Exception as e:
            print(f"[Fort Worth Permits] Error for '{keyword}': {e}")
            continue

    # Deduplicate by permit number
    seen = set()
    unique_records = []
    for record in all_records:
        permit_id = record.get("permit_number", "")
        if permit_id:
            if permit_id not in seen:
                seen.add(permit_id)
                unique_records.append(record)
        else:
            unique_records.append(record)

    print(f"[Fort Worth Permits] Found {len(unique_records)} restaurant/bar-related permits")
    return unique_records


def to_source_events(rows: list[dict]) -> list[dict]:
    """
    Map raw Fort Worth permit rows to normalized source_events dicts.
    """
    events = []

    for row in rows:
        # Extract permit number
        permit_number = row.get("permit_number", "")

        # Extract date
        date_str = row.get("date", "")
        event_date = None
        if date_str:
            try:
                event_date = datetime.strptime(date_str, "%m/%d/%Y").strftime("%Y-%m-%d")
            except ValueError:
                pass

        # Extract description/project name
        raw_name = (
            row.get("project_name") or
            row.get("description") or
            ""
        )

        # Extract address
        raw_address = row.get("address", "")

        event = {
            "source_system": "FORTWORTH_PERMIT",
            "source_record_id": permit_number,
            "event_type": "permit_issued",
            "event_date": event_date,
            "raw_name": raw_name,
            "raw_address": raw_address,
            "city": "Fort Worth",
            "url": "https://aca-prod.accela.com/CFW/",
            "payload_json": json.dumps(row, default=str)
        }

        events.append(event)

    return events
