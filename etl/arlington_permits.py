"""
Arlington Building Permit data extractor.
Scrapes the SmartGuide-based permit portal.

Portal: https://ap.arlingtontx.gov/AP/sfjsp?interviewID=PublicSearch
"""

import json
import requests
from datetime import datetime
from bs4 import BeautifulSoup
try:
    from config import ARLINGTON_PORTAL_URL
except ImportError:
    ARLINGTON_PORTAL_URL = "https://ap.arlingtontx.gov/AP"

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


def fetch_arlington_permits_since(since_date: str) -> list[dict]:
    """
    Fetch Arlington building permit records from SmartGuide portal.

    Args:
        since_date: ISO date string 'YYYY-MM-DD'

    Returns:
        List of permit records filtered for restaurant/bar keywords
    """
    if not ARLINGTON_PORTAL_URL:
        print("[Arlington] Portal URL not configured.")
        return []

    print(f"[Arlington] Fetching permits since {since_date}...")

    try:
        since_dt = datetime.strptime(since_date, "%Y-%m-%d")
        since_formatted = since_dt.strftime("%m/%d/%Y")
    except ValueError:
        print(f"[Arlington] Invalid date format: {since_date}")
        return []

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest"
    })

    all_records = []

    try:
        # First, get the search page to establish session
        search_url = f"{ARLINGTON_PORTAL_URL}/sfjsp"
        params = {"interviewID": "PublicSearch"}
        response = session.get(search_url, params=params, timeout=30)

        if response.status_code != 200:
            print(f"[Arlington] Failed to load search page: {response.status_code}")
            return []

        # Try to submit a permit search via the SmartGuide API
        # SmartGuide uses AJAX calls for searches
        api_url = f"{ARLINGTON_PORTAL_URL}/sfjsp"

        # Search keywords
        search_keywords = ["restaurant", "bar", "food", "kitchen", "hood"]

        for keyword in search_keywords:
            try:
                # SmartGuide form submission
                form_data = {
                    "interviewID": "PublicSearch",
                    "com.alphinat.sgs.anticsrftoken": "null",
                    "btnclk": "btn-permitSearch-search",
                    "txt-permitSearch-issuedDate": since_formatted,
                    "txt-permitSearch-Address": "",
                    "txt-permitSearch-peopleName": keyword,
                    "txt-permitSearch-contractorBusName": ""
                }

                response = session.post(api_url, data=form_data, timeout=30)

                if response.status_code == 200:
                    # Try to parse JSON response
                    try:
                        data = response.json()
                        if isinstance(data, dict):
                            results = data.get("searchresult-permits", [])
                            if isinstance(results, list):
                                for record in results:
                                    combined_text = " ".join(str(v) for v in record.values())
                                    if _matches_keywords(combined_text):
                                        record["_search_keyword"] = keyword
                                        all_records.append(record)
                    except json.JSONDecodeError:
                        # Parse HTML response
                        soup = BeautifulSoup(response.text, "lxml")
                        # Look for data tables
                        for table in soup.find_all("table"):
                            rows = table.find_all("tr")
                            for row in rows[1:]:  # Skip header
                                cells = row.find_all("td")
                                if len(cells) >= 3:
                                    record = {
                                        "permit_number": cells[0].get_text(strip=True) if cells else "",
                                        "address": cells[1].get_text(strip=True) if len(cells) > 1 else "",
                                        "description": cells[2].get_text(strip=True) if len(cells) > 2 else "",
                                        "status": cells[3].get_text(strip=True) if len(cells) > 3 else "",
                                    }
                                    combined_text = " ".join(str(v) for v in record.values())
                                    if _matches_keywords(combined_text):
                                        record["_search_keyword"] = keyword
                                        all_records.append(record)

            except Exception as e:
                print(f"[Arlington] Error searching for '{keyword}': {e}")
                continue

    except requests.exceptions.RequestException as e:
        print(f"[Arlington] Request error: {e}")
        return []

    # Deduplicate
    seen = set()
    unique_records = []
    for record in all_records:
        permit_id = record.get("permit_number") or record.get("PermitNum", "")
        if permit_id:
            if permit_id not in seen:
                seen.add(permit_id)
                unique_records.append(record)
        else:
            unique_records.append(record)

    print(f"[Arlington] Found {len(unique_records)} restaurant/bar-related permits")
    return unique_records


def to_source_events(rows: list[dict]) -> list[dict]:
    """
    Map raw Arlington permit rows to normalized source_events dicts.
    """
    events = []

    for row in rows:
        # Extract permit number
        permit_number = (
            row.get("permit_number") or
            row.get("PermitNum") or
            row.get("txt-searchpermits-PermitNum") or
            ""
        )

        # Extract date
        date_str = (
            row.get("date") or
            row.get("IssueDate") or
            row.get("txt-permitSearch-issuedDate") or
            ""
        )

        event_date = None
        if date_str:
            for fmt in ["%m/%d/%Y", "%Y-%m-%d"]:
                try:
                    event_date = datetime.strptime(date_str.strip(), fmt).strftime("%Y-%m-%d")
                    break
                except ValueError:
                    continue

        # Extract name/description
        raw_name = (
            row.get("description") or
            row.get("Description") or
            row.get("txt-searchpermits-worktype") or
            ""
        )

        # Extract address
        raw_address = (
            row.get("address") or
            row.get("Address") or
            row.get("txt-searchpermits-AddressName") or
            ""
        )

        event = {
            "source_system": "ARLINGTON_PERMIT",
            "source_record_id": str(permit_number),
            "event_type": "permit_issued",
            "event_date": event_date,
            "raw_name": raw_name,
            "raw_address": raw_address,
            "city": "Arlington",
            "url": "https://ap.arlingtontx.gov/AP/sfjsp?interviewID=PublicSearch",
            "payload_json": json.dumps(row, default=str)
        }

        events.append(event)

    return events
