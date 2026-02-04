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
    "cafe", "grill", "bistro", "eatery", "food service",
    # Equipment/systems (early signals)
    "kitchen", "hood", "grease trap", "grease interceptor", "walk-in",
    "fire suppression", "fire hood", "ansul", "exhaust hood", "cooking",
    "type i hood", "type 1 hood",
    # Permit types
    "tenant finish", "finish-out", "finish out", "build-out", "build out",
    "commercial alteration", "commercial remodel", "commercial interior"
]


def _matches_keywords(text: str) -> bool:
    """Check if text contains any of the permit keywords."""
    if not text:
        return False
    text_lower = text.lower()
    return any(keyword in text_lower for keyword in PERMIT_KEYWORDS)


def _extract_viewstate(soup: BeautifulSoup) -> dict:
    """Extract ASP.NET ViewState and related tokens from page."""
    tokens = {}

    for field_name in ["__VIEWSTATE", "__VIEWSTATEGENERATOR", "__EVENTVALIDATION", "__EVENTTARGET", "__EVENTARGUMENT"]:
        field = soup.find("input", {"name": field_name})
        if field:
            tokens[field_name] = field.get("value", "")

    return tokens


def _parse_results_table(soup: BeautifulSoup) -> list[dict]:
    """Parse permit results from Accela HTML table."""
    records = []

    # Find results table - Accela uses specific ID patterns
    table = (
        soup.find("table", {"id": re.compile(r".*GridView.*", re.I)}) or
        soup.find("table", {"class": re.compile(r".*ACA_Grid.*", re.I)}) or
        soup.find("div", {"id": re.compile(r".*divGlobalSearchResult.*", re.I)})
    )

    if not table:
        # Try finding any data table with permit-like content
        tables = soup.find_all("table")
        for t in tables:
            text = t.get_text().lower()
            if "permit" in text or "record" in text:
                table = t
                break

    if not table:
        return records

    # Get all rows
    rows = table.find_all("tr")

    # Find header row
    headers = []
    for row in rows:
        ths = row.find_all(["th"])
        if ths:
            headers = [th.get_text(strip=True) for th in ths]
            break

    if not headers:
        # Try first row as header
        first_row = rows[0] if rows else None
        if first_row:
            headers = [td.get_text(strip=True) for td in first_row.find_all("td")]

    # Parse data rows
    for row in rows[1:] if headers else rows:
        cells = row.find_all("td")
        if cells and len(cells) >= 3:
            record = {}
            for i, cell in enumerate(cells):
                if i < len(headers):
                    record[headers[i]] = cell.get_text(strip=True)
                else:
                    record[f"col_{i}"] = cell.get_text(strip=True)

            # Also try to extract link for detail page
            link = row.find("a", href=True)
            if link:
                record["_detail_link"] = link.get("href", "")

            if record:
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

    records = []

    # Accela modules to search
    modules = ["Building", "Fire"]

    for module in modules:
        try:
            # Step 1: GET the search page to get ViewState tokens
            search_url = f"{FORTWORTH_ACCELA_URL}/Cap/CapHome.aspx?module={module}"
            response = session.get(search_url, timeout=30)

            if response.status_code != 200:
                continue

            soup = BeautifulSoup(response.text, "lxml")
            tokens = _extract_viewstate(soup)

            # Step 2: Navigate to permit search
            # Try global search with date range
            global_search_url = f"{FORTWORTH_ACCELA_URL}/Cap/GlobalSearchResults.aspx"

            # Search for each keyword
            for keyword in ["restaurant", "commercial alteration", "hood", "tenant finish"]:
                search_params = {
                    **tokens,
                    "ctl00$PlaceHolderMain$txtGSPermitNumber": "",
                    "ctl00$PlaceHolderMain$txtGSStartDate": since_formatted,
                    "ctl00$PlaceHolderMain$txtGSEndDate": end_formatted,
                    "ctl00$PlaceHolderMain$txtGSAddress": "",
                    "ctl00$PlaceHolderMain$txtGSProjectName": keyword,
                    "ctl00$PlaceHolderMain$btnSearch": "Search"
                }

                response = session.post(global_search_url, data=search_params, timeout=30)

                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, "lxml")
                    raw_records = _parse_results_table(soup)

                    for record in raw_records:
                        # Build combined text from all fields
                        combined_text = " ".join(str(v) for v in record.values())

                        if _matches_keywords(combined_text):
                            record["_module"] = module
                            record["_search_keyword"] = keyword
                            records.append(record)

        except requests.exceptions.RequestException as e:
            print(f"[Fort Worth Permits] Request error for {module}: {e}")
            continue
        except Exception as e:
            print(f"[Fort Worth Permits] Error for {module}: {e}")
            continue

    # Deduplicate by permit number if available
    seen = set()
    unique_records = []
    for record in records:
        permit_id = (
            record.get("Record Number") or
            record.get("Permit Number") or
            record.get("Record #") or
            record.get("col_0") or
            ""
        )
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
        permit_number = (
            row.get("Record Number") or
            row.get("Permit Number") or
            row.get("Record #") or
            row.get("Permit #") or
            row.get("col_0") or
            ""
        )

        # Extract date
        date_str = (
            row.get("Date") or
            row.get("Issue Date") or
            row.get("Issued Date") or
            row.get("Open Date") or
            row.get("col_1") or
            ""
        )

        event_date = None
        if date_str:
            for fmt in ["%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"]:
                try:
                    event_date = datetime.strptime(date_str.strip(), fmt).strftime("%Y-%m-%d")
                    break
                except ValueError:
                    continue

        # Extract description/project name
        raw_name = (
            row.get("Project Name") or
            row.get("Description") or
            row.get("Record Type") or
            row.get("Type") or
            row.get("col_2") or
            ""
        )

        # Extract address
        raw_address = (
            row.get("Address") or
            row.get("Location") or
            row.get("Site Address") or
            row.get("col_3") or
            ""
        )

        # Determine event type based on module
        module = row.get("_module", "Building")
        event_type = "fire_permit" if module == "Fire" else "permit_issued"

        event = {
            "source_system": "FORTWORTH_PERMIT",
            "source_record_id": permit_number,
            "event_type": event_type,
            "event_date": event_date,
            "raw_name": raw_name,
            "raw_address": raw_address,
            "city": "Fort Worth",
            "url": "https://aca-prod.accela.com/CFW/",
            "payload_json": json.dumps(row, default=str)
        }

        events.append(event)

    return events
