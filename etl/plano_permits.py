"""
Plano Building Permit data extractor.
Uses eTRAKiT ASP.NET form scraping with BeautifulSoup.
"""

import json
import requests
from datetime import datetime
from bs4 import BeautifulSoup
from config import PLANO_ETRAKIT_URL

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


def _extract_viewstate(soup: BeautifulSoup) -> dict:
    """Extract ASP.NET ViewState and related tokens from page."""
    tokens = {}

    viewstate = soup.find("input", {"name": "__VIEWSTATE"})
    if viewstate:
        tokens["__VIEWSTATE"] = viewstate.get("value", "")

    viewstate_gen = soup.find("input", {"name": "__VIEWSTATEGENERATOR"})
    if viewstate_gen:
        tokens["__VIEWSTATEGENERATOR"] = viewstate_gen.get("value", "")

    event_validation = soup.find("input", {"name": "__EVENTVALIDATION"})
    if event_validation:
        tokens["__EVENTVALIDATION"] = event_validation.get("value", "")

    return tokens


def _parse_results_table(soup: BeautifulSoup) -> list[dict]:
    """Parse permit results from HTML table."""
    records = []

    # Find results table - common eTRAKiT patterns
    table = (
        soup.find("table", {"id": "ctl00_cphContent_gvResults"}) or
        soup.find("table", {"class": "rgMasterTable"}) or
        soup.find("table", {"id": "resultsGrid"}) or
        soup.find("table", {"class": "results-table"})
    )

    if not table:
        # Try finding any data table
        tables = soup.find_all("table")
        for t in tables:
            rows = t.find_all("tr")
            if len(rows) > 1:  # Has header + data rows
                table = t
                break

    if not table:
        return records

    # Get headers
    headers = []
    header_row = table.find("tr")
    if header_row:
        for th in header_row.find_all(["th", "td"]):
            headers.append(th.get_text(strip=True))

    # Parse data rows
    rows = table.find_all("tr")[1:]  # Skip header
    for row in rows:
        cells = row.find_all("td")
        if len(cells) >= len(headers) and headers:
            record = {}
            for i, header in enumerate(headers):
                if i < len(cells):
                    record[header] = cells[i].get_text(strip=True)
            if record:
                records.append(record)

    return records


def fetch_plano_permits_since(since_date: str) -> list[dict]:
    """
    Fetch Plano building permit records via eTRAKiT scraping.

    Args:
        since_date: ISO date string 'YYYY-MM-DD'

    Returns:
        List of permit records filtered for restaurant/bar keywords
    """
    if not PLANO_ETRAKIT_URL:
        print("[Plano] eTRAKiT URL not configured.")
        return []

    print(f"[Plano] Fetching permits since {since_date}...")

    try:
        since_dt = datetime.strptime(since_date, "%Y-%m-%d")
        # Format for eTRAKiT form (typically MM/DD/YYYY)
        since_formatted = since_dt.strftime("%m/%d/%Y")
        end_formatted = datetime.now().strftime("%m/%d/%Y")
    except ValueError:
        print(f"[Plano] Invalid date format: {since_date}")
        return []

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    })

    records = []

    try:
        # Step 1: GET the search page to get ViewState tokens
        search_url = f"{PLANO_ETRAKIT_URL}/Search/permit.aspx"
        response = session.get(search_url, timeout=30)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "lxml")
        tokens = _extract_viewstate(soup)

        if not tokens:
            print("[Plano] Could not extract ViewState tokens")
            return []

        # Step 2: POST search with date range
        # Common eTRAKiT field names
        form_data = {
            **tokens,
            "__EVENTTARGET": "",
            "__EVENTARGUMENT": "",
            "ctl00$cphContent$txtDateFrom": since_formatted,
            "ctl00$cphContent$txtDateTo": end_formatted,
            "ctl00$cphContent$btnSearch": "Search"
        }

        # Try different permit type selectors
        permit_type_fields = [
            "ctl00$cphContent$ddlPermitType",
            "ctl00$cphContent$cboPermitType",
            "ctl00$cphContent$lstPermitType"
        ]

        for field in permit_type_fields:
            # Try "Commercial" or "Building" permit type
            form_data[field] = "Commercial"

        response = session.post(search_url, data=form_data, timeout=30)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "lxml")

        # Step 3: Parse results
        raw_records = _parse_results_table(soup)

        # Filter for restaurant/bar keywords
        for record in raw_records:
            description = (
                record.get("Description") or
                record.get("Work Description") or
                record.get("Project") or
                record.get("Type") or
                ""
            )

            permit_type = (
                record.get("Permit Type") or
                record.get("Type") or
                ""
            )

            combined_text = f"{description} {permit_type}"

            if _matches_keywords(combined_text):
                records.append(record)

        print(f"[Plano] Found {len(records)} restaurant/bar-related permits")

    except requests.exceptions.RequestException as e:
        print(f"[Plano] Request error: {e}")
    except Exception as e:
        print(f"[Plano] Error: {e}")

    return records


def to_source_events(rows: list[dict]) -> list[dict]:
    """
    Map raw Plano permit rows to normalized source_events dicts.
    """
    events = []

    for row in rows:
        # Extract permit number
        permit_number = (
            row.get("Permit Number") or
            row.get("Permit #") or
            row.get("Permit No") or
            row.get("Number") or
            row.get("Case Number") or
            ""
        )

        # Extract date
        date_str = (
            row.get("Issue Date") or
            row.get("Issued Date") or
            row.get("Date") or
            row.get("Applied Date") or
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

        # Extract business/project name
        raw_name = (
            row.get("Applicant") or
            row.get("Owner") or
            row.get("Contractor") or
            row.get("Business Name") or
            row.get("Project Name") or
            row.get("Description") or
            ""
        )

        # Extract address
        raw_address = (
            row.get("Address") or
            row.get("Site Address") or
            row.get("Location") or
            row.get("Property Address") or
            ""
        )

        event = {
            "source_system": "PLANO_PERMIT",
            "source_record_id": permit_number,
            "event_type": "permit_issued",
            "event_date": event_date,
            "raw_name": raw_name,
            "raw_address": raw_address,
            "city": "Plano",
            "url": "https://trakit.plano.gov/etrakit_prod/Search/permit.aspx",
            "payload_json": json.dumps(row)
        }

        events.append(event)

    return events
