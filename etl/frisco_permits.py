"""
Frisco Building Permit data extractor.
Uses eTRAKiT ASP.NET form scraping with BeautifulSoup.
Requires Public login, then searches by address keywords.
"""

import json
import re
import requests
from datetime import datetime
from bs4 import BeautifulSoup
from config import FRISCO_ETRAKIT_URL

# Keywords to filter for restaurant/bar-related permits
PERMIT_KEYWORDS = [
    "restaurant", "bar", "lounge", "tavern", "pub",
    "kitchen", "hood", "grease trap", "walk-in",
    "tenant finish", "build-out", "commercial alteration",
    "food service", "cooking", "brewery", "brewpub",
    "cafe", "grill", "bistro", "eatery"
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


def _parse_etrakit_results(soup: BeautifulSoup) -> list[dict]:
    """Parse permit results from eTRAKiT search results page."""
    records = []

    # Find the RadGrid div and its master table
    radgrid = soup.find("div", {"class": re.compile("RadGrid")})
    if not radgrid:
        return records

    master_table = radgrid.find("table", {"class": re.compile("rgMasterTable")})
    if not master_table:
        return records

    rows = master_table.find_all("tr")
    if not rows:
        return records

    # First row is headers
    headers = [th.get_text(strip=True) for th in rows[0].find_all(["th", "td"])]
    if "PERMIT NO" not in headers:
        return records

    # Parse data rows (skip header and pagination rows)
    for row in rows[1:]:
        cells = row.find_all("td")
        if len(cells) < 3:
            continue

        cell_texts = [c.get_text(strip=True) for c in cells]

        # Skip pagination/control rows
        if "Buttons to move" in " ".join(cell_texts) or "page " in " ".join(cell_texts).lower():
            continue
        if all(not t for t in cell_texts):
            continue

        # Build record from cells matching headers
        record = {}
        for i, header in enumerate(headers):
            if i < len(cells):
                record[header] = cells[i].get_text(strip=True)

        # Only add if we have a valid permit number (format: YY-NNNNN)
        permit_no = record.get("PERMIT NO", "")
        if permit_no and re.match(r"\d{2}-\d+", permit_no):
            records.append(record)

    return records


def fetch_frisco_permits_since(since_date: str) -> list[dict]:
    """
    Fetch Frisco building permit records via eTRAKiT scraping.

    Args:
        since_date: ISO date string 'YYYY-MM-DD'

    Returns:
        List of permit records filtered for restaurant/bar keywords
    """
    if not FRISCO_ETRAKIT_URL:
        print("[Frisco] eTRAKiT URL not configured.")
        return []

    print(f"[Frisco] Fetching permits since {since_date}...")

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    })

    all_records = []
    search_url = f"{FRISCO_ETRAKIT_URL}/etrakit/Search/permit.aspx"

    try:
        # Step 1: GET the search page
        response = session.get(search_url, timeout=30)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "lxml")
        tokens = _extract_viewstate(soup)

        if not tokens:
            print("[Frisco] Could not extract ViewState tokens")
            return []

        # Step 2: Login as Public
        login_data = {
            **tokens,
            "__EVENTTARGET": "",
            "__EVENTARGUMENT": "",
            "ctl00$ucLogin$ddlSelLogin": "Public",
            "ctl00$ucLogin$txtLoginId": "",
            "ctl00$ucLogin$RadTextBox2": "",
            "ctl00$ucLogin$btnLogin": "Login"
        }

        response = session.post(search_url, data=login_data, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "lxml")
        tokens = _extract_viewstate(soup)

        # Step 3: Search using broad queries and filter by keywords
        # Note: Frisco eTRAKiT has limited public search - only "1" in address returns data
        search_terms = ["1"]  # Broad search that works

        for term in search_terms:
            try:
                search_data = {
                    **tokens,
                    "__EVENTTARGET": "ctl00$cplMain$btnSearch",
                    "__EVENTARGUMENT": "",
                    "ctl00$cplMain$ddSearchBy": "SITE ADDRESS",
                    "ctl00$cplMain$ddSearchOper": "Contains",
                    "ctl00$cplMain$txtSearchString": term,
                }

                response = session.post(search_url, data=search_data, timeout=30)
                soup = BeautifulSoup(response.text, "lxml")
                tokens = _extract_viewstate(soup)

                records = _parse_etrakit_results(soup)

                # Filter for restaurant/bar keywords
                for record in records:
                    contractor = record.get("CONTRACTOR NAME", "").lower()
                    address = record.get("SITE ADDRESS", "").lower()
                    combined = f"{contractor} {address}"
                    if _matches_keywords(combined):
                        record["_search_term"] = term
                        all_records.append(record)

            except Exception:
                continue

    except requests.exceptions.RequestException as e:
        print(f"[Frisco] Request error: {e}")
        return []
    except Exception as e:
        print(f"[Frisco] Error: {e}")
        return []

    # Deduplicate by permit number
    seen = set()
    unique_records = []
    for record in all_records:
        permit_id = record.get("PERMIT NO") or record.get("Permit No", "")
        if permit_id and permit_id not in seen:
            seen.add(permit_id)
            unique_records.append(record)

    print(f"[Frisco] Found {len(unique_records)} restaurant/bar-related permits")
    return unique_records


def to_source_events(rows: list[dict]) -> list[dict]:
    """
    Map raw Frisco permit rows to normalized source_events dicts.
    """
    events = []

    for row in rows:
        # Extract permit number (eTRAKiT uses uppercase field names)
        permit_number = (
            row.get("PERMIT NO") or
            row.get("Permit No") or
            row.get("Permit Number") or
            row.get("Permit #") or
            ""
        )

        # Extract date (if available in results)
        date_str = (
            row.get("ISSUE DATE") or
            row.get("Issue Date") or
            row.get("DATE") or
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

        # Extract contractor/business name
        raw_name = (
            row.get("CONTRACTOR NAME") or
            row.get("Contractor Name") or
            row.get("APPLICANT") or
            row.get("Applicant") or
            ""
        )

        # Extract address
        raw_address = (
            row.get("SITE ADDRESS") or
            row.get("Site Address") or
            row.get("ADDRESS") or
            row.get("Address") or
            ""
        )

        event = {
            "source_system": "FRISCO_PERMIT",
            "source_record_id": permit_number,
            "event_type": "permit_issued",
            "event_date": event_date,
            "raw_name": raw_name,
            "raw_address": raw_address,
            "city": "Frisco",
            "url": "https://etrakit.friscotexas.gov/etrakit/Search/permit.aspx",
            "payload_json": json.dumps(row)
        }

        events.append(event)

    return events
