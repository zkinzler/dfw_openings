"""
Venue matching and merging logic.
Processes unmatched source events and creates/updates venue records.
"""

import json
import sqlite3
from utils.normalize import normalize_name, normalize_address
from config import BAR_KEYWORDS, RESTAURANT_KEYWORDS
import db


def infer_venue_type_from_event(event_row: sqlite3.Row) -> str:
    """
    Infer venue type (bar or restaurant) from a source event.

    Args:
        event_row: A source_events row (sqlite3.Row)

    Returns:
        'bar', 'restaurant', or None
    """
    source_system = event_row["source_system"]
    raw_name = (event_row["raw_name"] or "").lower()
    payload_json = event_row["payload_json"]

    # Parse payload to check for additional signals
    try:
        payload = json.loads(payload_json) if payload_json else {}
    except json.JSONDecodeError:
        payload = {}

    # Check name for keywords
    for keyword in BAR_KEYWORDS:
        if keyword in raw_name:
            return "bar"

    # For TABC, check license_type in payload
    if source_system == "TABC":
        license_type = (payload.get("license_type") or "").lower()
        for keyword in BAR_KEYWORDS:
            if keyword in license_type:
                return "bar"
        # Default TABC to restaurant if not clearly a bar
        return "restaurant"

    # For CO sources, check occupancy/use field
    if source_system in ("DALLAS_CO", "FORTWORTH_CO"):
        # Dallas uses 'occupancy' field
        occupancy = (payload.get("occupancy") or "").lower()
        use_desc = (payload.get("USE_DESC") or "").lower()
        combined = occupancy + " " + use_desc

        for keyword in BAR_KEYWORDS:
            if keyword in combined:
                return "bar"

        for keyword in RESTAURANT_KEYWORDS:
            if keyword in combined:
                return "restaurant"
                
    # For Sales Tax, check NAICS code
    if source_system == "SALES_TAX":
        naics = payload.get("naics_code", "")
        if naics == "722410": # Drinking Places
            return "bar"
        elif naics.startswith("7225"): # Restaurants
            return "restaurant"

    # Check name for restaurant keywords as fallback
    for keyword in RESTAURANT_KEYWORDS:
        if keyword in raw_name:
            return "restaurant"

    return None


def infer_status_from_event(event_row: sqlite3.Row) -> str:
    """
    Infer venue status from a source event.

    Args:
        event_row: A source_events row (sqlite3.Row)

    Returns:
        'permitting', 'opening_soon', 'open', or 'unknown'
    """
    source_system = event_row["source_system"]

    # TABC licenses indicate permitting stage
    if source_system == "TABC":
        return "permitting"

    # Sales Tax permits are also permitting stage
    if source_system == "SALES_TAX":
        return "permitting"

    # Building permits indicate early permitting stage
    if source_system in ("LEWISVILLE_PERMIT", "MESQUITE_PERMIT", "CARROLLTON_PERMIT",
                          "PLANO_PERMIT", "FRISCO_PERMIT", "DALLAS_PERMIT",
                          "ARLINGTON_PERMIT", "DENTON_PERMIT"):
        return "permitting"

    # COs indicate opening soon (space is approved)
    if source_system in ("DALLAS_CO", "FORTWORTH_CO"):
        return "opening_soon"

    return "unknown"


def calculate_priority_score(venue_type: str, status: str) -> int:
    """
    Calculate priority score for a venue.
    Bars > Restaurants, and later stages > earlier stages.

    Args:
        venue_type: 'bar' or 'restaurant'
        status: 'permitting', 'opening_soon', 'open', 'unknown'

    Returns:
        Integer score (higher = more interesting)
    """
    base_score = 0

    # Venue type scoring
    if venue_type == "bar":
        base_score = 100
    elif venue_type == "restaurant":
        base_score = 80
    else:
        base_score = 50

    # Status bonus
    status_bonus = {
        "open": 30,
        "opening_soon": 20,
        "permitting": 10,
        "unknown": 0
    }

    return base_score + status_bonus.get(status, 0)


def update_venues_from_unmatched_events(conn: sqlite3.Connection) -> None:
    """
    Process all unmatched source_events:
    - Normalize name and address
    - Find or create matching venue
    - Update venue metadata
    - Link event to venue
    """
    unmatched_events = db.get_unmatched_source_events(conn)

    print(f"[Merge] Processing {len(unmatched_events)} unmatched events")

    for event in unmatched_events:
        # Normalize name and address
        norm_name = normalize_name(event["raw_name"])
        norm_address = normalize_address(event["raw_address"])

        if not norm_name or not norm_address:
            # Skip events without sufficient data
            continue

        # Infer venue properties from this event
        venue_type = infer_venue_type_from_event(event)
        status = infer_status_from_event(event)
        priority_score = calculate_priority_score(venue_type, status)
        
        # Extract additional fields if available
        payload_json = event["payload_json"]
        try:
            payload = json.loads(payload_json) if payload_json else {}
        except json.JSONDecodeError:
            payload = {}
            
        naics_code = payload.get("naics_code")

        # Prepare venue data
        venue_data = {
            "name": event["raw_name"],
            "normalized_name": norm_name,
            "address": event["raw_address"],
            "normalized_address": norm_address,
            "city": event["city"],
            "state": "TX",
            "zip": None,  # Could extract from address if needed
            "venue_type": venue_type,
            "status": status,
            "first_seen_date": event["event_date"],
            "last_seen_date": event["event_date"],
            "priority_score": priority_score,
            "naics_code": naics_code
        }

        # Upsert venue (will match existing or create new)
        venue_id = db.upsert_venue(conn, venue_data)

        # Link event to venue
        db.update_source_event_venue(conn, event["id"], venue_id)

    print(f"[Merge] Completed processing {len(unmatched_events)} events")
