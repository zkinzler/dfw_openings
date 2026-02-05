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
                          "ARLINGTON_PERMIT", "DENTON_PERMIT", "MCKINNEY_PERMIT",
                          "SOUTHLAKE_PERMIT", "FORTWORTH_PERMIT"):
        return "permitting"

    # COs indicate opening soon (space is approved)
    if source_system in ("DALLAS_CO", "FORTWORTH_CO"):
        return "opening_soon"

    return "unknown"


def calculate_priority_score(venue_type: str, status: str,
                             first_seen_date: str = None,
                             has_phone: bool = False,
                             has_website: bool = False,
                             source_count: int = 1) -> int:
    """
    Calculate urgency-based priority score for POS sales.

    For POS sales, TIMING is everything. Freshest leads with contact info
    bubble to the top. Score based on:
    - Recency (most important - fresh leads are hot)
    - Stage (permitting is early access, opening_soon is urgent)
    - Venue type (bars often higher volume)
    - Contact info availability (can we actually reach them?)
    - Multi-source validation (more legit)

    Args:
        venue_type: 'bar' or 'restaurant'
        status: 'permitting', 'opening_soon', 'open', 'unknown'
        first_seen_date: When the lead was first discovered (YYYY-MM-DD)
        has_phone: Whether venue has phone number
        has_website: Whether venue has website
        source_count: Number of data sources that found this venue

    Returns:
        Integer score (higher = more urgent/valuable)
    """
    from datetime import datetime, date

    score = 0

    # RECENCY IS KING - freshest leads are most valuable
    if first_seen_date:
        try:
            seen_date = datetime.strptime(first_seen_date, "%Y-%m-%d").date()
            days_old = (date.today() - seen_date).days

            if days_old <= 3:
                score += 50   # Brand new - HOT
            elif days_old <= 7:
                score += 40
            elif days_old <= 14:
                score += 25
            elif days_old <= 30:
                score += 10
            # Older leads get no recency bonus
        except (ValueError, TypeError):
            pass  # Invalid date, no bonus

    # Stage bonus (permitting is actually good - early access!)
    if status == 'permitting':
        score += 30   # Early access before competitors
    elif status == 'opening_soon':
        score += 40   # Urgent - they're deciding NOW
    elif status == 'open':
        score += 10   # May already have POS, but worth trying

    # Venue type
    if venue_type == 'bar':
        score += 20   # Bars often higher volume
    elif venue_type == 'restaurant':
        score += 15

    # Has contact info (can actually reach them)
    if has_phone:
        score += 25   # Critical for outreach
    if has_website:
        score += 5

    # Multi-source validation (more legit)
    if source_count >= 2:
        score += 15

    return score


def calculate_priority_score_simple(venue_type: str, status: str) -> int:
    """
    Simple priority scoring for backward compatibility during ETL.
    Uses default values for optional parameters.
    """
    return calculate_priority_score(venue_type, status)


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
        # Use simple scoring during initial ETL; full scoring happens in recalculation
        priority_score = calculate_priority_score(venue_type, status,
                                                  first_seen_date=event["event_date"])
        
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


def recalculate_all_priority_scores(conn: sqlite3.Connection) -> int:
    """
    Recalculate priority scores for all venues using full scoring algorithm.
    Should be run after enrichment (phone/website) or periodically.

    Returns:
        Number of venues updated
    """
    cursor = conn.cursor()

    # Get all venues with their source counts
    cursor.execute("""
        SELECT
            v.id, v.venue_type, v.status, v.first_seen_date,
            v.phone, v.website,
            COUNT(DISTINCT se.source_system) as source_count
        FROM venues v
        LEFT JOIN source_events se ON v.id = se.venue_id
        GROUP BY v.id
    """)

    venues = cursor.fetchall()
    updated = 0

    for venue in venues:
        new_score = calculate_priority_score(
            venue_type=venue["venue_type"],
            status=venue["status"],
            first_seen_date=venue["first_seen_date"],
            has_phone=bool(venue["phone"]),
            has_website=bool(venue["website"]),
            source_count=venue["source_count"] or 1
        )

        cursor.execute("""
            UPDATE venues SET priority_score = ? WHERE id = ?
        """, (new_score, venue["id"]))
        updated += 1

    conn.commit()
    print(f"[Merge] Recalculated priority scores for {updated} venues")
    return updated
