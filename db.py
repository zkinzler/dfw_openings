"""
Database module for DFW Openings pipeline.
Handles SQLite connection, schema creation, and data operations.
"""

import sqlite3
import json
from typing import Optional, Any
from config import DB_PATH


def get_connection() -> sqlite3.Connection:
    """
    Open a SQLite connection with row factory configured.
    Returns a connection object.
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_schema(conn: sqlite3.Connection) -> None:
    """
    Create tables if they don't exist.
    """
    cursor = conn.cursor()

    # venues table with normalized_address column for matching
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS venues (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            normalized_name TEXT,
            address TEXT NOT NULL,
            normalized_address TEXT,
            city TEXT,
            state TEXT DEFAULT 'TX',
            zip TEXT,
            latitude REAL,
            longitude REAL,
            phone TEXT,
            website TEXT,
            google_place_id TEXT,
            naics_code TEXT,
            venue_type TEXT,
            status TEXT,
            first_seen_date TEXT,
            last_seen_date TEXT,
            priority_score INTEGER,
            notes TEXT,
            lead_status TEXT DEFAULT 'new',
            next_follow_up DATE,
            competitor TEXT,
            lost_reason TEXT
        )
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_venues_name_addr
            ON venues (normalized_name, normalized_address, city)
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_venues_city_type
            ON venues (city, venue_type)
    """)


    # source_events table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS source_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            venue_id INTEGER,
            source_system TEXT NOT NULL,
            source_record_id TEXT,
            event_type TEXT,
            event_date TEXT,
            raw_name TEXT,
            raw_address TEXT,
            city TEXT,
            url TEXT,
            payload_json TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (venue_id) REFERENCES venues(id)
        )
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_source_events_date
            ON source_events (event_date)
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_source_events_source
            ON source_events (source_system, event_type)
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_source_events_venue
            ON source_events (venue_id)
    """)

    # lead_activities table for tracking outreach
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS lead_activities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            venue_id INTEGER NOT NULL,
            activity_type TEXT NOT NULL,
            activity_date TEXT DEFAULT (date('now')),
            notes TEXT,
            outcome TEXT,
            next_action_date DATE,
            created_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (venue_id) REFERENCES venues(id)
        )
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_lead_activities_venue
            ON lead_activities (venue_id)
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_lead_activities_date
            ON lead_activities (activity_date)
    """)

    # etl_runs table for logging
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS etl_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_started_at TEXT,
            run_finished_at TEXT,
            lookback_days INTEGER,
            rows_tabc INTEGER,
            rows_dallas_co INTEGER,
            rows_fortworth_co INTEGER,
            rows_sales_tax INTEGER,
            notes TEXT
        )
    """)

    conn.commit()

    # Migration: Add latitude/longitude if they don't exist (for existing DBs)
    try:
        cursor.execute("SELECT latitude FROM venues LIMIT 1")
    except sqlite3.OperationalError:
        print("Migrating database: Adding latitude/longitude columns...")
        cursor.execute("ALTER TABLE venues ADD COLUMN latitude REAL")
        cursor.execute("ALTER TABLE venues ADD COLUMN longitude REAL")
        conn.commit()

    try:
        cursor.execute("SELECT phone FROM venues LIMIT 1")
    except sqlite3.OperationalError:
        print("Migrating database: Adding phone/website/google_place_id/naics_code columns...")
        cursor.execute("ALTER TABLE venues ADD COLUMN phone TEXT")
        cursor.execute("ALTER TABLE venues ADD COLUMN website TEXT")
        cursor.execute("ALTER TABLE venues ADD COLUMN google_place_id TEXT")
        cursor.execute("ALTER TABLE venues ADD COLUMN naics_code TEXT")
        conn.commit()
        
    try:
        cursor.execute("SELECT rows_sales_tax FROM etl_runs LIMIT 1")
    except sqlite3.OperationalError:
        print("Migrating database: Adding rows_sales_tax to etl_runs...")
        cursor.execute("ALTER TABLE etl_runs ADD COLUMN rows_sales_tax INTEGER")
        conn.commit()

    # Migration: Add building permit source columns to etl_runs
    try:
        cursor.execute("SELECT rows_lewisville FROM etl_runs LIMIT 1")
    except sqlite3.OperationalError:
        print("Migrating database: Adding building permit columns to etl_runs...")
        cursor.execute("ALTER TABLE etl_runs ADD COLUMN rows_lewisville INTEGER")
        cursor.execute("ALTER TABLE etl_runs ADD COLUMN rows_mesquite INTEGER")
        cursor.execute("ALTER TABLE etl_runs ADD COLUMN rows_carrollton INTEGER")
        cursor.execute("ALTER TABLE etl_runs ADD COLUMN rows_plano INTEGER")
        cursor.execute("ALTER TABLE etl_runs ADD COLUMN rows_frisco INTEGER")
        conn.commit()

    # Migration: Add new building permit source columns (Dallas, Arlington, Denton)
    try:
        cursor.execute("SELECT rows_dallas_permits FROM etl_runs LIMIT 1")
    except sqlite3.OperationalError:
        print("Migrating database: Adding Dallas/Arlington/Denton columns to etl_runs...")
        cursor.execute("ALTER TABLE etl_runs ADD COLUMN rows_dallas_permits INTEGER")
        cursor.execute("ALTER TABLE etl_runs ADD COLUMN rows_arlington INTEGER")
        cursor.execute("ALTER TABLE etl_runs ADD COLUMN rows_denton INTEGER")
        conn.commit()

    # Migration: Add McKinney, Southlake, Fort Worth permit columns
    try:
        cursor.execute("SELECT rows_mckinney FROM etl_runs LIMIT 1")
    except sqlite3.OperationalError:
        print("Migrating database: Adding McKinney/Southlake/FortWorth permit columns...")
        cursor.execute("ALTER TABLE etl_runs ADD COLUMN rows_mckinney INTEGER")
        cursor.execute("ALTER TABLE etl_runs ADD COLUMN rows_southlake INTEGER")
        cursor.execute("ALTER TABLE etl_runs ADD COLUMN rows_fortworth_permits INTEGER")
        conn.commit()

    # Migration: Add lead tracking columns
    try:
        cursor.execute("SELECT lead_status FROM venues LIMIT 1")
    except sqlite3.OperationalError:
        print("Migrating database: Adding lead tracking columns...")
        cursor.execute("ALTER TABLE venues ADD COLUMN lead_status TEXT DEFAULT 'new'")
        cursor.execute("ALTER TABLE venues ADD COLUMN next_follow_up DATE")
        cursor.execute("ALTER TABLE venues ADD COLUMN competitor TEXT")
        cursor.execute("ALTER TABLE venues ADD COLUMN lost_reason TEXT")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_venues_lead_status ON venues (lead_status)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_venues_follow_up ON venues (next_follow_up)")
        conn.commit()


def insert_source_events(conn: sqlite3.Connection, events: list[dict]) -> None:
    """
    Bulk insert source events.
    Each event dict should have keys: source_system, source_record_id,
    event_type, event_date, raw_name, raw_address, city, url, payload_json
    """
    if not events:
        return

    cursor = conn.cursor()
    cursor.executemany("""
        INSERT INTO source_events
        (source_system, source_record_id, event_type, event_date,
         raw_name, raw_address, city, url, payload_json)
        VALUES (:source_system, :source_record_id, :event_type, :event_date,
                :raw_name, :raw_address, :city, :url, :payload_json)
    """, events)
    conn.commit()


def update_source_event_venue(conn: sqlite3.Connection, event_id: int, venue_id: int) -> None:
    """
    Update a source_event to link it to a venue.
    """
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE source_events
        SET venue_id = ?
        WHERE id = ?
    """, (venue_id, event_id))
    conn.commit()


def upsert_venue(conn: sqlite3.Connection, venue_data: dict) -> int:
    """
    Insert or update a venue record.
    Returns the venue ID.

    venue_data should contain:
    - name, normalized_name, address, normalized_address
    - city, state, zip (optional)
    - venue_type, status
    - first_seen_date, last_seen_date
    - priority_score
    - phone, website, google_place_id, naics_code (optional)
    """
    cursor = conn.cursor()

    # Try to find existing venue by normalized name + address + city
    cursor.execute("""
        SELECT id, first_seen_date, last_seen_date, venue_type, status, phone, website, google_place_id, naics_code, priority_score
        FROM venues
        WHERE normalized_name = ?
          AND normalized_address = ?
          AND city = ?
    """, (
        venue_data.get("normalized_name"),
        venue_data.get("normalized_address"),
        venue_data.get("city")
    ))

    existing = cursor.fetchone()

    if existing:
        venue_id = existing["id"]

        # Update last_seen_date to the max of existing and new
        new_last_seen = venue_data.get("last_seen_date", "")
        current_last_seen = existing["last_seen_date"] or ""
        updated_last_seen = max(new_last_seen, current_last_seen)

        # Determine if we should update venue_type and status
        # Priority: CO events (opening_soon) > TABC (permitting)
        new_status = venue_data.get("status", "unknown")
        current_status = existing["status"] or "unknown"

        # Status priority: open > opening_soon > permitting > unknown
        status_priority = {"open": 3, "opening_soon": 2, "permitting": 1, "unknown": 0}
        if status_priority.get(new_status, 0) > status_priority.get(current_status, 0):
            updated_status = new_status
        else:
            updated_status = current_status

        # Update venue_type if we have a more specific one
        updated_venue_type = venue_data.get("venue_type") or existing["venue_type"]
        
        # Update enrichment fields if they are provided and currently empty
        updated_phone = venue_data.get("phone") or existing["phone"]
        updated_website = venue_data.get("website") or existing["website"]
        updated_google_place_id = venue_data.get("google_place_id") or existing["google_place_id"]
        updated_naics_code = venue_data.get("naics_code") or existing["naics_code"]

        cursor.execute("""
            UPDATE venues
            SET last_seen_date = ?,
                venue_type = ?,
                status = ?,
                priority_score = ?,
                phone = ?,
                website = ?,
                google_place_id = ?,
                naics_code = ?
            WHERE id = ?
        """, (
            updated_last_seen,
            updated_venue_type,
            updated_status,
            venue_data.get("priority_score", existing["priority_score"] if existing["priority_score"] else 50),
            updated_phone,
            updated_website,
            updated_google_place_id,
            updated_naics_code,
            venue_id
        ))

        conn.commit()
        return venue_id
    else:
        # Insert new venue
        cursor.execute("""
            INSERT INTO venues
            (name, normalized_name, address, normalized_address, city, state, zip,
             venue_type, status, first_seen_date, last_seen_date, priority_score,
             phone, website, google_place_id, naics_code)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            venue_data.get("name"),
            venue_data.get("normalized_name"),
            venue_data.get("address"),
            venue_data.get("normalized_address"),
            venue_data.get("city"),
            venue_data.get("state", "TX"),
            venue_data.get("zip"),
            venue_data.get("venue_type"),
            venue_data.get("status", "unknown"),
            venue_data.get("first_seen_date"),
            venue_data.get("last_seen_date"),
            venue_data.get("priority_score", 50),
            venue_data.get("phone"),
            venue_data.get("website"),
            venue_data.get("google_place_id"),
            venue_data.get("naics_code")
        ))

        conn.commit()
        return cursor.lastrowid


def get_unmatched_source_events(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    """
    Get all source events that haven't been linked to a venue yet.
    """
    cursor = conn.cursor()
    cursor.execute("""
        SELECT * FROM source_events
        WHERE venue_id IS NULL
        ORDER BY event_date DESC
    """)
    return cursor.fetchall()


def insert_etl_run(conn: sqlite3.Connection, run_data: dict) -> int:
    """
    Insert an ETL run record.
    Returns the run ID.
    """
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO etl_runs
        (run_started_at, run_finished_at, lookback_days,
         rows_tabc, rows_dallas_co, rows_fortworth_co, rows_sales_tax,
         rows_lewisville, rows_mesquite, rows_carrollton, rows_plano, rows_frisco,
         rows_dallas_permits, rows_arlington, rows_denton,
         rows_mckinney, rows_southlake, rows_fortworth_permits, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        run_data.get("run_started_at"),
        run_data.get("run_finished_at"),
        run_data.get("lookback_days"),
        run_data.get("rows_tabc", 0),
        run_data.get("rows_dallas_co", 0),
        run_data.get("rows_fortworth_co", 0),
        run_data.get("rows_sales_tax", 0),
        run_data.get("rows_lewisville", 0),
        run_data.get("rows_mesquite", 0),
        run_data.get("rows_carrollton", 0),
        run_data.get("rows_plano", 0),
        run_data.get("rows_frisco", 0),
        run_data.get("rows_dallas_permits", 0),
        run_data.get("rows_arlington", 0),
        run_data.get("rows_denton", 0),
        run_data.get("rows_mckinney", 0),
        run_data.get("rows_southlake", 0),
        run_data.get("rows_fortworth_permits", 0),
        run_data.get("notes", "")
    ))
    conn.commit()
    return cursor.lastrowid


def get_venues_by_city(conn: sqlite3.Connection, city: str) -> list[sqlite3.Row]:
    """
    Get all venues in a specific city.
    Used for fuzzy matching candidates.
    """
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, normalized_name, normalized_address
        FROM venues
        WHERE city = ?
    """, (city,))
    return cursor.fetchall()


def update_lead_status(conn: sqlite3.Connection, venue_id: int, lead_status: str,
                       next_follow_up: str = None, competitor: str = None,
                       lost_reason: str = None) -> None:
    """
    Update the lead status for a venue.

    Args:
        venue_id: The venue ID
        lead_status: One of 'new', 'contacted', 'demo_scheduled', 'won', 'lost', 'not_interested'
        next_follow_up: Optional follow-up date (YYYY-MM-DD)
        competitor: Optional competitor name if lost
        lost_reason: Optional reason for lost/not_interested
    """
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE venues
        SET lead_status = ?,
            next_follow_up = COALESCE(?, next_follow_up),
            competitor = COALESCE(?, competitor),
            lost_reason = COALESCE(?, lost_reason)
        WHERE id = ?
    """, (lead_status, next_follow_up, competitor, lost_reason, venue_id))
    conn.commit()


def add_lead_activity(conn: sqlite3.Connection, venue_id: int, activity_type: str,
                      notes: str = None, outcome: str = None,
                      next_action_date: str = None) -> int:
    """
    Add an activity record for a lead.

    Args:
        venue_id: The venue ID
        activity_type: One of 'call', 'email', 'visit', 'demo', 'note'
        notes: Activity notes
        outcome: One of 'no_answer', 'callback', 'interested', 'not_interested', 'demo_booked'
        next_action_date: Optional next action date (YYYY-MM-DD)

    Returns:
        The activity ID
    """
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO lead_activities
        (venue_id, activity_type, notes, outcome, next_action_date)
        VALUES (?, ?, ?, ?, ?)
    """, (venue_id, activity_type, notes, outcome, next_action_date))
    conn.commit()
    return cursor.lastrowid


def get_lead_activities(conn: sqlite3.Connection, venue_id: int) -> list[sqlite3.Row]:
    """
    Get all activities for a venue, newest first.
    """
    cursor = conn.cursor()
    cursor.execute("""
        SELECT * FROM lead_activities
        WHERE venue_id = ?
        ORDER BY activity_date DESC, created_at DESC
    """, (venue_id,))
    return cursor.fetchall()


def get_venues_needing_followup(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    """
    Get venues where next_follow_up date is today or earlier.
    """
    cursor = conn.cursor()
    cursor.execute("""
        SELECT * FROM venues
        WHERE next_follow_up IS NOT NULL
          AND next_follow_up <= date('now')
          AND lead_status NOT IN ('won', 'lost', 'not_interested')
        ORDER BY next_follow_up ASC
    """)
    return cursor.fetchall()


def get_hot_leads(conn: sqlite3.Connection, days: int = 7) -> list[sqlite3.Row]:
    """
    Get new leads from the last N days, sorted by priority score.
    """
    cursor = conn.cursor()
    cursor.execute("""
        SELECT * FROM venues
        WHERE lead_status = 'new'
          AND first_seen_date >= date('now', ? || ' days')
        ORDER BY priority_score DESC, first_seen_date DESC
    """, (f'-{days}',))
    return cursor.fetchall()


def get_lead_counts_by_status(conn: sqlite3.Connection) -> dict:
    """
    Get count of venues by lead status.
    """
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            COALESCE(lead_status, 'new') as status,
            COUNT(*) as count
        FROM venues
        GROUP BY lead_status
    """)
    return {row['status']: row['count'] for row in cursor.fetchall()}
