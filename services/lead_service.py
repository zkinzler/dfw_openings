"""
Lead management service for DFW Openings.
Provides business logic for lead workflow, activities, and metrics.
"""

import sqlite3
from datetime import datetime, timedelta
from typing import Optional
import db


# Lead status options
LEAD_STATUSES = [
    'new',
    'contacted',
    'demo_scheduled',
    'won',
    'lost',
    'not_interested'
]

# Activity types
ACTIVITY_TYPES = ['call', 'email', 'visit', 'demo', 'note']

# Activity outcomes
ACTIVITY_OUTCOMES = [
    'no_answer',
    'callback',
    'interested',
    'not_interested',
    'demo_booked',
    'left_voicemail'
]

# Lost reasons
LOST_REASONS = [
    'chose_toast',
    'chose_square',
    'chose_clover',
    'chose_other',
    'price_too_high',
    'already_has_pos',
    'not_opening',
    'bad_timing',
    'other'
]


def mark_contacted(conn: sqlite3.Connection, venue_id: int,
                   notes: str = None, next_follow_up: str = None) -> None:
    """
    Mark a lead as contacted with optional notes and follow-up.
    """
    db.update_lead_status(conn, venue_id, 'contacted', next_follow_up=next_follow_up)
    if notes:
        db.add_lead_activity(conn, venue_id, 'call', notes=notes,
                            next_action_date=next_follow_up)


def schedule_demo(conn: sqlite3.Connection, venue_id: int,
                  demo_date: str, notes: str = None) -> None:
    """
    Schedule a demo for a lead.
    """
    db.update_lead_status(conn, venue_id, 'demo_scheduled', next_follow_up=demo_date)
    db.add_lead_activity(conn, venue_id, 'demo',
                        notes=notes or f"Demo scheduled for {demo_date}",
                        outcome='demo_booked',
                        next_action_date=demo_date)


def mark_won(conn: sqlite3.Connection, venue_id: int, notes: str = None) -> None:
    """
    Mark a lead as won (closed deal).
    """
    db.update_lead_status(conn, venue_id, 'won')
    db.add_lead_activity(conn, venue_id, 'note',
                        notes=notes or "Deal closed - won!",
                        outcome='interested')


def mark_lost(conn: sqlite3.Connection, venue_id: int,
              competitor: str = None, lost_reason: str = None,
              notes: str = None) -> None:
    """
    Mark a lead as lost with optional competitor and reason.
    """
    db.update_lead_status(conn, venue_id, 'lost',
                         competitor=competitor,
                         lost_reason=lost_reason)
    activity_notes = notes or f"Lost to {competitor or 'unknown'}: {lost_reason or 'no reason given'}"
    db.add_lead_activity(conn, venue_id, 'note',
                        notes=activity_notes,
                        outcome='not_interested')


def mark_not_interested(conn: sqlite3.Connection, venue_id: int,
                        reason: str = None, notes: str = None) -> None:
    """
    Mark a lead as not interested (disqualified).
    """
    db.update_lead_status(conn, venue_id, 'not_interested', lost_reason=reason)
    db.add_lead_activity(conn, venue_id, 'note',
                        notes=notes or f"Not interested: {reason or 'no reason given'}",
                        outcome='not_interested')


def log_call(conn: sqlite3.Connection, venue_id: int,
             outcome: str, notes: str = None,
             next_follow_up: str = None) -> None:
    """
    Log a call activity.
    """
    db.add_lead_activity(conn, venue_id, 'call',
                        notes=notes, outcome=outcome,
                        next_action_date=next_follow_up)
    if next_follow_up:
        # Update venue's next follow-up date
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE venues SET next_follow_up = ? WHERE id = ?
        """, (next_follow_up, venue_id))
        conn.commit()


def update_follow_up(conn: sqlite3.Connection, venue_id: int,
                     follow_up_date: str) -> None:
    """
    Update the follow-up date for a venue.
    """
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE venues SET next_follow_up = ? WHERE id = ?
    """, (follow_up_date, venue_id))
    conn.commit()


def update_notes(conn: sqlite3.Connection, venue_id: int, notes: str) -> None:
    """
    Update the notes field for a venue.
    """
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE venues SET notes = ? WHERE id = ?
    """, (notes, venue_id))
    conn.commit()


def get_pipeline_metrics(conn: sqlite3.Connection) -> dict:
    """
    Get pipeline metrics for the dashboard.
    """
    counts = db.get_lead_counts_by_status(conn)

    # Calculate win rate
    demos = counts.get('demo_scheduled', 0)
    won = counts.get('won', 0)
    lost = counts.get('lost', 0)

    completed_demos = won + lost
    win_rate = (won / completed_demos * 100) if completed_demos > 0 else 0

    return {
        'counts': counts,
        'total_leads': sum(counts.values()),
        'active_leads': counts.get('new', 0) + counts.get('contacted', 0) + counts.get('demo_scheduled', 0),
        'won': won,
        'lost': lost,
        'win_rate': win_rate,
        'needs_follow_up': len(db.get_venues_needing_followup(conn))
    }


def get_activity_summary(conn: sqlite3.Connection, days: int = 7) -> dict:
    """
    Get activity summary for the last N days.
    """
    cursor = conn.cursor()

    # Total activities
    cursor.execute("""
        SELECT activity_type, COUNT(*) as count
        FROM lead_activities
        WHERE activity_date >= date('now', ? || ' days')
        GROUP BY activity_type
    """, (f'-{days}',))

    activities_by_type = {row['activity_type']: row['count'] for row in cursor.fetchall()}

    # Outcomes
    cursor.execute("""
        SELECT outcome, COUNT(*) as count
        FROM lead_activities
        WHERE activity_date >= date('now', ? || ' days')
          AND outcome IS NOT NULL
        GROUP BY outcome
    """, (f'-{days}',))

    outcomes = {row['outcome']: row['count'] for row in cursor.fetchall()}

    return {
        'activities': activities_by_type,
        'outcomes': outcomes,
        'total_calls': activities_by_type.get('call', 0),
        'total_demos': activities_by_type.get('demo', 0),
        'demos_booked': outcomes.get('demo_booked', 0)
    }


def get_source_effectiveness(conn: sqlite3.Connection) -> list[dict]:
    """
    Analyze which data sources produce the most won leads.
    """
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            se.source_system,
            COUNT(DISTINCT v.id) as total_leads,
            SUM(CASE WHEN v.lead_status = 'won' THEN 1 ELSE 0 END) as won,
            SUM(CASE WHEN v.lead_status = 'contacted' THEN 1 ELSE 0 END) as contacted,
            SUM(CASE WHEN v.lead_status = 'demo_scheduled' THEN 1 ELSE 0 END) as demos
        FROM source_events se
        JOIN venues v ON se.venue_id = v.id
        GROUP BY se.source_system
        ORDER BY won DESC, total_leads DESC
    """)

    return [dict(row) for row in cursor.fetchall()]


def get_city_performance(conn: sqlite3.Connection) -> list[dict]:
    """
    Analyze performance by city.
    """
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            city,
            COUNT(*) as total_leads,
            SUM(CASE WHEN lead_status = 'won' THEN 1 ELSE 0 END) as won,
            SUM(CASE WHEN lead_status = 'contacted' THEN 1 ELSE 0 END) as contacted,
            SUM(CASE WHEN lead_status = 'demo_scheduled' THEN 1 ELSE 0 END) as demos,
            SUM(CASE WHEN lead_status IN ('lost', 'not_interested') THEN 1 ELSE 0 END) as lost
        FROM venues
        WHERE city IS NOT NULL
        GROUP BY city
        ORDER BY won DESC, total_leads DESC
    """)

    return [dict(row) for row in cursor.fetchall()]
