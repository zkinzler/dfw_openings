#!/usr/bin/env python3
"""
Enrich venues with Google Places data (phone, website).
Requires GOOGLE_PLACES_API_KEY environment variable.

Usage:
    python scripts/enrich_venues.py [--limit N] [--priority-only]

Options:
    --limit N         Only process N venues (default: all)
    --priority-only   Only enrich venues with priority_score >= 70
"""

import os
import sys
import time
import argparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import db
from utils.google_places import enrich_venue


def main():
    parser = argparse.ArgumentParser(description='Enrich venues with Google Places data')
    parser.add_argument('--limit', type=int, default=0, help='Max venues to process (0=all)')
    parser.add_argument('--priority-only', action='store_true', help='Only high-priority venues')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be enriched')
    args = parser.parse_args()

    # Check for API key
    api_key = os.getenv('GOOGLE_PLACES_API_KEY')
    if not api_key and not args.dry_run:
        print("ERROR: GOOGLE_PLACES_API_KEY environment variable not set")
        print()
        print("To get an API key:")
        print("1. Go to https://console.cloud.google.com/")
        print("2. Create a project (or select existing)")
        print("3. Enable 'Places API'")
        print("4. Create credentials → API key")
        print("5. Set: export GOOGLE_PLACES_API_KEY='your-key-here'")
        print()
        print("Cost: ~$17 per 1,000 lookups")
        sys.exit(1)

    conn = db.get_connection()
    db.ensure_schema(conn)
    cursor = conn.cursor()

    print("=" * 60)
    print("VENUE ENRICHMENT - Google Places")
    print("=" * 60)
    print()

    # Build query for venues needing enrichment
    query = """
        SELECT id, name, address, city
        FROM venues
        WHERE (phone IS NULL OR phone = '')
          AND lead_status NOT IN ('lost', 'not_interested')
    """

    if args.priority_only:
        query += " AND priority_score >= 70"

    query += " ORDER BY priority_score DESC, first_seen_date DESC"

    if args.limit > 0:
        query += f" LIMIT {args.limit}"

    cursor.execute(query)
    venues = cursor.fetchall()

    print(f"Found {len(venues)} venues needing enrichment")

    if args.priority_only:
        print("(Filtering to priority_score >= 70)")

    print()

    if args.dry_run:
        print("DRY RUN - Would enrich:")
        for i, v in enumerate(venues[:20], 1):
            print(f"  {i}. {v['name']} - {v['city']}")
        if len(venues) > 20:
            print(f"  ... and {len(venues) - 20} more")
        return

    # Process venues
    enriched = 0
    failed = 0
    already_has = 0

    for i, venue in enumerate(venues, 1):
        print(f"[{i}/{len(venues)}] {venue['name']} ({venue['city']})...", end=" ", flush=True)

        try:
            result = enrich_venue(
                conn,
                venue['id'],
                venue['name'],
                venue['address'],
                venue['city']
            )

            if result and result.get('phone'):
                print(f"✓ Phone: {result['phone']}")
                enriched += 1
            elif result:
                print("✓ (no phone found)")
                already_has += 1
            else:
                print("✗ Not found")
                failed += 1

        except Exception as e:
            print(f"✗ Error: {e}")
            failed += 1

        # Rate limit: 1 request per second to avoid quota issues
        time.sleep(1.0)

    conn.close()

    print()
    print("=" * 60)
    print(f"Enriched with phone: {enriched}")
    print(f"Not found/no phone:  {failed + already_has}")
    print(f"Total processed:     {len(venues)}")
    print("=" * 60)


if __name__ == "__main__":
    main()
