#!/usr/bin/env python3
"""
DFW Restaurant/Bar Openings ETL Pipeline
Main orchestration script.

Usage:
    python run_etl.py [--days N]

Options:
    --days N    Number of days to look back (default: 7)
"""

import argparse
import sys
from datetime import datetime, timedelta, timezone

import db
from etl import tabc, dallas_co, fortworth_co, sales_tax, merge
from config import DEFAULT_LOOKBACK_DAYS


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Fetch and process DFW restaurant/bar openings data"
    )
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_LOOKBACK_DAYS,
        help=f"Number of days to look back (default: {DEFAULT_LOOKBACK_DAYS})"
    )
    return parser.parse_args()


def main():
    """Main ETL orchestration."""
    args = parse_args()
    lookback_days = args.days

    print(f"\n{'='*60}")
    print(f"DFW Restaurant/Bar Openings ETL Pipeline")
    print(f"{'='*60}")
    print(f"Lookback period: {lookback_days} days\n")

    # Calculate since_date
    run_started_at = datetime.now(timezone.utc)
    since_date = (run_started_at - timedelta(days=lookback_days)).date()

    # Format for different API types
    since_iso_socrata = f"{since_date.isoformat()}T00:00:00.000"
    since_iso_date = since_date.isoformat()

    print(f"Run started: {run_started_at.isoformat()}")
    print(f"Fetching records since: {since_date}\n")

    # Step 1: Fetch data from all sources
    print("Step 1: Fetching data from sources...")
    print("-" * 60)

    tabc_rows = tabc.fetch_tabc_licenses_since(since_iso_socrata)
    dallas_rows = dallas_co.fetch_dallas_cos_since(since_iso_socrata)
    fortworth_rows = fortworth_co.fetch_fortworth_cos_since(since_iso_date)
    sales_tax_rows = sales_tax.fetch_sales_tax_permits_since(lookback_days)

    print()

    # Step 2: Transform to source events
    print("Step 2: Transforming to source events...")
    print("-" * 60)

    tabc_events = tabc.to_source_events(tabc_rows)
    dallas_events = dallas_co.to_source_events(dallas_rows)
    fortworth_events = fortworth_co.to_source_events(fortworth_rows)
    sales_tax_events = sales_tax.to_source_events(sales_tax_rows)

    all_events = tabc_events + dallas_events + fortworth_events + sales_tax_events

    print(f"[Transform] Total events: {len(all_events)}")
    print(f"  - TABC: {len(tabc_events)}")
    print(f"  - Dallas CO: {len(dallas_events)}")
    print(f"  - Fort Worth CO: {len(fortworth_events)}")
    print(f"  - Sales Tax: {len(sales_tax_events)}\n")

    # Step 3: Load into database
    print("Step 3: Loading data into database...")
    print("-" * 60)

    conn = db.get_connection()

    try:
        # Ensure schema exists
        db.ensure_schema(conn)
        print("[Database] Schema ready")

        # Insert source events
        db.insert_source_events(conn, all_events)
        print(f"[Database] Inserted {len(all_events)} source events")

        # Step 4: Match and merge into venues
        print("\nStep 4: Matching events to venues...")
        print("-" * 60)

        merge.update_venues_from_unmatched_events(conn)

        # Step 5: Log the ETL run
        run_finished_at = datetime.now(timezone.utc)

        run_data = {
            "run_started_at": run_started_at.isoformat(),
            "run_finished_at": run_finished_at.isoformat(),
            "lookback_days": lookback_days,
            "rows_tabc": len(tabc_events),
            "rows_dallas_co": len(dallas_events),
            "rows_fortworth_co": len(fortworth_events),
            "rows_sales_tax": len(sales_tax_events),
            "notes": f"Processed {len(all_events)} total events"
        }

        run_id = db.insert_etl_run(conn, run_data)
        print(f"\n[Database] ETL run logged (run_id={run_id})")

        # Print summary statistics
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) as count FROM venues")
        total_venues = cursor.fetchone()["count"]

        cursor.execute("SELECT COUNT(*) as count FROM venues WHERE venue_type = 'bar'")
        bar_count = cursor.fetchone()["count"]

        cursor.execute("SELECT COUNT(*) as count FROM venues WHERE venue_type = 'restaurant'")
        restaurant_count = cursor.fetchone()["count"]

        cursor.execute("SELECT COUNT(*) as count FROM source_events")
        total_events = cursor.fetchone()["count"]

        print("\n" + "="*60)
        print("Summary")
        print("="*60)
        print(f"Total venues in database: {total_venues}")
        print(f"  - Bars: {bar_count}")
        print(f"  - Restaurants: {restaurant_count}")
        print(f"  - Other/Unknown: {total_venues - bar_count - restaurant_count}")
        print(f"\nTotal source events: {total_events}")
        print(f"\nDatabase: {db.DB_PATH}")
        print(f"Duration: {(run_finished_at - run_started_at).total_seconds():.2f}s")
        print("="*60 + "\n")

    except Exception as e:
        print(f"\n[ERROR] ETL failed: {e}")
        import traceback
        traceback.print_exc()
        conn.rollback()
        sys.exit(1)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
