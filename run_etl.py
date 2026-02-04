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
from etl import lewisville_permits, mesquite_permits, carrollton_permits, plano_permits, frisco_permits
from etl import dallas_permits, arlington_permits, denton_permits
from etl import mckinney_permits, southlake_permits, fortworth_permits
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

    # Fetch from building permit sources - existing 5 cities
    lewisville_rows = lewisville_permits.fetch_lewisville_permits_since(since_iso_date)
    mesquite_rows = mesquite_permits.fetch_mesquite_permits_since(since_iso_date)
    carrollton_rows = carrollton_permits.fetch_carrollton_permits_since(since_iso_date)
    plano_rows = plano_permits.fetch_plano_permits_since(since_iso_date)
    frisco_rows = frisco_permits.fetch_frisco_permits_since(since_iso_date)

    # Fetch from building permit sources - additional cities
    dallas_permit_rows = dallas_permits.fetch_dallas_permits_since(since_iso_socrata)
    arlington_rows = arlington_permits.fetch_arlington_permits_since(since_iso_date)
    denton_rows = denton_permits.fetch_denton_permits_since(since_iso_date)

    # Fetch from EnerGov and Accela cities
    mckinney_rows = mckinney_permits.fetch_mckinney_permits_since(since_iso_date)
    southlake_rows = southlake_permits.fetch_southlake_permits_since(since_iso_date)
    fortworth_permit_rows = fortworth_permits.fetch_fortworth_permits_since(since_iso_date)

    print()

    # Step 2: Transform to source events
    print("Step 2: Transforming to source events...")
    print("-" * 60)

    tabc_events = tabc.to_source_events(tabc_rows)
    dallas_events = dallas_co.to_source_events(dallas_rows)
    fortworth_events = fortworth_co.to_source_events(fortworth_rows)
    sales_tax_events = sales_tax.to_source_events(sales_tax_rows)

    # Transform building permit sources - existing 5 cities
    lewisville_events = lewisville_permits.to_source_events(lewisville_rows)
    mesquite_events = mesquite_permits.to_source_events(mesquite_rows)
    carrollton_events = carrollton_permits.to_source_events(carrollton_rows)
    plano_events = plano_permits.to_source_events(plano_rows)
    frisco_events = frisco_permits.to_source_events(frisco_rows)

    # Transform building permit sources - additional cities
    dallas_permit_events = dallas_permits.to_source_events(dallas_permit_rows)
    arlington_events = arlington_permits.to_source_events(arlington_rows)
    denton_events = denton_permits.to_source_events(denton_rows)

    # Transform EnerGov and Accela cities
    mckinney_events = mckinney_permits.to_source_events(mckinney_rows)
    southlake_events = southlake_permits.to_source_events(southlake_rows)
    fortworth_permit_events = fortworth_permits.to_source_events(fortworth_permit_rows)

    all_events = (tabc_events + dallas_events + fortworth_events + sales_tax_events +
                  lewisville_events + mesquite_events + carrollton_events +
                  plano_events + frisco_events +
                  dallas_permit_events + arlington_events + denton_events +
                  mckinney_events + southlake_events + fortworth_permit_events)

    print(f"[Transform] Total events: {len(all_events)}")
    print(f"  - TABC: {len(tabc_events)}")
    print(f"  - Dallas CO: {len(dallas_events)}")
    print(f"  - Fort Worth CO: {len(fortworth_events)}")
    print(f"  - Sales Tax: {len(sales_tax_events)}")
    print(f"  - Lewisville Permits: {len(lewisville_events)}")
    print(f"  - Mesquite Permits: {len(mesquite_events)}")
    print(f"  - Carrollton Permits: {len(carrollton_events)}")
    print(f"  - Plano Permits: {len(plano_events)}")
    print(f"  - Frisco Permits: {len(frisco_events)}")
    print(f"  - Dallas Permits: {len(dallas_permit_events)}")
    print(f"  - Arlington Permits: {len(arlington_events)}")
    print(f"  - Denton Permits: {len(denton_events)}")
    print(f"  - McKinney Permits: {len(mckinney_events)}")
    print(f"  - Southlake Permits: {len(southlake_events)}")
    print(f"  - Fort Worth Permits: {len(fortworth_permit_events)}\n")

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
            "rows_lewisville": len(lewisville_events),
            "rows_mesquite": len(mesquite_events),
            "rows_carrollton": len(carrollton_events),
            "rows_plano": len(plano_events),
            "rows_frisco": len(frisco_events),
            "rows_dallas_permits": len(dallas_permit_events),
            "rows_arlington": len(arlington_events),
            "rows_denton": len(denton_events),
            "rows_mckinney": len(mckinney_events),
            "rows_southlake": len(southlake_events),
            "rows_fortworth_permits": len(fortworth_permit_events),
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
