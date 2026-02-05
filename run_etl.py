#!/usr/bin/env python3
"""
DFW Restaurant/Bar Openings ETL Pipeline
Main orchestration script.

Usage:
    python run_etl.py [--days N] [--cities CITY1,CITY2,...] [--list-cities]

Options:
    --days N              Number of days to look back (default: 14)
    --cities CITY1,CITY2  Comma-separated list of cities to fetch (default: all)
    --list-cities         Show available cities and exit

Examples:
    python run_etl.py --days 14
    python run_etl.py --cities fortworth,dallas
    python run_etl.py --cities tabc,salestax,fortworth --days 30
"""

import argparse
import sys
from datetime import datetime, timedelta, timezone

import db
from config import DEFAULT_LOOKBACK_DAYS

# Import all ETL modules
from etl import tabc, dallas_co, fortworth_co, sales_tax, merge
from etl import lewisville_permits, mesquite_permits, carrollton_permits, plano_permits, frisco_permits
from etl import dallas_permits, arlington_permits, denton_permits
from etl import mckinney_permits, southlake_permits, fortworth_permits


# Define all available sources with their fetch and transform functions
# Status: "working" = returning data, "limited" = partial data, "unavailable" = no public API
SOURCES = {
    # Core sources (statewide data) - WORKING
    "tabc": {
        "name": "TABC Liquor Licenses",
        "description": "Texas liquor licenses for DFW counties",
        "fetch": lambda since, _: tabc.fetch_tabc_licenses_since(since),
        "transform": tabc.to_source_events,
        "date_format": "socrata",  # YYYY-MM-DDT00:00:00.000
        "category": "statewide",
        "status": "working"
    },
    "salestax": {
        "name": "Sales Tax Permits",
        "description": "Texas sales tax permits for food/beverage",
        "fetch": lambda _, days: sales_tax.fetch_sales_tax_permits_since(days),
        "transform": sales_tax.to_source_events,
        "date_format": "days",  # Uses lookback days directly
        "category": "statewide",
        "status": "working"
    },

    # Dallas area
    "dallas": {
        "name": "Dallas Building Permits",
        "description": "Dallas OpenData building permits (Socrata)",
        "fetch": lambda since, _: dallas_permits.fetch_dallas_permits_since(since),
        "transform": dallas_permits.to_source_events,
        "date_format": "socrata",
        "category": "dallas",
        "status": "working"
    },
    "dallas_co": {
        "name": "Dallas Certificates of Occupancy",
        "description": "Dallas CO records",
        "fetch": lambda since, _: dallas_co.fetch_dallas_cos_since(since),
        "transform": dallas_co.to_source_events,
        "date_format": "socrata",
        "category": "dallas",
        "status": "limited"  # Returns 0 records recently
    },

    # Fort Worth / Tarrant
    "fortworth": {
        "name": "Fort Worth Building Permits",
        "description": "Fort Worth Accela portal scraper",
        "fetch": lambda since, _: fortworth_permits.fetch_fortworth_permits_since(since),
        "transform": fortworth_permits.to_source_events,
        "date_format": "iso",  # YYYY-MM-DD
        "category": "tarrant",
        "status": "working"
    },
    "fortworth_co": {
        "name": "Fort Worth Certificates of Occupancy",
        "description": "Fort Worth CO records (ArcGIS)",
        "fetch": lambda since, _: fortworth_co.fetch_fortworth_cos_since(since),
        "transform": fortworth_co.to_source_events,
        "date_format": "iso",
        "category": "tarrant",
        "status": "unavailable"  # Endpoint not configured
    },
    "arlington": {
        "name": "Arlington Building Permits",
        "description": "Arlington SmartGuide portal scraper",
        "fetch": lambda since, _: arlington_permits.fetch_arlington_permits_since(since),
        "transform": arlington_permits.to_source_events,
        "date_format": "iso",
        "category": "tarrant",
        "status": "unavailable"  # SmartGuide requires authentication
    },
    "southlake": {
        "name": "Southlake Building Permits",
        "description": "Southlake EnerGov API",
        "fetch": lambda since, _: southlake_permits.fetch_southlake_permits_since(since),
        "transform": southlake_permits.to_source_events,
        "date_format": "iso",
        "category": "tarrant",
        "status": "unavailable"  # EnerGov API requires authentication
    },

    # Collin County
    "plano": {
        "name": "Plano Building Permits",
        "description": "Plano eTRAKiT scraper",
        "fetch": lambda since, _: plano_permits.fetch_plano_permits_since(since),
        "transform": plano_permits.to_source_events,
        "date_format": "iso",
        "category": "collin",
        "status": "limited"  # eTRAKiT has limited public access
    },
    "frisco": {
        "name": "Frisco Building Permits",
        "description": "Frisco eTRAKiT scraper",
        "fetch": lambda since, _: frisco_permits.fetch_frisco_permits_since(since),
        "transform": frisco_permits.to_source_events,
        "date_format": "iso",
        "category": "collin",
        "status": "limited"  # eTRAKiT has limited public access
    },
    "mckinney": {
        "name": "McKinney Building Permits",
        "description": "McKinney EnerGov API",
        "fetch": lambda since, _: mckinney_permits.fetch_mckinney_permits_since(since),
        "transform": mckinney_permits.to_source_events,
        "date_format": "iso",
        "category": "collin",
        "status": "unavailable"  # EnerGov API requires authentication
    },
    "carrollton": {
        "name": "Carrollton Building Permits",
        "description": "Carrollton CityView/ArcGIS",
        "fetch": lambda since, _: carrollton_permits.fetch_carrollton_permits_since(since),
        "transform": carrollton_permits.to_source_events,
        "date_format": "iso",
        "category": "collin",
        "status": "unavailable"  # ArcGIS URL not discovered
    },

    # Denton County
    "denton": {
        "name": "Denton Building Permits",
        "description": "Denton eTRAKiT scraper",
        "fetch": lambda since, _: denton_permits.fetch_denton_permits_since(since),
        "transform": denton_permits.to_source_events,
        "date_format": "iso",
        "category": "denton",
        "status": "limited"  # eTRAKiT has limited public access
    },
    "lewisville": {
        "name": "Lewisville Building Permits",
        "description": "Lewisville CSV API",
        "fetch": lambda since, _: lewisville_permits.fetch_lewisville_permits_since(since),
        "transform": lewisville_permits.to_source_events,
        "date_format": "iso",
        "category": "denton",
        "status": "unavailable"  # API returning empty data
    },

    # Other
    "mesquite": {
        "name": "Mesquite Building Permits",
        "description": "Mesquite EnerGov API",
        "fetch": lambda since, _: mesquite_permits.fetch_mesquite_permits_since(since),
        "transform": mesquite_permits.to_source_events,
        "date_format": "iso",
        "category": "dallas",
        "status": "unavailable"  # EnerGov API requires authentication
    },
}

# City groups for convenience
CITY_GROUPS = {
    "all": list(SOURCES.keys()),
    "working": ["tabc", "salestax", "fortworth", "dallas"],  # Currently returning data
    "statewide": ["tabc", "salestax"],
    "dallas_area": ["dallas", "dallas_co", "mesquite"],
    "tarrant": ["fortworth", "fortworth_co", "arlington", "southlake"],
    "collin": ["plano", "frisco", "mckinney", "carrollton"],
    "denton_area": ["denton", "lewisville"],
}


def list_cities():
    """Print available cities and groups."""
    print("\n" + "="*60)
    print("Available Data Sources")
    print("="*60)

    # Status icons
    status_icons = {
        "working": "✓",
        "limited": "~",
        "unavailable": "✗"
    }

    # Group by category
    categories = {}
    for key, source in SOURCES.items():
        cat = source.get("category", "other")
        if cat not in categories:
            categories[cat] = []
        categories[cat].append((key, source))

    for cat in ["statewide", "dallas", "tarrant", "collin", "denton"]:
        if cat in categories:
            print(f"\n{cat.upper()}:")
            for key, source in categories[cat]:
                status = source.get("status", "unknown")
                icon = status_icons.get(status, "?")
                print(f"  [{icon}] {key:13} - {source['name']}")
                print(f"                   {source['description']}")

    print("\n" + "-"*60)
    print("Status Legend:  [✓] Working  [~] Limited  [✗] Unavailable")

    print("\n" + "-"*60)
    print("City Groups (shortcuts):")
    print("-"*60)
    for group, cities in CITY_GROUPS.items():
        print(f"  {group:15} = {', '.join(cities)}")

    print("\n" + "-"*60)
    print("Examples:")
    print("-"*60)
    print("  python run_etl.py --cities fortworth,dallas --days 14")
    print("  python run_etl.py --cities working")
    print("  python run_etl.py --cities tarrant,collin")
    print("="*60 + "\n")


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Fetch and process DFW restaurant/bar openings data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_etl.py --days 14                    # All sources, 14 days
  python run_etl.py --cities fortworth           # Just Fort Worth
  python run_etl.py --cities working             # Only working sources
  python run_etl.py --cities tabc,fortworth      # TABC + Fort Worth
  python run_etl.py --list-cities                # Show all options
        """
    )
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_LOOKBACK_DAYS,
        help=f"Number of days to look back (default: {DEFAULT_LOOKBACK_DAYS})"
    )
    parser.add_argument(
        "--cities",
        type=str,
        default="all",
        help="Comma-separated list of cities/sources to fetch (default: all)"
    )
    parser.add_argument(
        "--list-cities",
        action="store_true",
        help="Show available cities and exit"
    )
    return parser.parse_args()


def resolve_cities(city_arg: str) -> list[str]:
    """Resolve city argument to list of source keys."""
    cities = []
    for item in city_arg.split(","):
        item = item.strip().lower()
        if item in CITY_GROUPS:
            cities.extend(CITY_GROUPS[item])
        elif item in SOURCES:
            cities.append(item)
        else:
            print(f"Warning: Unknown source '{item}', skipping")

    # Remove duplicates while preserving order
    seen = set()
    unique = []
    for c in cities:
        if c not in seen:
            seen.add(c)
            unique.append(c)

    return unique


def main():
    """Main ETL orchestration."""
    args = parse_args()

    if args.list_cities:
        list_cities()
        return

    lookback_days = args.days
    selected_cities = resolve_cities(args.cities)

    if not selected_cities:
        print("No valid sources selected. Use --list-cities to see options.")
        sys.exit(1)

    print(f"\n{'='*60}")
    print(f"DFW Restaurant/Bar Openings ETL Pipeline")
    print(f"{'='*60}")
    print(f"Lookback period: {lookback_days} days")
    print(f"Sources: {', '.join(selected_cities)}\n")

    # Calculate since_date
    run_started_at = datetime.now(timezone.utc)
    since_date = (run_started_at - timedelta(days=lookback_days)).date()

    # Format for different API types
    since_iso_socrata = f"{since_date.isoformat()}T00:00:00.000"
    since_iso_date = since_date.isoformat()

    print(f"Run started: {run_started_at.isoformat()}")
    print(f"Fetching records since: {since_date}\n")

    # Step 1: Fetch data from selected sources
    print("Step 1: Fetching data from sources...")
    print("-" * 60)

    results = {}
    for city_key in selected_cities:
        source = SOURCES[city_key]

        # Determine which date format to use
        if source["date_format"] == "socrata":
            since_arg = since_iso_socrata
        elif source["date_format"] == "days":
            since_arg = since_iso_date  # Will be ignored, uses days
        else:
            since_arg = since_iso_date

        try:
            rows = source["fetch"](since_arg, lookback_days)
            events = source["transform"](rows)
            results[city_key] = events
        except Exception as e:
            print(f"[{source['name']}] Error: {e}")
            results[city_key] = []

    print()

    # Step 2: Transform to source events
    print("Step 2: Summary of fetched events...")
    print("-" * 60)

    all_events = []
    for city_key in selected_cities:
        events = results.get(city_key, [])
        source = SOURCES[city_key]
        print(f"  - {source['name']}: {len(events)}")
        all_events.extend(events)

    print(f"\n[Total] {len(all_events)} events\n")

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

        # Build run_data with counts for each source
        run_data = {
            "run_started_at": run_started_at.isoformat(),
            "run_finished_at": run_finished_at.isoformat(),
            "lookback_days": lookback_days,
            "notes": f"Sources: {','.join(selected_cities)}. Processed {len(all_events)} events"
        }

        # Add counts for standard sources
        source_to_column = {
            "tabc": "rows_tabc",
            "dallas_co": "rows_dallas_co",
            "fortworth_co": "rows_fortworth_co",
            "salestax": "rows_sales_tax",
            "lewisville": "rows_lewisville",
            "mesquite": "rows_mesquite",
            "carrollton": "rows_carrollton",
            "plano": "rows_plano",
            "frisco": "rows_frisco",
            "dallas": "rows_dallas_permits",
            "arlington": "rows_arlington",
            "denton": "rows_denton",
            "mckinney": "rows_mckinney",
            "southlake": "rows_southlake",
            "fortworth": "rows_fortworth_permits",
        }

        for city_key, column in source_to_column.items():
            run_data[column] = len(results.get(city_key, []))

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
