#!/usr/bin/env python3
"""
Clean up junk data from the venues table.
Removes construction companies, liquor stores, convenience stores, etc.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import db

# Keywords that indicate NON-restaurant venues
EXCLUDE_KEYWORDS = [
    # Construction/Contractors
    'construction', 'contractor', 'electric', 'electrical', 'plumbing', 'plumber',
    'hvac', 'roofing', 'crane', 'geotechnical', 'excavating', 'paving',
    'concrete', 'drywall', 'flooring', 'painting', 'carpentry', 'welding',
    'demolition', 'engineering', 'architect', 'surveying',

    # Retail (not restaurants)
    'liquor', 'liquors', 'wine & spirits', 'total wine',
    'convenience', 'food mart', 'mart ', '7-eleven', '7 eleven',
    'shell ', 'exxon', 'chevron', 'texaco', 'valero', 'murphy',
    'dollar general', 'dollar tree', 'family dollar',
    'gas station', 'fuel', 'gasoline',
    'grocery', 'supermarket',

    # Other non-restaurant
    'church', 'school', 'hospital', 'clinic', 'medical',
    'office', 'warehouse', 'storage',
    'salon', 'barber', 'spa ', 'nail',
    'auto ', 'car wash', 'tire', 'mechanic',
]

# Keywords that indicate it IS a restaurant/bar (override exclusion)
INCLUDE_KEYWORDS = [
    'restaurant', 'grill', 'kitchen', 'cafe', 'bistro', 'diner',
    'bar', 'pub', 'tavern', 'brewery', 'brewpub', 'taproom', 'saloon',
    'pizza', 'burger', 'taco', 'sushi', 'bbq', 'barbecue',
    'steakhouse', 'seafood', 'wings', 'chicken', 'mexican', 'italian',
    'chinese', 'thai', 'indian', 'japanese', 'korean', 'vietnamese',
    'bakery', 'donut', 'coffee', 'tea house', 'ice cream', 'frozen yogurt',
]


def should_exclude(name: str) -> bool:
    """Check if venue name should be excluded."""
    if not name:
        return True

    name_lower = name.lower()

    # First check if it's clearly a restaurant/bar
    for keyword in INCLUDE_KEYWORDS:
        if keyword in name_lower:
            return False  # Keep it

    # Then check exclusion keywords
    for keyword in EXCLUDE_KEYWORDS:
        if keyword in name_lower:
            return True  # Exclude it

    return False  # Keep by default


def clean_venues():
    """Remove non-restaurant venues from database."""
    conn = db.get_connection()
    db.ensure_schema(conn)
    cursor = conn.cursor()

    print("=" * 60)
    print("DATA CLEANUP - Removing Non-Restaurant Venues")
    print("=" * 60)
    print()

    # Get all venues
    cursor.execute("SELECT id, name, city FROM venues")
    all_venues = cursor.fetchall()

    to_delete = []
    for venue in all_venues:
        if should_exclude(venue['name']):
            to_delete.append(venue)

    print(f"Found {len(to_delete)} venues to remove out of {len(all_venues)} total")
    print()

    if not to_delete:
        print("No junk venues found!")
        return

    # Show sample of what will be deleted
    print("Sample venues to be removed:")
    for venue in to_delete[:20]:
        print(f"  - {venue['name']} ({venue['city']})")

    if len(to_delete) > 20:
        print(f"  ... and {len(to_delete) - 20} more")

    print()
    confirm = input("Delete these venues? (yes/no): ").strip().lower()

    if confirm != 'yes':
        print("Cancelled.")
        return

    # Delete venues and their source events
    venue_ids = [v['id'] for v in to_delete]

    # Delete source events first (foreign key)
    cursor.execute(f"""
        DELETE FROM source_events
        WHERE venue_id IN ({','.join('?' * len(venue_ids))})
    """, venue_ids)
    events_deleted = cursor.rowcount

    # Delete lead activities
    cursor.execute(f"""
        DELETE FROM lead_activities
        WHERE venue_id IN ({','.join('?' * len(venue_ids))})
    """, venue_ids)
    activities_deleted = cursor.rowcount

    # Delete venues
    cursor.execute(f"""
        DELETE FROM venues
        WHERE id IN ({','.join('?' * len(venue_ids))})
    """, venue_ids)
    venues_deleted = cursor.rowcount

    conn.commit()
    conn.close()

    print()
    print(f"Deleted {venues_deleted} venues")
    print(f"Deleted {events_deleted} source events")
    print(f"Deleted {activities_deleted} lead activities")
    print("Done!")


if __name__ == "__main__":
    clean_venues()
