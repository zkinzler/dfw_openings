#!/usr/bin/env python3
"""
Normalize city names to consistent title case.
Fixes: "DALLAS" vs "Dallas" vs "dallas"
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import db


def normalize_cities():
    """Normalize all city names to title case."""
    conn = db.get_connection()
    db.ensure_schema(conn)
    cursor = conn.cursor()

    print("=" * 60)
    print("CITY NAME NORMALIZATION")
    print("=" * 60)
    print()

    # Find all unique cities with case variations
    cursor.execute("""
        SELECT LOWER(city) as normalized, GROUP_CONCAT(DISTINCT city) as variations, COUNT(*) as count
        FROM venues
        WHERE city IS NOT NULL
        GROUP BY LOWER(city)
        ORDER BY count DESC
    """)

    cities = cursor.fetchall()
    print(f"Found {len(cities)} unique cities")
    print()

    updates = 0
    for row in cities:
        normalized = row['normalized']
        variations = row['variations'].split(',')

        # Convert to proper title case
        # Handle special cases
        proper_name = normalized.title()

        # Fix common issues
        proper_name = proper_name.replace(' Of ', ' of ')
        proper_name = proper_name.replace(' The ', ' the ')

        # Update all variations to proper name
        for variation in variations:
            if variation != proper_name:
                cursor.execute("""
                    UPDATE venues SET city = ? WHERE city = ?
                """, (proper_name, variation))
                count = cursor.rowcount
                if count > 0:
                    print(f"  '{variation}' → '{proper_name}' ({count} venues)")
                    updates += count

    conn.commit()
    conn.close()

    print()
    print(f"Updated {updates} venue records")
    print("Done!")


if __name__ == "__main__":
    normalize_cities()
