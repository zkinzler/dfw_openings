#!/usr/bin/env python3
"""
Recalculate priority scores for all venues.
Run this after enrichment (adding phone/website) or periodically to update scores.
"""

import db
from etl.merge import recalculate_all_priority_scores

def main():
    print("DFW Openings - Priority Score Recalculation")
    print("=" * 50)

    conn = db.get_connection()
    db.ensure_schema(conn)

    print("\nRecalculating priority scores...")
    updated = recalculate_all_priority_scores(conn)
    print(f"\nUpdated {updated} venues.")

    # Show score distribution
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            CASE
                WHEN priority_score >= 100 THEN 'Hot (100+)'
                WHEN priority_score >= 70 THEN 'Warm (70-99)'
                WHEN priority_score >= 40 THEN 'Cool (40-69)'
                ELSE 'Cold (<40)'
            END as bucket,
            COUNT(*) as count
        FROM venues
        GROUP BY bucket
        ORDER BY priority_score DESC
    """)

    print("\nScore Distribution:")
    for row in cursor.fetchall():
        print(f"  {row['bucket']}: {row['count']}")

    conn.close()
    print("\nDone!")


if __name__ == "__main__":
    main()
