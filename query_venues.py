#!/usr/bin/env python3
"""
Simple script to query and display venues from the database.
"""

import sqlite3
import argparse
from config import DB_PATH


def print_table(rows, headers):
    """Print rows in a simple table format."""
    if not rows:
        print("No results found.")
        return

    # Calculate column widths
    widths = [len(h) for h in headers]
    for row in rows:
        for i, val in enumerate(row):
            widths[i] = max(widths[i], len(str(val or "")))

    # Print header
    header_str = " | ".join(h.ljust(widths[i]) for i, h in enumerate(headers))
    print(header_str)
    print("-" * len(header_str))

    # Print rows
    for row in rows:
        row_str = " | ".join(str(val or "").ljust(widths[i]) for i, val in enumerate(row))
        print(row_str)


def main():
    parser = argparse.ArgumentParser(description="Query DFW venues")
    parser.add_argument("--type", choices=["bar", "restaurant", "all"], default="all",
                        help="Filter by venue type")
    parser.add_argument("--city", help="Filter by city")
    parser.add_argument("--limit", type=int, default=20, help="Max results to show")
    parser.add_argument("--status", help="Filter by status (permitting, opening_soon, open)")
    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Build query
    where_clauses = []
    params = []

    if args.type != "all":
        where_clauses.append("venue_type = ?")
        params.append(args.type)

    if args.city:
        where_clauses.append("LOWER(city) = LOWER(?)")
        params.append(args.city)

    if args.status:
        where_clauses.append("status = ?")
        params.append(args.status)

    where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"

    query = f"""
        SELECT name, city, venue_type, status, first_seen_date, priority_score
        FROM venues
        WHERE {where_sql}
        ORDER BY priority_score DESC, first_seen_date DESC
        LIMIT ?
    """
    params.append(args.limit)

    cursor.execute(query, params)
    rows = cursor.fetchall()

    headers = ["Name", "City", "Type", "Status", "First Seen", "Score"]
    print_table(rows, headers)

    print(f"\nTotal results: {len(rows)}")
    conn.close()


if __name__ == "__main__":
    main()
