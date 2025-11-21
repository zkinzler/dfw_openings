#!/usr/bin/env python3
"""
Geocoding and Enrichment script for DFW Openings.
Fetches coordinates and Google Places details (phone, website, etc.).
"""

import sqlite3
import sys
from utils import geocode, google_places
import db

def get_venues_needing_geocoding(conn):
    """Get venues that have no latitude/longitude."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, name, address, city, state, zip
        FROM venues
        WHERE latitude IS NULL OR longitude IS NULL
        ORDER BY last_seen_date DESC
    """)
    return cursor.fetchall()

def get_venues_needing_enrichment(conn):
    """Get venues that have no phone number (and haven't been checked yet)."""
    # Note: We might want a 'enrichment_status' column later to avoid re-checking failed ones.
    # For now, we just check if phone is NULL.
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, name, address, city, state, zip
        FROM venues
        WHERE phone IS NULL
        ORDER BY last_seen_date DESC
        LIMIT 50  -- Rate limit protection
    """)
    return cursor.fetchall()

def update_venue_coordinates(conn, venue_id, lat, lon):
    """Update venue with new coordinates."""
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE venues
        SET latitude = ?, longitude = ?
        WHERE id = ?
    """, (lat, lon, venue_id))
    conn.commit()

def update_venue_enrichment(conn, venue_id, details):
    """Update venue with Google Places details."""
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE venues
        SET phone = ?,
            website = ?,
            google_place_id = ?
        WHERE id = ?
    """, (
        details.get("phone"),
        details.get("website"),
        details.get("google_place_id"),
        venue_id
    ))
    conn.commit()

def main():
    print("🌍 Starting Geocoding & Enrichment Process...")
    conn = db.get_connection()
    
    try:
        # 1. Geocoding
        venues = get_venues_needing_geocoding(conn)
        print(f"\n--- Geocoding ({len(venues)} venues) ---")
        
        geo_success = 0
        
        for i, venue in enumerate(venues):
            print(f"[{i+1}/{len(venues)}] Geocoding: {venue['name']}...", end=" ", flush=True)
            
            coords = geocode.geocode_address(
                venue['address'], 
                venue['city'], 
                venue['state'] or "TX", 
                venue['zip']
            )
            
            if coords:
                lat, lon = coords
                update_venue_coordinates(conn, venue['id'], lat, lon)
                print(f"✅ ({lat:.4f}, {lon:.4f})")
                geo_success += 1
            else:
                print(f"❌ Failed")
                
        # 2. Google Places Enrichment
        if google_places.GOOGLE_PLACES_API_KEY:
            venues_enrich = get_venues_needing_enrichment(conn)
            print(f"\n--- Google Places Enrichment ({len(venues_enrich)} venues) ---")
            
            enrich_success = 0
            
            for i, venue in enumerate(venues_enrich):
                print(f"[{i+1}/{len(venues_enrich)}] Enriching: {venue['name']}...", end=" ", flush=True)
                
                details = google_places.enrich_venue(
                    venue['name'],
                    venue['address'],
                    venue['city']
                )
                
                if details:
                    update_venue_enrichment(conn, venue['id'], details)
                    print(f"✅ Found: {details.get('phone') or 'No Phone'}")
                    enrich_success += 1
                else:
                    print(f"❌ Not found")
        else:
            print("\n⚠️ Google Places API Key not set. Skipping enrichment.")
                
        print("\n" + "="*40)
        print(f"Process Complete")
        print(f"Geocoded: {geo_success}/{len(venues)}")
        if google_places.GOOGLE_PLACES_API_KEY:
            print(f"Enriched: {enrich_success}/{len(venues_enrich)}")
        print("="*40)
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
