"""
Google Places API integration for DFW Openings.
Fetches phone number, website, and place ID for venues.
"""

import os
import requests
import time
from typing import Optional, Dict, Tuple

# You need to set this environment variable
GOOGLE_PLACES_API_KEY = os.getenv("GOOGLE_PLACES_API_KEY")

def find_place_id(name: str, address: str, city: str) -> Optional[str]:
    """
    Find the Google Place ID for a venue.
    """
    if not GOOGLE_PLACES_API_KEY:
        return None
        
    url = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"
    
    query = f"{name} {address} {city} TX"
    
    params = {
        "input": query,
        "inputtype": "textquery",
        "fields": "place_id",
        "key": GOOGLE_PLACES_API_KEY
    }
    
    try:
        response = requests.get(url, params=params)
        data = response.json()
        
        if data.get("status") == "OK" and data.get("candidates"):
            return data["candidates"][0]["place_id"]
            
    except Exception as e:
        print(f"Error finding place ID for {name}: {e}")
        
    return None

def get_place_details(place_id: str) -> Dict[str, str]:
    """
    Get details (phone, website, etc.) for a Place ID.
    """
    if not GOOGLE_PLACES_API_KEY:
        return {}
        
    url = "https://maps.googleapis.com/maps/api/place/details/json"
    
    params = {
        "place_id": place_id,
        "fields": "formatted_phone_number,website,url,rating,user_ratings_total",
        "key": GOOGLE_PLACES_API_KEY
    }
    
    try:
        response = requests.get(url, params=params)
        data = response.json()
        
        if data.get("status") == "OK":
            result = data.get("result", {})
            return {
                "phone": result.get("formatted_phone_number"),
                "website": result.get("website"),
                "google_url": result.get("url"),
                "rating": result.get("rating"),
                "review_count": result.get("user_ratings_total")
            }
            
    except Exception as e:
        print(f"Error getting details for {place_id}: {e}")
        
    return {}

def enrich_venue(name: str, address: str, city: str) -> Dict[str, str]:
    """
    Find and get details for a venue in one go.
    """
    if not GOOGLE_PLACES_API_KEY:
        return {}
        
    place_id = find_place_id(name, address, city)
    if place_id:
        details = get_place_details(place_id)
        details["google_place_id"] = place_id
        return details
        
    return {}
