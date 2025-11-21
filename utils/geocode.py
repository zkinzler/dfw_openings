"""
Geocoding utility for DFW Openings.
Uses geopy to fetch coordinates for addresses.
"""

import time
import ssl
import certifi
from typing import Optional, Tuple
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError

# Initialize geocoder with a unique user agent and proper SSL context
ctx = ssl.create_default_context(cafile=certifi.where())
geolocator = Nominatim(user_agent="dfw_openings_pipeline_v1", ssl_context=ctx, timeout=10)


def geocode_address(address: str, city: str, state: str = "TX", zip_code: str = None) -> Optional[Tuple[float, float]]:
    """
    Geocode an address to get latitude and longitude.
    Returns (latitude, longitude) or None if not found.
    """
    # Construct search query
    query = f"{address}, {city}, {state}"
    if zip_code:
        query += f" {zip_code}"

    try:
        # Add a small delay to respect rate limits (Nominatim requires 1s between requests)
        time.sleep(1.1)
        
        location = geolocator.geocode(query)
        
        if location:
            return (location.latitude, location.longitude)
        
        # If exact match fails, try without zip
        if zip_code:
            query = f"{address}, {city}, {state}"
            location = geolocator.geocode(query)
            if location:
                return (location.latitude, location.longitude)
                
        return None
        
    except (GeocoderTimedOut, GeocoderServiceError) as e:
        print(f"Geocoding error for {query}: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error geocoding {query}: {e}")
        return None
