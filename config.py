"""
Configuration module for DFW Openings pipeline.
Loads environment variables and provides constants.
"""

import os
from dotenv import load_dotenv

# Load .env file if it exists
load_dotenv()

# Database configuration
DB_PATH = os.getenv("DFW_DB_PATH", "dfw_openings.sqlite")

# API tokens (optional)
SOCRATA_APP_TOKEN = os.getenv("SOCRATA_APP_TOKEN")
FORTWORTH_ARCGIS_TOKEN = os.getenv("FORTWORTH_ARCGIS_TOKEN")

# ETL configuration
DEFAULT_LOOKBACK_DAYS = int(os.getenv("DEFAULT_LOOKBACK_DAYS", "7"))

# Geographic scope
TARGET_COUNTIES = ["DALLAS", "TARRANT", "COLLIN", "DENTON"]

# Data source endpoints
TABC_ENDPOINT = "https://data.texas.gov/resource/7hf9-qc9f.json"
DALLAS_CO_ENDPOINT = "https://www.dallasopendata.com/resource/9qet-qt9e.json"
# Fort Worth endpoint to be determined - currently under investigation
FORTWORTH_CO_ENDPOINT = None  # Placeholder for future API support

# Fort Worth CSV fallback path
FORTWORTH_CO_CSV_PATH = os.getenv(
    "FORTWORTH_CO_CSV_PATH",
    os.path.join(os.path.dirname(__file__), "data", "fortworth_co.csv")
)

# Venue classification keywords
BAR_KEYWORDS = ["bar", "pub", "taproom", "saloon", "tavern", "lounge", "brewery", "brewpub"]
RESTAURANT_KEYWORDS = ["restaurant", "cafe", "bistro", "eatery", "grill", "diner"]
