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

# Building permit data sources - Existing 5 cities
LEWISVILLE_CSV_API = os.getenv("LEWISVILLE_CSV_API", "https://query.cityoflewisville.com/v2/?Building/PermitsIssued_ReportLinks")
MESQUITE_ENERGOV_URL = os.getenv("MESQUITE_ENERGOV_URL", "https://energov.cityofmesquite.com")
CARROLLTON_ARCGIS_URL = os.getenv("CARROLLTON_ARCGIS_URL", "")
PLANO_ETRAKIT_URL = os.getenv("PLANO_ETRAKIT_URL", "https://trakit.plano.gov/etrakit_prod")
FRISCO_ETRAKIT_URL = os.getenv("FRISCO_ETRAKIT_URL", "https://etrakit.friscotexas.gov")

# Building permit data sources - Additional cities
DALLAS_PERMITS_ENDPOINT = os.getenv("DALLAS_PERMITS_ENDPOINT", "https://www.dallasopendata.com/resource/e7gq-4sah.json")
ARLINGTON_ARCGIS_URL = os.getenv("ARLINGTON_ARCGIS_URL", "")  # TBD: Need to discover FeatureServer URL
DENTON_ETRAKIT_URL = os.getenv("DENTON_ETRAKIT_URL", "https://dntn-trk.aspgov.com/eTRAKiT")

# EnerGov-based cities
MCKINNEY_ENERGOV_URL = os.getenv("MCKINNEY_ENERGOV_URL", "https://egov.mckinneytexas.org/EnerGov_Prod/SelfService")
SOUTHLAKE_ENERGOV_URL = os.getenv("SOUTHLAKE_ENERGOV_URL", "https://energov.cityofsouthlake.com/EnerGov_Prod/SelfService")

# Accela-based cities
FORTWORTH_ACCELA_URL = os.getenv("FORTWORTH_ACCELA_URL", "https://aca-prod.accela.com/CFW")

# Venue classification keywords
BAR_KEYWORDS = ["bar", "pub", "taproom", "saloon", "tavern", "lounge", "brewery", "brewpub"]
RESTAURANT_KEYWORDS = ["restaurant", "cafe", "bistro", "eatery", "grill", "diner"]
