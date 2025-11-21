# Project Status: DFW Openings Lead Generator
**Date:** 2025-11-21

## Current State
The project is fully functional and installed in `/Users/zachkinzler/dfw_openings/`.

### Features Implemented
- **ETL Pipeline:** Fetches data from TABC, Dallas CO, Fort Worth CO (ArcGIS API), and Texas Sales Tax.
- **Database:** SQLite database (`dfw_openings.sqlite`) stores venues and source events.
- **Geocoding:** Automatically finds coordinates for addresses.
- **Dashboard:** Streamlit app with Map View, Filters, and Venue Details.
- **Notifications:** Code for Email/Slack alerts is ready (`utils/notifications.py`).
- **Google Places:** Code for phone/website enrichment is ready (`utils/google_places.py`).

### Missing Configuration
To enable **Phone Numbers**, **Websites**, and **Notifications**, you need to edit the `.env` file and add:
1.  `GOOGLE_PLACES_API_KEY`
2.  `SLACK_WEBHOOK_URL` (optional)
3.  `SMTP_PASSWORD` (optional for email)

## How to Resume
1.  **Activate Environment:** Ensure you have the dependencies installed (`pip install -r requirements.txt`).
2.  **Run Pipeline:** `python3 run_etl.py --days 30`
3.  **Run Geocoding:** `python3 run_geocoding.py`
4.  **Start Dashboard:** `streamlit run dashboard.py`

## Files of Interest
- `dashboard.py`: The main UI.
- `run_etl.py`: The data fetcher.
- `config.py`: Configuration settings.
- `.env`: Your API keys (keep this secret).
