# DFW Restaurant/Bar Openings Pipeline

A Python ETL pipeline that tracks new and upcoming bars and restaurants in the Dallas-Fort Worth area by monitoring public data sources:

- **TABC** (Texas Alcoholic Beverage Commission) - New liquor licenses
- **Dallas** - Certificates of Occupancy
- **Fort Worth** - Certificates of Occupancy *(endpoint needs verification)*

## Features

- **No API keys required** - All data sources are public (though optional tokens are recommended)
- **SQLite database** - Simple, local storage with normalized schema
- **Smart matching** - Deduplicates venues across multiple data sources
- **Configurable lookback** - Fetch data for any time period
- **Venue classification** - Automatically identifies bars vs. restaurants
- **Priority scoring** - Ranks venues by type and stage (bars > restaurants)

## Quick Start

### 1. Setup

```bash
# Clone or download this project
cd dfw_openings

# Create virtual environment
python3 -m venv .venv

# Activate virtual environment
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Configuration (Optional)

The pipeline works out of the box without any configuration. However, you can optionally set up API tokens for better rate limits:

```bash
# Copy the example env file
cp .env.example .env

# Edit .env and add your Socrata app token (optional)
# Get one at https://dev.socrata.com/register
```

**`.env` variables:**

- `SOCRATA_APP_TOKEN` - Optional token for TABC and Dallas APIs (recommended for higher rate limits)
- `FORTWORTH_ARCGIS_TOKEN` - Optional token for Fort Worth API (if required)
- `DFW_DB_PATH` - Database file path (default: `dfw_openings.sqlite`)
- `DEFAULT_LOOKBACK_DAYS` - Default lookback period (default: 7 days)

### 3. Run the Pipeline

```bash
# Fetch data from the last 7 days (default)
python run_etl.py

# Fetch data from the last 30 days
python run_etl.py --days 30

# First time? Try a longer period to populate your database
python run_etl.py --days 90
```

### 4. Explore the Data

The pipeline creates a SQLite database at `dfw_openings.sqlite` with three tables.

#### Option A: Web Dashboard (Recommended)

Launch the interactive Streamlit dashboard:

```bash
streamlit run dashboard.py
```

The dashboard provides:
- **Interactive filters** - Filter by city, venue type, status, and date range
- **Stats overview** - Quick metrics on total venues, bars, and restaurants
- **Sortable table** - View all venues with key details
- **Venue details** - Drill down to see source events for any venue
- **CSV export** - Download filtered results
- **Auto-refresh** - Reload data after running ETL

Your browser will automatically open to `http://localhost:8501`

#### Option B: Command-Line Query Script

```bash
# Show all bars
python query_venues.py --type bar

# Show restaurants in Dallas
python query_venues.py --type restaurant --city Dallas

# Show top 10 venues
python query_venues.py --limit 10

# Show venues that are opening soon
python query_venues.py --status opening_soon
```

#### Option C: SQL Directly

**`venues`** - Deduplicated list of bars and restaurants
```sql
SELECT name, city, venue_type, status, first_seen_date
FROM venues
WHERE venue_type = 'bar'
ORDER BY priority_score DESC
LIMIT 10;
```

**`source_events`** - Raw events from each data source
```sql
SELECT source_system, event_type, raw_name, event_date
FROM source_events
ORDER BY event_date DESC
LIMIT 20;
```

**`etl_runs`** - Pipeline execution history
```sql
SELECT * FROM etl_runs ORDER BY run_started_at DESC LIMIT 5;
```

## Database Schema

### venues
- `id` - Primary key
- `name` - Business name (original)
- `normalized_name` - Cleaned name for matching
- `address` - Street address (original)
- `normalized_address` - Cleaned address for matching
- `city` - City (Dallas, Fort Worth, etc.)
- `venue_type` - `'bar'` or `'restaurant'`
- `status` - `'permitting'`, `'opening_soon'`, `'open'`, or `'unknown'`
- `first_seen_date` - First appearance in data
- `last_seen_date` - Most recent appearance
- `priority_score` - Ranking score (bars > restaurants)

### source_events
- `id` - Primary key
- `venue_id` - Foreign key to venues
- `source_system` - `'TABC'`, `'DALLAS_CO'`, or `'FORTWORTH_CO'`
- `event_type` - `'license_issued'` or `'co_issued'`
- `event_date` - Date of the event
- `raw_name` - Original business name from source
- `raw_address` - Original address from source
- `payload_json` - Full JSON of original record

## Data Sources

### TABC - Texas Alcoholic Beverage Commission
- **What:** New liquor license applications
- **Coverage:** All of Texas (filtered to DFW counties)
- **Signal:** Early-stage indicator (permitting phase)
- **API:** Texas Socrata Open Data
- **URL:** https://data.texas.gov/dataset/TABC-License-Information/7hf9-qc9f

### Dallas - Certificates of Occupancy
- **What:** Building approvals for restaurant/bar occupancy
- **Coverage:** City of Dallas only
- **Signal:** Late-stage indicator (ready to open)
- **API:** Dallas Socrata Open Data
- **URL:** https://www.dallasopendata.com/Services/Building-Inspection-Certificates-Of-Occupancy/9qet-qt9e

### Fort Worth - Certificates of Occupancy (CSV Fallback)
- **What:** Building approvals for restaurant/bar occupancy
- **Coverage:** City of Fort Worth only
- **Signal:** Late-stage indicator (ready to open)
- **Status:** 📁 **Using CSV fallback** - The Fort Worth open data portal API is currently unavailable. This module uses a local CSV file as a data source.

**To use Fort Worth CO data:**

1. **Download the data from Fort Worth's portal:**
   - Visit https://data.fortworthtexas.gov/
   - Search for "Certificates of Occupancy"
   - Export the dataset to CSV format

2. **Prepare the CSV file:**
   - Ensure it has these columns: `co_number`, `business_name`, `address`, `city`, `zip`, `use_description`, `issue_date`
   - Filter to restaurant/bar uses if needed (or let the pipeline filter by date)
   - Save the file as `data/fortworth_co.csv` in your project directory

3. **Run the pipeline:**
   ```bash
   python run_etl.py --days 30
   ```

The pipeline will automatically load Fort Worth CO data from the CSV file and merge it with TABC and Dallas data.

**CSV Format Example:**
```csv
co_number,business_name,address,city,zip,use_description,issue_date
123456,"New Restaurant","123 Main St","Fort Worth","76102","RESTAURANT","2025-11-15"
123457,"Sample Bar","456 Oak Ave","Fort Worth","76104","BAR","2025-11-16"
```

**Note:** When the Fort Worth API becomes available again, the module can be updated to use it instead of the CSV file.

## How It Works

1. **Extract** - Fetches recent records from TABC and Dallas CO APIs, loads Fort Worth CO from CSV
2. **Transform** - Normalizes data into standardized `source_events` format
3. **Load** - Inserts events into SQLite database
4. **Merge** - Matches events to venues using fuzzy name/address matching
5. **Classify** - Infers venue type (bar/restaurant) and status (permitting/opening_soon)
6. **Deduplicate** - Updates existing venues when new events are found for the same location

## Project Structure

```
dfw_openings/
├── config.py              # Configuration and constants
├── db.py                  # Database operations
├── run_etl.py            # Main orchestration script
├── etl/
│   ├── tabc.py           # TABC data fetcher
│   ├── dallas_co.py      # Dallas CO fetcher
│   ├── fortworth_co.py   # Fort Worth CO CSV loader
│   └── merge.py          # Venue matching logic
├── utils/
│   └── normalize.py      # Name/address normalization
├── data/
│   └── fortworth_co.csv  # Fort Worth CO data (user-provided)
├── requirements.txt      # Python dependencies
├── .env.example         # Example environment config
└── README.md            # This file
```

## Development

### Running a Test Fetch

```bash
# Test with just the last day to see if APIs are working
python run_etl.py --days 1
```

### Inspecting the Database

```bash
# Open SQLite CLI
sqlite3 dfw_openings.sqlite

# View schema
.schema venues

# Run queries
SELECT COUNT(*) FROM venues;
SELECT * FROM venues WHERE city = 'Dallas' LIMIT 5;

# Exit
.quit
```

### Adding More Data Sources

To add a new data source:

1. Create a new module in `etl/` (e.g., `etl/plano_co.py`)
2. Implement `fetch_*_since()` and `to_source_events()` functions
3. Add the source to `run_etl.py` orchestration
4. Update `config.py` with any new constants

## Limitations & Future Enhancements

**Current Limitations:**
- Fort Worth CO endpoint needs to be verified and updated
- No geocoding (lat/long) - addresses are not validated
- Name/address matching is simple (no fuzzy matching library)
- No deduplication across different variations of the same name

**Potential Enhancements:**
- Add geocoding to validate addresses and populate lat/long
- Use Google Places API or similar to enrich venue data
- Add web scraping for additional sources (Eater Dallas, CultureMap, etc.)
- Implement email/Slack notifications for new high-priority venues
- Add a simple web UI for browsing results
- Schedule regular runs using cron or similar

## Troubleshooting

**"No module named 'dotenv'"**
- Make sure you activated the virtual environment: `source .venv/bin/activate`
- Install dependencies: `pip install -r requirements.txt`

**"No records found"**
- Try increasing `--days` (e.g., `--days 90`) for initial runs
- Check that APIs are accessible from your network
- Verify your `SOCRATA_APP_TOKEN` if using one

**"Fort Worth CO: Endpoint not configured"**
- This is expected - the Fort Worth endpoint needs to be manually updated
- See the Fort Worth section above for how to help identify the correct endpoint
- The pipeline will continue to work with just TABC and Dallas data

## License

This is a personal/educational project. The data comes from public government sources. Please review the terms of use for each data source before any commercial use.

## Contributing

Found the Fort Worth CO endpoint? Have improvements to the matching logic?

1. Fork the repo
2. Make your changes
3. Test with `python run_etl.py --days 7`
4. Submit a pull request

## Questions?

Open an issue or contact the maintainer.

---

**Built with ❤️ for the DFW food & beverage community**
