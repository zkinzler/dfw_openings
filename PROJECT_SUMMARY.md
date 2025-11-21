# DFW Restaurant/Bar Openings Pipeline - Project Summary

**Created:** November 2025
**Author:** Built with Claude Code
**Purpose:** Track new and upcoming bars and restaurants in the Dallas-Fort Worth area

---

## 📋 Project Overview

This project is a complete ETL (Extract, Transform, Load) pipeline that automatically discovers and tracks new bars and restaurants across the DFW metroplex by monitoring public data sources from:

- **TABC** (Texas Alcoholic Beverage Commission) - Liquor license applications
- **Dallas** - Certificates of Occupancy
- **Fort Worth** - Certificates of Occupancy (CSV import)

---

## 🏗️ Project Structure

```
dfw_openings/
├── config.py                    # Configuration and environment variables
├── db.py                        # SQLite database operations
├── run_etl.py                   # Main ETL orchestration script
├── dashboard.py                 # Streamlit web dashboard
├── query_venues.py              # CLI query utility
├── etl/
│   ├── __init__.py
│   ├── tabc.py                  # TABC liquor license fetcher
│   ├── dallas_co.py             # Dallas CO fetcher
│   ├── fortworth_co.py          # Fort Worth CO CSV loader
│   └── merge.py                 # Venue matching and deduplication
├── utils/
│   ├── __init__.py
│   └── normalize.py             # Name/address normalization
├── data/
│   ├── fortworth_co_sample.csv  # Example CSV format
│   └── fortworth_co.csv         # User-provided Fort Worth data (gitignored)
├── .venv/                       # Python virtual environment (gitignored)
├── dfw_openings.sqlite          # SQLite database (gitignored)
├── requirements.txt             # Python dependencies
├── .env.example                 # Environment variable template
├── .gitignore                   # Git ignore rules
└── README.md                    # Full documentation
```

---

## 💾 Database Schema

### Tables

**`venues`** - Deduplicated list of bars and restaurants
- `id` - Primary key
- `name` - Business name
- `normalized_name` - Cleaned name for matching
- `address` - Street address
- `normalized_address` - Cleaned address for matching
- `city`, `state`, `zip` - Location
- `venue_type` - 'bar' or 'restaurant'
- `status` - 'permitting', 'opening_soon', 'open', 'unknown'
- `first_seen_date` - First detected
- `last_seen_date` - Most recent detection
- `priority_score` - Ranking (bars > restaurants)

**`source_events`** - All raw events from data sources
- `id` - Primary key
- `venue_id` - Links to venues table
- `source_system` - 'TABC', 'DALLAS_CO', 'FORTWORTH_CO'
- `event_type` - 'license_issued', 'co_issued'
- `event_date` - When the event occurred
- `raw_name`, `raw_address`, `city` - Original data
- `url` - Link to source (if available)
- `payload_json` - Full original record

**`etl_runs`** - Pipeline execution history
- `id` - Primary key
- `run_started_at`, `run_finished_at` - Timestamps
- `lookback_days` - How far back the run looked
- `rows_tabc`, `rows_dallas_co`, `rows_fortworth_co` - Counts
- `notes` - Additional info

---

## 🚀 How to Use

### Initial Setup

```bash
cd ~/dfw_openings
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Run the ETL Pipeline

```bash
# Fetch last 7 days (default)
python run_etl.py

# Fetch last 30 days
python run_etl.py --days 30

# Fetch last 90 days
python run_etl.py --days 90
```

### View the Web Dashboard

```bash
streamlit run dashboard.py
# Opens at http://localhost:8501
```

### Query from Command Line

```bash
python query_venues.py --type bar --city Dallas --limit 10
```

---

## 📊 Current Data Status

As of last run:
- **126 venues** tracked (12 bars, 114 restaurants)
- **154 source events** from TABC
- **30-day lookback** period
- **Data sources:**
  - ✅ TABC: Working (154 records)
  - ❌ Dallas CO: Working but 0 recent records
  - ❌ Fort Worth CO: Requires manual CSV download

---

## 🎯 Data Pipeline Flow

1. **Extract** - Fetch data from TABC API, Dallas CO API, Fort Worth CSV
2. **Transform** - Normalize names/addresses, classify venue types
3. **Load** - Insert into SQLite database
4. **Merge** - Match events to venues, deduplicate
5. **Classify** - Infer venue type (bar/restaurant) and status

### Status Meanings

- **`permitting`** - Just received liquor license (6-18 months from opening)
- **`opening_soon`** - Received Certificate of Occupancy (weeks from opening)
- **`open`** - Manually verified as open

### Venue Type Detection

**Bars** - Detected by keywords in name or license type:
- bar, pub, taproom, saloon, tavern, lounge, brewery, brewpub

**Restaurants** - Everything else with a liquor license

---

## 🔧 Configuration

### Environment Variables (optional)

Create a `.env` file:

```env
# Optional Socrata app token for better rate limits
SOCRATA_APP_TOKEN=your_token_here

# Optional Fort Worth ArcGIS token (if needed)
FORTWORTH_ARCGIS_TOKEN=

# Database path (default: dfw_openings.sqlite)
DFW_DB_PATH=dfw_openings.sqlite

# Default lookback period (default: 7)
DEFAULT_LOOKBACK_DAYS=7

# Fort Worth CSV path (default: data/fortworth_co.csv)
FORTWORTH_CO_CSV_PATH=data/fortworth_co.csv
```

---

## 📝 Fort Worth CO Data (Manual Process)

Since the Fort Worth API is currently unavailable:

1. Visit https://data.fortworthtexas.gov/
2. Search for "Certificates of Occupancy"
3. Export to CSV with these columns:
   - `co_number`, `business_name`, `address`, `city`, `zip`, `use_description`, `issue_date`
4. Save as `data/fortworth_co.csv`
5. Run `python run_etl.py --days 30`

---

## 🎨 Web Dashboard Features

**Filters:**
- City (multi-select)
- Venue type (bar/restaurant)
- Status (permitting/opening_soon/open)
- Date range (by first_seen_date)

**Views:**
- Stats overview (total venues, bars, restaurants)
- Sortable venues table
- CSV export
- Venue detail view with source events
- Clickable links to TABC/Dallas CO portals
- Raw JSON payload inspection

**Access:**
- Local: http://localhost:8501
- Network: http://192.168.1.219:8501 (same WiFi)

---

## 🔄 Recommended Workflow

### Weekly Maintenance

```bash
# Update data weekly
cd ~/dfw_openings
source .venv/bin/activate
python run_etl.py --days 30

# View results
streamlit run dashboard.py
```

### Monthly Fort Worth Update

1. Download Fort Worth CO CSV (monthly)
2. Save as `data/fortworth_co.csv`
3. Run ETL to incorporate new data

---

## 🛠️ Technical Details

**Languages & Frameworks:**
- Python 3.10+
- Streamlit (web dashboard)
- Pandas (data manipulation)
- SQLite (database)
- Requests (HTTP)

**APIs Used:**
- Texas TABC Socrata Open Data
- Dallas Socrata Open Data
- Fort Worth (manual CSV - API unavailable)

**Key Features:**
- Smart name/address normalization
- Cross-source venue deduplication
- Automatic venue type classification
- Priority scoring
- Incremental data updates

---

## 📈 Future Enhancements

**Possible additions:**
- Automated Fort Worth API integration (when available)
- Google Places API integration (verify if open)
- Email/Slack notifications for new high-priority venues
- Geocoding for map visualization
- Fuzzy matching for better deduplication
- Historical tracking (track status changes over time)
- Public deployment (Streamlit Cloud)
- Scheduled automated runs (cron job)

---

## 🐛 Known Issues

1. **Fort Worth CO API unavailable** - Requires manual CSV download
2. **Dallas CO sometimes has 0 records** - Timing issue or no recent issuances
3. **City name inconsistencies** - Some cities have mixed case (Dallas vs DALLAS)
4. **No geocoding** - Addresses not validated, no lat/long

---

## 📞 Support

For questions or issues:
1. Check the README.md for detailed documentation
2. Review this PROJECT_SUMMARY.md for overview
3. Inspect the code comments in each module

---

## 🎉 Project Status

**Status:** ✅ Fully Functional
**Last Updated:** November 2025
**Current Data:** 126 venues, 154 events, 30-day lookback

**What's Working:**
- ✅ TABC data extraction
- ✅ Dallas CO data extraction
- ✅ Fort Worth CO CSV import
- ✅ Venue deduplication
- ✅ Type classification
- ✅ Web dashboard
- ✅ CLI queries
- ✅ Database storage

**Next Steps:**
- 📥 Download Fort Worth CO CSV manually
- 🔄 Set up weekly ETL schedule
- 🌐 Consider public deployment

---

**Built with ❤️ for the DFW food & beverage community**
