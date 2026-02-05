# DFW POS Lead Tracker - Project Status & Next Steps

**Last Updated:** 2026-02-04
**Status:** Phase 1 Complete - Ready for Enrichment
**GitHub:** https://github.com/zkinzler/dfw_openings

---

## What This Tool Does

A lead tracking system for selling POS/credit card processing systems (like Toast, Square, Clover) to new restaurants and bars in Dallas-Fort Worth. The goal is to find venues EARLY (during permitting stage) before competitors reach them.

### Current Capabilities

1. **ETL Pipeline** - Automatically pulls data from 4 sources:
   - TABC (Texas liquor licenses)
   - Texas Sales Tax permits
   - Dallas building permits
   - Fort Worth building permits

2. **Lead Management** - Full sales workflow:
   - Lead statuses: New → Contacted → Demo Scheduled → Won/Lost
   - Activity logging (calls, emails, visits, demos)
   - Follow-up reminders
   - Notes per venue

3. **Priority Scoring** - Urgency-based ranking:
   - Recency bonus (newer = hotter)
   - Stage bonus (opening_soon > permitting)
   - Contact info bonus (has phone = higher priority)
   - Venue type (bars slightly higher than restaurants)

4. **Dashboard** - Streamlit web interface:
   - Hot Leads tab (new leads, sorted by priority)
   - Pipeline view (kanban-style)
   - Map view (geocoded venues)
   - Analytics (win rate, city performance, source effectiveness)

5. **Notifications** - Slack/Email alerts:
   - Hot lead alerts
   - Follow-up reminders
   - Daily digest

---

## What Was Done (2026-02-04 Session)

### 1. Implemented Phase 1 Lead Management
- Added `lead_status`, `next_follow_up`, `competitor`, `lost_reason` columns to venues
- Created `lead_activities` table for tracking all outreach
- Built `services/lead_service.py` with workflow functions
- Rewrote priority scoring algorithm for urgency-based ranking
- Enhanced dashboard with 5 tabs: Hot Leads, Pipeline, All Leads, Map, Analytics
- Added quick action buttons (Mark Contacted, Schedule Demo, etc.)
- Added click-to-call phone links
- Updated notifications for hot leads and follow-up reminders

### 2. Due Diligence & Data Cleanup
- Identified critical issues (0% phone numbers, junk data)
- Created cleanup scripts in `scripts/` directory
- Removed 174 junk venues (liquor stores, convenience stores, construction companies)
- Normalized city names (fixed DALLAS vs Dallas inconsistencies)
- Exported clean lead list (476 venues)

### 3. Documentation Created
- `DUE_DILIGENCE_REPORT.md` - Technical findings on data quality
- `USABILITY_PLAN.md` - How to make it great for the client
- `PROJECT_STATUS.md` - This file

---

## Current Data Summary

| Metric | Value |
|--------|-------|
| Total clean leads | 476 |
| Bars | 40 |
| Restaurants | 212 |
| Unknown type | 224 |
| Has phone | 0% (needs enrichment) |
| Geocoded | ~5% |
| Hot leads (last 7 days) | ~47 |

### Top Cities
- Fort Worth: 133
- Dallas: 91
- Arlington: 33

---

## Immediate Next Steps

### 1. Google Places Enrichment (CRITICAL - No Phone Numbers Yet!)
```bash
# Set API key in .env
export GOOGLE_PLACES_API_KEY='your-key-here'

# Test with 10 venues
python scripts/enrich_venues.py --limit 10

# Enrich all high-priority leads
python scripts/enrich_venues.py --priority-only
```
**Cost:** ~$10-15 for full enrichment (~$17 per 1,000 lookups)

### 2. Run Geocoding (for map)
```bash
python run_geocoding.py
```

### 3. Recalculate Priority Scores
```bash
python run_scoring.py
```

### 4. Start Dashboard
```bash
streamlit run dashboard.py
```

---

## File Structure

```
dfw_openings/
├── dashboard.py          # Main Streamlit dashboard
├── db.py                 # Database schema and operations
├── config.py             # Configuration and API endpoints
├── run_etl.py            # ETL orchestration
├── run_geocoding.py      # Geocode venues (OpenStreetMap)
├── run_scoring.py        # Recalculate priority scores
│
├── etl/                  # Data source modules
│   ├── tabc.py           # TABC liquor licenses
│   ├── sales_tax.py      # Texas sales tax permits
│   ├── dallas_permits.py # Dallas building permits
│   ├── fortworth_permits.py
│   └── merge.py          # Priority scoring & venue matching
│
├── services/
│   └── lead_service.py   # Lead management business logic
│
├── utils/
│   ├── normalize.py      # Name/address normalization
│   ├── geocode.py        # Geocoding (Nominatim)
│   ├── google_places.py  # Google Places enrichment
│   └── notifications.py  # Slack/Email alerts
│
├── scripts/              # Utility scripts
│   ├── clean_data.py     # Remove junk venues
│   ├── normalize_cities.py
│   └── enrich_venues.py  # Google Places enrichment
│
├── data/                 # CSV data files
├── DUE_DILIGENCE_REPORT.md
├── USABILITY_PLAN.md
└── PROJECT_STATUS.md     # This file
```

---

## Environment Variables

Required in `.env`:
```bash
# Database
DFW_DB_PATH=dfw_openings.sqlite

# API Keys (optional but recommended)
GOOGLE_PLACES_API_KEY=     # For phone/website enrichment
SOCRATA_APP_TOKEN=         # For faster API access

# Notifications (optional)
SLACK_WEBHOOK_URL=         # Slack alerts
SMTP_USER=                 # Email alerts
SMTP_PASSWORD=
EMAIL_RECIPIENT=
```

---

## Ideas for Improvement

### Short-term (This Month)

1. **Fix Dallas Permit ETL** - Currently pulling contractor names instead of restaurant names. Need to find actual tenant/business name field or mark as "address only" leads.

2. **Add More Data Sources:**
   - Certificate of Occupancy data (means they're about to open)
   - Google Maps new business listings
   - Yelp new restaurant alerts
   - Social media mentions of "opening soon"

3. **Mobile Optimization** - Make dashboard work better on phone for field sales

4. **Daily Email Digest** - Automatic morning email with hot leads

### Medium-term (Next Quarter)

5. **Competitor Tracking** - Record which competitor won lost deals, analyze patterns

6. **Route Planning** - Optimize driving routes for field visits

7. **Calendar Integration** - Sync demos to Google Calendar

8. **Email/SMS Templates** - One-click follow-up messages

---

## Expansion Opportunities

### Other Businesses That Need POS/Payment Systems

Beyond restaurants and bars, these businesses also need credit card processing:

#### High-Volume Retail
| Business Type | Why They Need POS | Data Source |
|--------------|-------------------|-------------|
| Convenience stores | High transaction volume | Texas Sales Tax (NAICS retail codes) |
| Gas stations | Outdoor payment terminals | Fuel retailer licenses |
| Liquor stores | Cash-heavy, need POS | TABC retail licenses |

#### Food Service Adjacent
| Business Type | Why They Need POS | Data Source |
|--------------|-------------------|-------------|
| Food trucks | Mobile POS solutions | Mobile food vendor permits |
| Catering companies | Invoice + payment | Health dept food service licenses |
| Ghost kitchens | Online ordering integration | Commercial kitchen permits |
| Coffee shops | High volume, tips | Same as restaurants |

#### Service Businesses
| Business Type | Why They Need POS | Data Source |
|--------------|-------------------|-------------|
| Salons/Spas | Appointment + POS combo | Texas TDLR cosmetology licenses |
| Gyms/Fitness studios | Membership + retail | Gym facility permits |
| Auto repair shops | High ticket transactions | Auto repair registrations |
| Medical/Dental offices | Payment plans | Professional licenses |

#### Entertainment & Hospitality
| Business Type | Why They Need POS | Data Source |
|--------------|-------------------|-------------|
| Event venues | Ticketing + concessions | Venue permits |
| Hotels | Front desk, restaurant, bar, spa | Hotel occupancy tax registrations |
| Vacation rentals | Payment processing | Short-term rental permits |

### New Data Sources to Consider

1. **Texas Comptroller** - Sales tax permits by NAICS code
   - Can filter for retail, food service, hospitality
   - API: https://data.texas.gov

2. **County Assumed Name Certificates** - New business filings
   - Dallas County, Tarrant County clerk records

3. **Commercial Real Estate** - New lease signings
   - CoStar, LoopNet (paid)
   - "Coming soon" signs

4. **Social Media**
   - Instagram location tags for "opening soon"
   - Facebook business page creation

5. **Google Maps API**
   - New place additions
   - "Opening soon" status

6. **Health Department**
   - Food service establishment permits
   - Food handler certifications

---

## Running the System

### Daily Operations
```bash
# Activate environment
source .venv/bin/activate

# Fetch new leads (run daily via cron)
python run_etl.py --cities working --days 7

# Send notifications
python utils/notifications.py hot_leads
```

### Weekly Maintenance
```bash
# Full ETL run
python run_etl.py --cities working --days 30

# Geocode new venues
python run_geocoding.py

# Recalculate scores
python run_scoring.py

# Enrich new venues (if API key set)
python scripts/enrich_venues.py --priority-only
```

### Starting the Dashboard
```bash
streamlit run dashboard.py
```
Dashboard runs at: http://localhost:8501

---

## Known Issues

1. **No phone numbers** - Need Google Places API key to enrich
2. **Dallas Permits ETL broken** - Pulling contractor names, not restaurant names
3. **224 venues with unknown type** - Need better classification or manual review
4. **Only 5% geocoded** - Need to run geocoding script

See `DUE_DILIGENCE_REPORT.md` for full details.

---

## Quick Start for Next Session

```bash
cd /Users/zachkinzler/dfw_openings
source .venv/bin/activate

# Check current status
python -c "import db; conn=db.get_connection(); c=conn.cursor(); c.execute('SELECT COUNT(*) FROM venues'); print(f'Venues: {c.fetchone()[0]}')"

# Start dashboard
streamlit run dashboard.py
```

---

*Built for POS/payment sales in DFW. Find them early, close them fast.*
