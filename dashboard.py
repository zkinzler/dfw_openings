#!/usr/bin/env python3
"""
DFW Restaurant & Bar Openings Dashboard
A Streamlit web interface for exploring venue data from TABC, Dallas CO, and Fort Worth CO.
"""

import streamlit as st
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from streamlit_autorefresh import st_autorefresh
from config import DB_PATH


# ============================================================================
# Database Helpers
# ============================================================================

@st.cache_resource
def get_connection():
    """Get a cached database connection."""
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


@st.cache_data(ttl=60)
def load_venues():
    """Load all venues from the database."""
    conn = get_connection()
    # Include new fields in query
    df = pd.read_sql_query("SELECT * FROM venues", conn)
    return df


@st.cache_data(ttl=60)
def load_source_events(venue_id: int):
    """Load source events for a specific venue."""
    conn = get_connection()
    df = pd.read_sql_query(
        "SELECT * FROM source_events WHERE venue_id = ? ORDER BY event_date DESC",
        conn,
        params=(venue_id,),
    )
    return df


@st.cache_data(ttl=60)
def load_etl_runs():
    """Load ETL run history."""
    conn = get_connection()
    df = pd.read_sql_query(
        "SELECT * FROM etl_runs ORDER BY run_started_at DESC LIMIT 10",
        conn
    )
    return df


# ============================================================================
# Page Configuration
# ============================================================================

st.set_page_config(
    page_title="DFW Openings Dashboard",
    page_icon="🍽️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Auto-refresh every 60 seconds (60000 milliseconds)
st_autorefresh(interval=60000, limit=None, key="data_refresh")

st.title("🍽️ DFW Restaurant & Bar Openings")
st.caption("Data from TABC, Dallas CO, Fort Worth CO, and Texas Sales Tax")

# ============================================================================
# Load Data
# ============================================================================

try:
    df_venues = load_venues()

    if df_venues.empty:
        st.warning("⚠️ No venues found in database. Please run `python run_etl.py` first.")
        st.stop()

except Exception as e:
    st.error(f"❌ Error loading data: {e}")
    st.info("Make sure you've run `python run_etl.py` to populate the database.")
    st.stop()


# ============================================================================
# Sidebar Filters
# ============================================================================

st.sidebar.header("🔍 Filters")

# City filter
all_cities = sorted(df_venues["city"].dropna().unique().tolist())
selected_cities = st.sidebar.multiselect(
    "Cities",
    options=all_cities,
    default=all_cities,
    help="Select one or more cities to filter"
)

# Venue type filter
all_types = sorted(df_venues["venue_type"].dropna().unique().tolist())
selected_types = st.sidebar.multiselect(
    "Venue Types",
    options=all_types,
    default=all_types,
    help="Filter by bar or restaurant"
)

# Status filter
all_statuses = sorted(df_venues["status"].dropna().unique().tolist())
selected_statuses = st.sidebar.multiselect(
    "Status",
    options=all_statuses,
    default=all_statuses,
    help="Filter by permitting, opening_soon, etc."
)

# Phone filter
has_phone = st.sidebar.checkbox("Has Phone Number", value=False)

# Date range filter
st.sidebar.subheader("Date Range")
st.sidebar.caption("Filter by first_seen_date")

# Quick date range presets
date_preset = st.sidebar.selectbox(
    "Quick Select",
    options=[
        "Last 7 Days",
        "Last 30 Days",
        "Last 90 Days",
        "Last 6 Months",
        "Last Year",
        "All Time",
        "Custom Range"
    ],
    index=1,  # Default to Last 30 Days
    help="Choose a preset date range or select Custom Range for manual selection"
)

# Calculate date range based on preset
today = datetime.now().date()
if date_preset == "Last 7 Days":
    start_date = today - timedelta(days=7)
    end_date = today
elif date_preset == "Last 30 Days":
    start_date = today - timedelta(days=30)
    end_date = today
elif date_preset == "Last 90 Days":
    start_date = today - timedelta(days=90)
    end_date = today
elif date_preset == "Last 6 Months":
    start_date = today - timedelta(days=180)
    end_date = today
elif date_preset == "Last Year":
    start_date = today - timedelta(days=365)
    end_date = today
elif date_preset == "All Time":
    if not df_venues["first_seen_date"].isna().all():
        start_date = pd.to_datetime(df_venues["first_seen_date"].min()).date()
    else:
        start_date = today - timedelta(days=365)
    end_date = today
else:  # Custom Range
    start_date = today - timedelta(days=30)
    end_date = today

# Get min/max dates from data
if not df_venues["first_seen_date"].isna().all():
    min_date = pd.to_datetime(df_venues["first_seen_date"].min()).date()
    max_date = pd.to_datetime(df_venues["first_seen_date"].max()).date()
else:
    min_date = today - timedelta(days=365)
    max_date = today

# Show custom date picker only when Custom Range is selected
if date_preset == "Custom Range":
    date_range = st.sidebar.date_input(
        "Select date range",
        value=(start_date, end_date),
        min_value=min_date,
        max_value=today,
        help="Filter venues by when they were first seen"
    )
    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_date, end_date = date_range

start_date_str = start_date.strftime("%Y-%m-%d")
end_date_str = end_date.strftime("%Y-%m-%d")

# Show selected date range
st.sidebar.caption(f"Showing: {start_date_str} to {end_date_str}")

# Refresh button
if st.sidebar.button("🔄 Refresh Data", use_container_width=True):
    st.cache_data.clear()
    st.rerun()

# Show last ETL run info
st.sidebar.divider()
st.sidebar.subheader("ℹ️ Data Status")
try:
    df_runs = load_etl_runs()
    if not df_runs.empty:
        last_run = df_runs.iloc[0]
        last_run_time = pd.to_datetime(last_run["run_started_at"])
        st.sidebar.caption(f"Last ETL run: {last_run_time.strftime('%Y-%m-%d %H:%M')}")
        st.sidebar.caption(f"Lookback: {last_run['lookback_days']} days")
except Exception:
    pass


# ============================================================================
# Apply Filters
# ============================================================================

mask = pd.Series(True, index=df_venues.index)

# City filter
if selected_cities:
    mask &= df_venues["city"].isin(selected_cities)

# Venue type filter
if selected_types:
    mask &= df_venues["venue_type"].isin(selected_types)

# Status filter
if selected_statuses:
    mask &= df_venues["status"].isin(selected_statuses)

# Phone filter
if has_phone:
    mask &= df_venues["phone"].notna() & (df_venues["phone"] != "")

# Date range filter
if start_date_str and end_date_str:
    mask &= (df_venues["first_seen_date"] >= start_date_str) & (df_venues["first_seen_date"] <= end_date_str)

filtered = df_venues[mask].copy()


# ============================================================================
# Stats Row
# ============================================================================

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Total Venues (DB)", len(df_venues))

with col2:
    st.metric("Filtered Venues", len(filtered))

with col3:
    bars_count = len(filtered[filtered["venue_type"] == "bar"])
    st.metric("Bars", bars_count)

with col4:
    restaurants_count = len(filtered[filtered["venue_type"] == "restaurant"])
    st.metric("Restaurants", restaurants_count)

st.divider()


# ============================================================================
# Map View
# ============================================================================

st.subheader("🗺️ Map View")

# Filter for venues with coordinates
map_data = filtered.dropna(subset=["latitude", "longitude"]).copy()

if not map_data.empty:
    st.map(
        map_data,
        latitude="latitude",
        longitude="longitude",
        size=20,
        color="#FF4B4B"  # Streamlit red
    )
    st.caption(f"Showing {len(map_data)} venues with coordinates.")
else:
    st.info("No venues with coordinates found. Run `python run_geocoding.py` to fetch locations.")

st.divider()


# ============================================================================
# Venues Table
# ============================================================================

st.subheader("📋 Venues")

if filtered.empty:
    st.info("No venues match the current filters. Try adjusting your selections.")
else:
    # Sort by priority and date
    filtered_sorted = filtered.sort_values(
        by=["priority_score", "first_seen_date"],
        ascending=[False, False]
    )

    # Select display columns
    display_cols = [
        "name",
        "address",
        "city",
        "venue_type",
        "status",
        "phone",
        "website",
        "first_seen_date",
        "priority_score"
    ]

    # Ensure columns exist (for migration safety)
    available_cols = [c for c in display_cols if c in filtered_sorted.columns]
    filtered_display = filtered_sorted[available_cols].copy()

    # Rename columns for better display
    column_map = {
        "name": "Name",
        "address": "Address",
        "city": "City",
        "venue_type": "Type",
        "status": "Status",
        "phone": "Phone",
        "website": "Website",
        "first_seen_date": "First Seen",
        "priority_score": "Priority"
    }
    filtered_display.rename(columns=column_map, inplace=True)

    st.dataframe(
        filtered_display,
        use_container_width=True,
        hide_index=True,
        height=400,
        column_config={
            "Website": st.column_config.LinkColumn("Website"),
        }
    )

    # Download button
    csv = filtered_display.to_csv(index=False)
    st.download_button(
        label="📥 Download CSV",
        data=csv,
        file_name=f"dfw_venues_{datetime.now().strftime('%Y%m%d')}.csv",
        mime="text/csv"
    )


# ============================================================================
# Venue Detail Section
# ============================================================================

st.divider()
st.subheader("🔍 Venue Details")

if not filtered.empty:
    # Create options for selectbox
    options = filtered_sorted.copy()

    # Build label map
    label_map = {
        int(row["id"]): f"{row['name']} – {row['city']} ({row['venue_type']})"
        for _, row in options.iterrows()
    }

    selected_id = st.selectbox(
        "View details for a specific venue:",
        options=list(label_map.keys()),
        format_func=lambda vid: label_map[vid],
        key="venue_selector"
    )

    if selected_id:
        # Get venue details
        venue = df_venues[df_venues["id"] == selected_id].iloc[0]

        # Display venue card
        col1, col2 = st.columns([2, 1])

        with col1:
            st.markdown(f"### {venue['name']}")
            st.markdown(f"**Address:** {venue['address']}")
            if venue['city'] or venue['state'] or venue['zip']:
                location = f"{venue['city']}, {venue['state']} {venue['zip']}".strip()
                st.markdown(f"**Location:** {location}")
            
            # Contact Info
            st.markdown("#### Contact Info")
            if venue.get('phone'):
                st.markdown(f"📞 **Phone:** {venue['phone']}")
            else:
                st.markdown("📞 **Phone:** N/A")
                
            if venue.get('website'):
                st.markdown(f"🌐 **Website:** [{venue['website']}]({venue['website']})")
            else:
                st.markdown("🌐 **Website:** N/A")

        with col2:
            st.markdown(f"**Type:** {venue['venue_type']}")
            st.markdown(f"**Status:** {venue['status']}")
            st.markdown(f"**Priority Score:** {venue['priority_score']}")
            st.markdown(f"**First Seen:** {venue['first_seen_date']}")
            st.markdown(f"**Last Seen:** {venue['last_seen_date']}")
            
            if venue.get('google_place_id'):
                st.markdown(f"[View on Google Maps](https://www.google.com/maps/place/?q=place_id:{venue['google_place_id']})")

        st.divider()

        # Load and display source events
        st.subheader("📄 Source Events")

        try:
            df_events = load_source_events(selected_id)

            if not df_events.empty:
                # Display events with clickable links
                st.markdown("Click on source links to view the original permit/license data:")

                for idx, event in df_events.iterrows():
                    with st.container():
                        col1, col2, col3 = st.columns([2, 2, 1])

                        with col1:
                            st.markdown(f"**{event['source_system']}** - {event['event_type']}")
                            st.caption(f"📅 {event['event_date']}")

                        with col2:
                            st.markdown(f"**{event['raw_name']}**")
                            st.caption(f"📍 {event['raw_address']}, {event['city']}")

                        with col3:
                            # Create source-specific links
                            if event['source_system'] == 'TABC':
                                # TABC search link - search by business name
                                search_name = event['raw_name'].replace(' ', '+')
                                link_url = f"https://www.tabc.texas.gov/verify-license-permit/"
                                st.markdown(f"[🔗 View on TABC]({link_url})")
                            elif event['source_system'] == 'DALLAS_CO':
                                # Dallas CO portal
                                link_url = "https://www.dallasopendata.com/Services/Building-Inspection-Certificates-Of-Occupancy/9qet-qt9e"
                                st.markdown(f"[🔗 View Dallas CO]({link_url})")
                            elif event['source_system'] == 'FORTWORTH_CO' and event['url']:
                                st.markdown(f"[🔗 View Source]({event['url']})")
                            elif event['source_system'] == 'SALES_TAX':
                                st.markdown(f"[🔗 View Source]({event['url']})")
                            else:
                                st.caption("No link available")

                        st.divider()

                # Show raw payload in expander
                with st.expander("View Raw Event Data (JSON)"):
                    for idx, event in df_events.iterrows():
                        st.markdown(f"**{event['source_system']} - {event['event_date']}**")
                        st.json(event["payload_json"])

            else:
                st.info("⚠️ No source events found for this venue (unexpected).")

        except Exception as e:
            st.error(f"Error loading source events: {e}")
else:
    st.info("Select filters above to see venues and their details.")


# ============================================================================
# Footer
# ============================================================================

st.divider()
st.caption("💡 **Tip:** Use the filters in the sidebar to narrow down results. Click 'Refresh Data' to reload after running ETL.")
