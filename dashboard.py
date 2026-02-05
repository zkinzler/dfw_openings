#!/usr/bin/env python3
"""
DFW Restaurant & Bar Openings Dashboard
A Streamlit web interface for POS/Payment lead tracking.
"""

import streamlit as st
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from streamlit_autorefresh import st_autorefresh
from config import DB_PATH
import db
from services.lead_service import (
    LEAD_STATUSES, ACTIVITY_TYPES, ACTIVITY_OUTCOMES, LOST_REASONS,
    mark_contacted, schedule_demo, mark_won, mark_lost, mark_not_interested,
    log_call, update_follow_up, update_notes, get_pipeline_metrics,
    get_activity_summary, get_source_effectiveness, get_city_performance
)


# ============================================================================
# Database Helpers
# ============================================================================

@st.cache_resource
def get_connection():
    """Get a cached database connection."""
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    # Ensure schema is up to date (runs migrations if needed)
    db.ensure_schema(conn)
    return conn


@st.cache_data(ttl=60)
def load_venues():
    """Load all venues from the database."""
    conn = get_connection()
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
def load_lead_activities(venue_id: int):
    """Load lead activities for a specific venue."""
    conn = get_connection()
    df = pd.read_sql_query(
        "SELECT * FROM lead_activities WHERE venue_id = ? ORDER BY activity_date DESC, created_at DESC",
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


def refresh_data():
    """Clear cache and refresh data."""
    st.cache_data.clear()


# ============================================================================
# Page Configuration
# ============================================================================

st.set_page_config(
    page_title="DFW POS Lead Tracker",
    page_icon="📞",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Auto-refresh every 60 seconds
st_autorefresh(interval=60000, limit=None, key="data_refresh")

st.title("📞 DFW POS Lead Tracker")
st.caption("Track and close POS/credit card system deals for new restaurants & bars")

# ============================================================================
# Load Data
# ============================================================================

try:
    df_venues = load_venues()

    if df_venues.empty:
        st.warning("No venues found in database. Please run `python run_etl.py` first.")
        st.stop()

except Exception as e:
    st.error(f"Error loading data: {e}")
    st.info("Make sure you've run `python run_etl.py` to populate the database.")
    st.stop()


# ============================================================================
# Sidebar Filters
# ============================================================================

st.sidebar.header("Filters")

# Lead status filter (PRIMARY for sales workflow)
all_lead_statuses = ['new', 'contacted', 'demo_scheduled', 'won', 'lost', 'not_interested']
# Default to active leads
default_lead_statuses = ['new', 'contacted', 'demo_scheduled']
selected_lead_statuses = st.sidebar.multiselect(
    "Lead Status",
    options=all_lead_statuses,
    default=default_lead_statuses,
    help="Filter by sales pipeline stage"
)

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

# Status filter (venue status: permitting/opening_soon/open)
all_statuses = sorted(df_venues["status"].dropna().unique().tolist())
selected_statuses = st.sidebar.multiselect(
    "Venue Status",
    options=all_statuses,
    default=all_statuses,
    help="Filter by permitting, opening_soon, etc."
)

# Phone filter
has_phone = st.sidebar.checkbox("Has Phone Number", value=False)

# Follow-up filter
needs_followup = st.sidebar.checkbox("Needs Follow-up", value=False,
                                     help="Show leads with past-due follow-up dates")

# Date range filter
st.sidebar.subheader("Date Range")
st.sidebar.caption("Filter by first_seen_date")

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
    index=1,
    help="Choose a preset date range"
)

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
else:
    start_date = today - timedelta(days=30)
    end_date = today

if not df_venues["first_seen_date"].isna().all():
    min_date = pd.to_datetime(df_venues["first_seen_date"].min()).date()
    max_date = pd.to_datetime(df_venues["first_seen_date"].max()).date()
else:
    min_date = today - timedelta(days=365)
    max_date = today

if date_preset == "Custom Range":
    date_range = st.sidebar.date_input(
        "Select date range",
        value=(start_date, end_date),
        min_value=min_date,
        max_value=today,
    )
    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_date, end_date = date_range

start_date_str = start_date.strftime("%Y-%m-%d")
end_date_str = end_date.strftime("%Y-%m-%d")

st.sidebar.caption(f"Showing: {start_date_str} to {end_date_str}")

# Refresh button
if st.sidebar.button("Refresh Data", use_container_width=True):
    refresh_data()
    st.rerun()

# Data status
st.sidebar.divider()
st.sidebar.subheader("Data Status")
try:
    df_runs = load_etl_runs()
    if not df_runs.empty:
        last_run = df_runs.iloc[0]
        last_run_time = pd.to_datetime(last_run["run_started_at"])
        st.sidebar.caption(f"Last ETL: {last_run_time.strftime('%Y-%m-%d %H:%M')}")
except Exception:
    pass


# ============================================================================
# Apply Filters
# ============================================================================

mask = pd.Series(True, index=df_venues.index)

# Lead status filter
if selected_lead_statuses:
    # Handle null lead_status as 'new'
    lead_status_filled = df_venues["lead_status"].fillna('new')
    mask &= lead_status_filled.isin(selected_lead_statuses)

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

# Follow-up filter
if needs_followup:
    today_str = today.strftime("%Y-%m-%d")
    mask &= df_venues["next_follow_up"].notna() & (df_venues["next_follow_up"] <= today_str)

# Date range filter
if start_date_str and end_date_str:
    mask &= (df_venues["first_seen_date"] >= start_date_str) & (df_venues["first_seen_date"] <= end_date_str)

filtered = df_venues[mask].copy()


# ============================================================================
# Tabs for different views
# ============================================================================

tab_hot, tab_pipeline, tab_all, tab_map, tab_analytics = st.tabs([
    "🔥 Hot Leads", "📊 Pipeline", "📋 All Leads", "🗺️ Map", "📈 Analytics"
])


# ============================================================================
# Hot Leads Tab
# ============================================================================

with tab_hot:
    st.subheader("Hot Leads - New in Last 7 Days")
    st.caption("Freshest leads sorted by priority. These should be contacted ASAP!")

    # Get hot leads (new, last 7 days)
    hot_mask = (
        (df_venues["lead_status"].fillna('new') == 'new') &
        (df_venues["first_seen_date"] >= (today - timedelta(days=7)).strftime("%Y-%m-%d"))
    )
    hot_leads = df_venues[hot_mask].sort_values(
        by=["priority_score", "first_seen_date"],
        ascending=[False, False]
    )

    # Stats row
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Hot Leads", len(hot_leads))
    with col2:
        has_phone_count = hot_leads["phone"].notna().sum()
        st.metric("With Phone", has_phone_count)
    with col3:
        bars = len(hot_leads[hot_leads["venue_type"] == "bar"])
        st.metric("Bars", bars)
    with col4:
        restaurants = len(hot_leads[hot_leads["venue_type"] == "restaurant"])
        st.metric("Restaurants", restaurants)

    st.divider()

    if hot_leads.empty:
        st.info("No new hot leads in the last 7 days. Run ETL to fetch fresh data!")
    else:
        for idx, venue in hot_leads.iterrows():
            with st.container():
                col1, col2, col3 = st.columns([3, 2, 2])

                with col1:
                    # Priority badge
                    priority = venue.get('priority_score', 0) or 0
                    if priority >= 100:
                        priority_badge = "🔥🔥🔥"
                    elif priority >= 70:
                        priority_badge = "🔥🔥"
                    elif priority >= 40:
                        priority_badge = "🔥"
                    else:
                        priority_badge = ""

                    st.markdown(f"### {priority_badge} {venue['name']}")
                    st.caption(f"{venue['address']}, {venue['city']}")
                    st.caption(f"Type: {venue['venue_type']} | Status: {venue['status']} | Score: {priority}")

                with col2:
                    # Contact info with click-to-call
                    if venue.get('phone') and pd.notna(venue['phone']):
                        phone = venue['phone']
                        st.markdown(f"📞 [{phone}](tel:{phone})")
                    else:
                        st.caption("📞 No phone")

                    if venue.get('website') and pd.notna(venue['website']):
                        st.markdown(f"🌐 [Website]({venue['website']})")

                with col3:
                    # Quick actions
                    venue_id = int(venue['id'])
                    if st.button("✅ Contacted", key=f"contacted_{venue_id}"):
                        conn = get_connection()
                        mark_contacted(conn, venue_id)
                        refresh_data()
                        st.rerun()

                    if st.button("❌ Not Interested", key=f"not_int_{venue_id}"):
                        conn = get_connection()
                        mark_not_interested(conn, venue_id, reason="not_opening")
                        refresh_data()
                        st.rerun()

            st.divider()


# ============================================================================
# Pipeline Tab
# ============================================================================

with tab_pipeline:
    st.subheader("Sales Pipeline")

    # Get pipeline metrics
    conn = get_connection()
    metrics = get_pipeline_metrics(conn)
    counts = metrics['counts']

    # Pipeline metrics row
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("New", counts.get('new', 0) + counts.get(None, 0))
    with col2:
        st.metric("Contacted", counts.get('contacted', 0))
    with col3:
        st.metric("Demos", counts.get('demo_scheduled', 0))
    with col4:
        st.metric("Won", counts.get('won', 0))
    with col5:
        st.metric("Win Rate", f"{metrics['win_rate']:.0f}%")

    st.divider()

    # Follow-up reminders
    followups = db.get_venues_needing_followup(conn)
    if followups:
        st.warning(f"⏰ {len(followups)} leads need follow-up today or earlier!")

        with st.expander("View Follow-up Reminders", expanded=True):
            for venue in followups[:10]:  # Show top 10
                col1, col2, col3 = st.columns([3, 2, 1])
                with col1:
                    st.markdown(f"**{venue['name']}** - {venue['city']}")
                    st.caption(f"Follow-up: {venue['next_follow_up']}")
                with col2:
                    if venue.get('phone'):
                        st.markdown(f"📞 [{venue['phone']}](tel:{venue['phone']})")
                with col3:
                    if st.button("Done", key=f"followup_done_{venue['id']}"):
                        conn = get_connection()
                        update_follow_up(conn, venue['id'], None)
                        refresh_data()
                        st.rerun()

    st.divider()

    # Pipeline columns
    st.markdown("### Pipeline View")
    col_new, col_contacted, col_demo = st.columns(3)

    with col_new:
        st.markdown("#### New Leads")
        new_leads = df_venues[df_venues["lead_status"].fillna('new') == 'new'].head(10)
        for _, v in new_leads.iterrows():
            with st.container():
                st.markdown(f"**{v['name']}**")
                st.caption(f"{v['city']} | {v['venue_type']}")
                if v.get('phone') and pd.notna(v['phone']):
                    st.caption(f"📞 {v['phone']}")

    with col_contacted:
        st.markdown("#### Contacted")
        contacted = df_venues[df_venues["lead_status"] == 'contacted'].head(10)
        for _, v in contacted.iterrows():
            with st.container():
                st.markdown(f"**{v['name']}**")
                st.caption(f"{v['city']} | {v['venue_type']}")
                if v.get('next_follow_up') and pd.notna(v['next_follow_up']):
                    st.caption(f"📅 Follow-up: {v['next_follow_up']}")

    with col_demo:
        st.markdown("#### Demo Scheduled")
        demos = df_venues[df_venues["lead_status"] == 'demo_scheduled'].head(10)
        for _, v in demos.iterrows():
            with st.container():
                st.markdown(f"**{v['name']}**")
                st.caption(f"{v['city']} | {v['venue_type']}")
                if v.get('next_follow_up') and pd.notna(v['next_follow_up']):
                    st.caption(f"📅 Demo: {v['next_follow_up']}")


# ============================================================================
# All Leads Tab
# ============================================================================

with tab_all:
    st.subheader("All Leads")

    # Stats row
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total (DB)", len(df_venues))
    with col2:
        st.metric("Filtered", len(filtered))
    with col3:
        bars_count = len(filtered[filtered["venue_type"] == "bar"])
        st.metric("Bars", bars_count)
    with col4:
        restaurants_count = len(filtered[filtered["venue_type"] == "restaurant"])
        st.metric("Restaurants", restaurants_count)

    st.divider()

    if filtered.empty:
        st.info("No venues match the current filters.")
    else:
        # Sort by priority and date
        filtered_sorted = filtered.sort_values(
            by=["priority_score", "first_seen_date"],
            ascending=[False, False]
        )

        # Display columns
        display_cols = [
            "name", "address", "city", "venue_type", "status",
            "lead_status", "phone", "first_seen_date", "priority_score"
        ]
        available_cols = [c for c in display_cols if c in filtered_sorted.columns]
        filtered_display = filtered_sorted[available_cols].copy()

        # Fill null lead_status
        if 'lead_status' in filtered_display.columns:
            filtered_display['lead_status'] = filtered_display['lead_status'].fillna('new')

        column_map = {
            "name": "Name",
            "address": "Address",
            "city": "City",
            "venue_type": "Type",
            "status": "Stage",
            "lead_status": "Lead Status",
            "phone": "Phone",
            "first_seen_date": "First Seen",
            "priority_score": "Priority"
        }
        filtered_display.rename(columns=column_map, inplace=True)

        st.dataframe(
            filtered_display,
            use_container_width=True,
            hide_index=True,
            height=400,
        )

        # Download button
        csv = filtered_display.to_csv(index=False)
        st.download_button(
            label="Download CSV",
            data=csv,
            file_name=f"dfw_leads_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv"
        )

    st.divider()

    # Venue Detail Section
    st.subheader("Venue Details")

    if not filtered.empty:
        filtered_sorted = filtered.sort_values(
            by=["priority_score", "first_seen_date"],
            ascending=[False, False]
        )

        label_map = {
            int(row["id"]): f"{row['name']} – {row['city']} ({row['venue_type']})"
            for _, row in filtered_sorted.iterrows()
        }

        selected_id = st.selectbox(
            "Select a venue:",
            options=list(label_map.keys()),
            format_func=lambda vid: label_map[vid],
            key="venue_selector"
        )

        if selected_id:
            venue = df_venues[df_venues["id"] == selected_id].iloc[0]
            conn = get_connection()

            col1, col2 = st.columns([2, 1])

            with col1:
                st.markdown(f"### {venue['name']}")
                st.markdown(f"**Address:** {venue['address']}")
                if venue['city'] or venue['state'] or venue['zip']:
                    location = f"{venue['city']}, {venue['state']} {venue['zip'] or ''}".strip()
                    st.markdown(f"**Location:** {location}")

                # Contact Info with click-to-call
                st.markdown("#### Contact Info")
                if venue.get('phone') and pd.notna(venue['phone']):
                    phone = venue['phone']
                    st.markdown(f"📞 **Phone:** [{phone}](tel:{phone})")
                else:
                    st.markdown("📞 **Phone:** N/A")

                if venue.get('website') and pd.notna(venue['website']):
                    st.markdown(f"🌐 **Website:** [{venue['website']}]({venue['website']})")
                else:
                    st.markdown("🌐 **Website:** N/A")

            with col2:
                st.markdown(f"**Type:** {venue['venue_type']}")
                st.markdown(f"**Stage:** {venue['status']}")
                st.markdown(f"**Lead Status:** {venue.get('lead_status') or 'new'}")
                st.markdown(f"**Priority:** {venue['priority_score']}")
                st.markdown(f"**First Seen:** {venue['first_seen_date']}")

                if venue.get('next_follow_up') and pd.notna(venue['next_follow_up']):
                    st.markdown(f"**Follow-up:** {venue['next_follow_up']}")

                if venue.get('competitor') and pd.notna(venue['competitor']):
                    st.markdown(f"**Competitor:** {venue['competitor']}")

            st.divider()

            # Quick Actions
            st.markdown("#### Quick Actions")
            action_col1, action_col2, action_col3, action_col4 = st.columns(4)

            with action_col1:
                if st.button("✅ Mark Contacted", key="action_contacted"):
                    mark_contacted(conn, selected_id)
                    refresh_data()
                    st.rerun()

            with action_col2:
                if st.button("📅 Schedule Demo", key="action_demo"):
                    st.session_state['show_demo_form'] = True

            with action_col3:
                if st.button("🏆 Mark Won", key="action_won"):
                    mark_won(conn, selected_id)
                    refresh_data()
                    st.rerun()

            with action_col4:
                if st.button("❌ Mark Lost", key="action_lost"):
                    st.session_state['show_lost_form'] = True

            # Demo scheduling form
            if st.session_state.get('show_demo_form'):
                with st.form("demo_form"):
                    st.markdown("**Schedule Demo**")
                    demo_date = st.date_input("Demo Date", value=today + timedelta(days=3))
                    demo_notes = st.text_area("Notes", placeholder="Demo details...")
                    submitted = st.form_submit_button("Schedule")
                    if submitted:
                        schedule_demo(conn, selected_id, demo_date.strftime("%Y-%m-%d"), demo_notes)
                        st.session_state['show_demo_form'] = False
                        refresh_data()
                        st.rerun()

            # Lost form
            if st.session_state.get('show_lost_form'):
                with st.form("lost_form"):
                    st.markdown("**Mark as Lost**")
                    competitor = st.selectbox("Lost to competitor",
                        options=['', 'Toast', 'Square', 'Clover', 'Other'])
                    reason = st.selectbox("Reason",
                        options=['', 'Price', 'Features', 'Already has POS', 'Bad timing', 'Other'])
                    notes = st.text_area("Notes", placeholder="Additional details...")
                    submitted = st.form_submit_button("Mark Lost")
                    if submitted:
                        mark_lost(conn, selected_id, competitor or None, reason or None, notes or None)
                        st.session_state['show_lost_form'] = False
                        refresh_data()
                        st.rerun()

            st.divider()

            # Add Note Form
            st.markdown("#### Add Note / Log Activity")
            with st.form("add_note_form"):
                note_col1, note_col2 = st.columns(2)
                with note_col1:
                    activity_type = st.selectbox("Activity Type",
                        options=['note', 'call', 'email', 'visit', 'demo'])
                    outcome = st.selectbox("Outcome (optional)",
                        options=['', 'no_answer', 'callback', 'interested', 'not_interested', 'demo_booked', 'left_voicemail'])

                with note_col2:
                    next_action = st.date_input("Follow-up Date (optional)",
                        value=None, key="note_followup")
                    note_text = st.text_area("Notes", placeholder="What happened?")

                submitted = st.form_submit_button("Add Activity")
                if submitted and note_text:
                    next_date = next_action.strftime("%Y-%m-%d") if next_action else None
                    db.add_lead_activity(conn, selected_id, activity_type,
                                        notes=note_text,
                                        outcome=outcome or None,
                                        next_action_date=next_date)
                    if next_date:
                        update_follow_up(conn, selected_id, next_date)
                    refresh_data()
                    st.rerun()

            st.divider()

            # Activity History
            st.markdown("#### Activity History")
            df_activities = load_lead_activities(selected_id)

            if not df_activities.empty:
                for _, activity in df_activities.iterrows():
                    with st.container():
                        col1, col2 = st.columns([1, 3])
                        with col1:
                            st.caption(f"📅 {activity['activity_date']}")
                            st.caption(f"Type: {activity['activity_type']}")
                            if activity.get('outcome'):
                                st.caption(f"Outcome: {activity['outcome']}")
                        with col2:
                            st.markdown(activity.get('notes') or '*No notes*')
                    st.divider()
            else:
                st.info("No activity recorded yet.")

            # Source Events
            st.markdown("#### Source Events")
            df_events = load_source_events(selected_id)

            if not df_events.empty:
                for _, event in df_events.iterrows():
                    with st.container():
                        col1, col2 = st.columns([2, 1])
                        with col1:
                            st.markdown(f"**{event['source_system']}** - {event['event_type']}")
                            st.caption(f"📅 {event['event_date']}")
                            st.caption(f"{event['raw_name']} - {event['raw_address']}")
                        with col2:
                            if event['source_system'] == 'TABC':
                                st.markdown("[View on TABC](https://www.tabc.texas.gov/verify-license-permit/)")
                            elif event['source_system'] == 'DALLAS_CO':
                                st.markdown("[View Dallas CO](https://www.dallasopendata.com/Services/Building-Inspection-Certificates-Of-Occupancy/9qet-qt9e)")
                    st.divider()


# ============================================================================
# Map Tab
# ============================================================================

with tab_map:
    st.subheader("Map View")

    # Map filters
    map_col1, map_col2 = st.columns(2)
    with map_col1:
        map_lead_status = st.selectbox(
            "Show leads by status",
            options=['All Active', 'New Only', 'Contacted', 'Demo Scheduled', 'All'],
            key="map_status_filter"
        )
    with map_col2:
        map_venue_type = st.selectbox(
            "Venue type",
            options=['All', 'bar', 'restaurant'],
            key="map_type_filter"
        )

    # Filter map data
    map_data = df_venues.dropna(subset=["latitude", "longitude"]).copy()

    if map_lead_status == 'New Only':
        map_data = map_data[map_data["lead_status"].fillna('new') == 'new']
    elif map_lead_status == 'Contacted':
        map_data = map_data[map_data["lead_status"] == 'contacted']
    elif map_lead_status == 'Demo Scheduled':
        map_data = map_data[map_data["lead_status"] == 'demo_scheduled']
    elif map_lead_status == 'All Active':
        map_data = map_data[map_data["lead_status"].fillna('new').isin(['new', 'contacted', 'demo_scheduled'])]

    if map_venue_type != 'All':
        map_data = map_data[map_data["venue_type"] == map_venue_type]

    if not map_data.empty:
        st.map(
            map_data,
            latitude="latitude",
            longitude="longitude",
            size=20,
            color="#FF4B4B"
        )
        st.caption(f"Showing {len(map_data)} venues with coordinates.")
    else:
        st.info("No venues with coordinates found for this filter. Run `python run_geocoding.py` to fetch locations.")


# ============================================================================
# Analytics Tab
# ============================================================================

with tab_analytics:
    st.subheader("Analytics & Metrics")

    conn = get_connection()
    metrics = get_pipeline_metrics(conn)

    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Leads", metrics['total_leads'])
    with col2:
        st.metric("Active Pipeline", metrics['active_leads'])
    with col3:
        st.metric("Deals Won", metrics['won'])
    with col4:
        st.metric("Win Rate", f"{metrics['win_rate']:.1f}%")

    st.divider()

    # Activity summary
    st.markdown("### Activity This Week")
    activity = get_activity_summary(conn, days=7)

    act_col1, act_col2, act_col3 = st.columns(3)
    with act_col1:
        st.metric("Calls Made", activity['total_calls'])
    with act_col2:
        st.metric("Demos Scheduled", activity['demos_booked'])
    with act_col3:
        st.metric("Total Activities", sum(activity['activities'].values()))

    st.divider()

    # City performance
    st.markdown("### Performance by City")
    city_perf = get_city_performance(conn)

    if city_perf:
        city_df = pd.DataFrame(city_perf)
        city_df = city_df[city_df['total_leads'] >= 3]  # Only cities with 3+ leads

        if not city_df.empty:
            city_df['win_rate'] = (city_df['won'] / city_df['total_leads'] * 100).round(1)
            city_df = city_df.sort_values('won', ascending=False).head(10)

            st.dataframe(
                city_df[['city', 'total_leads', 'contacted', 'demos', 'won', 'lost', 'win_rate']],
                use_container_width=True,
                hide_index=True,
                column_config={
                    'city': 'City',
                    'total_leads': 'Total',
                    'contacted': 'Contacted',
                    'demos': 'Demos',
                    'won': 'Won',
                    'lost': 'Lost',
                    'win_rate': st.column_config.NumberColumn('Win %', format="%.1f%%")
                }
            )
    else:
        st.info("No city performance data yet.")

    st.divider()

    # Source effectiveness
    st.markdown("### Lead Source Effectiveness")
    source_perf = get_source_effectiveness(conn)

    if source_perf:
        source_df = pd.DataFrame(source_perf)
        st.dataframe(
            source_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                'source_system': 'Source',
                'total_leads': 'Total Leads',
                'contacted': 'Contacted',
                'demos': 'Demos',
                'won': 'Won'
            }
        )
    else:
        st.info("No source data yet.")


# ============================================================================
# Footer
# ============================================================================

st.divider()
st.caption("Use the sidebar filters to narrow results. Hot Leads tab shows freshest opportunities!")
