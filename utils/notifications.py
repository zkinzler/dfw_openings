"""
Notification utility for DFW Openings.
Sends alerts via Email (SMTP) and Slack (Webhook).

Alert Types:
1. Hot Leads - New high-priority leads discovered
2. Follow-up Reminders - Leads needing follow-up today
3. Daily Digest - Summary of pipeline and activity
"""

import os
import smtplib
import requests
import sqlite3
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta

# Configuration
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
EMAIL_RECIPIENT = os.getenv("EMAIL_RECIPIENT")

# Alert thresholds
HOT_LEAD_MIN_SCORE = int(os.getenv("HOT_LEAD_MIN_SCORE", "70"))


def get_new_high_priority_venues(conn, days=1, min_score=None):
    """
    Get high priority venues first seen in the last N days.

    Args:
        conn: Database connection
        days: Look back this many days
        min_score: Minimum priority score (default: HOT_LEAD_MIN_SCORE)
    """
    if min_score is None:
        min_score = HOT_LEAD_MIN_SCORE

    since_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    cursor = conn.cursor()
    cursor.execute("""
        SELECT name, venue_type, status, city, address, phone, website, priority_score, first_seen_date
        FROM venues
        WHERE first_seen_date >= ?
          AND priority_score >= ?
          AND COALESCE(lead_status, 'new') = 'new'
        ORDER BY priority_score DESC
    """, (since_date, min_score))

    return cursor.fetchall()


def get_venues_needing_followup(conn):
    """Get venues with follow-up due today or earlier."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT name, venue_type, city, phone, next_follow_up, lead_status
        FROM venues
        WHERE next_follow_up IS NOT NULL
          AND next_follow_up <= date('now')
          AND lead_status NOT IN ('won', 'lost', 'not_interested')
        ORDER BY next_follow_up ASC
    """)
    return cursor.fetchall()


def get_pipeline_summary(conn):
    """Get current pipeline counts."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            COALESCE(lead_status, 'new') as status,
            COUNT(*) as count
        FROM venues
        GROUP BY lead_status
    """)
    return {row['status']: row['count'] for row in cursor.fetchall()}


def get_activity_summary(conn, days=1):
    """Get activity counts for the last N days."""
    since_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    cursor = conn.cursor()
    cursor.execute("""
        SELECT activity_type, COUNT(*) as count
        FROM lead_activities
        WHERE activity_date >= ?
        GROUP BY activity_type
    """, (since_date,))
    return {row['activity_type']: row['count'] for row in cursor.fetchall()}


# =============================================================================
# Hot Leads Alert
# =============================================================================

def format_hot_leads_slack(venues):
    """Format hot leads for Slack."""
    if not venues:
        return None

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"🔥 {len(venues)} Hot Leads Found!"
            }
        },
        {"type": "divider"}
    ]

    for v in venues[:10]:
        icon = "🍸" if v['venue_type'] == 'bar' else "🍽️"

        # Priority badge
        score = v['priority_score'] or 0
        if score >= 100:
            priority = "🔥🔥🔥"
        elif score >= 70:
            priority = "🔥🔥"
        else:
            priority = "🔥"

        text = f"*{icon} {v['name']}* ({v['city']}) {priority}\n"
        text += f"Stage: {v['status']} | Score: {score}\n"
        text += f"📍 {v['address']}\n"
        if v['phone']:
            text += f"📞 {v['phone']}\n"
        if v['website']:
            text += f"🌐 {v['website']}\n"

        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": text
            }
        })

    if len(venues) > 10:
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": f"...and {len(venues) - 10} more."}]
        })

    return {"blocks": blocks}


def format_hot_leads_email(venues):
    """Format hot leads for Email (HTML)."""
    if not venues:
        return None

    html = f"""
    <h2>🔥 {len(venues)} Hot Leads Found!</h2>
    <p>New high-priority leads discovered. Contact them before competitors!</p>
    <table border="1" cellpadding="8" cellspacing="0" style="border-collapse: collapse; font-family: Arial, sans-serif;">
        <tr style="background-color: #FF6B6B; color: white;">
            <th>Name</th>
            <th>Type</th>
            <th>Stage</th>
            <th>City</th>
            <th>Phone</th>
            <th>Score</th>
        </tr>
    """

    for v in venues:
        phone_link = f'<a href="tel:{v["phone"]}">{v["phone"]}</a>' if v['phone'] else 'N/A'
        html += f"""
        <tr>
            <td><b>{v['name']}</b><br><small>{v['address']}</small></td>
            <td>{v['venue_type']}</td>
            <td>{v['status']}</td>
            <td>{v['city']}</td>
            <td>{phone_link}</td>
            <td><b>{v['priority_score']}</b></td>
        </tr>
        """

    html += "</table>"
    return html


# =============================================================================
# Follow-up Reminder Alert
# =============================================================================

def format_followup_slack(venues):
    """Format follow-up reminders for Slack."""
    if not venues:
        return None

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"⏰ {len(venues)} Follow-ups Due Today"
            }
        },
        {"type": "divider"}
    ]

    for v in venues[:10]:
        icon = "🍸" if v['venue_type'] == 'bar' else "🍽️"

        text = f"*{icon} {v['name']}* ({v['city']})\n"
        text += f"Status: {v['lead_status']} | Due: {v['next_follow_up']}\n"
        if v['phone']:
            text += f"📞 {v['phone']}\n"

        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": text
            }
        })

    return {"blocks": blocks}


def format_followup_email(venues):
    """Format follow-up reminders for Email (HTML)."""
    if not venues:
        return None

    html = f"""
    <h2>⏰ {len(venues)} Follow-ups Due</h2>
    <p>These leads need your attention today:</p>
    <table border="1" cellpadding="8" cellspacing="0" style="border-collapse: collapse; font-family: Arial, sans-serif;">
        <tr style="background-color: #FFA500; color: white;">
            <th>Name</th>
            <th>City</th>
            <th>Status</th>
            <th>Phone</th>
            <th>Due Date</th>
        </tr>
    """

    for v in venues:
        phone_link = f'<a href="tel:{v["phone"]}">{v["phone"]}</a>' if v['phone'] else 'N/A'
        html += f"""
        <tr>
            <td><b>{v['name']}</b></td>
            <td>{v['city']}</td>
            <td>{v['lead_status']}</td>
            <td>{phone_link}</td>
            <td>{v['next_follow_up']}</td>
        </tr>
        """

    html += "</table>"
    return html


# =============================================================================
# Daily Digest
# =============================================================================

def format_daily_digest_slack(hot_leads, followups, pipeline, activity):
    """Format daily digest for Slack."""
    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"📊 Daily Digest - {datetime.now().strftime('%Y-%m-%d')}"
            }
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Pipeline Status*\n"
                        f"• New: {pipeline.get('new', 0) + pipeline.get(None, 0)}\n"
                        f"• Contacted: {pipeline.get('contacted', 0)}\n"
                        f"• Demo Scheduled: {pipeline.get('demo_scheduled', 0)}\n"
                        f"• Won: {pipeline.get('won', 0)}"
            }
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Today's Summary*\n"
                        f"• 🔥 Hot leads: {len(hot_leads)}\n"
                        f"• ⏰ Follow-ups due: {len(followups)}\n"
                        f"• 📞 Calls logged: {activity.get('call', 0)}\n"
                        f"• 📅 Demos logged: {activity.get('demo', 0)}"
            }
        }
    ]

    return {"blocks": blocks}


def format_daily_digest_email(hot_leads, followups, pipeline, activity):
    """Format daily digest for Email (HTML)."""
    html = f"""
    <h2>📊 Daily Digest - {datetime.now().strftime('%Y-%m-%d')}</h2>

    <h3>Pipeline Status</h3>
    <table border="1" cellpadding="8" cellspacing="0" style="border-collapse: collapse;">
        <tr>
            <td><b>New</b></td>
            <td>{pipeline.get('new', 0) + pipeline.get(None, 0)}</td>
        </tr>
        <tr>
            <td><b>Contacted</b></td>
            <td>{pipeline.get('contacted', 0)}</td>
        </tr>
        <tr>
            <td><b>Demo Scheduled</b></td>
            <td>{pipeline.get('demo_scheduled', 0)}</td>
        </tr>
        <tr style="background-color: #90EE90;">
            <td><b>Won</b></td>
            <td>{pipeline.get('won', 0)}</td>
        </tr>
    </table>

    <h3>Today's Summary</h3>
    <ul>
        <li>🔥 <b>{len(hot_leads)}</b> hot leads discovered</li>
        <li>⏰ <b>{len(followups)}</b> follow-ups due</li>
        <li>📞 <b>{activity.get('call', 0)}</b> calls logged</li>
        <li>📅 <b>{activity.get('demo', 0)}</b> demos logged</li>
    </ul>
    """

    return html


# =============================================================================
# Send Functions
# =============================================================================

def send_slack_alert(payload):
    """Send alert to Slack."""
    if not SLACK_WEBHOOK_URL:
        print("Slack Webhook URL not set. Skipping Slack alert.")
        return False

    if not payload:
        return False

    try:
        response = requests.post(SLACK_WEBHOOK_URL, json=payload)
        response.raise_for_status()
        print("Slack alert sent.")
        return True
    except Exception as e:
        print(f"Error sending Slack alert: {e}")
        return False


def send_email_alert(subject, body):
    """Send alert via Email."""
    if not (SMTP_USER and SMTP_PASSWORD and EMAIL_RECIPIENT):
        print("SMTP credentials not set. Skipping Email alert.")
        return False

    if not body:
        return False

    msg = MIMEMultipart()
    msg['From'] = SMTP_USER
    msg['To'] = EMAIL_RECIPIENT
    msg['Subject'] = subject

    msg.attach(MIMEText(body, 'html'))

    try:
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(SMTP_USER, SMTP_PASSWORD)
        server.send_message(msg)
        server.quit()
        print("Email alert sent.")
        return True
    except Exception as e:
        print(f"Error sending Email alert: {e}")
        return False


# =============================================================================
# Main Alert Functions
# =============================================================================

def send_hot_leads_alert(conn, days=1):
    """Send alert for new hot leads."""
    print(f"Checking for hot leads (last {days} day(s))...")
    venues = get_new_high_priority_venues(conn, days=days)

    if not venues:
        print("No new hot leads found.")
        return

    print(f"Found {len(venues)} hot leads.")

    # Send Slack
    slack_payload = format_hot_leads_slack(venues)
    send_slack_alert(slack_payload)

    # Send Email
    email_body = format_hot_leads_email(venues)
    send_email_alert(
        f"🔥 {len(venues)} Hot Leads - {datetime.now().strftime('%Y-%m-%d')}",
        email_body
    )


def send_followup_reminder(conn):
    """Send reminder for leads needing follow-up."""
    print("Checking for follow-up reminders...")
    venues = get_venues_needing_followup(conn)

    if not venues:
        print("No follow-ups due today.")
        return

    print(f"Found {len(venues)} leads needing follow-up.")

    # Send Slack
    slack_payload = format_followup_slack(venues)
    send_slack_alert(slack_payload)

    # Send Email
    email_body = format_followup_email(venues)
    send_email_alert(
        f"⏰ {len(venues)} Follow-ups Due - {datetime.now().strftime('%Y-%m-%d')}",
        email_body
    )


def send_daily_digest(conn):
    """Send daily summary digest."""
    print("Generating daily digest...")

    hot_leads = get_new_high_priority_venues(conn, days=1)
    followups = get_venues_needing_followup(conn)
    pipeline = get_pipeline_summary(conn)
    activity = get_activity_summary(conn, days=1)

    # Send Slack
    slack_payload = format_daily_digest_slack(hot_leads, followups, pipeline, activity)
    send_slack_alert(slack_payload)

    # Send Email
    email_body = format_daily_digest_email(hot_leads, followups, pipeline, activity)
    send_email_alert(
        f"📊 DFW Lead Tracker Daily Digest - {datetime.now().strftime('%Y-%m-%d')}",
        email_body
    )


def run_notifications(conn, alert_type='all'):
    """
    Run notification checks.

    Args:
        conn: Database connection
        alert_type: 'hot_leads', 'followups', 'digest', or 'all'
    """
    print("Running notifications...")

    if alert_type in ('hot_leads', 'all'):
        send_hot_leads_alert(conn)

    if alert_type in ('followups', 'all'):
        send_followup_reminder(conn)

    if alert_type in ('digest', 'all'):
        send_daily_digest(conn)

    print("Notifications complete.")


if __name__ == "__main__":
    import sys
    import db

    # Parse command line args
    alert_type = sys.argv[1] if len(sys.argv) > 1 else 'all'

    conn = db.get_connection()
    db.ensure_schema(conn)
    run_notifications(conn, alert_type)
    conn.close()
