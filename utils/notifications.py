"""
Notification utility for DFW Openings.
Sends alerts via Email (SMTP) and Slack (Webhook).
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

def get_new_high_priority_venues(conn, days=1):
    """
    Get high priority venues (bars or opening soon) first seen in the last N days.
    """
    since_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    
    cursor = conn.cursor()
    cursor.execute("""
        SELECT name, venue_type, status, city, address, phone, priority_score
        FROM venues
        WHERE first_seen_date >= ?
          AND (venue_type = 'bar' OR status = 'opening_soon' OR priority_score >= 80)
        ORDER BY priority_score DESC
    """, (since_date,))
    
    return cursor.fetchall()

def format_slack_message(venues):
    """Format list of venues for Slack."""
    if not venues:
        return None
        
    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"🚀 {len(venues)} New High-Priority Leads Found!"
            }
        },
        {"type": "divider"}
    ]
    
    for v in venues[:10]:  # Limit to top 10 to avoid hitting limits
        icon = "🍸" if v['venue_type'] == 'bar' else "🍽️"
        status_emoji = "🚧" if v['status'] == 'permitting' else "🔜"
        
        text = f"*{icon} {v['name']}* ({v['city']})\n"
        text += f"{status_emoji} Status: {v['status']}\n"
        text += f"📍 {v['address']}\n"
        if v['phone']:
            text += f"📞 {v['phone']}\n"
            
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

def send_slack_alert(venues):
    """Send alert to Slack."""
    if not SLACK_WEBHOOK_URL:
        print("⚠️ Slack Webhook URL not set. Skipping Slack alert.")
        return
        
    payload = format_slack_message(venues)
    if not payload:
        return
        
    try:
        response = requests.post(SLACK_WEBHOOK_URL, json=payload)
        response.raise_for_status()
        print("✅ Slack alert sent.")
    except Exception as e:
        print(f"❌ Error sending Slack alert: {e}")

def format_email_body(venues):
    """Format list of venues for Email (HTML)."""
    if not venues:
        return None
        
    html = f"""
    <h2>🚀 {len(venues)} New High-Priority Leads Found</h2>
    <table border="1" cellpadding="5" cellspacing="0" style="border-collapse: collapse;">
        <tr style="background-color: #f2f2f2;">
            <th>Name</th>
            <th>Type</th>
            <th>Status</th>
            <th>City</th>
            <th>Phone</th>
        </tr>
    """
    
    for v in venues:
        html += f"""
        <tr>
            <td><b>{v['name']}</b><br><small>{v['address']}</small></td>
            <td>{v['venue_type']}</td>
            <td>{v['status']}</td>
            <td>{v['city']}</td>
            <td>{v['phone'] or 'N/A'}</td>
        </tr>
        """
        
    html += "</table>"
    return html

def send_email_alert(venues):
    """Send alert via Email."""
    if not (SMTP_USER and SMTP_PASSWORD and EMAIL_RECIPIENT):
        print("⚠️ SMTP credentials not set. Skipping Email alert.")
        return
        
    body = format_email_body(venues)
    if not body:
        return
        
    msg = MIMEMultipart()
    msg['From'] = SMTP_USER
    msg['To'] = EMAIL_RECIPIENT
    msg['Subject'] = f"🚀 {len(venues)} New DFW Leads - {datetime.now().strftime('%Y-%m-%d')}"
    
    msg.attach(MIMEText(body, 'html'))
    
    try:
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(SMTP_USER, SMTP_PASSWORD)
        server.send_message(msg)
        server.quit()
        print("✅ Email alert sent.")
    except Exception as e:
        print(f"❌ Error sending Email alert: {e}")

def run_notifications(conn):
    """Check for new leads and send alerts."""
    print("🔔 Checking for new leads to notify...")
    venues = get_new_high_priority_venues(conn)
    
    if not venues:
        print("No new high-priority venues found today.")
        return
        
    print(f"Found {len(venues)} new high-priority venues.")
    
    send_slack_alert(venues)
    send_email_alert(venues)

if __name__ == "__main__":
    import db
    conn = db.get_connection()
    run_notifications(conn)
    conn.close()
