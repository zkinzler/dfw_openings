# DFW POS Lead Tracker - Usability Improvement Plan

**Goal:** Make this tool the most effective possible for a single salesperson selling POS/credit card systems to new restaurants and bars in DFW.

---

## Understanding the User's Workflow

A POS salesperson's daily routine:
1. **Morning:** Check for new hot leads
2. **Make calls:** Work through lead list, dial prospects
3. **Log activities:** Track calls, outcomes, follow-ups
4. **Schedule demos:** Book meetings with interested prospects
5. **Field visits:** Visit venues in person
6. **End of day:** Review pipeline, plan tomorrow

---

## Critical Features (Must Have)

### 1. One-Click Calling (Mobile-First)
**Current:** Phone numbers missing; when present, not clickable on mobile
**Fix:**
- Phone numbers as large, tappable `tel:` links
- "Call" button prominently displayed
- After call, prompt for outcome logging

```
┌─────────────────────────────────┐
│ 🔥 AWESOME BBQ & GRILL          │
│ 📍 1234 Main St, Dallas         │
│                                 │
│ ┌─────────────────────────────┐ │
│ │      📞 CALL NOW            │ │
│ │     (214) 555-1234          │ │
│ └─────────────────────────────┘ │
│                                 │
│ [✅ Contacted] [❌ Not Int.]    │
└─────────────────────────────────┘
```

### 2. Hot Leads Dashboard
**Current:** Hot Leads tab exists but needs improvement
**Enhancements:**
- Morning notification: "You have 5 new hot leads today"
- Sort by: Newest first, Has Phone first, Highest priority
- Quick filters: Bars only, Has Phone, My City
- Card-based view optimized for scanning

### 3. Activity Logging in 2 Taps
**Current:** Form-based, too many fields
**Fix:**
- After call, show quick outcome buttons:
  - ✅ Interested
  - 📞 Left Voicemail
  - 🔄 Call Back
  - ❌ Not Interested
- Optional: Add note (expandable)
- Auto-suggest follow-up date based on outcome

### 4. Daily Call List
**Current:** No dedicated call list view
**Add:**
- "Today's Calls" view showing:
  - Scheduled follow-ups due today
  - New leads from last 48 hours
  - Leads marked "callback"
- Check-off as completed
- Running tally: "5/12 calls made today"

### 5. Smart Follow-Up Reminders
**Current:** Basic follow-up date, no notifications
**Add:**
- Morning email/Slack: "3 follow-ups due today"
- Color coding: Overdue (red), Today (yellow), Upcoming (green)
- Snooze option: "Remind me tomorrow"

---

## High-Impact Features (Should Have)

### 6. Venue Details at a Glance
Show everything needed to prepare for a call:
```
┌─────────────────────────────────────────┐
│ AWESOME BBQ & GRILL                 🔥95│
├─────────────────────────────────────────┤
│ 📍 1234 Main St, Dallas, TX 75201       │
│ 📞 (214) 555-1234                       │
│ 🌐 awesomebbq.com                       │
├─────────────────────────────────────────┤
│ Type: Restaurant | Stage: Opening Soon  │
│ First Seen: 2 days ago                  │
│ Sources: TABC, Sales Tax                │
├─────────────────────────────────────────┤
│ 📝 Last Activity: Called 1/15, no answer│
│ 📅 Follow-up: Tomorrow                  │
└─────────────────────────────────────────┘
```

### 7. Quick Notes with Voice
- Voice-to-text for adding notes (mobile)
- Pre-filled templates: "LVM", "Spoke with owner", "Needs to discuss with partner"
- Notes visible at a glance on venue card

### 8. Pipeline Kanban Board
Visual pipeline with drag-and-drop:
```
┌──────────┬──────────┬──────────┬──────────┐
│   NEW    │CONTACTED │  DEMO    │   WON    │
│   (47)   │   (23)   │   (5)    │   (3)    │
├──────────┼──────────┼──────────┼──────────┤
│ Card 1   │ Card 4   │ Card 8   │ Card 12  │
│ Card 2   │ Card 5   │ Card 9   │          │
│ Card 3   │ Card 6   │          │          │
│   ...    │ Card 7   │          │          │
└──────────┴──────────┴──────────┴──────────┘
```

### 9. Route Planning / Map View
- Filter map by: Today's calls, My area, New leads
- Cluster nearby venues
- "Get Directions" button
- Optimized route suggestion

### 10. Personal Stats Dashboard
- Calls this week: 45 (vs 52 last week)
- Demos booked: 3
- Win rate: 40%
- Best day: Tuesday
- Best city: Dallas (60% close rate)

---

## Nice-to-Have Features

### 11. Competitor Tracking
- When losing a deal, record competitor: Toast, Square, Clover, Other
- Dashboard: "Lost 5 to Toast, 2 to Square this month"
- Insights: "Toast is winning in Fort Worth"

### 12. Venue Intel from Web
- Auto-fetch from website/social:
  - Opening date mentions
  - Owner name
  - Cuisine type
  - Size estimate (seats)

### 13. Email Templates
- One-click send follow-up email
- Templates: Introduction, After Demo, After No-Answer

### 14. Calendar Integration
- Sync demos to Google/Outlook calendar
- View day's schedule in app

### 15. Export & Reporting
- Weekly PDF report: Activity summary, pipeline status
- Export leads to CSV with all fields
- Integration with CRM if needed later

---

## Mobile Optimization (Critical)

The salesperson will use this in the field. Must be mobile-friendly:

1. **Large touch targets** - Buttons at least 44x44px
2. **Minimal typing** - Use buttons, dropdowns, voice
3. **Offline capability** - Cache today's leads (future)
4. **Quick load** - Under 3 seconds
5. **One-handed use** - Key actions reachable by thumb

### Recommended Layout (Mobile)

```
┌─────────────────────────┐
│ ☰ DFW POS Tracker   🔔  │
├─────────────────────────┤
│ [🔥 Hot] [📋 Calls] [📊]│
├─────────────────────────┤
│                         │
│   Lead Cards            │
│   (Scrollable)          │
│                         │
├─────────────────────────┤
│ [➕ Add Note] [📞 Call] │
└─────────────────────────┘
```

---

## Implementation Priority

### Phase 1: Make It Usable (This Week)
1. ✅ Fix data quality (phone numbers!)
2. Add large "Call" buttons with tel: links
3. Simplify activity logging to 2 taps
4. Mobile-responsive styling
5. Morning Slack/Email alerts working

### Phase 2: Make It Efficient (Next Week)
6. Daily Call List view
7. Improved Hot Leads sorting
8. Quick-add notes
9. Pipeline kanban view
10. Follow-up color coding

### Phase 3: Make It Powerful (Month 2)
11. Route planning / map improvements
12. Stats dashboard
13. Competitor tracking
14. Email templates
15. Voice notes

---

## Quick Wins to Implement Now

1. **Increase font size** for mobile readability
2. **Add tel: links** for all phone numbers
3. **Move "Call" button** to top of venue card
4. **Add outcome buttons** after marking contacted
5. **Color code** follow-up urgency
6. **Show "days ago"** instead of dates (e.g., "2 days ago")
7. **Hide non-essential fields** on mobile
8. **Add "Today's Priority"** section at top

---

## Success Metrics

Track these to know if it's working:

| Metric | Target |
|--------|--------|
| Leads contacted per day | 20+ |
| Calls logged per day | 15+ |
| Demos scheduled per week | 3+ |
| Time to log activity | <10 seconds |
| Daily active usage | 100% |

---

## Summary

The tool needs to be:
1. **Fast** - Find and call leads in seconds
2. **Simple** - Log activity in 2 taps
3. **Mobile** - Work perfectly on phone
4. **Proactive** - Push hot leads and reminders

Focus first on fixing data quality (phone numbers), then on streamlining the call → log → follow-up workflow.
