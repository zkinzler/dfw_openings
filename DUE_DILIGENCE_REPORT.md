# DFW POS Lead Tracker - Due Diligence Report

**Report Date:** 2026-02-04
**Total Venues:** 650
**Total Source Events:** 4,426

---

## Executive Summary

The lead tracker has **serious data quality issues** that must be fixed before it's useful for sales. The #1 problem is **zero phone numbers** - making it impossible to cold call leads. Additionally, the Dallas Permit data source is fundamentally broken, pulling in construction contractor names instead of actual restaurant names.

### Critical Issues (Blockers)
1. **0% of leads have phone numbers** - Can't make sales calls
2. **0% have websites** - Can't research prospects
3. **Dallas Permit ETL is broken** - Pulling contractor names, not restaurant names
4. **~30% of venues are NOT restaurants** - Liquor stores, convenience stores, gas stations, construction companies

### Data Quality Score: 3/10

---

## Detailed Findings

### 1. Contact Information (CRITICAL)

| Metric | Value | Status |
|--------|-------|--------|
| Has Phone | 0 / 650 (0%) | ❌ CRITICAL |
| Has Website | 0 / 650 (0%) | ❌ CRITICAL |
| Has Google Place ID | 0 / 650 (0%) | ❌ Missing |
| Geocoded | 24 / 650 (3.7%) | ⚠️ Poor |

**Impact:** Without phone numbers, the entire sales workflow is broken. Users cannot make outbound calls.

**Fix Required:** Enable Google Places API enrichment to fetch phone/website for each venue.

---

### 2. Data Source Quality

| Source | Events | Quality | Issues |
|--------|--------|---------|--------|
| TABC | 1,298 | ✅ Good | Real business names, valid license data |
| Sales Tax | 740 | ✅ Good | Real business names, NAICS codes |
| Dallas Permits | 2,214 | ❌ Broken | Pulling CONTRACTOR names, not restaurants |
| Fort Worth Permits | 174 | ⚠️ Fair | Limited data but valid |

**Dallas Permit Problem Details:**
- The ETL is extracting `contractor` field as the business name
- Result: "SYSTEM ELECTRIC COMPANY", "CROCKER CRANE", "ANDRES CONSTRUCTION SERVICES" appear as "venues"
- These are contractors who pulled permits, NOT the restaurants being built
- 41 duplicate entries for one crane company alone

**Fix Required:**
- Dallas Permit ETL needs complete rewrite
- Should look for actual tenant/business name in permit data, OR
- Mark these as "address only" leads that need manual research

---

### 3. Venue Classification

| Venue Type | Count | Percentage |
|------------|-------|------------|
| Unknown | 354 | 54% |
| Restaurant | 255 | 39% |
| Bar | 41 | 6% |

**Issues Found:**
- Many venues classified as "restaurant" are actually:
  - Liquor stores (LIQUOR KING, BIG JIM DISCOUNT LIQUOR, etc.)
  - Convenience stores (7-Eleven, Shell Food Mart, etc.)
  - Gas stations (Gas Trip, Tiger Mart)
  - Grocery stores (Dollar General)

- These venues **don't need restaurant POS systems** - they need retail POS

**Misclassified Examples:**
- "Holiday Liquor" → classified as restaurant
- "7-Eleven Convenience Store #41714H" → classified as restaurant
- "Dollar General Store #4606" → classified as restaurant
- "Touchdown Tap House" → classified as restaurant (should be bar)

---

### 4. Geographic Coverage

| City | Leads | Notes |
|------|-------|-------|
| Dallas | 224 | 34% - includes duplicates |
| Fort Worth | 151 | 23% - case inconsistencies |
| Arlington | 34 | 5% |
| McKinney | 11 | 2% |
| Plano | 13 | 2% |
| Other | ~217 | 33% - scattered |

**Issues:**
- City name inconsistencies: "DALLAS" vs "Dallas" vs "dallas" (224 leads affected)
- "FORT WORTH" vs "Fort Worth" (151 leads affected)
- Will cause filtering problems in dashboard

---

### 5. Lead Freshness

| Age | Count | % of Total |
|-----|-------|------------|
| Last 7 days | 47 | 7% |
| Last 30 days | 177 | 27% |
| Last 90 days | 144 | 22% |
| Older than 90 days | 282 | 43% |

**Note:** Oldest lead dates to 2018. Many leads may be stale or already have POS systems.

---

### 6. Duplicate/Junk Data

Found significant duplicate and non-restaurant data:
- "crocker crane po box 141539 irving tx" → 41 entries
- "system electric" → 13 entries
- "andres construction services" → 12 entries
- Various plumbing, electrical, and construction companies

---

## Recommendations

### Immediate (Before Client Use)

1. **Enable Google Places Enrichment**
   - Set `GOOGLE_PLACES_API_KEY` environment variable
   - Run enrichment script to fetch phone/website
   - Cost: ~$17 per 1,000 lookups = ~$11 for current data

2. **Clean Junk Data**
   - Delete all venues with construction/contractor keywords
   - Delete liquor stores, convenience stores, gas stations
   - Implement keyword blocklist in ETL

3. **Fix Dallas Permit ETL**
   - Stop using "contractor" as business name
   - Either find actual tenant name field, or
   - Store as "address-only" leads requiring manual research

4. **Normalize City Names**
   - Standardize to title case: "Dallas", "Fort Worth"
   - Run update script on existing data

### Short-term (This Week)

5. **Add Venue Type Filters**
   - Add exclusion keywords: liquor, convenience, mart, gas, fuel, 7-eleven, dollar general
   - Improve bar detection keywords

6. **Fix Status Inference**
   - All venues showing "permitting" - need to differentiate based on source
   - CO events should show "opening_soon"

7. **Run Full Geocoding**
   - Only 3.7% geocoded currently
   - Map feature nearly useless without this

### Medium-term (This Month)

8. **Add Manual Lead Verification**
   - Flag for "verified" vs "unverified" leads
   - Let user confirm venue is real restaurant/bar

9. **Improve Priority Scoring**
   - Currently no recency bonus working (all same scores)
   - Ensure phone number bonus is factored in

10. **Add Data Source Toggle**
    - Let user enable/disable sources (e.g., disable broken Dallas Permits)

---

## What Works Well

- ✅ TABC data is high quality with real business names
- ✅ Sales Tax data has good business names and NAICS codes
- ✅ Fort Worth permit data appears valid
- ✅ Lead status workflow is implemented
- ✅ Dashboard UI is functional
- ✅ Priority scoring algorithm is well-designed (just needs data)

---

## Bottom Line

**The tool is not ready for client use.** The zero phone number rate makes outbound sales impossible. Recommend:

1. Get Google Places API key and run enrichment
2. Clean out junk/non-restaurant data
3. Fix city name normalization
4. Then re-evaluate

**Estimated effort to fix critical issues:** 2-4 hours of work + API costs (~$15-20)
