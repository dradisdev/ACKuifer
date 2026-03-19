# ACKuifer — Claude Code Project Brief

## What This Is
ACKuifer (ackuifer.org) is a public-interest web service that monitors two sources of Nantucket PFAS well test data, displays all results on an interactive map, and sends proactive email/SMS alerts to subscribers by neighborhood. It is operated independently of the Town of Nantucket.

The two data sources are:
- **Board of Health / Laserfiche** — voluntary and Chapter 386 pre-sale residential well tests posted to the Town's Laserfiche portal
- **MassDEP Source Discovery** — RTN 4-0029612, a targeted contamination investigation; results published as PDFs to the Massachusetts EEA portal

**Full product spec:** `ACKuifer_PRD_v9.docx` in the project root. Read it before building any feature.

---

## This Is Not a Greenfield Build
Working prototype code already exists in `/prototype/pfas_monitor/`. Do not rewrite it. Integrate it.

### Confirmed working prototype files:
| File | What it does |
|------|-------------|
| `pfas_monitor_v2.py` | Playwright-based Laserfiche scraper (validated for Map 21; full-island traversal is the production target) |
| `pfas_monitor.py` | Earlier version — reference only, superseded by v2 |
| `test_report_parser.py` | Extracts all 6 PFAS compounds, PFAS6 sum, address, sample date, J-qualifiers, pass/fail from Barnstable County Lab plain-text reports |
| `pace_lab_parser.py` | Parser for Pace Lab report format — a second known lab format; confirm scope with operator |
| `eea_monitor.py` | EEA portal monitor for RTN 4-0029612 (MassDEP Source Discovery) |
| `source_discovery_parser.py` | Two-format PDF parser for Source Discovery reports (auto-detects format) |
| `sd_geocoder.py` | Geocoder for Source Discovery sample location addresses |
| `source_discovery_db.py` | Database operations for Source Discovery data |
| `address_lookup.py` | Address-to-map-number lookup via Nantucket MapGeo portal |
| `pfas_reports.json` | ~16 parsed Laserfiche records (Map 21) — use for migration and geo pipeline testing |
| `source_discovery.json` | 105+ parsed Source Discovery locations — use for migration and geo pipeline testing |

### Exploration/debug scripts (reference only, not for integration):
`debug_links.py`, `explore_portal.py`, `interactive_explorer.py`, `targeted_run.py`, `test_source_discovery.py`

### Laserfiche portal constants (from prototype):
```python
BASE_URL = "https://portal.laserfiche.com"
REPO_ID = "r-ec7bdbfe"
ROOT_FOLDER_ID = "145009"
```

### pfas_reports.json structure (keyed by Laserfiche document ID string):
```json
{
  "reports": {
    "12345": {
      "id": "12345",
      "name": "PFAS_Sampling_20241028",
      "url": "https://...",
      "path": "Map 21/Wells",
      "first_seen": "2024-01-15T10:30:00",
      "map_number": "21",
      "folder": "Wells"
    }
  },
  "last_updated": "2024-01-15T10:30:00"
}
```
The document ID string is the dedup key for the `seen_documents` table. Migration must preserve these IDs.

---

## Tech Stack
- **Backend:** Python 3.11+, FastAPI
- **Database:** PostgreSQL (Railway managed)
- **Scraping:** Playwright (Chromium)
- **Geo:** Shapely (point-in-polygon), static GeoJSON reference files
- **Frontend:** Jinja2 templates + Mapbox GL JS
- **Email:** Resend (transactional)
- **SMS:** Twilio
- **Deployment:** Railway (web service + 2 cron jobs + managed PostgreSQL)
- **PDF parsing:** pdfplumber (Source Discovery pipeline)

---

## Key Constants (never hardcode — always use named constants from config)
```python
MCL = 20.0               # Massachusetts PFAS6 maximum contaminant level (ppt)
SMS_THRESHOLD = 16.0     # 80% of MCL; triggers standalone SMS alert
IH_THRESHOLD = 90.0      # MassDEP Imminent Hazard threshold; triggers HAZARD status
INACTIVITY_MONTHS = 12   # months before re-confirmation email
RETENTION_DAYS_AFTER_UNSUBSCRIBE = 30
DEADMANS_WINDOW_DAYS = 10  # configurable via env var
```

## Four-Tier Result Status Classification
Applied consistently to ALL results from BOTH data sources at parse time:
| Status | Condition |
|--------|-----------|
| `NON-DETECT` | PFAS6 = 0 |
| `DETECT` | PFAS6 > 0 and ≤ 20.0 ppt (20.0 exactly = DETECT) |
| `HIGH-DETECT` | PFAS6 > 20.0 and < 90.0 ppt |
| `HAZARD` | PFAS6 ≥ 90.0 ppt — unconditional SMS regardless of subscriber tier |

ND (non-detect) values = null in compound fields, treated as 0 in PFAS6 sum.
J-qualified values included in PFAS6 sum as reported.

---

## Reference Data Files (static, in `/data`)
- `L3_SHP_M197_Nantucket/` — MassGIS Level 3 Parcels, Nantucket municipality (Shapefile). Must be converted to GeoJSON in Step 1 of build using geopandas. Parcel lookup key: map_number + parcel_number (e.g. "21-80"). Used for Laserfiche centroid lookup. Output file after conversion: `nantucket_parcels.geojson`.
- `nantucket_neighborhoods.geojson` — OSM village/hamlet polygon boundaries exported from Overpass Turbo. Used for point-in-polygon neighborhood assignment (Shapely).
- `pwsdep_pt/` — MassGIS Public Water Supplies (Shapefile, statewide, data as of 09/03/2025). Must be filtered to Nantucket and processed in Step 1 to produce `nantucket_water_service.geojson` for the municipal water soft warning at signup. Source: https://www.mass.gov/info-details/massgis-data-public-water-supplies. Note: this layer covers public water supply source locations — confirm it is suitable for service area detection before building the signup check; if not, the water warning feature should be stubbed pending a better polygon source.

Fallback neighborhood for unresolved parcels or out-of-polygon centroids: `Nantucket (Island-wide)`.

Expected neighborhoods from OSM: Nantucket (town center), Madaket, Cisco, Surfside, Tom Nevers, Sconset (Siasconset), Quidnet, Wauwinet, Polpis.

---

## Database Tables (PostgreSQL)
Six tables — see PRD Section 5 for full schema:
- `users` — subscribers (email, mobile, tier, active status)
- `subscriptions` — user ↔ neighborhood mappings
- `pfas_results` — parsed Laserfiche/Board of Health results
- `source_discovery_results` — parsed MassDEP Source Discovery results (separate table, same status scheme)
- `seen_documents` — dedup registry for both scrapers (keyed by Laserfiche doc ID or EEA PDF URL)
- `scrape_runs` — scraper run history and status

Schema must support future paid tier without migration (see PRD Section 12). `users.tier` defaults to `'free'`.

---

## Application Structure
```
app/
├── main.py               # FastAPI app entry point
├── config.py             # All constants and env var loading
├── database.py           # DB connection and session management
├── models/               # SQLAlchemy models (one file per table group)
├── routers/              # FastAPI route handlers
│   ├── map.py
│   ├── signup.py
│   ├── admin.py
│   └── api.py            # JSON endpoints for map frontend
├── scrapers/             # Integrated from /prototype/pfas_monitor/
│   ├── laserfiche.py     # Adapted from pfas_monitor_v2.py + test_report_parser.py
│   └── massdep.py        # Adapted from eea_monitor.py + source_discovery_parser.py + sd_geocoder.py
├── geo/                  # Geo pipeline
│   ├── parcel_lookup.py  # MassGIS parcel → centroid
│   └── neighborhood.py   # centroid → neighborhood (Shapely)
├── notifications/
│   ├── email.py          # Resend digest logic
│   └── sms.py            # Twilio alert logic
└── templates/            # Jinja2 HTML templates
    ├── index.html         # Landing page
    ├── map.html           # Map page (Mapbox GL JS)
    ├── signup.html        # Alert signup flow
    ├── pfas_info.html     # PFAS information page
    └── admin.html         # Admin interface
```

---

## Git Workflow
Commit directly to the current branch. Do not create feature branches unless explicitly asked.

---

## Deployment (Railway)
Three Railway services in one project:
1. **Web service** — FastAPI app (`uvicorn app.main:app`)
2. **Laserfiche cron** — runs `app/scrapers/laserfiche.py` on schedule
3. **Source Discovery cron** — runs `app/scrapers/massdep.py` on schedule

All secrets via environment variables. See `.env.example` for full list. No secrets in code or version control.

---

## Data Privacy Rules (non-negotiable)
- Street addresses entered at signup are **never stored** — used only for neighborhood resolution, then discarded
- House numbers are **never displayed** publicly — street name only
- No analytics, no third-party pixels, no advertising
- Subscriber list never shared or sold

---

## Build Sequence
1. Database schema + JSON data migration
2. Geo pipeline (parcel → centroid → neighborhood)
3. FastAPI skeleton + map page serving existing data
4. Laserfiche scraper integration
5. Source Discovery pipeline integration
6. Notification system (email + SMS)
7. Signup flow + subscription management
8. Admin interface
9. Railway deployment
