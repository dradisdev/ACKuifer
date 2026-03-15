# ACKuifer

**Nantucket PFAS Plume Visualization & Alert Service**

ACKuifer monitors public PFAS well test data for Nantucket, Massachusetts, displays results on an interactive map, and sends proactive email and SMS alerts to subscribers by neighborhood. It operates independently of the Town of Nantucket.

🌐 [ackuifer.org](https://ackuifer.org)

---

## What It Does

Nantucket has a documented PFAS contamination problem. Test results are public — but they're buried in the Town's Laserfiche portal and MassDEP's EEA database in ways that are inaccessible to ordinary residents. ACKuifer makes this data legible and actionable:

- **Map** — all results color-coded by detection level, from both data sources
- **Alerts** — email digest and SMS notifications when new results appear near your neighborhood
- **History** — searchable record of all past results by neighborhood

### Data Sources
- **Town of Nantucket Board of Health** (Laserfiche portal) — voluntary and Chapter 386 pre-sale residential well tests
- **MassDEP Source Discovery Investigation** (RTN 4-0029612) — targeted contamination investigation; significantly higher detection rates than residential data

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Backend | Python 3.11+, FastAPI |
| Database | PostgreSQL |
| Scraping | Playwright (Chromium) |
| Geo | Shapely, static GeoJSON (MassGIS + OSM) |
| Frontend | Jinja2 + Mapbox GL JS |
| Email | Resend |
| SMS | Twilio |
| Deployment | Railway |

---

## Project Structure

```
ACKuifer/
├── CLAUDE.md                  # Claude Code project brief (read first)
├── ACKuifer_PRD_v9.docx       # Full product requirements document
├── README.md
├── .env.example               # Environment variable template
├── requirements.txt           # Python dependencies
├── prototype/                 # Working prototype code (do not modify)
│   ├── pfas_monitor_v2.py
│   ├── test_report_parser.py
│   ├── address_lookup.py
│   ├── massdep_monitor.py
│   ├── massdep_parser.py
│   ├── pfas_reports.json
│   └── source_discovery_results.json
├── data/                      # Static reference GeoJSON files
│   ├── nantucket_parcels.geojson
│   ├── nantucket_neighborhoods.geojson
│   └── nantucket_water_service.geojson
└── app/                       # FastAPI application
    ├── main.py
    ├── config.py
    ├── database.py
    ├── models/
    ├── routers/
    ├── scrapers/
    ├── geo/
    ├── notifications/
    └── templates/
```

---

## Setup

### Prerequisites
- Python 3.11+
- PostgreSQL (or Railway managed Postgres)
- Node.js (for Claude Code)
- Playwright Chromium: `playwright install chromium`

### Local Development

```bash
# Clone the repo
git clone https://github.com/YOUR_USERNAME/ACKuifer.git
cd ACKuifer

# Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Copy and configure environment variables
cp .env.example .env
# Edit .env with your actual values

# Run the app
uvicorn app.main:app --reload
```

---

## Environment Variables

See `.env.example` for all required variables. Never commit `.env` to version control.

---

## Deployment

Deployed on Railway with three services:
1. Web service (FastAPI)
2. Laserfiche scraper cron job
3. MassDEP Source Discovery scraper cron job

See Railway dashboard for deployment status and logs.

---

## Data Privacy

- Signup addresses are used only for neighborhood resolution and are never stored
- House numbers are never displayed publicly
- No analytics, tracking pixels, or advertising
- Subscriber data is never shared or sold

---

## License

Private. All rights reserved. This project is not open source.
