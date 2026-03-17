"""MassDEP Source Discovery scraper — EEA portal traversal + PDF parsing.

Integrates prototype code from:
  - prototype/pfas_monitor/eea_monitor.py (portal scraper)
  - prototype/pfas_monitor/source_discovery_parser.py (lab_cert + field_report parsers)
  - prototype/pfas_monitor/pace_lab_parser.py (Pace lab format + STREET_EXPANSIONS)
  - prototype/pfas_monitor/sd_geocoder.py (well ID geocoding)

Monitors RTN 4-0029612 on the MassDEP EEA portal for new PDF filings,
downloads them, parses PFAS results, geocodes sample locations, and
stores results in the source_discovery_results table.
"""

import json
import logging
import re
import tempfile
import time
import urllib.parse
import urllib.request
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Optional

import pdfplumber
from playwright.sync_api import sync_playwright, Page, Browser

from app.config import classify_result_status, FALLBACK_NEIGHBORHOOD
from app.database import SessionLocal
from app.geo.neighborhood import lookup_neighborhood
from app.models.results import SourceDiscoveryResult
from app.models.scraper import SeenDocument, ScrapeRun

logger = logging.getLogger(__name__)

# =============================================================================
# Constants
# =============================================================================

RTN = "4-0029612"
PORTAL_URL = f"https://eeaonline.eea.state.ma.us/portal/dep/wastesite/viewer/{RTN}"

# Regulated PFAS6 compounds — Massachusetts 310 CMR 22.07G
REGULATED = ["PFOS", "PFOA", "PFNA", "PFHxS", "PFHpA", "PFDA"]

ALL_COMPOUNDS = [
    "PFOS", "PFOA", "PFNA", "PFHxS", "PFHpA", "PFDA",
    "PFHxA", "PFHpS", "PFBS", "PFBA", "PFPeA",
    "PFUnDA", "PFDoDA", "PFTrDA", "PFTeDA",
    "HFPO-DA", "NEtFOSAA", "NMeFOSAA",
]

# Known monitoring well coordinates for RTN 4-0029612
KNOWN_WELL_COORDS: dict[str, dict] = {
    "VDT-WAITT-12": {"lat": 41.2845, "lng": -70.0598, "address": "Waitt Drive, Nantucket"},
    "VDT-4FG-4":    {"lat": 41.2801, "lng": -70.0623, "address": "4 Fairgrounds Road, Nantucket"},
    "VDT-2FG-5":    {"lat": 41.2799, "lng": -70.0628, "address": "2 Fairgrounds Road, Nantucket"},
    "VDT-2FG-6":    {"lat": 41.2797, "lng": -70.0631, "address": "2 Fairgrounds Road, Nantucket"},
    "VDT-6FG-7":    {"lat": 41.2803, "lng": -70.0617, "address": "6 Fairgrounds Road, Nantucket"},
    "VDT-6FG-8":    {"lat": 41.2806, "lng": -70.0612, "address": "6 Fairgrounds Road, Nantucket"},
    "VDT-6FG-9":    {"lat": 41.2808, "lng": -70.0609, "address": "6 Fairgrounds Road, Nantucket"},
    "VDT-6FG-10":   {"lat": 41.2810, "lng": -70.0605, "address": "6 Fairgrounds Road, Nantucket"},
    "VDT-6FG-11":   {"lat": 41.2812, "lng": -70.0601, "address": "6 Fairgrounds Road, Nantucket"},
    "VDT-OSR-1":    {"lat": 41.2825, "lng": -70.0585, "address": "Old South Road, Nantucket"},
    "VDT-OSR-2":    {"lat": 41.2828, "lng": -70.0582, "address": "Old South Road, Nantucket"},
    "VDT-OSR-3":    {"lat": 41.2831, "lng": -70.0578, "address": "Old South Road, Nantucket"},
    "MW-1":  {"lat": 41.2533, "lng": -70.0621, "address": "Airport Road, Nantucket"},
    "MW-2":  {"lat": 41.2539, "lng": -70.0615, "address": "Airport Road, Nantucket"},
    "MW-3":  {"lat": 41.2545, "lng": -70.0608, "address": "Airport Road, Nantucket"},
}

PROJECT_CENTROID = {"lat": 41.2802, "lng": -70.0625}

# Street name expansion table for Pace lab Client ID addresses
STREET_EXPANSIONS = {
    "FULLING MILL":    "Fulling Mill Road",
    "HAMMOCK POND":    "Hammock Pond Road",
    "HAMMOCK":         "Hammock Pond Road",
    "POLPIS":          "Polpis Road",
    "MILESTONE":       "Milestone Road",
    "MIACOMET":        "Miacomet Road",
    "MADEQUECHAM":     "Madequecham Valley Road",
    "SURFSIDE":        "Surfside Road",
    "HUMMOCK POND":    "Hummock Pond Road",
    "HUMMOCK":         "Hummock Pond Road",
    "FAIRGROUNDS":     "Fairgrounds Road",
    "OLD SOUTH":       "Old South Road",
    "TOMS WAY":        "Toms Way",
    "TOM":             "Toms Way",
    "QUIDNET":         "Quidnet Road",
    "MONOMOY":         "Monomoy Road",
    "WAUWINET":        "Wauwinet Road",
    "SIASCONSET":      "Siasconset",
    "SANKATY":         "Sankaty Road",
    "LOW BEACH":       "Low Beach Road",
    "POCOMO":          "Pocomo Road",
    "AMES":            "Ames Avenue",
    "SQUAM":           "Squam Road",
    "CLIFF":           "Cliff Road",
    "UPPER TAWPAWSHAW": "Upper Tawpawshaw Road",
    "TAWPAWSHAW":      "Tawpawshaw Road",
    "PLAINFIELD":      "Plainfield Road",
    "LONG POND":       "Long Pond Drive",
    "NORWICH":         "Norwich Way",
    "NAUSHON":         "Naushon Way",
}

# MapGeo Nantucket GIS API base
MAPGEO_BASE = "https://nantucket.mapgeo.io"

# =============================================================================
# Regex patterns (from source_discovery_parser.py)
# =============================================================================

WELL_ID_PATTERN = re.compile(
    r"\b(VDT[-_]?[A-Z0-9]+[-_]?\d+[A-Z]?\d*|MW[-_]?\d+[A-Z]?|"
    r"SB[-_]?\d+[A-Z]?|GW[-_]?\d+[A-Z]?|EB[-_]?\d+[A-Z]?)\b",
    re.I,
)

PFAS6_LINE_PATTERN = re.compile(
    r"PFAS[-\s]?6[:\s=]+([<>]?\s*\d+\.?\d*)\s*(ng/L|ug/kg|µg/kg)?",
    re.I,
)

INDIVIDUAL_COMPOUND_PATTERN = re.compile(
    r"(PFOS|PFOA|PFNA|PFHxS|PFHpA|PFHpS|PFDA|PFHxA|PFBS|PFBA|PFPeA|"
    r"PFUnDA|PFDoDA|PFTrDA|PFTeDA|HFPO-DA|NEtFOSAA|NMeFOSAA)"
    r"\s+([<>]?ND|[<>]?\s*\d+\.?\d*(?:E[+-]?\d+)?)\s*(ng/L|ug/kg|µg/kg)?",
    re.I,
)

GPS_PATTERN = re.compile(
    r"(VDT[-_]?[A-Z0-9\-]+|MW[-_]?\d+[A-Z]?|SB[-_]?\d+[A-Z]?)\s+"
    r"(41\.\d{4,6})\s+(-70\.\d{4,6})",
    re.I,
)

# Pace lab compound line regex
PACE_COMPOUND_LINE_RE = re.compile(
    r'^(Perfluoro.+?\([A-Za-z0-9\-]+\))'
    r'\s+(ND|\d[\d\.]*(?:E[+-]?\d+)?)'
    r'\s*(?:[A-Z]\s+)?'
    r'(ng/l|ug/kg)',
    re.I,
)


# =============================================================================
# Utility helpers
# =============================================================================

def _normalise_compound_name(name: str) -> str:
    canonical = {
        "PFOS": "PFOS", "PFOA": "PFOA", "PFNA": "PFNA",
        "PFHXS": "PFHxS", "PFHPS": "PFHpS", "PFHPA": "PFHpA",
        "PFDA": "PFDA", "PFHXA": "PFHxA", "PFBS": "PFBS",
        "PFBA": "PFBA", "PFPEA": "PFPeA", "PFUNDA": "PFUnDA",
        "PFDODA": "PFDoDA", "PFTRDA": "PFTrDA", "PFTEDA": "PFTeDA",
        "HFPO-DA": "HFPO-DA", "NETFOSAA": "NEtFOSAA", "NMEFOSAA": "NMeFOSAA",
    }
    return canonical.get(name.upper(), name)


def _parse_number(s: Optional[str]) -> Optional[float]:
    if s is None:
        return None
    s = str(s).strip()
    if re.match(r"^[<>]?\s*ND$", s, re.I):
        return 0.0
    m = re.search(r"[<>]?\s*(\d+\.?\d*(?:E[+-]?\d+)?)", s, re.I)
    if m:
        try:
            return float(m.group(1))
        except ValueError:
            return None
    return None


def _find_col(header: list, keywords: list) -> Optional[int]:
    for i, h in enumerate(header):
        if any(kw in h for kw in keywords):
            return i
    return None


def _normalise_medium(s: str) -> str:
    s = s.lower()
    if "soil" in s or "solid" in s:
        return "soil"
    return "groundwater"


# =============================================================================
# EEA Portal scraper (adapted from eea_monitor.py — sync Playwright)
# =============================================================================

def _fetch_document_list(page: Page) -> list[dict]:
    """Scrape the EEA portal document list. Returns list of doc dicts."""
    documents = []

    logger.info("Loading EEA portal: %s", PORTAL_URL)
    try:
        page.goto(PORTAL_URL, wait_until="networkidle", timeout=30000)
    except Exception:
        page.goto(PORTAL_URL, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(5000)

    try:
        page.wait_for_selector(
            "table, .document-list, [class*='document'], [class*='report'], "
            "a[href*='.pdf'], a[href*='file']",
            timeout=15000,
        )
    except Exception:
        logger.warning("Document list selector timed out, scraping anyway")

    page.wait_for_timeout(3000)

    # Strategy 1: PDF links directly
    pdf_links = page.eval_on_selector_all(
        "a[href]",
        """elements => elements
            .map(el => ({href: el.href, text: el.textContent.trim()}))
            .filter(l => l.href.toLowerCase().includes('.pdf')
                      || l.href.toLowerCase().includes('file')
                      || l.href.toLowerCase().includes('document'))
        """,
    )
    if pdf_links:
        for link in pdf_links:
            documents.append({
                "title": link["text"] or "Unknown",
                "url": link["href"],
                "date_filed": _extract_date_from_text(link["text"]) or _extract_date_from_text(link["href"]),
                "doc_type": _infer_doc_type(link["text"]),
                "filename": _url_to_filename(link["href"], link["text"]),
            })
        logger.info("Found %d document links via PDF strategy", len(documents))

    # Strategy 2: table rows
    if not documents:
        rows = page.eval_on_selector_all(
            "table tr",
            """rows => rows.map(row => {
                const cells = Array.from(row.querySelectorAll('td, th'));
                const link  = row.querySelector('a[href]');
                return {
                    cells: cells.map(c => c.textContent.trim()),
                    href:  link ? link.href  : null,
                    text:  link ? link.textContent.trim() : null,
                };
            }).filter(r => r.href)""",
        )
        for row in rows:
            title = row.get("text") or (row["cells"][0] if row["cells"] else "Unknown")
            url = row.get("href", "")
            date_filed = _extract_date_from_cells(row.get("cells", []))
            documents.append({
                "title": title,
                "url": url,
                "date_filed": date_filed,
                "doc_type": _infer_doc_type(title),
                "filename": _url_to_filename(url, title),
            })
        if documents:
            logger.info("Found %d documents via table strategy", len(documents))

    # Strategy 3: JSON in page source
    if not documents:
        content = page.content()
        matches = re.findall(r'"(?:url|href|link)"\s*:\s*"([^"]*\.pdf[^"]*)"', content, re.I)
        for url in set(matches):
            documents.append({
                "title": Path(url).stem.replace("_", " "),
                "url": url,
                "date_filed": None,
                "doc_type": _infer_doc_type(url),
                "filename": _url_to_filename(url, url),
            })
        if documents:
            logger.info("Found %d documents via JSON-in-source strategy", len(documents))

    # Deduplicate by URL
    seen = set()
    unique = []
    for d in documents:
        if d["url"] not in seen:
            seen.add(d["url"])
            unique.append(d)

    return unique


def _download_pdf(url: str, dest: Path, browser: Browser) -> bool:
    """Download a PDF, trying direct HTTP first, then Playwright."""
    # Direct HTTP
    try:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/121.0.0.0 Safari/537.36"
            ),
            "Referer": PORTAL_URL,
        }
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=30) as resp:
            if resp.headers.get("Content-Type", "").startswith("application/pdf"):
                dest.write_bytes(resp.read())
                logger.info("Downloaded (direct): %s", dest.name)
                return True
    except Exception as e:
        logger.debug("Direct download failed (%s), trying Playwright", e)

    # Playwright download capture
    try:
        context = browser.new_context(accept_downloads=True)
        dl_page = context.new_page()
        with dl_page.expect_download(timeout=20000) as dl_info:
            dl_page.goto(url, timeout=20000)
        download = dl_info.value
        download.save_as(str(dest))
        context.close()
        logger.info("Downloaded (Playwright): %s", dest.name)
        return True
    except Exception as e:
        logger.warning("Download failed for %s: %s", url, e)
        return False


# =============================================================================
# Document metadata helpers (from eea_monitor.py)
# =============================================================================

def _infer_doc_type(text: str) -> str:
    t = text.lower()
    if "phase i" in t:                                   return "Phase I Site Assessment"
    if "phase ii" in t:                                  return "Phase II Site Assessment"
    if "sampling plan" in t:                             return "Sampling Plan"
    if "field activ" in t or "field inv" in t:           return "Field Activity Report"
    if "analytical" in t or "lab data" in t or "lab report" in t: return "Laboratory Results"
    if "groundwater" in t:                               return "Groundwater Sampling Report"
    if "soil" in t and "sampling" in t:                  return "Soil Sampling Report"
    if "sampling result" in t or "sample result" in t:   return "Sampling Results"
    if "tier" in t:                                      return "Tier Classification"
    if "rao" in t or "response action" in t:             return "RAO Statement"
    if "notification" in t:                              return "Notification"
    if "permit" in t:                                    return "Permit"
    if "transmittal" in t:                               return "Transmittal"
    if "well install" in t or "boring" in t:             return "Well Installation Report"
    if "ins-meet" in t or "inspection" in t or "meeting form" in t: return "Inspection / Meeting"
    if "document upload" in t:                           return "Document Upload"
    if "release amendment" in t or "bwsc102" in t:       return "Release Amendment"
    if "release log" in t or "bwsc101" in t:             return "Release Log"
    if "intake form" in t:                               return "Intake Form"
    if "sarss" in t or "data eval" in t:                 return "Field Investigation Summary"
    if "private well" in t or "residential well" in t:   return "Private Well Sampling"
    if "geothermal" in t:                                return "Geothermal Sampling"
    if "pfas" in t and "sampling" in t:                  return "PFAS Sampling Report"
    return "Report"


def _extract_date_from_text(text: str) -> Optional[str]:
    if not text:
        return None
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", text)
    if m:
        return m.group()
    m = re.search(r"(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})", text)
    if m:
        month, day, year = m.group(1), m.group(2), m.group(3)
        if len(year) == 2:
            year = f"20{year}" if int(year) < 50 else f"19{year}"
        return f"{month.zfill(2)}/{day.zfill(2)}/{year}"
    return None


def _extract_date_from_cells(cells: list) -> Optional[str]:
    for cell in cells:
        result = _extract_date_from_text(cell)
        if result:
            return result
    return None


def _url_to_filename(url: str, title: str) -> str:
    path_part = url.split("?")[0].split("/")[-1]
    if path_part.lower().endswith(".pdf"):
        return re.sub(r"[^\w\-.]", "_", path_part)
    clean = re.sub(r"[^\w\s\-]", "", title)
    clean = re.sub(r"\s+", "_", clean.strip())
    return f"{clean[:80]}_{int(time.time())}.pdf"


# =============================================================================
# PDF format detection (from source_discovery_parser.py)
# =============================================================================

def _detect_format(text: str) -> str:
    """Returns 'lab_cert', 'pace_lab_cert', or 'field_report'."""
    early_text = text[:5000]

    # Check for Pace Analytical lab cert first
    if re.search(r"Pace\s+Analytical", text[:3000], re.I):
        if re.search(r"Lab\s+Sample\s+Collection", text, re.I):
            return "pace_lab_cert"

    narrative_signals = 0
    if re.search(r"table of contents", early_text, re.I):
        narrative_signals += 2
    if re.search(r"executive summary", early_text, re.I):
        narrative_signals += 2
    if re.search(r"statement of purpose", early_text, re.I):
        narrative_signals += 2
    if re.search(r"field investigation", early_text, re.I):
        narrative_signals += 1
    if re.search(r"this report presents", early_text, re.I):
        narrative_signals += 1
    if re.search(r"1\.0\s+INTRODUCTION", early_text):
        narrative_signals += 2
    has_early_narrative = narrative_signals >= 2

    has_client_id = bool(re.search(r"Client\s+ID", text, re.I))
    has_lab_sample = bool(re.search(r"Lab\s+Sample\s+ID", text, re.I))
    has_sample_loc = bool(re.search(r"Sample\s+Location", text, re.I))
    has_gps_table = bool(re.search(r"41\.\d{4,6}\s+-70\.\d{4,6}", text))

    if has_early_narrative and (has_gps_table or len(text) > 20000):
        return "field_report"
    if (has_client_id or has_lab_sample) and has_sample_loc:
        return "lab_cert"
    if has_gps_table:
        return "field_report"
    return "lab_cert"


# =============================================================================
# Lab cert parser (from source_discovery_parser.py)
# =============================================================================

def _parse_lab_cert(text: str, tables: list, pages_text: list) -> list[dict]:
    """Parse lab analytical certificates (Format A)."""
    locations_by_key: dict[tuple, dict] = {}

    for table in tables:
        _parse_lab_cert_table(table, locations_by_key)

    page_blocks = _split_into_sample_blocks(text, pages_text)
    for block in page_blocks:
        _parse_lab_cert_block(block, locations_by_key)

    locations = list(locations_by_key.values())
    return _dedup_locations(locations)


def _split_into_sample_blocks(text: str, pages_text: list) -> list[str]:
    parts = re.split(r"(?=Client\s+ID\s*[:\t])", text, flags=re.I)
    if len(parts) <= 1:
        parts = re.split(r"(?=Lab\s+Sample\s+ID\s*[:\t])", text, flags=re.I)
    return [p for p in parts if p.strip()]


def _parse_lab_cert_block(block: str, locations: dict):
    """Parse one sample block from a lab certificate."""
    # Extract Client ID
    client_id = None
    m = re.search(r"Client\s+ID\s*[:\t]+\s*([^\n\r]+)", block, re.I)
    if m:
        client_id = m.group(1).strip().rstrip(".,;")

    if not client_id:
        m2 = re.search(r"Lab\s+Sample\s+ID\s*[:\t]+\s*([^\n\r]+)", block, re.I)
        if m2:
            client_id = f"LAB-{m2.group(1).strip()}"

    if not client_id:
        return

    # Clean Client ID
    client_id = re.split(r"\s+Date\s+Received:", client_id, flags=re.I)[0].strip()
    client_id = re.sub(r"\s*\[[\d.\-']*\]?\s*$", "", client_id).strip()
    client_id = re.split(r"\s{2,}", client_id)[0].strip()

    # Skip QC/batch control entries
    if re.match(r"^(MS|DUP|LCS|MB|MSD|LCSD)\b", client_id, re.I):
        return
    if re.search(r"\b(MS Sample|DUP Sample|Method Blank|Lab Blank|"
                 r"Equipment Blank|Field Blank|Duplicate\s*\d*)\b", client_id, re.I):
        return

    first_500 = block[:500]
    if re.search(r"Solids,\s*Total|QC\s+Batch|Sample\s+Receipt|Standard\s+Reference", first_500, re.I):
        return

    # Skip truncated Client IDs
    if re.match(r"^\d+$", client_id):
        return
    if re.match(r"^\d+\s+[A-Za-z]", client_id):
        road_suffixes = (r"\b(WAY|ROAD|RD|STREET|ST|LANE|LN|DRIVE|DR|AVE|AVENUE|"
                         r"CIRCLE|CIR|COURT|CT|PLACE|PL|PATH|TRAIL|TRL|BLVD|"
                         r"BOULEVARD|TERRACE|TER|PIKE|HWY|HIGHWAY)\b")
        if not re.search(road_suffixes, client_id, re.I):
            return

    # Sample date
    sample_date = None
    dm = re.search(
        r"(?:Sample\s+Date|Date\s+Collected|Collection\s+Date)\s*[:\t]+\s*"
        r"(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})",
        block, re.I,
    )
    if dm:
        sample_date = dm.group(1)

    # Medium
    medium = None
    matrix_m = re.search(r"Matrix\s*[:\t]+\s*(\S+)", block, re.I)
    if matrix_m:
        mx = matrix_m.group(1).lower()
        if mx in ("dw", "drinkingwater", "drinking"):
            medium = "drinking_water"
        elif mx in ("gw", "groundwater", "water", "aq"):
            medium = "groundwater"
        elif mx in ("soil", "solid", "sediment", "so"):
            medium = "soil"
    if medium is None:
        if re.search(r"\bng/g\b|\bug/kg\b|µg/kg", block, re.I):
            medium = "soil"
        elif re.search(r"\bng/l\b|\bug/l\b|µg/l", block, re.I):
            medium = "groundwater"
    if medium is None:
        for (cid, med) in locations:
            if cid == client_id:
                medium = med
                break
    if medium is None:
        medium = "groundwater"

    # Extract compound results (3 strategies)
    compounds = {}

    # Strategy A: short-form "PFOS  5.2  ng/L"
    for cm in INDIVIDUAL_COMPOUND_PATTERN.finditer(block):
        name = _normalise_compound_name(cm.group(1))
        val = _parse_number(cm.group(2))
        if name not in compounds:
            compounds[name] = val

    # Strategy B: full compound name "(ABBREV) value units"
    for line in block.split("\n"):
        pm = re.search(
            r"\(("
            r"PFOS|PFOA|PFNA|PFHxS|PFHpA|PFHpS|PFDA|PFHxA|PFBS|PFBA|PFPeA|"
            r"PFUnDA|PFDoDA|PFTrDA|PFTeDA|HFPO-DA|NEtFOSAA|NMeFOSAA"
            r")\)\s+([<>]?ND|\d+\.?\d*)\s*([Jj])?\s*(ng/l|ug/kg|µg/kg)?",
            line, re.I,
        )
        if pm:
            name = _normalise_compound_name(pm.group(1))
            val = _parse_number(pm.group(2))
            if name not in compounds:
                compounds[name] = val

    # Strategy C: line-start short-form
    for line in block.split("\n"):
        lm = re.match(
            r"\s*(PFOS|PFOA|PFNA|PFHxS|PFHpA|PFHpS|PFDA|PFHxA|PFBS|PFBA|PFPeA|"
            r"PFUnDA|PFDoDA|PFTrDA|PFTeDA|HFPO-DA|NEtFOSAA|NMeFOSAA)"
            r"\s+([<>]?ND|\d+\.?\d*(?:E[+-]?\d+)?)\s*(ng/L|ug/kg|µg/kg)?",
            line, re.I,
        )
        if lm:
            name = _normalise_compound_name(lm.group(1))
            val = _parse_number(lm.group(2))
            if name not in compounds:
                compounds[name] = val

    # Compute PFAS6
    pfas6 = None
    pfas6_m = PFAS6_LINE_PATTERN.search(block)
    if pfas6_m:
        pfas6 = _parse_number(pfas6_m.group(1))
    else:
        reg_vals = [v for k, v in compounds.items() if k in REGULATED and v is not None]
        if reg_vals:
            pfas6 = round(sum(reg_vals), 3)

    if not compounds and pfas6 is None:
        return

    # Merge into locations dict
    loc_key = (client_id, medium)
    if loc_key in locations:
        existing = locations[loc_key]
        if pfas6 is not None and (existing.get("pfas6") or 0) < pfas6:
            existing["pfas6"] = pfas6
            existing["compounds"] = {**existing["compounds"], **compounds}
        if sample_date and not existing.get("sample_date"):
            existing["sample_date"] = sample_date
    else:
        locations[loc_key] = {
            "well_id": client_id,
            "medium": medium,
            "depth_ft": None,
            "sample_date": sample_date,
            "pfas6": pfas6,
            "compounds": compounds,
            "lat": None,
            "lng": None,
            "address": None,
            "id_source": "client_id",
        }


def _parse_lab_cert_table(table: list, locations: dict):
    """Attempt to parse a pdfplumber-extracted structured table."""
    if not table or len(table) < 2:
        return
    # Lab cert structured tables don't reliably carry Client ID context;
    # the block parser handles this better.
    return


def _dedup_locations(locations: list[dict]) -> list[dict]:
    """Merge locations sharing the same base well ID and sample date."""
    if len(locations) <= 1:
        return locations

    from collections import defaultdict

    def _base_key(well_id: str) -> str:
        key = re.sub(r"[-–]\d+$", "", well_id).strip()
        return re.sub(r"\s+", " ", key).upper()

    def _medium_class(m: str) -> str:
        return "water" if m in ("groundwater", "drinking_water") else m

    groups: dict[tuple, list[dict]] = defaultdict(list)
    for loc in locations:
        base = _base_key(loc["well_id"])
        mclass = _medium_class(loc.get("medium", ""))
        groups[(base, mclass)].append(loc)

    merged = []
    for (_base, _medium), group in groups.items():
        dated = [l for l in group if l.get("sample_date")]
        undated = [l for l in group if not l.get("sample_date")]

        by_date: dict[str, list[dict]] = defaultdict(list)
        for l in dated:
            by_date[l["sample_date"]].append(l)

        if dated and undated:
            first_date = next(iter(by_date))
            by_date[first_date].extend(undated)
        elif undated:
            by_date[""] = undated

        for _date, subgroup in by_date.items():
            if len(subgroup) == 1:
                merged.append(subgroup[0])
                continue
            best = max(subgroup, key=lambda l: (len(l["well_id"]), l.get("pfas6") or 0))
            all_compounds = {}
            for loc in subgroup:
                all_compounds.update(loc.get("compounds", {}))
            best["compounds"] = all_compounds
            pfas6_vals = [l.get("pfas6") for l in subgroup if l.get("pfas6") is not None]
            if pfas6_vals:
                best["pfas6"] = max(pfas6_vals)
            merged.append(best)

    return merged


# =============================================================================
# Pace lab cert parser (from pace_lab_parser.py)
# =============================================================================

def _parse_pace_lab_cert(text: str, pages_text: list) -> list[dict]:
    """Parse Pace Analytical compiled lab certificate PDFs."""
    # Pass 1: build sample index from Lab Sample Collection tables
    sample_index: dict[str, dict] = {}
    LAB_SAMPLE_ROW_RE = re.compile(
        r'(L\d+[-\d]+)\s+'
        r'(.+?)\s+'
        r'(DW|GW|SW|SO|Dw)\s+'
        r'(.+?)\s+'
        r'(\d{2}/\d{2}/\d{2})',
        re.I,
    )
    for page_text in pages_text:
        if "Lab Sample Collection" not in page_text:
            continue
        for line in page_text.split('\n'):
            m = LAB_SAMPLE_ROW_RE.search(line)
            if m:
                lab_id = m.group(1)
                client_id = m.group(2).strip()
                date_str = m.group(5)
                sample_index[lab_id] = {
                    "client_id": client_id,
                    "matrix": "groundwater",
                    "sample_date": date_str,
                }

    # Pass 2: extract compound results per sample
    samples: dict[str, dict] = {}
    current_client_id = None

    for page_text in pages_text:
        if "SAMPLE RESULTS" in page_text and "Client ID:" in page_text:
            lab_m = re.search(r'Lab ID:\s*(L\d+[-\d]+)', page_text)
            cid_m = re.search(r'Client ID:\s+(.+?)(?:\n|Date Received)', page_text)
            if lab_m and cid_m:
                current_lab_id = lab_m.group(1)
                current_client_id = cid_m.group(1).strip()

                if current_client_id not in samples:
                    parsed_addr = _parse_pace_client_id(current_client_id)
                    meta = sample_index.get(current_lab_id, {})
                    samples[current_client_id] = {
                        "client_id": current_client_id,
                        "address": parsed_addr["address"],
                        "house_number": parsed_addr["house_number"],
                        "street_full": parsed_addr["street_full"],
                        "suffix": parsed_addr["suffix"],
                        "sample_date": meta.get("sample_date"),
                        "medium": meta.get("matrix", "groundwater"),
                        "compounds": {},
                        "lat": None,
                        "lng": None,
                    }

        if current_client_id and f"Client ID: {current_client_id}" in page_text:
            for line in page_text.split('\n'):
                cm = PACE_COMPOUND_LINE_RE.match(line)
                if not cm:
                    continue
                full_name = cm.group(1)
                val = _parse_number(cm.group(2))
                abbrev_m = re.search(r'\(([A-Za-z0-9\-]+)\)', full_name)
                if abbrev_m:
                    abbrev = _normalise_compound_name(abbrev_m.group(1))
                    if abbrev in REGULATED:
                        rec = samples.get(current_client_id)
                        if rec and abbrev not in rec["compounds"]:
                            rec["compounds"][abbrev] = val

    if not samples:
        return []

    # Pass 3: compute PFAS6
    locations = []
    for cid, rec in sorted(samples.items()):
        compounds = rec["compounds"]
        regulated_vals = [v for k, v in compounds.items() if k in REGULATED and v is not None]
        pfas6 = round(sum(regulated_vals), 3) if regulated_vals else None

        locations.append({
            "well_id": cid,
            "medium": rec["medium"],
            "depth_ft": None,
            "sample_date": rec["sample_date"],
            "pfas6": pfas6,
            "compounds": compounds,
            "lat": rec["lat"],
            "lng": rec["lng"],
            "address": rec["address"],
            "id_source": "client_id_address",
        })

    return locations


def _parse_pace_client_id(raw: str) -> dict:
    """Parse a Pace Client ID into address components."""
    raw = raw.strip()
    suffix = None

    if raw.endswith("_INF"):
        suffix = "INF"
        raw = raw[:-4].strip()
    elif raw.endswith("_EFF"):
        suffix = "EFF"
        raw = raw[:-4].strip()

    m = re.match(r'^(\d+)\s+(.+)$', raw)
    if not m:
        return {"raw": raw, "address": f"{raw}, Nantucket, MA", "suffix": suffix,
                "house_number": None, "street_full": None}

    house_num = m.group(1)
    street_abbrev = m.group(2).strip().upper()

    street_full = None
    for abbrev in sorted(STREET_EXPANSIONS.keys(), key=len, reverse=True):
        if abbrev in street_abbrev:
            street_full = STREET_EXPANSIONS[abbrev]
            break

    full_address = f"{house_num} {street_full or street_abbrev}, Nantucket, MA"
    return {
        "raw": raw,
        "address": full_address,
        "house_number": house_num,
        "street_full": street_full,
        "suffix": suffix,
    }


# =============================================================================
# Field report parser (from source_discovery_parser.py)
# =============================================================================

def _parse_field_report(text: str, tables: list) -> list[dict]:
    """Parse field investigation / narrative reports."""
    locations = []

    for table in tables:
        locs = _parse_pfas_table(table)
        if locs:
            locations.extend(locs)

    if not locations:
        locations = _parse_free_text_locations(text)

    _attach_coordinates(locations, text)
    return locations


def _parse_pfas_table(table: list) -> list[dict]:
    """Parse a pdfplumber-extracted PFAS summary table."""
    if not table or len(table) < 2:
        return []

    compound_header = None
    compound_header_idx = 0
    for i, row in enumerate(table[:8]):
        if row and any(re.search(r"PFAS|PFOS|PFOA", str(c) or "", re.I) for c in row):
            compound_header = [str(c).strip().lower() if c else "" for c in row]
            compound_header_idx = i
            break

    if not compound_header:
        return []

    col = {
        "well_id": _find_col(compound_header, ["well", "location", "station", "sample id", "boring", "client", "sample"]),
        "pfas6":   _find_col(compound_header, ["pfas6", "pfas 6", "pfas-6", "sum", "total pfas6"]),
        "pfos":    _find_col(compound_header, ["pfos"]),
        "pfoa":    _find_col(compound_header, ["pfoa"]),
        "pfna":    _find_col(compound_header, ["pfna"]),
        "pfhxs":   _find_col(compound_header, ["pfhxs"]),
        "pfhpa":   _find_col(compound_header, ["pfhpa"]),
        "pfda":    _find_col(compound_header, ["pfda"]),
        "medium":  _find_col(compound_header, ["medium", "matrix", "type"]),
        "depth":   _find_col(compound_header, ["depth", "ft", "feet"]),
        "date":    _find_col(compound_header, ["date", "collected", "sampled"]),
    }

    # If well_id not found, check subsequent rows
    if col["well_id"] is None:
        for i, row in enumerate(table[compound_header_idx + 1:compound_header_idx + 8]):
            if row and any(re.search(r"^sample$|^well", str(c) or "", re.I) for c in row):
                sub_header = [str(c).strip().lower() if c else "" for c in row]
                col["well_id"] = _find_col(sub_header, ["sample", "well", "boring"])
                if col["date"] is None:
                    col["date"] = _find_col(sub_header, ["date", "collected"])
                break
        if col["well_id"] is None:
            for row in table[compound_header_idx + 1:]:
                if row and row[0] and re.search(r"VDT-|MW-|SB-", str(row[0]), re.I):
                    col["well_id"] = 0
                    break

    if col["well_id"] is None:
        return []

    locations = []
    for row in table[compound_header_idx + 1:]:
        if not row or all(c is None or str(c).strip() == "" for c in row):
            continue

        def cell(key):
            idx = col.get(key)
            if idx is None or idx >= len(row):
                return None
            return str(row[idx]).strip() if row[idx] else None

        well_id = cell("well_id")
        if not well_id or len(well_id) < 2:
            continue
        if re.search(r"^(pfas|well|location|sample$|massachusetts|duplicate)", well_id, re.I):
            continue
        if re.search(r"^\d+\s+(Fairgrounds|Old South|Ticcoma|Waitt)", well_id, re.I):
            continue
        if re.search(r"(DUPLICATE|BLANK|GW DUPLICATE|SOIL DUPLICATE)", well_id, re.I):
            continue

        compounds = {
            "PFOS":  _parse_number(cell("pfos")),
            "PFOA":  _parse_number(cell("pfoa")),
            "PFNA":  _parse_number(cell("pfna")),
            "PFHxS": _parse_number(cell("pfhxs")),
            "PFHpA": _parse_number(cell("pfhpa")),
            "PFDA":  _parse_number(cell("pfda")),
        }
        pfas6 = _parse_number(cell("pfas6"))
        if pfas6 is None:
            vals = [v for v in compounds.values() if v is not None]
            pfas6 = round(sum(vals), 3) if vals else None

        medium_val = cell("medium") or ""
        if not medium_val and compound_header_idx + 1 < len(table):
            units_row = table[compound_header_idx + 1]
            if units_row:
                units_str = " ".join(str(c) or "" for c in units_row).lower()
                if "ng/g" in units_str or "ug/kg" in units_str:
                    medium_val = "soil"
                else:
                    medium_val = "groundwater"

        locations.append({
            "well_id": well_id,
            "medium": _normalise_medium(medium_val),
            "depth_ft": _parse_number(cell("depth")),
            "sample_date": cell("date"),
            "pfas6": pfas6,
            "compounds": compounds,
            "lat": None,
            "lng": None,
            "address": None,
            "id_source": "field_table",
        })

    return locations


def _parse_free_text_locations(text: str) -> list[dict]:
    """Parse space-aligned data rows in field reports."""
    locations = []
    lines = text.split("\n")

    header_cols = []
    header_line_idx = None
    for i, line in enumerate(lines):
        if re.search(r"\bPFAS[-\s]?6\b", line, re.I) and re.search(r"\bPFOS\b", line, re.I):
            parts = re.split(r"\s{2,}", line.strip())
            header_cols = [p.strip().upper() for p in parts if p.strip()]
            header_line_idx = i
            break

    for i, line in enumerate(lines):
        m = WELL_ID_PATTERN.search(line)
        if not m or i == header_line_idx:
            continue

        well_id = m.group()
        window = "\n".join(lines[max(0, i - 3):min(len(lines), i + 8)])

        pfas6 = None
        pm = PFAS6_LINE_PATTERN.search(window)
        if pm:
            pfas6 = _parse_number(pm.group(1))

        tabular_compounds = {}
        if pfas6 is None:
            rest = line[m.end():].strip()
            tokens = re.split(r"\s+", rest)
            numeric_tokens = [_parse_number(t) for t in tokens]

            if numeric_tokens and numeric_tokens[0] is not None:
                if header_cols:
                    for hi, hcol in enumerate(header_cols):
                        if re.search(r"PFAS[-\s]?6", hcol):
                            pfas6_col = hi - 1
                            if 0 <= pfas6_col < len(numeric_tokens):
                                pfas6 = numeric_tokens[pfas6_col]
                            for reg in REGULATED:
                                for hj, hcol2 in enumerate(header_cols):
                                    if hcol2 == reg:
                                        cidx = hj - 1
                                        if 0 <= cidx < len(numeric_tokens) and numeric_tokens[cidx] is not None:
                                            tabular_compounds[reg] = numeric_tokens[cidx]
                            break
                else:
                    pfas6 = numeric_tokens[0]

        compounds = {}
        for cm in INDIVIDUAL_COMPOUND_PATTERN.finditer(window):
            name = _normalise_compound_name(cm.group(1))
            val = _parse_number(cm.group(2))
            compounds[name] = val
        compounds.update(tabular_compounds)

        medium = "groundwater"
        if re.search(r"\bsoil\b|\bSB[-_\d]|\bboring\b", window, re.I):
            medium = "soil"

        if pfas6 is None and not compounds:
            continue
        if any(l["well_id"] == well_id for l in locations):
            continue

        if pfas6 is None and compounds:
            reg_vals = [v for k, v in compounds.items() if k in REGULATED and v is not None]
            pfas6 = round(sum(reg_vals), 3) if reg_vals else None

        locations.append({
            "well_id": well_id,
            "medium": medium,
            "depth_ft": None,
            "sample_date": None,
            "pfas6": pfas6,
            "compounds": compounds,
            "lat": None,
            "lng": None,
            "address": None,
            "id_source": "free_text",
        })

    return locations


def _attach_coordinates(locations: list, text: str):
    """Match GPS coordinate table entries to well IDs."""
    coord_map = {}
    for m in GPS_PATTERN.finditer(text):
        coord_map[m.group(1).upper()] = {
            "lat": float(m.group(2)),
            "lng": float(m.group(3)),
        }
    for loc in locations:
        wid = loc["well_id"].upper()
        if wid in coord_map:
            loc["lat"] = coord_map[wid]["lat"]
            loc["lng"] = coord_map[wid]["lng"]


# =============================================================================
# PDF parsing entry point
# =============================================================================

def _parse_pdf(pdf_path: str) -> Optional[list[dict]]:
    """Parse a Source Discovery PDF and return list of sample locations.

    Returns None if parsing fails or no data found.
    """
    path = Path(pdf_path)
    try:
        with pdfplumber.open(str(path)) as pdf:
            pages_text = [page.extract_text() or "" for page in pdf.pages]
            all_text = "\n".join(pages_text)
            tables = []
            for page in pdf.pages:
                t = page.extract_tables()
                if t:
                    tables.extend(t)
    except Exception as e:
        logger.error("pdfplumber error on %s: %s", path.name, e)
        return None

    fmt = _detect_format(all_text)
    logger.info("  Detected format: %s", fmt)

    if fmt == "pace_lab_cert":
        locations = _parse_pace_lab_cert(all_text, pages_text)
    elif fmt == "lab_cert":
        locations = _parse_lab_cert(all_text, tables, pages_text)
    else:
        locations = _parse_field_report(all_text, tables)

    if not locations:
        return None

    # Apply status via production classify_result_status
    for loc in locations:
        pfas6 = loc.get("pfas6")
        medium = loc.get("medium", "groundwater")
        if medium == "soil":
            loc["status"] = "DETECT" if pfas6 and pfas6 > 0 else "NON-DETECT"
        else:
            loc["status"] = classify_result_status(pfas6)

    return locations


# =============================================================================
# Geocoding (adapted from sd_geocoder.py)
# =============================================================================

def _address_to_latlong(address: str) -> Optional[dict]:
    """Convert a Nantucket address to {lat, lng} via MapGeo API."""
    if "nantucket" not in address.lower():
        address = f"{address}, Nantucket, MA"

    encoded = urllib.parse.quote(address)
    url = f"{MAPGEO_BASE}/datasets/parcels/query?query={encoded}&limit=5"

    try:
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": "ACKuifer/1.0",
                "Accept": "application/json",
                "Referer": MAPGEO_BASE,
            },
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
        candidates = data.get("features") or data.get("results") or []
        if not candidates:
            return None

        top = candidates[0]
        geom = top.get("geometry") or {}
        coords = geom.get("coordinates", [])
        if not coords or len(coords) < 2:
            return None

        return {"lat": coords[1], "lng": coords[0]}
    except Exception as e:
        logger.debug("MapGeo lookup failed for '%s': %s", address, e)
        return None


def _derive_address_from_well_id(well_id: str) -> Optional[str]:
    """Parse Verdantas well ID naming convention to infer address."""
    m = re.search(
        r"(?:VDT[-_]?)?((\d+)FG|WAITT|OSR|TW|TOMSWAY|FAIRGROUNDS|AIRPORT)",
        well_id, re.I,
    )
    if not m:
        return None

    code = m.group(1).upper()
    num = m.group(2)

    mapping = {
        "FG": f"{num} Fairgrounds Road, Nantucket, MA" if num else "Fairgrounds Road, Nantucket, MA",
        "WAITT": "Waitt Drive, Nantucket, MA",
        "OSR": "Old South Road, Nantucket, MA",
        "TW": "Tom's Way, Nantucket, MA",
        "TOMSWAY": "Tom's Way, Nantucket, MA",
        "FAIRGROUNDS": "Fairgrounds Road, Nantucket, MA",
        "AIRPORT": "14 Airport Road, Nantucket, MA",
    }

    for key, addr in mapping.items():
        if code.endswith(key) or code == key:
            return addr
    return None


def _geocode_location(loc: dict) -> dict:
    """Geocode a parsed sample location. Returns {lat, lng, geocode_method, review_needed}."""
    well_id = loc.get("well_id", "")
    wid_upper = well_id.upper().replace(" ", "-")

    # Already has GPS from PDF
    if loc.get("lat") and loc.get("lng"):
        return {
            "lat": loc["lat"],
            "lng": loc["lng"],
            "geocode_method": "pdf_gps",
            "review_needed": False,
        }

    # Known well table
    if wid_upper in KNOWN_WELL_COORDS:
        entry = KNOWN_WELL_COORDS[wid_upper]
        return {
            "lat": entry["lat"],
            "lng": entry["lng"],
            "geocode_method": "known_table",
            "review_needed": False,
        }

    # Derive address from well ID
    derived = _derive_address_from_well_id(well_id)
    if derived:
        coords = _address_to_latlong(derived)
        if coords:
            return {
                "lat": coords["lat"],
                "lng": coords["lng"],
                "geocode_method": "derived_address",
                "review_needed": False,
            }
        time.sleep(0.3)

    # Address from Pace Client ID parsing
    if loc.get("address"):
        coords = _address_to_latlong(loc["address"])
        if coords:
            return {
                "lat": coords["lat"],
                "lng": coords["lng"],
                "geocode_method": "client_id_address",
                "review_needed": False,
            }
        time.sleep(0.3)

    # Centroid fallback — flag for review
    return {
        "lat": PROJECT_CENTROID["lat"],
        "lng": PROJECT_CENTROID["lng"],
        "geocode_method": "centroid_fallback",
        "review_needed": True,
    }


def geocode_location(loc: dict) -> dict:
    """Public interface to _geocode_location for use by Task 3."""
    return _geocode_location(loc)


# =============================================================================
# Date parsing
# =============================================================================

def _parse_sample_date(raw: Optional[str]) -> Optional[datetime]:
    """Parse date string in various formats."""
    if not raw:
        return None
    for fmt in ("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d", "%m-%d-%Y", "%m-%d-%y"):
        try:
            return datetime.strptime(raw.strip(), fmt)
        except ValueError:
            continue
    return None


# =============================================================================
# Main scraper
# =============================================================================

def run_massdep_scraper(
    headless: bool = True,
    force: bool = False,
) -> dict:
    """Run the MassDEP Source Discovery scraper.

    Args:
        headless: Run browser in headless mode.
        force: Re-download and re-parse all documents.

    Returns:
        Summary dict with run stats.
    """
    # Create scrape run record (short-lived session)
    with SessionLocal() as db:
        run = ScrapeRun(source="massdep", status="running")
        db.add(run)
        db.commit()
        db.refresh(run)
        run_id = str(run.id)

    stats = {
        "new_docs_found": 0,
        "new_docs_parsed": 0,
        "parse_errors": 0,
        "skipped_seen": 0,
        "errors": [],
    }

    final_status = "success"
    error_message = None

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=headless)
            page = browser.new_page()
            page.set_default_timeout(30000)

            # Fetch document list from EEA portal
            documents = _fetch_document_list(page)
            logger.info("Found %d documents on EEA portal", len(documents))

            if not documents:
                logger.warning(
                    "No documents found on EEA portal. The portal may require "
                    "interactive login or the page structure has changed."
                )

            for doc in documents:
                _process_eea_document(doc, stats, browser, force=force)

            browser.close()

    except Exception as e:
        logger.error("Scraper run failed: %s", e, exc_info=True)
        final_status = "error"
        error_message = str(e)[:2000]

    # Finalize scrape run record (fresh session)
    with SessionLocal() as db:
        run = db.query(ScrapeRun).get(run_id)
        if run:
            run.status = final_status
            run.error_message = error_message
            run.completed_at = datetime.utcnow()
            run.new_docs_found = stats["new_docs_found"]
            run.new_docs_parsed = stats["new_docs_parsed"]
            run.parse_errors = stats["parse_errors"]
            db.commit()

    # Send notifications if the run succeeded and found new results
    if final_status == "success" and stats["new_docs_parsed"] > 0:
        try:
            from app.notifications.engine import run_notifications
            with SessionLocal() as db:
                notif_summary = run_notifications(run_id, db)
                logger.info("Notifications: %s", notif_summary)
        except Exception:
            logger.exception("Notification dispatch failed (scrape data is safe)")

    return {
        "run_id": run_id,
        "status": final_status,
        "new_docs_found": stats["new_docs_found"],
        "new_docs_parsed": stats["new_docs_parsed"],
        "parse_errors": stats["parse_errors"],
        "skipped_seen": stats["skipped_seen"],
        "errors": stats["errors"],
    }


def _process_eea_document(
    doc: dict,
    stats: dict,
    browser: Browser,
    force: bool = False,
):
    """Check, download, parse, and store a single EEA document."""
    doc_url = doc["url"]

    # Check if already seen (short-lived session)
    with SessionLocal() as db:
        existing = db.query(SeenDocument).filter_by(doc_key=doc_url).first()
        if existing and not force:
            stats["skipped_seen"] += 1
            logger.debug("SKIP (seen): %s", doc["title"])
            return

        if not existing:
            # Record in seen_documents BEFORE parsing
            seen = SeenDocument(
                doc_key=doc_url,
                source="massdep",
                parse_status="pending",
            )
            db.add(seen)
            db.commit()

    logger.info("NEW: %s", doc["title"])
    stats["new_docs_found"] += 1

    # Download PDF to temp file
    with tempfile.TemporaryDirectory() as tmpdir:
        pdf_path = Path(tmpdir) / doc["filename"]
        if not _download_pdf(doc_url, pdf_path, browser):
            with SessionLocal() as db:
                seen = db.query(SeenDocument).filter_by(doc_key=doc_url).first()
                if seen:
                    seen.parse_status = "error"
                    seen.error_message = "Download failed"
                db.commit()
            stats["parse_errors"] += 1
            stats["errors"].append({"doc_id": doc_url, "error": "Download failed"})
            return

        # Parse PDF
        try:
            locations = _parse_pdf(str(pdf_path))
            if not locations:
                raise ValueError("No sample locations found in PDF")

            # Filter to groundwater/drinking_water only (skip soil for map display)
            gw_locations = [
                loc for loc in locations
                if loc.get("medium") in ("groundwater", "drinking_water")
            ]

            saved_count = 0
            for loc in gw_locations:
                _save_location(loc, doc_url, stats)
                saved_count += 1

            logger.info(
                "  Parsed: %d groundwater locations from %s",
                saved_count, doc["filename"],
            )

            # Mark seen_document as success
            with SessionLocal() as db:
                seen = db.query(SeenDocument).filter_by(doc_key=doc_url).first()
                if seen:
                    seen.parse_status = "success"
                db.commit()

        except Exception as e:
            logger.error("Parse error for %s: %s", doc["title"], e, exc_info=True)
            with SessionLocal() as db:
                seen = db.query(SeenDocument).filter_by(doc_key=doc_url).first()
                if seen:
                    seen.parse_status = "error"
                    seen.error_message = str(e)[:2000]
                db.commit()
            stats["parse_errors"] += 1
            stats["errors"].append({"doc_id": doc_url, "error": str(e)})


def _save_location(loc: dict, doc_url: str, stats: dict):
    """Geocode and save a single sample location to DB.

    Deduplicates by (sample_location, sample_date, medium) composite key.
    When a match exists, merges compound values (max non-null per compound),
    recalculates pfas6_sum and result_status.
    """
    geo = _geocode_location(loc)

    sample_date = _parse_sample_date(loc.get("sample_date"))
    sample_date_val = sample_date.date() if sample_date else None

    well_id = loc.get("well_id", "unknown")
    medium = loc.get("medium", "groundwater")
    depth_ft = loc.get("depth_ft")
    depth_str = str(depth_ft) if depth_ft is not None else None

    # Build source_doc_url for record provenance (not used for dedup)
    unique_url = f"{doc_url}#{well_id}"

    neighborhood = lookup_neighborhood(geo["lat"], geo["lng"]) if geo["lat"] and geo["lng"] else FALLBACK_NEIGHBORHOOD

    compounds = loc.get("compounds", {})

    def to_dec(val):
        if val is None or val == 0:
            return None
        return Decimal(str(val))

    compound_fields = {
        "pfos": to_dec(compounds.get("PFOS")),
        "pfoa": to_dec(compounds.get("PFOA")),
        "pfhxs": to_dec(compounds.get("PFHxS")),
        "pfna": to_dec(compounds.get("PFNA")),
        "pfhpa": to_dec(compounds.get("PFHpA")),
        "pfda": to_dec(compounds.get("PFDA")),
    }

    with SessionLocal() as db:
        # Composite-key dedup: same sample from a different PDF gets merged
        existing = db.query(SourceDiscoveryResult).filter_by(
            sample_location=well_id,
            sample_date=sample_date_val,
            medium=medium,
        ).first()

        if existing:
            # Merge compounds: keep max non-null for each
            merged_any = False
            for field, new_val in compound_fields.items():
                old_val = getattr(existing, field)
                if new_val is not None:
                    if old_val is None or new_val > old_val:
                        setattr(existing, field, new_val)
                        merged_any = True

            if merged_any:
                # Recalculate pfas6_sum from merged compounds
                merged_vals = [
                    float(getattr(existing, f))
                    for f in compound_fields
                    if getattr(existing, f) is not None
                ]
                new_pfas6 = round(sum(merged_vals), 2)
                existing.pfas6_sum = Decimal(str(new_pfas6))
                existing.result_status = classify_result_status(new_pfas6)
                db.commit()
                logger.info(
                    "    Merged: %s PFAS6=%.1f status=%s (cross-PDF merge)",
                    well_id, new_pfas6, existing.result_status,
                )
            else:
                logger.debug("  Skipping duplicate (no new data): %s", well_id)
            return

        # New record — insert with depth and medium
        result = SourceDiscoveryResult(
            source_doc_url=unique_url,
            sample_location=well_id,
            sample_date=sample_date_val,
            **compound_fields,
            pfas6_sum=to_dec(loc.get("pfas6")),
            result_status=loc.get("status", "NON-DETECT"),
            neighborhood=neighborhood,
            latitude=Decimal(str(geo["lat"])),
            longitude=Decimal(str(geo["lng"])),
            depth=depth_str,
            medium=medium,
            geocode_review_needed=geo["review_needed"],
        )
        db.add(result)
        db.commit()

    stats["new_docs_parsed"] += 1
    logger.info(
        "    Saved: %s PFAS6=%.1f status=%s neighborhood=%s geo=%s",
        well_id,
        float(to_dec(loc.get("pfas6")) or 0),
        loc.get("status"),
        neighborhood,
        geo["geocode_method"],
    )
