"""
pace_lab_parser.py
------------------
Parses Pace Analytical lab certificate PDFs filed under RTN 4-0029612.

These are compiled PDFs containing multiple individual Pace lab reports,
each covering one or two samples. Structure:
  - Each sub-report is ~30 pages (with QC, blanks, etc.)
  - Lab Sample Collection table (page 2 of each sub-report) = sample index
  - SAMPLE RESULTS pages (page 7+) = compound-level data

Key field mapping (verified against Dec 2025 filing):
  Client ID       → PROPERTY ADDRESS (e.g. "4 FULLING MILL" = 4 Fulling Mill Road)
  Sample Location → STAGING LOCATION ("FAIRGROUNDS FIRE DEPARTMENT") — NOT the sample address
  _INF suffix     → Influent: raw well water before any treatment filter
  _EFF suffix     → Effluent: water after filter/treatment system

The Client ID is the correct location key. Sample Location is always ignored
for geocoding purposes.

Compound results format (single-space separated):
  "Perfluorohexanesulfonic Acid (PFHxS) 143 ng/l 1.84 0.614 1"
  "Perfluorooctanoic Acid (PFOA) ND ng/l 1.84 0.614 1"
  "Perfluoroheptanoic Acid (PFHpA) 0.808 J ng/l 1.84 0.614 1"  (J = estimated)
"""

import re
from pathlib import Path
from datetime import datetime
from typing import Optional, Union

try:
    import pdfplumber
except ImportError:
    raise ImportError("Run: pip install pdfplumber --break-system-packages")

# ── Regulated PFAS6 compounds ─────────────────────────────────────────────────
# (same 6 as MA MCL standard)
REGULATED_ABBREVS = {"PFOS", "PFOA", "PFNA", "PFHxS", "PFHpS", "PFDA"}

# ── Street name expansion table for Nantucket ─────────────────────────────────
# Keys are the uppercase abbreviations used in Pace Client IDs.
# Order matters — check longer strings first to avoid partial matches.
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

# Regex to match compound result lines
# Handles: "Compound Name (ABBREV) value [qualifier] units RL MDL dilution"
COMPOUND_LINE_RE = re.compile(
    r'^(Perfluoro.+?\([A-Za-z0-9\-]+\))'
    r'\s+(ND|\d[\d\.]*(?:E[+-]?\d+)?)'
    r'\s*(?:[A-Z]\s+)?'      # optional data qualifier like 'J'
    r'(ng/l|ug/kg)',
    re.I
)


# ── Address parsing ───────────────────────────────────────────────────────────

def parse_client_id(raw: str) -> dict:
    """
    Parse a Pace Client ID into address components.

    Examples:
      "4 FULLING MILL"        → {address: "4 Fulling Mill Road, Nantucket, MA", suffix: None}
      "9 FULLING MILL_INF"   → {address: "9 Fulling Mill Road, Nantucket, MA", suffix: "INF"}
      "11 FULLING MILL_EFF"  → {address: "11 Fulling Mill Road, Nantucket, MA", suffix: "EFF"}
      "82 HAMMOCK"           → {address: "82 Hammock Pond Road, Nantucket, MA", suffix: None}
    """
    raw = raw.strip()
    suffix = None

    if raw.endswith("_INF"):
        suffix = "INF"
        raw = raw[:-4].strip()
    elif raw.endswith("_EFF"):
        suffix = "EFF"
        raw = raw[:-4].strip()

    # Split house number from street abbreviation
    m = re.match(r'^(\d+)\s+(.+)$', raw)
    if not m:
        return {"raw": raw, "address": f"{raw}, Nantucket, MA", "suffix": suffix,
                "house_number": None, "street_full": None}

    house_num    = m.group(1)
    street_abbrev = m.group(2).strip().upper()

    # Expand abbreviation to full street name (longest match first)
    street_full = None
    for abbrev in sorted(STREET_EXPANSIONS.keys(), key=len, reverse=True):
        if abbrev in street_abbrev:
            street_full = STREET_EXPANSIONS[abbrev]
            break

    full_address = f"{house_num} {street_full or street_abbrev}, Nantucket, MA"

    return {
        "raw":          raw,
        "address":      full_address,
        "house_number": house_num,
        "street_full":  street_full,
        "suffix":       suffix,
        "suffix_label": {
            "INF": "influent (raw well water, before filter)",
            "EFF": "effluent (after treatment filter)",
            None:  "untreated well water",
        }[suffix],
    }


# ── Value parsing ─────────────────────────────────────────────────────────────

def parse_result(s: str) -> Optional[float]:
    """Convert 'ND', '143', '0.808', '1.28 J', etc. → float (ND → 0.0)."""
    s = s.strip()
    if s == 'ND' or s.startswith('<'):
        return 0.0
    m = re.match(r'(\d+\.?\d*(?:E[+-]?\d+)?)', s)
    return float(m.group(1)) if m else None


# ── Status helpers ────────────────────────────────────────────────────────────

def status_from_pfas6(pfas6: Optional[float]) -> str:
    if pfas6 is None:   return "UNKNOWN"
    if pfas6 == 0:      return "NON-DETECT"
    if pfas6 <= 20.0:   return "DETECT"
    if pfas6 <= 89.9:   return "HIGH-DETECT"
    return "HAZARD"

STATUS_COLOR = {
    "NON-DETECT":  "green",
    "DETECT":      "yellow",
    "HIGH-DETECT": "red",
    "HAZARD":      "purple",
    "UNKNOWN":     "gray",
}


# ── Main parser ───────────────────────────────────────────────────────────────

def parse_pace_lab_pdf(pdf_path: str, doc_meta: dict) -> Optional[dict]:
    """
    Parse a Pace Analytical compiled lab certificate PDF.
    Returns a structured report dict for source_discovery_db, or None on failure.
    """
    path = Path(pdf_path)
    if not path.exists():
        print(f"  [Pace Parser] File not found: {pdf_path}")
        return None

    try:
        with pdfplumber.open(str(path)) as pdf:
            pages_text = [page.extract_text() or "" for page in pdf.pages]
    except Exception as e:
        print(f"  [Pace Parser] pdfplumber error: {e}")
        return None

    # ── Pass 1: build sample index from Lab Sample Collection tables ──────────
    # These appear on page 2 of each ~30-page sub-report
    # Format: "L2580387-01  4 FULLING MILL  DW  FAIRGROUNDS FIRE DEPARTMENT  12/16/25 12:51  12/17/25"
    sample_index: dict[str, dict] = {}  # lab_sample_id → {client_id, date, matrix}
    LAB_SAMPLE_ROW_RE = re.compile(
        r'(L\d+[-\d]+)\s+'        # Lab Sample ID
        r'(.+?)\s+'               # Client ID (address)
        r'(DW|GW|SW|SO|Dw)\s+'   # Matrix
        r'(.+?)\s+'               # Sample Location (staging — ignored)
        r'(\d{2}/\d{2}/\d{2})',   # Date
        re.I
    )
    for text in pages_text:
        if "Lab Sample Collection" not in text:
            continue
        for line in text.split('\n'):
            m = LAB_SAMPLE_ROW_RE.search(line)
            if m:
                lab_id   = m.group(1)
                client_id = m.group(2).strip()
                matrix   = m.group(3).upper()
                date_str = m.group(5)
                sample_index[lab_id] = {
                    "client_id":   client_id,
                    "matrix":      "groundwater",  # DW = drinking water = well water
                    "sample_date": date_str,
                }

    # ── Pass 2: extract compound results per sample ────────────────────────────
    samples: dict[str, dict] = {}  # client_id → full sample record
    current_lab_id = None
    current_client_id = None

    for text in pages_text:
        # New sample result section?
        if "SAMPLE RESULTS" in text and "Client ID:" in text:
            lab_m = re.search(r'Lab ID:\s*(L\d+[-\d]+)', text)
            cid_m = re.search(r'Client ID:\s+(.+?)(?:\n|Date Received)', text)
            if lab_m and cid_m:
                current_lab_id    = lab_m.group(1)
                current_client_id = cid_m.group(1).strip()

                if current_client_id not in samples:
                    parsed_addr = parse_client_id(current_client_id)
                    meta = sample_index.get(current_lab_id, {})
                    samples[current_client_id] = {
                        "client_id":    current_client_id,
                        "lab_id":       current_lab_id,
                        "address":      parsed_addr["address"],
                        "house_number": parsed_addr["house_number"],
                        "street_full":  parsed_addr["street_full"],
                        "suffix":       parsed_addr["suffix"],
                        "suffix_label": parsed_addr["suffix_label"],
                        "sample_date":  meta.get("sample_date"),
                        "medium":       meta.get("matrix", "groundwater"),
                        "compounds":    {},
                        "lat":          None,
                        "lng":          None,
                    }

        # Extract compound results from any page belonging to current sample
        if current_client_id and f"Client ID: {current_client_id}" in text:
            for line in text.split('\n'):
                cm = COMPOUND_LINE_RE.match(line)
                if not cm:
                    continue
                full_name = cm.group(1)
                val       = parse_result(cm.group(2))
                abbrev_m  = re.search(r'\(([A-Za-z0-9\-]+)\)', full_name)
                if abbrev_m:
                    abbrev = abbrev_m.group(1)
                    if abbrev in REGULATED_ABBREVS:
                        rec = samples.get(current_client_id)
                        if rec and abbrev not in rec["compounds"]:
                            rec["compounds"][abbrev] = val

    if not samples:
        print("  [Pace Parser] No samples found in PDF")
        return None

    # ── Pass 3: compute PFAS6 and status for each sample ─────────────────────
    sample_locations = []
    for cid, rec in sorted(samples.items()):
        compounds = rec["compounds"]
        regulated_vals = [v for k, v in compounds.items()
                          if k in REGULATED_ABBREVS and v is not None]
        pfas6  = round(sum(regulated_vals), 3) if regulated_vals else None
        status = status_from_pfas6(pfas6)

        location = {
            "well_id":      cid,          # using Client ID as the location key
            "address":      rec["address"],
            "house_number": rec["house_number"],
            "street":       rec["street_full"],
            "suffix":       rec["suffix"],
            "suffix_label": rec["suffix_label"],
            "lab_id":       rec["lab_id"],
            "medium":       rec["medium"],
            "sample_date":  rec["sample_date"],
            "pfas6":        pfas6,
            "compounds":    compounds,
            "lat":          rec["lat"],
            "lng":          rec["lng"],
            "status":       status,
            "map_color":    STATUS_COLOR[status],
            "id_source":    "client_id_address",
        }
        sample_locations.append(location)
        print(f"  [Pace] {cid}: PFAS6={pfas6} ng/L → {status}")

    # ── Assemble report ───────────────────────────────────────────────────────
    gw = [l for l in sample_locations if l["suffix"] != "EFF"]  # use INF/untreated
    
    report_date_m = None
    for text in pages_text[:5]:
        report_date_m = re.search(r'Report Date:\s*(\d{2}/\d{2}/\d{2})', text)
        if report_date_m:
            break

    result = {
        "rtn":              "4-0029612",
        "source":           "MassDEP Source Discovery",
        "report_format":    "pace_lab_cert",
        "lab":              "Pace Analytical / Alpha Analytical",
        "doc_url":          doc_meta.get("url", ""),
        "doc_type":         doc_meta.get("doc_type", "Laboratory Results"),
        "doc_title":        doc_meta.get("title", path.stem),
        "pdf_path":         str(path),
        "date_filed":       doc_meta.get("date_filed"),
        "date_parsed":      datetime.now().isoformat(),
        "report_date":      report_date_m.group(1) if report_date_m else None,
        "sample_date":      sample_locations[0]["sample_date"] if sample_locations else None,
        "project_name":     "NANTUCKET SITE DISCOVERY",
        "project_number":   "102203",
        "staging_location": "FAIRGROUNDS FIRE DEPARTMENT",  # stored for context, not geocoding
        "consulting_firm":  None,
        "lsp":              None,
        "project_address":  None,
        "sample_locations": sample_locations,
        "groundwater_locations_count": len([l for l in sample_locations if l["medium"] == "groundwater"]),
        "soil_locations_count": 0,
        "max_pfas6_gw":  max((l["pfas6"] for l in gw if l["pfas6"] is not None), default=None),
        "max_pfas6_soil": None,
        "worst_status":  _worst_status(sample_locations),
        "has_exceedance": any(l["status"] in ("HIGH-DETECT", "HAZARD") for l in sample_locations),
        "summary_text":  "\n".join(pages_text[:2])[:2000],
    }
    return result


def _worst_status(locations: list) -> str:
    order = ["NON-DETECT", "DETECT", "HIGH-DETECT", "HAZARD", "UNKNOWN"]
    statuses = [l.get("status", "UNKNOWN") for l in locations]
    for s in reversed(order):
        if s in statuses:
            return s
    return "UNKNOWN"


# ── CLI test ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys, json
    if len(sys.argv) < 2:
        print("Usage: python pace_lab_parser.py <path.pdf>")
        sys.exit(1)
    result = parse_pace_lab_pdf(
        sys.argv[1],
        {"url": "", "doc_type": "Laboratory Results", "title": Path(sys.argv[1]).stem, "date_filed": None}
    )
    if result:
        # Print summary
        print(f"\n{'='*60}")
        print(f"Lab: {result['lab']}")
        print(f"Report Date: {result['report_date']}")
        print(f"Sample Date: {result['sample_date']}")
        print(f"Samples: {len(result['sample_locations'])}")
        print(f"Has Exceedance: {result['has_exceedance']}")
        print(f"Worst Status: {result['worst_status']}")
        print(f"Max PFAS6 (GW): {result['max_pfas6_gw']} ng/L")
        print(f"\n{'─'*60}")
        for loc in result['sample_locations']:
            sfx = f" [{loc['suffix']}]" if loc['suffix'] else ""
            print(f"  {loc['well_id']}{sfx}")
            print(f"    Address: {loc['address']}")
            print(f"    PFAS6:   {loc['pfas6']} ng/L → {loc['status']}")
            if loc['pfas6'] and loc['pfas6'] > 0:
                for k, v in loc['compounds'].items():
                    if v and v > 0:
                        print(f"      {k}: {v} ng/L")
