"""
source_discovery_parser.py
--------------------------
Parses MassDEP Source Discovery investigation PDFs for RTN 4-0029612.

Two PDF formats are filed under this RTN:

FORMAT A — Lab Analytical Certificates (primary format in the 2025-26 filings)
  Standard EPA 537.1 / EPA 533 lab reports, structurally identical to the
  Barnstable County certificates in the voluntary well-testing programme.
  Key fields on each result page:
    Client ID       -> monitoring well ID (e.g. "VDT-4FG-4")  <- THE LOCATION KEY
    Lab Sample ID   -> unique lab tracking number per bottle
    Sample Location -> site-level label ("Fairgrounds Fire Department")
                       REPEATS across all samples — describes the project site,
                       NOT the individual well. Do NOT use as a location key.
    Sample Date     -> date collected
    Result / Units  -> individual compound values in ng/L

FORMAT B — Field Investigation / Narrative Reports (Verdantas, TRC, etc.)
  Longer documents with space-aligned summary tables and GPS coordinate tables.
  Example row:  "VDT-4FG-4  22.33  9.80  7.20 ..."

The parser auto-detects the format and applies the appropriate strategy.
"""

import re
import json
from datetime import datetime
from pathlib import Path
from typing import Optional, Union

try:
    import pdfplumber
except ImportError:
    raise ImportError("Run: pip install pdfplumber --break-system-packages")

# ── Compound catalogue ────────────────────────────────────────────────────────

REGULATED = ["PFOS", "PFOA", "PFNA", "PFHxS", "PFHpS", "PFDA"]

ALL_COMPOUNDS = [
    "PFOS", "PFOA", "PFNA", "PFHxS", "PFHpS", "PFDA",
    "PFHxA", "PFHpA", "PFBS", "PFBA", "PFPeA",
    "PFUnDA", "PFDoDA", "PFTrDA", "PFTeDA",
    "HFPO-DA", "NEtFOSAA", "NMeFOSAA",
]

# ── Status thresholds ─────────────────────────────────────────────────────────

def status_from_pfas6(pfas6: Optional[float], medium: str = "groundwater") -> str:
    if pfas6 is None:
        return "UNKNOWN"
    if medium.lower() == "soil":
        return "DETECT" if pfas6 > 0 else "NON-DETECT"
    if pfas6 == 0:
        return "NON-DETECT"
    if pfas6 <= 20.0:
        return "DETECT"
    if pfas6 <= 89.9:
        return "HIGH-DETECT"
    return "HAZARD"


STATUS_COLOR = {
    "NON-DETECT":  "green",
    "DETECT":      "yellow",
    "HIGH-DETECT": "red",
    "HAZARD":      "purple",
    "UNKNOWN":     "gray",
}

# ── Regex patterns ────────────────────────────────────────────────────────────

# Monitoring well / boring IDs (Verdantas convention + generic)
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
    r"(PFOS|PFOA|PFNA|PFHxS|PFHpS|PFDA|PFHxA|PFBS|PFBA|PFPeA|PFHpA|"
    r"PFUnDA|PFDoDA|PFTrDA|PFTeDA|HFPO-DA|NEtFOSAA|NMeFOSAA)"
    r"\s+([<>]?ND|[<>]?\s*\d+\.?\d*(?:E[+-]?\d+)?)\s*(ng/L|ug/kg|µg/kg)?",
    re.I,
)

GPS_PATTERN = re.compile(
    r"(VDT[-_]?[A-Z0-9\-]+|MW[-_]?\d+[A-Z]?|SB[-_]?\d+[A-Z]?)\s+"
    r"(41\.\d{4,6})\s+(-70\.\d{4,6})",
    re.I,
)

# ── Format detection ──────────────────────────────────────────────────────────

def _detect_format(text: str) -> str:
    """
    Returns 'lab_cert' or 'field_report'.

    Lab certificates (Format A) are identified by the presence of both
    'Client ID' and 'Lab Sample ID' fields, which are standard lab report headers.
    Field reports (Format B) typically contain narrative text and GPS tables.

    Mixed documents (e.g. SARSS field investigation with lab cert appendices)
    are classified as 'field_report' when narrative signals appear in the
    first ~5000 chars (i.e., the main body), even if lab cert pages follow.
    """
    # Check the first ~5000 chars (cover page + first few pages) for narrative
    # signals. Field reports open with TOC, executive summary, section headers.
    # We require strong structural indicators — not just legal language that
    # might appear in cover letters or email attachments.
    early_text = text[:5000]
    narrative_signals = 0
    # Strong signals: structural document elements
    if re.search(r"table of contents", early_text, re.I):
        narrative_signals += 2
    if re.search(r"executive summary", early_text, re.I):
        narrative_signals += 2
    if re.search(r"statement of purpose", early_text, re.I):
        narrative_signals += 2
    # Moderate signals: report-style phrases (can appear in letters too)
    if re.search(r"field investigation", early_text, re.I):
        narrative_signals += 1
    if re.search(r"this report presents", early_text, re.I):
        narrative_signals += 1
    if re.search(r"1\.0\s+INTRODUCTION", early_text):
        narrative_signals += 2
    has_early_narrative = narrative_signals >= 2

    has_client_id    = bool(re.search(r"Client\s+ID", text, re.I))
    has_lab_sample   = bool(re.search(r"Lab\s+Sample\s+ID", text, re.I))
    has_sample_loc   = bool(re.search(r"Sample\s+Location", text, re.I))
    has_gps_table    = bool(re.search(r"41\.\d{4,6}\s+-70\.\d{4,6}", text))

    # If the document opens with narrative content, it's a field report
    # even if it has lab cert appendices later in the document
    if has_early_narrative and (has_gps_table or len(text) > 20000):
        return "field_report"
    if (has_client_id or has_lab_sample) and has_sample_loc:
        return "lab_cert"
    if has_gps_table:
        return "field_report"
    # Default: try lab cert first since that's the primary format
    return "lab_cert"


# ── Main entry point ──────────────────────────────────────────────────────────

def parse_source_discovery_pdf(pdf_path: str, doc_meta: dict) -> Optional[dict]:
    """
    Parse a single Source Discovery PDF.
    Returns a structured dict ready for source_discovery_db, or None on failure.
    """
    path = Path(pdf_path)
    if not path.exists():
        print(f"  [Parser] File not found: {pdf_path}")
        return None

    try:
        with pdfplumber.open(str(path)) as pdf:
            pages_text = [page.extract_text() or "" for page in pdf.pages]
            all_text   = "\n".join(pages_text)
            tables = []
            for page in pdf.pages:
                t = page.extract_tables()
                if t:
                    tables.extend(t)
    except Exception as e:
        print(f"  [Parser] pdfplumber error: {e}")
        return None

    fmt = _detect_format(all_text)
    print(f"  [Parser] Detected format: {fmt}")

    result = {
        "rtn":              "4-0029612",
        "source":           "MassDEP Source Discovery",
        "doc_url":          doc_meta.get("url", ""),
        "doc_type":         doc_meta.get("doc_type", "Report"),
        "doc_title":        doc_meta.get("title", path.stem),
        "pdf_path":         str(path),
        "date_filed":       doc_meta.get("date_filed"),
        "date_parsed":      datetime.now().isoformat(),
        "report_format":    fmt,
        "report_date":      _extract_report_date(all_text),
        "consulting_firm":  _extract_firm(all_text),
        "lsp":              _extract_lsp(all_text),
        "project_address":  _extract_project_address(all_text),
        "sample_location_label": _extract_sample_location_label(all_text),
        "sample_locations": [],
        "summary_text":     all_text[:2000],
    }

    # ── Extract per-location data using the appropriate strategy ──────────────
    if fmt == "lab_cert":
        locations = _parse_lab_cert(all_text, tables, pages_text)
    else:
        locations = _parse_field_report(all_text, tables)

    # ── Attach status + color ─────────────────────────────────────────────────
    for loc in locations:
        pfas6  = loc.get("pfas6")
        medium = loc.get("medium", "groundwater")
        loc["status"]    = status_from_pfas6(pfas6, medium)
        loc["map_color"] = STATUS_COLOR[loc["status"]]

    result["sample_locations"] = locations

    # ── Summary ───────────────────────────────────────────────────────────────
    gw_locs   = [l for l in locations if l["medium"] in ("groundwater", "drinking_water")]
    soil_locs = [l for l in locations if l["medium"] == "soil"]

    result["groundwater_locations_count"] = len(gw_locs)
    result["soil_locations_count"]        = len(soil_locs)
    result["max_pfas6_gw"]   = max((l["pfas6"] for l in gw_locs   if l["pfas6"] is not None), default=None)
    result["max_pfas6_soil"] = max((l["pfas6"] for l in soil_locs if l["pfas6"] is not None), default=None)
    result["worst_status"]   = _worst_status(locations)
    result["has_exceedance"] = any(l["status"] in ("HIGH-DETECT", "HAZARD") for l in locations)

    return result


# ═══════════════════════════════════════════════════════════════════════════════
# FORMAT A — Lab Certificate Parser
# ═══════════════════════════════════════════════════════════════════════════════

def _parse_lab_cert(text: str, tables: list, pages_text: list) -> list[dict]:
    """
    Parse lab analytical certificates.

    These reports have one page (or section) per sample.  The key fields are:

      Client ID:        VDT-4FG-4          <- monitoring well ID (location key)
      Lab Sample ID:    2025123456          <- unique per bottle
      Sample Location:  Fairgrounds Fire Department  <- project-level, not well-level
      Sample Date:      01/16/2025
      Collected By:     Verdantas

    Then a results table with compound | result | units | MDL | MRL etc.

    We group results by Client ID.  If multiple bottles have the same Client ID
    (duplicates / QC splits), we keep the one with the higher PFAS6 (conservative).
    """
    locations_by_client_id: dict[str, dict] = {}

    # ── Try structured table extraction first ──────────────────────────────────
    for table in tables:
        _parse_lab_cert_table(table, locations_by_client_id)

    # ── Fall back to per-page text parsing ────────────────────────────────────
    # Lab certs repeat key:value pairs at the top of each page/section
    page_blocks = _split_into_sample_blocks(text, pages_text)
    for block in page_blocks:
        _parse_lab_cert_block(block, locations_by_client_id)

    locations = list(locations_by_client_id.values())
    return _dedup_locations(locations)


def _dedup_locations(locations: list[dict]) -> list[dict]:
    """
    Merge locations that share the same base address and sample date.

    Handles cases where pdfplumber produces both a full Client ID
    (e.g. "4 TOMS WAY-3") and a truncated variant (e.g. "4 TOMS WAY")
    that somehow survived filtering. We keep the entry with the longer
    (more complete) well_id and the higher PFAS6 value.
    """
    if len(locations) <= 1:
        return locations

    # Normalise key: strip trailing dash+digits (unit suffixes like "-3")
    # and collapse whitespace for grouping
    def _base_key(well_id: str) -> str:
        key = re.sub(r"[-–]\d+$", "", well_id).strip()
        return re.sub(r"\s+", " ", key).upper()

    from collections import defaultdict

    # Group by (base_well_id, medium_class) where drinking_water and
    # groundwater are treated as the same class ("water") for dedup
    def _medium_class(m: str) -> str:
        return "water" if m in ("groundwater", "drinking_water") else m

    groups: dict[tuple, list[dict]] = defaultdict(list)
    for loc in locations:
        base = _base_key(loc["well_id"])
        mclass = _medium_class(loc.get("medium", ""))
        groups[(base, mclass)].append(loc)

    merged = []
    for (_base, _medium), group in groups.items():
        # Separate dated and undated entries
        dated = [l for l in group if l.get("sample_date")]
        undated = [l for l in group if not l.get("sample_date")]

        # Sub-group dated entries by date (different dates = different samples)
        by_date: dict[str, list[dict]] = defaultdict(list)
        for l in dated:
            by_date[l["sample_date"]].append(l)

        # Merge undated entries into the dated group (they're continuation pages)
        # If no dated entries exist, keep undated as-is
        if dated and undated:
            # Merge undated into the first dated group
            first_date = next(iter(by_date))
            by_date[first_date].extend(undated)
        elif undated:
            by_date[""] = undated

        for _date, subgroup in by_date.items():
            if len(subgroup) == 1:
                merged.append(subgroup[0])
                continue
            # Pick the entry with the longest (most complete) well_id
            best = max(subgroup, key=lambda l: (
                len(l["well_id"]),
                l.get("pfas6") or 0,
            ))
            # Merge compounds from all entries
            all_compounds = {}
            for loc in subgroup:
                all_compounds.update(loc.get("compounds", {}))
            best["compounds"] = all_compounds
            # Keep the highest PFAS6
            pfas6_vals = [l.get("pfas6") for l in subgroup if l.get("pfas6") is not None]
            if pfas6_vals:
                best["pfas6"] = max(pfas6_vals)
            merged.append(best)

    return merged


def _split_into_sample_blocks(text: str, pages_text: list) -> list[str]:
    """
    Split the full document text into per-sample blocks.
    Lab certs repeat header fields at the start of each sample's results.
    We split on 'Client ID' occurrences.
    """
    # Split on "Client ID" which signals the start of a new sample result section
    parts = re.split(r"(?=Client\s+ID\s*[:\t])", text, flags=re.I)
    # Also try splitting on "Lab Sample ID" if Client ID isn't present
    if len(parts) <= 1:
        parts = re.split(r"(?=Lab\s+Sample\s+ID\s*[:\t])", text, flags=re.I)
    return [p for p in parts if p.strip()]


def _parse_lab_cert_block(block: str, locations: dict):
    """
    Parse one sample block from a lab certificate.
    Extracts Client ID, sample date, and all compound results.
    """
    # ── Extract Client ID (the monitoring well ID) ────────────────────────────
    client_id = None
    m = re.search(
        r"Client\s+ID\s*[:\t]+\s*([^\n\r]+)",
        block, re.I
    )
    if m:
        client_id = m.group(1).strip().rstrip(".,;")

    # If no Client ID found, try Lab Sample ID as a fallback key
    if not client_id:
        m2 = re.search(r"Lab\s+Sample\s+ID\s*[:\t]+\s*([^\n\r]+)", block, re.I)
        if m2:
            client_id = f"LAB-{m2.group(1).strip()}"

    if not client_id:
        return

    # Strip trailing metadata that pdfplumber merges onto the same line
    # e.g. "12 SCOTT'S WAY Date Received: 12/05/24" → "12 SCOTT'S WAY"
    client_id = re.split(r"\s+Date\s+Received:", client_id, flags=re.I)[0].strip()

    # Strip soil boring depth brackets like "[0.5-0.75']" or "[34-36']"
    # Also handles truncated brackets like "[0.5-" (split across lines)
    # e.g. "VDT-TIC-11 [0.5-0.75']" → "VDT-TIC-11"
    client_id = re.sub(r"\s*\[[\d.\-']*\]?\s*$", "", client_id).strip()

    # Normalise: strip any trailing words that aren't part of the well ID
    # e.g. "VDT-4FG-4  GW" -> "VDT-4FG-4"
    client_id = re.split(r"\s{2,}", client_id)[0].strip()

    # Skip QC/batch control entries — these are not real sample locations
    if re.match(r"^(MS|DUP|LCS|MB|MSD|LCSD)\b", client_id, re.I):
        return
    if re.search(r"\b(MS Sample|DUP Sample|Method Blank|Lab Blank|"
                 r"Equipment Blank|Field Blank|Duplicate\s*\d*)\b", client_id, re.I):
        return

    # Skip QC batch summary / chain-of-custody blocks that happen to contain
    # a Client ID reference. These have "Solids, Total" or "QC Batch" or
    # "Sample Receipt" near the top, not actual PFAS results.
    first_500 = block[:500]
    if re.search(r"Solids,\s*Total|QC\s+Batch|Sample\s+Receipt|Standard\s+Reference", first_500, re.I):
        return

    # Skip truncated Client IDs — bare numbers or incomplete street names
    # that result from pdfplumber splitting a cell across columns
    # e.g. "4" (truncated from "4 TOMS WAY-3") or "20 TOMS" (missing "WAY-3")
    if re.match(r"^\d+$", client_id):
        return  # bare number, not a valid well ID or address
    # Address-like IDs must contain a road suffix (WAY, ROAD, ST, etc.) or a
    # well-ID pattern (VDT-, MW-, letters+digits with dash)
    if re.match(r"^\d+\s+[A-Za-z]", client_id):
        # Looks like a street address — require a road suffix
        road_suffixes = (r"\b(WAY|ROAD|RD|STREET|ST|LANE|LN|DRIVE|DR|AVE|AVENUE|"
                         r"CIRCLE|CIR|COURT|CT|PLACE|PL|PATH|TRAIL|TRL|BLVD|"
                         r"BOULEVARD|TERRACE|TER|PIKE|HWY|HIGHWAY)\b")
        if not re.search(road_suffixes, client_id, re.I):
            return  # truncated address missing road suffix

    # ── Sample date ───────────────────────────────────────────────────────────
    sample_date = None
    dm = re.search(
        r"(?:Sample\s+Date|Date\s+Collected|Collection\s+Date)\s*[:\t]+\s*"
        r"(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})",
        block, re.I
    )
    if dm:
        sample_date = dm.group(1)

    # ── Medium ────────────────────────────────────────────────────────────────
    # Check the Matrix field first (most reliable), then fall back to text scan,
    # then inherit from existing entry for same Client ID (continuation pages)
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
    # Check units in the block — ng/g indicates soil, ng/l indicates water
    if medium is None:
        if re.search(r"\bng/g\b|\bug/kg\b|µg/kg", block, re.I):
            medium = "soil"
        elif re.search(r"\bng/l\b|\bug/l\b|µg/l", block, re.I):
            medium = "groundwater"
    # Inherit from existing entry for same Client ID (continuation pages)
    if medium is None:
        for (cid, med) in locations:
            if cid == client_id:
                medium = med
                break
    if medium is None:
        medium = "groundwater"

    # ── Extract compound results ──────────────────────────────────────────────
    compounds = {}

    # Strategy A: short-form matches like "PFOS  5.2  ng/L"
    for cm in INDIVIDUAL_COMPOUND_PATTERN.finditer(block):
        name = _normalise_compound_name(cm.group(1))
        val  = _parse_number(cm.group(2))
        if name not in compounds:
            compounds[name] = val

    # Strategy B: Pace lab cert format — full compound name with abbreviation
    # in parentheses, e.g.:
    #   "Perfluorooctanoic Acid (PFOA) ND ng/l 1.85 0.618 1"
    #   "Perfluorobutanesulfonic Acid (PFBS) 3.07 ng/l 1.85 0.618 1"
    #   "Perfluoropentanoic Acid (PFPeA) 1.48 J ng/l 1.85 0.618 1"
    for line in block.split("\n"):
        pm = re.search(
            r"\(("
            r"PFOS|PFOA|PFNA|PFHxS|PFHpS|PFDA|PFHxA|PFBS|PFBA|PFPeA|PFHpA|"
            r"PFUnDA|PFDoDA|PFTrDA|PFTeDA|HFPO-DA|NEtFOSAA|NMeFOSAA"
            r")\)\s+([<>]?ND|\d+\.?\d*)\s*([Jj])?\s*(ng/l|ug/kg|µg/kg)?",
            line, re.I
        )
        if pm:
            name = _normalise_compound_name(pm.group(1))
            val  = _parse_number(pm.group(2))
            if name not in compounds:
                compounds[name] = val

    # Strategy C: line-start short-form "PFOS  5.2  ng/L  ..."
    for line in block.split("\n"):
        lm = re.match(
            r"\s*(PFOS|PFOA|PFNA|PFHxS|PFHpS|PFDA|PFHxA|PFBS|PFBA|PFPeA|"
            r"PFHpA|PFUnDA|PFDoDA|PFTrDA|PFTeDA|HFPO-DA|NEtFOSAA|NMeFOSAA)"
            r"\s+([<>]?ND|\d+\.?\d*(?:E[+-]?\d+)?)\s*(ng/L|ug/kg|µg/kg)?",
            line, re.I
        )
        if lm:
            name = _normalise_compound_name(lm.group(1))
            val  = _parse_number(lm.group(2))
            if name not in compounds:
                compounds[name] = val

    # ── Compute PFAS6 ─────────────────────────────────────────────────────────
    pfas6 = None
    pfas6_m = PFAS6_LINE_PATTERN.search(block)
    if pfas6_m:
        pfas6 = _parse_number(pfas6_m.group(1))
    else:
        reg_vals = [v for k, v in compounds.items() if k in REGULATED and v is not None]
        if reg_vals:
            pfas6 = round(sum(reg_vals), 3)

    if not compounds and pfas6 is None:
        return  # Nothing useful in this block

    # ── Merge into locations dict ────────────────────────────────────────────
    # Key by (client_id, medium) so the same well with soil + groundwater
    # samples produces separate location entries
    loc_key = (client_id, medium)
    if loc_key in locations:
        existing = locations[loc_key]
        # Keep whichever has the higher PFAS6 (conservative)
        if pfas6 is not None and (existing.get("pfas6") or 0) < pfas6:
            existing["pfas6"]      = pfas6
            existing["compounds"]  = {**existing["compounds"], **compounds}
        if sample_date and not existing.get("sample_date"):
            existing["sample_date"] = sample_date
    else:
        locations[loc_key] = {
            "well_id":     client_id,    # Client ID = monitoring well ID
            "medium":      medium,
            "depth_ft":    None,
            "sample_date": sample_date,
            "pfas6":       pfas6,
            "compounds":   compounds,
            "lat":         None,
            "lng":         None,
            "address":     None,
            "id_source":   "client_id",  # track how we got the location key
        }


def _parse_lab_cert_table(table: list, locations: dict):
    """
    If pdfplumber found a structured table in the lab cert, try to parse it.
    Lab cert tables often have rows: Analyte | Result | Units | MDL | MRL | Qual
    """
    if not table or len(table) < 2:
        return

    # Find header row
    header = None
    for row in table[:3]:
        if row and any(re.search(r"analyte|compound|parameter", str(c) or "", re.I) for c in row):
            header = [str(c).strip().lower() if c else "" for c in row]
            break

    if not header:
        return

    result_col = _find_col(header, ["result", "value", "conc", "concentration"])
    analyte_col = _find_col(header, ["analyte", "compound", "parameter", "name"])

    if result_col is None or analyte_col is None:
        return

    # These tables don't have a Client ID column — they're per-sample
    # We can't reliably assign them without the block context, so skip
    # (the block parser handles this better)
    return


# ═══════════════════════════════════════════════════════════════════════════════
# FORMAT B — Field Report Parser
# ═══════════════════════════════════════════════════════════════════════════════

def _parse_field_report(text: str, tables: list) -> list[dict]:
    """
    Parse field investigation / narrative reports.
    These contain summary tables and GPS coordinates.
    """
    locations = []

    # Try structured tables first
    for table in tables:
        locs = _parse_pfas_table(table)
        if locs:
            locations.extend(locs)

    # Fall back to free-text tabular parsing
    if not locations:
        locations = _parse_free_text_locations(text)

    # Attach GPS coordinates found in the document
    _attach_coordinates(locations, text)

    return locations


def _parse_pfas_table(table: list) -> list[dict]:
    """Parse a pdfplumber-extracted summary table from a field report.

    Handles two header layouts:
    1. Standard: header row has well/sample + compound columns in one row
    2. Verdantas/SARSS: compound names in row 0 (columns 3+), well ID header
       ("Sample") in a later row (row 5+), data rows below that
    """
    if not table or len(table) < 2:
        return []

    # ── Find the compound header row (has PFAS compound names) ───────────
    compound_header = None
    compound_header_idx = 0
    for i, row in enumerate(table[:8]):
        if row and any(
            re.search(r"PFAS|PFOS|PFOA", str(c) or "", re.I)
            for c in row
        ):
            compound_header = [str(c).strip().lower() if c else "" for c in row]
            compound_header_idx = i
            break

    if not compound_header:
        return []

    # ── Build column index from compound header ─────────────────────────
    col = {
        "well_id": _find_col(compound_header, ["well", "location", "station", "sample id", "boring", "client", "sample"]),
        "pfas6":   _find_col(compound_header, ["pfas6", "pfas 6", "pfas-6", "sum", "total pfas6"]),
        "pfos":    _find_col(compound_header, ["pfos"]),
        "pfoa":    _find_col(compound_header, ["pfoa"]),
        "pfna":    _find_col(compound_header, ["pfna"]),
        "pfhxs":   _find_col(compound_header, ["pfhxs"]),
        "pfhps":   _find_col(compound_header, ["pfhps"]),
        "pfda":    _find_col(compound_header, ["pfda"]),
        "medium":  _find_col(compound_header, ["medium", "matrix", "type"]),
        "depth":   _find_col(compound_header, ["depth", "ft", "feet"]),
        "date":    _find_col(compound_header, ["date", "collected", "sampled"]),
    }

    # If well_id column not found in compound header, check later rows
    # for a secondary header (Verdantas format: "Sample", "Date", "Lab ID")
    if col["well_id"] is None:
        for i, row in enumerate(table[compound_header_idx + 1:compound_header_idx + 8]):
            if row and any(re.search(r"^sample$|^well", str(c) or "", re.I) for c in row):
                sub_header = [str(c).strip().lower() if c else "" for c in row]
                col["well_id"] = _find_col(sub_header, ["sample", "well", "boring"])
                if col["date"] is None:
                    col["date"] = _find_col(sub_header, ["date", "collected"])
                break
        # Final fallback: assume column 0 is well_id if data rows have VDT- patterns
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
        # Skip header-like rows, address rows, standard labels, and QC entries
        if re.search(r"^(pfas|well|location|sample$|massachusetts|duplicate)", well_id, re.I):
            continue
        if re.search(r"^\d+\s+(Fairgrounds|Old South|Ticcoma|Waitt)", well_id, re.I):
            continue  # address row, not a sample
        if re.search(r"(DUPLICATE|BLANK|GW DUPLICATE|SOIL DUPLICATE)", well_id, re.I):
            continue

        compounds = {
            "PFOS":  _parse_number(cell("pfos")),
            "PFOA":  _parse_number(cell("pfoa")),
            "PFNA":  _parse_number(cell("pfna")),
            "PFHxS": _parse_number(cell("pfhxs")),
            "PFHpS": _parse_number(cell("pfhps")),
            "PFDA":  _parse_number(cell("pfda")),
        }
        pfas6 = _parse_number(cell("pfas6"))
        if pfas6 is None:
            vals = [v for v in compounds.values() if v is not None]
            pfas6 = round(sum(vals), 3) if vals else None

        # Infer medium from context: check for ng/l vs ng/g or ug/kg in
        # the units row (typically row after compound header)
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
            "well_id":     well_id,
            "medium":      _normalise_medium(medium_val),
            "depth_ft":    _parse_number(cell("depth")),
            "sample_date": cell("date"),
            "pfas6":       pfas6,
            "compounds":   compounds,
            "lat":         None,
            "lng":         None,
            "address":     None,
            "id_source":   "field_table",
        })

    return locations


def _parse_free_text_locations(text: str) -> list[dict]:
    """
    Parse space-aligned data rows in field reports.
    Detects a header row to determine column order.
    """
    locations = []
    lines = text.split("\n")

    # Find header row
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
        window  = "\n".join(lines[max(0, i-3):min(len(lines), i+8)])

        pfas6 = None
        pm = PFAS6_LINE_PATTERN.search(window)
        if pm:
            pfas6 = _parse_number(pm.group(1))

        tabular_compounds = {}
        if pfas6 is None:
            rest   = line[m.end():].strip()
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
            val  = _parse_number(cm.group(2))
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
            "well_id":     well_id,
            "medium":      medium,
            "depth_ft":    None,
            "sample_date": None,
            "pfas6":       pfas6,
            "compounds":   compounds,
            "lat":         None,
            "lng":         None,
            "address":     None,
            "id_source":   "free_text",
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


# ── Metadata extractors ───────────────────────────────────────────────────────

def _extract_report_date(text: str) -> Optional[str]:
    patterns = [
        r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+20\d{2}",
        r"\d{4}-\d{2}-\d{2}",
        r"\d{1,2}/\d{1,2}/20\d{2}",
    ]
    for pat in patterns:
        m = re.search(pat, text)
        if m:
            return m.group()
    return None


def _extract_firm(text: str) -> Optional[str]:
    firms = [
        "Verdantas", "TRC", "Arcadis", "Stantec", "GZA", "Haley & Aldrich",
        "Tighe & Bond", "Weston & Sampson", "Kleinfelder", "Jacobs",
        "Environmental Partners", "Geosyntec", "Barnstable County",
        "Alpha Analytical", "TestAmerica", "Pace Analytical",
    ]
    for firm in firms:
        if re.search(re.escape(firm), text, re.I):
            return firm
    return None


def _extract_lsp(text: str) -> Optional[str]:
    m = re.search(r"Licensed Site Professional[:\s]+([A-Z][a-z]+ [A-Z][a-z]+)", text)
    if m:
        return m.group(1)
    m = re.search(r"LSP[:\s#]+([A-Z][a-z]+ [A-Z][a-z]+)", text)
    if m:
        return m.group(1)
    return None


def _extract_project_address(text: str) -> Optional[str]:
    """Extract the primary site address."""
    nantucket_pattern = re.compile(
        r"\d+\s+[A-Z][a-zA-Z\s]+(Road|Street|Avenue|Lane|Way|Drive|Court|Place|Boulevard)\b"
        r"[,\s]+Nantucket",
        re.I,
    )
    m = nantucket_pattern.search(text[:1500])
    if m:
        return m.group().strip()
    m2 = re.search(
        r"\d+\s+[A-Z][a-zA-Z\s]+(Road|Street|Avenue|Lane|Way|Drive|Court|Place|Boulevard)",
        text[:1000], re.I
    )
    if m2:
        addr = m2.group().strip()
        return addr + (", Nantucket, MA" if "Nantucket" in text[:1500] else "")
    return None


def _extract_sample_location_label(text: str) -> Optional[str]:
    """
    Extract the 'Sample Location' field from lab certs.
    This is the project-level site label (e.g. 'Fairgrounds Fire Department'),
    NOT the individual well ID.  We store it for context but don't use it as
    the location key — Client ID fills that role.
    """
    m = re.search(r"Sample\s+Location\s*[:\t]+\s*([^\n\r]+)", text, re.I)
    if m:
        return m.group(1).strip().rstrip(".,;")
    return None


# ── Utility helpers ───────────────────────────────────────────────────────────

def _normalise_compound_name(name: str) -> str:
    """Normalise compound name to canonical capitalisation."""
    canonical = {
        "PFOS": "PFOS", "PFOA": "PFOA", "PFNA": "PFNA",
        "PFHXS": "PFHxS", "PFHPS": "PFHpS", "PFDA": "PFDA",
        "PFHXA": "PFHxA", "PFHPA": "PFHpA", "PFBS": "PFBS",
        "PFBA": "PFBA", "PFPEA": "PFPeA", "PFUNDA": "PFUnDA",
        "PFDODA": "PFDoDA", "PFTRDA": "PFTrDA", "PFTEDA": "PFTeDA",
        "HFPO-DA": "HFPO-DA", "NETFOSAA": "NEtFOSAA", "NMEFOSAA": "NMeFOSAA",
    }
    return canonical.get(name.upper(), name)


def _find_col(header: list, keywords: list) -> Optional[int]:
    for i, h in enumerate(header):
        if any(kw in h for kw in keywords):
            return i
    return None


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


def _normalise_medium(s: str) -> str:
    s = s.lower()
    if "soil" in s or "solid" in s:
        return "soil"
    return "groundwater"


def _worst_status(locations: list) -> str:
    order = ["NON-DETECT", "DETECT", "HIGH-DETECT", "HAZARD", "UNKNOWN"]
    statuses = [l.get("status", "UNKNOWN") for l in locations]
    for s in reversed(order):
        if s in statuses:
            return s
    return "UNKNOWN"


# ── Standalone test ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python source_discovery_parser.py <path_to_pdf>")
        sys.exit(1)
    result = parse_source_discovery_pdf(
        sys.argv[1],
        {"url": "", "doc_type": "Test", "title": Path(sys.argv[1]).stem, "date_filed": None}
    )
    print(json.dumps(result, indent=2, default=str))
