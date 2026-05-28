"""Laserfiche PFAS report scraper — full-island traversal.

Integrates prototype code from prototype/pfas_monitor/pfas_monitor_v2.py
and prototype/pfas_monitor/test_report_parser.py.

Traversal hierarchy per PRD Section 4.1:
  Property Files/
    Maps 01-09/ → Map 02/, Map 03/, ...
    Maps 10-19/ → Map 10/, Map 11/, ...
    ...
    Maps 90-99/ → Map 90/, ... Map 98/
      [Parcel folder]/ → Well/ → Reports/ → 2025/ → PFAS_Sampling_YYYYMMDD
"""

import logging
import re
from datetime import datetime
from decimal import Decimal
from typing import Optional

from playwright.sync_api import sync_playwright, Page

from app.config import settings, classify_result_status, FALLBACK_NEIGHBORHOOD, MCL
from app.database import SessionLocal
from app.models.results import PfasResult
from app.models.scraper import SeenDocument, ScrapeRun
from app.geo import resolve_location

logger = logging.getLogger(__name__)

# --- All 18 PFAS compounds (from prototype) ---
# (short_name, long_name, in_pfas6)
PFAS_COMPOUNDS = [
    ("PFOS", "PERFLUOROOCTANESULFONIC ACID", True),
    ("PFOA", "PERFLUOROOCTANOIC ACID", True),
    ("PFHxS", "PERFLUOROHEXANESULFONIC ACID", True),
    ("PFNA", "PERFLUORONONANOIC ACID", True),
    ("PFHpA", "PERFLUOROHEPTANOIC ACID", True),
    ("PFDA", "PERFLUORODECANOIC ACID", True),
    ("PFBS", "PERFLUOROBUTANESULFONIC ACID", False),
    ("PFDoA", "PERFLUORODODECANOIC ACID", False),
    ("PFHxA", "PERFLUOROHEXANOIC ACID", False),
    ("PFTA", "PERFLUOROTETRADECANOIC ACID", False),
    ("PFTrDA", "PERFLUOROTRIDECANOIC ACID", False),
    ("PFUnA", "PERFLUOROUNDECANOIC ACID", False),
    ("NEtFOSAA", "N-ETHYL PERFLUOROOCTANESULFONAMIDOACETIC ACID", False),
    ("NMeFOSAA", "N-METHYL PERFLUOROOCTANESULFONAMIDOACETIC ACID", False),
    ("11Cl-PF3OUdS", "11-CHLOROEICOSAFLUORO-3-OXAUNDECANE-1-SULFONIC ACID", False),
    ("9Cl-PF3ONS", "9-CHLOROHEXADECAFLUORO-3-OXANONE-1-SULFONIC ACID", False),
    ("ADONA", "4,8-DIOXA-3H-PERFLUORONONANOIC ACID", False),
    ("HFPO-DA", "HEXAFLUOROPROPYLENE OXIDE DIMER ACID", False),
]

# Laserfiche viewer UI chrome that contaminates plain-text extraction
UI_GARBAGE = [
    "Fit window", "Fit width", "Fit height",
    "400%", "200%", "100%", "75%", "50%", "25%",
    "View images", "Text mode",
]


# =============================================================================
# URL helpers
# =============================================================================

def _browse_url(folder_id: str) -> str:
    return (
        f"{settings.laserfiche_base_url}/Portal/Browse.aspx"
        f"?id={folder_id}&repo={settings.laserfiche_repo_id}"
    )


def _doc_url(doc_id: str) -> str:
    return (
        f"{settings.laserfiche_base_url}/Portal/DocView.aspx"
        f"?id={doc_id}&repo={settings.laserfiche_repo_id}"
    )


def _parse_parcel_folder_name(parent_map_number: str, folder_name: str) -> tuple[str, str]:
    """Parse a Laserfiche parcel folder name into (map_number, parcel_number).

    Handles Nantucket assessor folder naming where a parcel folder inside
    a parent "Map XX" Laserfiche folder may belong to either:
      - the parent integer map         ("21 80"    → map="21",   parcel="80")
      - a decimal-extended sub-map     ("59.4 140" → map="59.4", parcel="140")

    Multi-parcel folder names ("21 37 & 122", "20, 21") are preserved as-is
    in parcel_number; the display layer in app/routers/api.py splits and
    resolves them at read time.

    Returns (parent_map_number, folder_name) unchanged if the folder name
    doesn't start with parent_map_number or a sub-map of it.
    """
    parts = folder_name.split(maxsplit=1)
    if len(parts) != 2:
        return (parent_map_number, folder_name)

    candidate_map, rest = parts
    if candidate_map == parent_map_number:
        return (parent_map_number, rest)
    if candidate_map.startswith(parent_map_number + "."):
        return (candidate_map, rest)

    return (parent_map_number, folder_name)


# =============================================================================
# Playwright navigation helpers (from prototype)
# =============================================================================

def _navigate_and_wait(page: Page, folder_id: str) -> bool:
    try:
        page.goto(_browse_url(folder_id), timeout=30000)
        page.wait_for_load_state("networkidle", timeout=15000)
        page.wait_for_timeout(2000)
        return True
    except Exception as e:
        logger.warning("Error navigating to folder %s: %s", folder_id, e)
        return False


def _extract_all_links_with_scroll(page: Page, max_scrolls: int = 50) -> list[dict]:
    """Scroll through virtualized list and extract all browse/doc links."""
    all_items = {}

    scroll_script = """
    () => {
        const candidates = document.querySelectorAll(
            'div, section, main, [class*="list"], [class*="content"], '
            + '[class*="scroll"], [class*="grid"]'
        );
        for (const el of candidates) {
            if (el.scrollHeight > el.clientHeight + 50) {
                el.scrollTop += 300;
                return true;
            }
        }
        return false;
    }
    """

    for _ in range(max_scrolls):
        for link in page.query_selector_all('a[href*="Browse"]'):
            try:
                href = link.get_attribute("href") or ""
                name = link.inner_text().strip().split("\n")[0]
                match = re.search(r"id=(\d+)", href)
                if match and name:
                    fid = match.group(1)
                    if fid not in all_items:
                        all_items[fid] = {"type": "folder", "id": fid, "name": name}
            except Exception:
                pass

        for link in page.query_selector_all('a[href*="DocView"]'):
            try:
                href = link.get_attribute("href") or ""
                name = link.inner_text().strip().split("\n")[0]
                match = re.search(r"id=(\d+)", href)
                if match and name:
                    did = match.group(1)
                    if did not in all_items:
                        all_items[did] = {"type": "document", "id": did, "name": name}
            except Exception:
                pass

        page.evaluate(scroll_script)
        page.wait_for_timeout(300)

    return list(all_items.values())


# =============================================================================
# Report parsing (from prototype, with address fix)
# =============================================================================

def _mcl_regex_pattern() -> str:
    """Build a regex fragment matching the MCL value as it appears in MassDEP
    DW Program forms. Handles integer MCL displayed without decimal ("20")
    and a possible future decimal form ("20.0", "18.5", etc.) so this won't
    silently break if the MA PFAS6 MCL is amended.
    """
    if MCL == int(MCL):
        # Integer MCL — accept either "20" or "20.0" form depending on lab
        return rf"{int(MCL)}(?:\.0+)?"
    # Decimal MCL — escape the dot for regex
    return re.escape(f"{MCL:g}")


def _extract_compound_value(
    content: str,
    short_name: str,
    long_name: str,
    is_dw_program: bool = False,
) -> Optional[float]:
    """Extract a compound value from report content. Returns None if not found, 0 if ND.

    When is_dw_program=True, uses a single targeted pattern that handles the
    MassDEP DW Program form's "MDL MRL VALUE" row layout where the MRL ("2.00")
    and VALUE are concatenated without a separator. The standard 1a/1b/1c/2
    patterns mis-fire on this layout (Pattern 2 captures only the trailing
    digit of the concatenated number due to regex backtracking) so we skip
    them entirely on DW Program forms.
    """
    if is_dw_program:
        # DW Program row format:
        #   "<CAS> (<SHORT>) <LONG NAME> <MDL> <MRL><VALUE>"
        # MRL is the MassDEP-mandated 2.00 ng/L Minimum Reporting Limit for
        # all PFAS6 compounds and is concatenated to VALUE in the rendered
        # plain-text. We anchor on (SHORT), then on the literal MRL, and
        # capture the trailing VALUE.
        m = re.search(
            rf"\({re.escape(short_name)}\)[^\n]+?\s+\d+\.\d+\s+2\.00(ND|\d+(?:\.\d+)?)",
            content,
            re.IGNORECASE,
        )
        if m:
            return 0.0 if m.group(1) == "ND" else float(m.group(1))
        return None

    # Pattern 1a: value ng/L ... LONG_NAME-SHORT_NAME
    m = re.search(
        rf"([\d.]+|ND)\s+ng/L\s+[\d.]+\s+[\d.]+\s+\d+{re.escape(long_name[:10])}[^\n]*-?{re.escape(short_name)}",
        content, re.IGNORECASE,
    )
    if m:
        return 0.0 if m.group(1) == "ND" else float(m.group(1))

    # Pattern 1b: value J? ng/L ... ACID-SHORT_NAME
    m = re.search(
        rf"([\d.]+|ND)\s*J?\s*ng/L[^\n]*ACID-{re.escape(short_name)}",
        content, re.IGNORECASE,
    )
    if m:
        return 0.0 if m.group(1) == "ND" else float(m.group(1))

    # Pattern 1c: value ng/L ... SHORT_NAME
    m = re.search(
        rf"([\d.]+|ND)\s+ng/L[^\n]*{re.escape(short_name)}\b",
        content, re.IGNORECASE,
    )
    if m:
        return 0.0 if m.group(1) == "ND" else float(m.group(1))

    # Pattern 2: (SHORT_NAME) ... value
    m = re.search(
        rf"\({re.escape(short_name)}\)[^\d]*[\d.]+\s+[\d.]+(ND|[\d.]+)",
        content, re.IGNORECASE,
    )
    if m:
        return 0.0 if m.group(1) == "ND" else float(m.group(1))

    return None


def _strip_ui_chrome(text: str) -> str:
    """Remove Laserfiche viewer UI garbage from extracted text."""
    for garbage in UI_GARBAGE:
        text = text.replace(garbage, "")
    # Also strip the zoom/mode block that appears before actual content.
    # Pattern: "2\n" followed by the viewer options then the report text.
    text = re.sub(
        r"^2\n(?:.*?\n)*?(?=Massachusetts|Collection|Barnstable|Pace|PFAS|Sample)",
        "",
        text,
        count=1,
        flags=re.MULTILINE | re.DOTALL,
    )
    return text


def _extract_street_name(address: str) -> Optional[str]:
    """Extract street name from full address, stripping house number."""
    if not address:
        return None
    # Take portion before first comma
    street_part = address.split(",")[0].strip()
    # Strip leading house number + optional letter suffix (e.g. "30R")
    street_part = re.sub(r"^\d+[A-Za-z]?\s+", "", street_part)
    return street_part if street_part else None


def _parse_report(page: Page, doc_id: str) -> Optional[dict]:
    """Open a Laserfiche document in plain-text mode and parse PFAS data.

    Returns None if the plain-text view could not be opened. Otherwise
    returns the dict produced by _parse_report_text.

    This function does only the Playwright-driven work of fetching the
    plain-text content from Laserfiche. All parsing logic lives in
    _parse_report_text, which is a pure function over a string and
    therefore testable without a browser.
    """
    page.goto(_doc_url(doc_id))
    page.wait_for_load_state("networkidle")
    page.wait_for_timeout(2000)

    # Click "Plain Text" link
    clicked = False
    for selector in ['text="Plain Text"', 'text="plain text"', 'text="Plain text"', 'a:has-text("Plain Text")']:
        try:
            el = page.query_selector(selector)
            if el and el.is_visible():
                el.click()
                clicked = True
                break
        except Exception:
            continue

    if not clicked:
        for link in page.query_selector_all("a, button"):
            try:
                if "plain" in link.inner_text().lower():
                    link.click()
                    clicked = True
                    break
            except Exception:
                continue

    if not clicked:
        return None

    page.wait_for_timeout(1500)

    # Detect total pages
    body_text = page.inner_text("body")
    total_pages = 1
    page_match = re.search(r"Page\s+\d+\s+of\s+(\d+)", body_text)
    if page_match:
        total_pages = int(page_match.group(1))

    # Collect text from all pages
    all_content = body_text
    for _ in range(2, total_pages + 1):
        try:
            next_btn = page.query_selector('[aria-label*="next" i], [title*="next" i]')
            if next_btn and next_btn.is_visible():
                next_btn.click()
                page.wait_for_timeout(1000)
                all_content += "\n" + page.inner_text("body")
            else:
                break
        except Exception:
            break

    content = _strip_ui_chrome(all_content)
    return _parse_report_text(content)


def _parse_report_text(content: str) -> dict:
    """Parse already-extracted plain-text content from a Laserfiche PFAS report.

    Pure function — no I/O, no browser. Takes the post-UI-chrome-stripped
    plain-text content and returns the parsed result dict that
    _process_document expects.

    Splitting this out from _parse_report enables dry-run testing of the
    parser against saved text dumps (see scripts/parse_laserfiche_text.py)
    without spinning up Playwright or hitting Laserfiche.
    """
    results = {
        "pfas6": None,
        "pass_fail": None,
        "sample_date": None,
        "sample_address": None,
        "compounds": {},
        "j_qualifier_present": False,
    }

    # Detect form type up-front. The MassDEP DW Program form has a different
    # row layout (CAS-anchored, MRL concatenated to value) than the standard
    # Barnstable lab format, so PFAS6 and compound extraction both need
    # form-aware paths.
    is_dw_program = bool(
        re.search(r"Drinking Water Program|PWS INFORMATION", content, re.IGNORECASE)
    )

    # Extract PFAS6 value
    if is_dw_program:
        # DW Program form: PFAS6 line is "...) =<MCL><VALUE>" — MCL and
        # VALUE are concatenated without a separator (e.g. "=2043.0" means
        # MCL 20 + value 43.0). Anchor on the literal MCL to skip past it
        # and capture VALUE cleanly. The previous fallback regex
        # `PFAS6[^=]+=\d+(ND|[\d.]+)` greedily consumed both numbers and
        # captured only ".0" — see Bug #4 in BUILD_NOTES.
        mcl_pattern = _mcl_regex_pattern()
        pfas6_match = re.search(
            rf"PFAS6[^=]+={mcl_pattern}(ND|\d+(?:\.\d+)?)",
            content,
            re.IGNORECASE | re.DOTALL,
        )
        if pfas6_match:
            val = pfas6_match.group(1)
            results["pfas6"] = 0.0 if val == "ND" else float(val)
    else:
        # Standard Barnstable lab format: "<VALUE> ng/L ... PFAS6"
        pfas6_match = re.search(
            r"([\d.]+|ND)\s+ng/L[^\n]*PFAS6", content, re.IGNORECASE
        )
        if pfas6_match:
            val = pfas6_match.group(1)
            results["pfas6"] = 0.0 if val == "ND" else float(val)
        else:
            # Older fallback for "PFAS6 ... = <MCL><VALUE>" forms that don't
            # match is_dw_program detection but use the same "=" syntax.
            pfas6_match = re.search(
                r"PFAS6[^=]+=\d+(ND|[\d.]+)", content, re.IGNORECASE
            )
            if pfas6_match:
                val = pfas6_match.group(1)
                results["pfas6"] = 0.0 if val == "ND" else float(val)

    # Extract all 18 compounds
    for short_name, long_name, _in_pfas6 in PFAS_COMPOUNDS:
        value = _extract_compound_value(
            content, short_name, long_name, is_dw_program=is_dw_program
        )
        results["compounds"][short_name] = value

    # Check for J-qualified values
    if re.search(r"\d+\.?\d*\s+J\s+ng/L", content, re.IGNORECASE):
        results["j_qualifier_present"] = True

    # Recompute PFAS6 from regulated compounds when:
    #   (a) the directly-extracted value is None (no PFAS6 line found), OR
    #   (b) the value is 0 but individual compounds show detections — this
    #       happens with labs that follow the regulatory exclusion convention
    #       and report PFAS6 as ND when all detections are J-qualified below
    #       the reporting limit. We still want subscribers to see those
    #       trace detections, so we override with the calculated sum and
    #       rely on `j_qualifier_present` being surfaced downstream
    #       (popup/alert/notification) so the J-qualified nature is clear.
    pfas6_compounds = [
        results["compounds"].get(name)
        for name, _, in_pfas6 in PFAS_COMPOUNDS
        if in_pfas6
    ]
    nonzero_compounds = [v for v in pfas6_compounds if v is not None and v > 0]
    needs_recompute = (
        results["pfas6"] is None
        or (results["pfas6"] == 0 and nonzero_compounds)
    )
    if needs_recompute and any(v is not None for v in pfas6_compounds):
        results["pfas6"] = sum(v or 0.0 for v in pfas6_compounds)

    # Pass/fail
    content_lower = content.lower()
    if "does not meet" in content_lower:
        results["pass_fail"] = "FAIL"
    elif "suitable for drinking" in content_lower:
        results["pass_fail"] = "PASS"
    elif results["pfas6"] is not None:
        results["pass_fail"] = "FAIL" if results["pfas6"] > MCL else "PASS"
    else:
        results["pass_fail"] = "UNKNOWN"

    # Address — cleaned of UI chrome (is_dw_program already computed above)
    if is_dw_program:
        # Primary: column-header-anchored line extraction.
        # The DW Program form has a unique header row "MassDEP Location Name
        # Sample Information Date Collected Collected By" with the address
        # rendered on the next line in a stable shape:
        #     "<ADDRESS>[, Nantucket[, m/p: <M> <P>]][ <COLLECTOR>]<DATE>(F)inished..."
        # where <COLLECTOR> is 2-3 uppercase initials (BG/MT/SP/DP/KM) or the
        # literal word "Customer", sometimes jammed against the date with no
        # space. Peel each layer off.
        #
        # This path catches variants the regex below misses: addresses with
        # apostrophes (Lover's Lane), addresses MassDEP truncated to no street
        # suffix (Hummock Pond), and jammed collector+date forms (Rd.BG…,
        # Road DP07/…) where the existing anchor's whitespace requirement fails.
        header_match = re.search(
            r"MassDEP\s+Location\s+Name\s+Sample\s+Information\s+Date\s+Collected"
            r"\s+Collected\s+By\s*\n([^\n]+)",
            content,
        )
        if header_match:
            line = header_match.group(1).strip()
            # Defensive: drop everything from "(F)inished" onward (next column).
            line = re.sub(r"\(F\)inished.*$", "", line).strip()
            # Strip date + optional collector token directly preceding it.
            # Case-sensitive on the initials class so we don't accidentally
            # consume a street suffix like "Rd"/"Ln".
            line = re.sub(
                r"\s*(?:[A-Z]{2,3}|\s+Customer)?\s*\d{1,2}/\d{1,2}/\d{4}.*$",
                "",
                line,
            ).strip()
            # Strip ", m/p: <map> <parcel>" admin metadata if present.
            line = re.sub(r",\s*m/p:?.*$", "", line, flags=re.I).strip()
            # Strip trailing ", Nantucket".
            line = re.sub(r",?\s*Nantucket\s*$", "", line, flags=re.I).strip()
            # Strip trailing punctuation noise.
            line = line.rstrip(",.").strip()
            if line:
                results["sample_address"] = line

        # Fallback: original DW Program regex. Retained for defensiveness if
        # the column-header anchor is absent or text-damaged. Strict in shape;
        # requires a recognized street suffix and explicit Nantucket-or-
        # state-code-and-date anchor.
        if not results["sample_address"]:
            addr_matches = re.findall(
                r"(\d+[A-Za-z]?\s+[A-Za-z][A-Za-z\s]{2,30}"
                r"(?:Rd|Road|St|Street|Ave|Avenue|Ln|Lane|Dr|Drive|"
                r"Way|Blvd|Ct|Court|Pl|Place|Cir|Circle|Ter|Terrace|"
                r"Path|Trail|Trl|Highway|Hwy|Pike)\.?)"
                r"(?:,?\s*Nantucket|\s+[A-Z]{2}\s+\d{2}/\d{2}/\d{4})",
                content,
                re.IGNORECASE,
            )
            if addr_matches:
                results["sample_address"] = addr_matches[-1].strip().rstrip(".")

    # Standard Barnstable County / Pace lab format — also used as fallback
    # when the DW Program path found nothing.
    if not results["sample_address"]:
        addr_match = re.search(
            r"Collection Address[:\s]+([^,]+,\s*Nantucket)[^\n]*", content
        )
        if addr_match:
            addr = addr_match.group(1).strip()
            addr = re.sub(r",?\s*$", "", addr)
            results["sample_address"] = addr
    if not results["sample_address"]:
        addr_match = re.search(
            r"(\d+\s+[A-Za-z][^,]+,\s*Nantucket)\s*[A-Z]{2}\d{2}/", content
        )
        if addr_match:
            results["sample_address"] = addr_match.group(1).strip()

    # Sample date
    date_match = re.search(r"Sampled[:\s]*([\d/]+)", content)
    if not date_match:
        # DW Program forms render the date inline with the location row as
        # "<address>, Nantucket, MA <COLLECTOR_INITIALS><MM/DD/YYYY>" (e.g.
        # "Nantucket, MA KM02/06/2024"). Allow an optional comma after
        # Nantucket and 0-3 trailing capital letters between the state code
        # and the date for collector initials.
        date_match = re.search(
            r"Nantucket,?\s*[A-Z]{2}\s*[A-Z]{0,3}(\d{2}/\d{2}/\d{4})", content
        )
    if date_match:
        results["sample_date"] = date_match.group(1)

    return results


# =============================================================================
# Date parsing helpers
# =============================================================================

def _parse_sample_date_from_filename(filename: str) -> Optional[str]:
    """Extract YYYYMMDD from filename like PFAS_Sampling_20251028."""
    m = re.search(r"(\d{8})$", filename)
    if m:
        try:
            datetime.strptime(m.group(1), "%Y%m%d")
            return m.group(1)
        except ValueError:
            pass
    return None


def _parse_date(raw: str) -> Optional[datetime]:
    """Parse date string in various formats."""
    for fmt in ("%Y%m%d", "%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw.strip(), fmt)
        except ValueError:
            continue
    return None


# =============================================================================
# Main scraper
# =============================================================================

def run_laserfiche_scraper(
    headless: bool = True,
    map_filter: Optional[str] = None,
) -> dict:
    """Run the full Laserfiche scraper.

    Args:
        headless: Run browser in headless mode.
        map_filter: If set, only scrape this map number (e.g. "21" for testing).

    Returns:
        Summary dict with run stats.
    """
    # Create scrape run record (short-lived session)
    with SessionLocal() as db:
        run = ScrapeRun(source="laserfiche", status="running")
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

            # Navigate to root: Property Files
            logger.info("Navigating to root folder %s", settings.laserfiche_root_folder_id)
            if not _navigate_and_wait(page, settings.laserfiche_root_folder_id):
                raise RuntimeError("Failed to navigate to root folder")

            # Get map group folders (Maps 01-09, Maps 10-19, ..., Maps 90-99)
            root_links = _extract_all_links_with_scroll(page, max_scrolls=20)
            map_groups = [
                l for l in root_links
                if l["type"] == "folder" and re.match(r"Maps?\s+\d", l["name"], re.IGNORECASE)
            ]
            logger.info("Found %d map group folders", len(map_groups))

            for group in map_groups:
                logger.info("Entering map group: %s", group["name"])
                if not _navigate_and_wait(page, group["id"]):
                    continue

                # Get individual map folders within this group
                group_links = _extract_all_links_with_scroll(page, max_scrolls=30)
                map_folders = [l for l in group_links if l["type"] == "folder"]
                logger.info("  %s contains %d map folders", group["name"], len(map_folders))

                for map_folder in map_folders:
                    # Extract map number from folder name (e.g. "Map 21" → "21")
                    map_match = re.search(r"(\d+)", map_folder["name"])
                    if not map_match:
                        continue
                    map_number = map_match.group(1)

                    # Apply map filter for testing
                    if map_filter and map_number != map_filter:
                        continue

                    logger.info("  Scanning Map %s (%s)", map_number, map_folder["name"])
                    if not _navigate_and_wait(page, map_folder["id"]):
                        continue

                    # Get all parcel folders within this map
                    parcel_links = _extract_all_links_with_scroll(page)
                    parcel_folders = [l for l in parcel_links if l["type"] == "folder"]
                    logger.info("    Map %s: %d parcel folders", map_number, len(parcel_folders))

                    for parcel_folder in parcel_folders:
                        _process_parcel(
                            page, stats,
                            map_number=map_number,
                            parcel_name=parcel_folder["name"],
                            parcel_folder_id=parcel_folder["id"],
                        )

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


def _process_parcel(
    page: Page,
    stats: dict,
    map_number: str,
    parcel_name: str,
    parcel_folder_id: str,
):
    """Process a single parcel folder: navigate to Well/Reports/year and find PFAS docs."""
    if not _navigate_and_wait(page, parcel_folder_id):
        return

    prop_links = _extract_all_links_with_scroll(page, max_scrolls=10)
    well_folders = [
        l for l in prop_links
        if l["type"] == "folder" and l["name"].lower() in ("well", "wells")
    ]

    if not well_folders:
        return

    if not _navigate_and_wait(page, well_folders[0]["id"]):
        return

    well_links = _extract_all_links_with_scroll(page, max_scrolls=10)
    reports_folders = [
        l for l in well_links
        if l["type"] == "folder" and "report" in l["name"].lower()
    ]

    if not reports_folders:
        return

    if not _navigate_and_wait(page, reports_folders[0]["id"]):
        return

    reports_links = _extract_all_links_with_scroll(page, max_scrolls=10)
    year_folders = [l for l in reports_links if l["type"] == "folder"]

    for year_folder in year_folders:
        if not _navigate_and_wait(page, year_folder["id"]):
            continue

        year_links = _extract_all_links_with_scroll(page, max_scrolls=10)
        pfas_docs = [
            l for l in year_links
            if l["type"] == "document"
            and l["name"].upper().startswith(("PFAS_SAMPLING", "PFAS_AND_WELL_SAMPLING"))
        ]

        for doc in pfas_docs:
            _process_document(page, stats, doc, map_number, parcel_name)


def _process_document(
    page: Page,
    stats: dict,
    doc: dict,
    map_number: str,
    parcel_name: str,
):
    """Check and parse a single PFAS document. Uses a fresh DB session."""
    doc_id = doc["id"]

    # Check if already seen (fresh session)
    with SessionLocal() as db:
        existing = db.query(SeenDocument).filter_by(doc_key=doc_id).first()
        if existing:
            stats["skipped_seen"] += 1
            logger.debug("      SKIP (seen): %s (doc_id=%s)", doc["name"], doc_id)
            return

        # Record in seen_documents BEFORE parsing (per PRD 4.2)
        seen = SeenDocument(
            doc_key=doc_id,
            source="laserfiche",
            parse_status="pending",
        )
        db.add(seen)
        db.commit()

    logger.info("      NEW: %s (doc_id=%s)", doc["name"], doc_id)
    stats["new_docs_found"] += 1

    # Parse parcel folder name into (actual_map, parcel). Handles integer
    # maps ("21 80") and Nantucket sub-maps ("59.4 140"). Multi-parcel
    # folder names are preserved; api.py resolves them at display time.
    actual_map_number, parcel_number = _parse_parcel_folder_name(map_number, parcel_name)

    # Parse sample date from filename (authoritative)
    date_str = _parse_sample_date_from_filename(doc["name"])

    # Parse the report
    try:
        parsed = _parse_report(page, doc_id)
        if parsed is None:
            raise ValueError("Could not open plain-text mode")

        # Determine sample date
        sample_date = None
        if date_str:
            sample_date = _parse_date(date_str)
        elif parsed.get("sample_date"):
            sample_date = _parse_date(parsed["sample_date"])

        # Extract street name
        street_name = _extract_street_name(parsed.get("sample_address"))

        # Compounds → Decimal
        def to_dec(val):
            if val is None:
                return None
            if val == 0:
                return None  # ND = null in DB
            return Decimal(str(val))

        pfas6_raw = parsed.get("pfas6")
        pfas6_sum = Decimal(str(pfas6_raw)) if pfas6_raw is not None else Decimal("0")

        result_status = classify_result_status(float(pfas6_sum))

        # Geo resolution
        geo = resolve_location(actual_map_number, parcel_number)
        neighborhood = geo["neighborhood"] if geo else FALLBACK_NEIGHBORHOOD

        compounds = parsed.get("compounds", {})
        result = PfasResult(
            laserfiche_doc_id=int(doc_id),
            map_number=actual_map_number,
            parcel_number=parcel_number,
            neighborhood=neighborhood,
            street_name=street_name,
            sample_date=sample_date.date() if sample_date else None,
            pfos=to_dec(compounds.get("PFOS")),
            pfoa=to_dec(compounds.get("PFOA")),
            pfhxs=to_dec(compounds.get("PFHxS")),
            pfna=to_dec(compounds.get("PFNA")),
            pfhpa=to_dec(compounds.get("PFHpA")),
            pfda=to_dec(compounds.get("PFDA")),
            pfas6_sum=pfas6_sum,
            j_qualifier_present=parsed.get("j_qualifier_present", False),
            pass_fail=parsed.get("pass_fail", "UNKNOWN"),
            result_status=result_status,
        )

        # Save result (fresh session)
        with SessionLocal() as db:
            db.add(result)
            seen = db.query(SeenDocument).filter_by(doc_key=doc_id).first()
            if seen:
                seen.parse_status = "success"
            db.commit()

        stats["new_docs_parsed"] += 1
        logger.info(
            "      Parsed: PFAS6=%.1f status=%s neighborhood=%s",
            float(pfas6_sum), result_status, neighborhood,
        )

    except Exception as e:
        logger.error("      Parse error for doc %s: %s", doc_id, e, exc_info=True)
        with SessionLocal() as db:
            seen = db.query(SeenDocument).filter_by(doc_key=doc_id).first()
            if seen:
                seen.parse_status = "error"
                seen.error_message = str(e)[:2000]
            db.commit()
        stats["parse_errors"] += 1
        stats["errors"].append({"doc_id": doc_id, "error": str(e)})