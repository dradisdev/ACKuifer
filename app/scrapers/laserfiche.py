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

from app.config import settings, classify_result_status, FALLBACK_NEIGHBORHOOD
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

def _extract_compound_value(content: str, short_name: str, long_name: str) -> Optional[float]:
    """Extract a compound value from report content. Returns None if not found, 0 if ND."""
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
    """Open a Laserfiche document in plain-text mode and parse PFAS data."""
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

    results = {
        "pfas6": None,
        "pass_fail": None,
        "sample_date": None,
        "sample_address": None,
        "compounds": {},
        "j_qualifier_present": False,
    }

    # Extract PFAS6 value
    pfas6_match = re.search(r"([\d.]+|ND)\s+ng/L[^\n]*PFAS6", content, re.IGNORECASE)
    if pfas6_match:
        val = pfas6_match.group(1)
        results["pfas6"] = 0.0 if val == "ND" else float(val)
    else:
        pfas6_match = re.search(r"PFAS6[^=]+=\d+(ND|[\d.]+)", content, re.IGNORECASE)
        if pfas6_match:
            val = pfas6_match.group(1)
            results["pfas6"] = 0.0 if val == "ND" else float(val)

    # Extract all 18 compounds
    for short_name, long_name, _in_pfas6 in PFAS_COMPOUNDS:
        value = _extract_compound_value(content, short_name, long_name)
        results["compounds"][short_name] = value

    # Check for J-qualified values
    if re.search(r"\d+\.?\d*\s+J\s+ng/L", content, re.IGNORECASE):
        results["j_qualifier_present"] = True

    # If PFAS6 wasn't found directly, calculate from the 6 regulated compounds
    if results["pfas6"] is None:
        pfas6_compounds = [
            results["compounds"].get(name)
            for name, _, in_pfas6 in PFAS_COMPOUNDS
            if in_pfas6
        ]
        if any(v is not None for v in pfas6_compounds):
            results["pfas6"] = sum(v or 0.0 for v in pfas6_compounds)

    # Pass/fail
    content_lower = content.lower()
    if "does not meet" in content_lower:
        results["pass_fail"] = "FAIL"
    elif "suitable for drinking" in content_lower:
        results["pass_fail"] = "PASS"
    elif results["pfas6"] is not None:
        results["pass_fail"] = "FAIL" if results["pfas6"] > 20 else "PASS"
    else:
        results["pass_fail"] = "UNKNOWN"

    # Address — cleaned of UI chrome
    # Check for MassDEP Drinking Water Program form format first
    if re.search(r"Drinking Water Program|PWS INFORMATION", content, re.IGNORECASE):
        # DW Program form: find the last street address before "Nantucket" appears
        # Use findall to get all matches and take the last one (closest to Nantucket)
        addr_matches = re.findall(
            r"(\d+[A-Za-z]?\s+[A-Za-z][A-Za-z\s]{2,30}"
            r"(?:Rd|Road|St|Street|Ave|Avenue|Ln|Lane|Dr|Drive|"
            r"Way|Blvd|Ct|Court|Pl|Place)\.?)",
            content.split("Nantucket")[0],
            re.IGNORECASE
        )
        if addr_matches:
            results["sample_address"] = addr_matches[-1].strip().rstrip(".")
    else:
        # Standard Barnstable County / Pace lab format
        # Primary: "Collection Address: 24 Sesachacha Road, Nantucket"
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
        date_match = re.search(r"Nantucket\s*[A-Z]{2}(\d{2}/\d{2}/\d{4})", content)
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

    # Extract parcel_number from parcel folder name
    # Folder name format: "21 80" or "21 37 & 122"
    parcel_number = parcel_name
    # Strip map number prefix if present
    if parcel_name.startswith(map_number + " "):
        parcel_number = parcel_name[len(map_number) + 1:]

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
        geo = resolve_location(map_number, parcel_number)
        neighborhood = geo["neighborhood"] if geo else FALLBACK_NEIGHBORHOOD

        compounds = parsed.get("compounds", {})
        result = PfasResult(
            laserfiche_doc_id=int(doc_id),
            map_number=map_number,
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
