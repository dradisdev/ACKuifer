"""
eea_monitor.py
--------------
Monitors the MassDEP / EEA Data Portal for new documents filed under
RTN 4-0029612 (Nantucket PFAS Source Discovery project).

The portal at https://eeaonline.eea.state.ma.us/portal/dep/wastesite/viewer/4-0029612
is a JavaScript-rendered single-page application.  We use Playwright to load it,
wait for the document list to populate, then extract links, download any PDFs that
are new since the last run, and hand them off to the parser.

Usage:
    python eea_monitor.py            # check for new docs, download & parse
    python eea_monitor.py --list     # just print the current document list
    python eea_monitor.py --force    # re-download and re-parse all docs
"""

import asyncio
import json
import os
import re
import sys
import time
from datetime import datetime
from typing import Optional, Union
from pathlib import Path

from playwright.async_api import async_playwright

from source_discovery_parser import parse_source_discovery_pdf
from source_discovery_db import SourceDiscoveryDB

# ── Configuration ──────────────────────────────────────────────────────────────

RTN = "4-0029612"
PORTAL_URL = f"https://eeaonline.eea.state.ma.us/portal/dep/wastesite/viewer/{RTN}"

# Alternative: the older viewer sometimes exposes a direct document list
LEGACY_VIEWER_URL = f"https://eeaonline.eea.state.ma.us/DEP/wsc_viewer/main.aspx?rtn={RTN}"

BASE_DIR = Path(__file__).parent
PDF_DIR = BASE_DIR / "pdfs"
DB_PATH = BASE_DIR / "source_discovery.json"

PDF_DIR.mkdir(exist_ok=True)

# ── Portal scraper ─────────────────────────────────────────────────────────────

async def fetch_document_list(visible: bool = False) -> list[dict]:
    """
    Load the EEA portal viewer and scrape the list of filed documents.
    Returns a list of dicts: {title, date_filed, doc_type, url, filename}
    """
    documents = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=not visible)
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/121.0.0.0 Safari/537.36"
            )
        )
        page = await context.new_page()

        print(f"[EEA Monitor] Loading portal: {PORTAL_URL}")
        try:
            await page.goto(PORTAL_URL, wait_until="networkidle", timeout=30000)
        except Exception:
            await page.goto(PORTAL_URL, wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(5)

        # Wait for document list to appear — the portal uses Angular/React
        # so we wait for any element that looks like a document row or table
        try:
            await page.wait_for_selector(
                "table, .document-list, [class*='document'], [class*='report'], a[href*='.pdf'], a[href*='file']",
                timeout=15000
            )
        except Exception:
            print("[EEA Monitor] Warning: document list selector timed out, scraping anyway")

        await asyncio.sleep(3)  # extra settle time for JS rendering

        # ── Strategy 1: look for PDF links directly ────────────────────────────
        pdf_links = await page.eval_on_selector_all(
            "a[href]",
            """elements => elements
                .map(el => ({href: el.href, text: el.textContent.trim()}))
                .filter(l => l.href.toLowerCase().includes('.pdf')
                          || l.href.toLowerCase().includes('file')
                          || l.href.toLowerCase().includes('document'))
            """
        )

        if pdf_links:
            for link in pdf_links:
                documents.append({
                    "title":       link["text"] or "Unknown",
                    "url":         link["href"],
                    "date_filed":  _extract_date_from_text(link["text"]) or _extract_date_from_text(link["href"]),
                    "doc_type":    _infer_doc_type(link["text"]),
                    "filename":    _url_to_filename(link["href"], link["text"]),
                })
            print(f"[EEA Monitor] Found {len(documents)} document links via PDF strategy")

        # ── Strategy 2: scrape table rows ─────────────────────────────────────
        if not documents:
            rows = await page.eval_on_selector_all(
                "table tr",
                """rows => rows.map(row => {
                    const cells = Array.from(row.querySelectorAll('td, th'));
                    const link  = row.querySelector('a[href]');
                    return {
                        cells: cells.map(c => c.textContent.trim()),
                        href:  link ? link.href  : null,
                        text:  link ? link.textContent.trim() : null,
                    };
                }).filter(r => r.href)"""
            )
            for row in rows:
                title = row.get("text") or (row["cells"][0] if row["cells"] else "Unknown")
                url   = row.get("href", "")
                date_filed = _extract_date_from_cells(row.get("cells", []))
                documents.append({
                    "title":      title,
                    "url":        url,
                    "date_filed": date_filed,
                    "doc_type":   _infer_doc_type(title),
                    "filename":   _url_to_filename(url, title),
                })
            if documents:
                print(f"[EEA Monitor] Found {len(documents)} documents via table strategy")

        # ── Strategy 3: check network requests for an API call ─────────────────
        if not documents:
            print("[EEA Monitor] No documents found via DOM — checking page source for API hints")
            content = await page.content()
            # look for JSON blobs containing document lists
            matches = re.findall(r'"(?:url|href|link)"\s*:\s*"([^"]*\.pdf[^"]*)"', content, re.I)
            for url in set(matches):
                documents.append({
                    "title":      Path(url).stem.replace("_", " "),
                    "url":        url,
                    "date_filed": None,
                    "doc_type":   _infer_doc_type(url),
                    "filename":   _url_to_filename(url, url),
                })
            if documents:
                print(f"[EEA Monitor] Found {len(documents)} documents via JSON-in-source strategy")

        await browser.close()

    # De-duplicate by URL
    seen = set()
    unique = []
    for d in documents:
        if d["url"] not in seen:
            seen.add(d["url"])
            unique.append(d)

    return unique


async def download_pdf(url: str, dest: Path, visible: bool = False) -> bool:
    """Download a single PDF, handling redirects and JS-gated downloads."""
    import urllib.request
    import urllib.error

    # Try direct HTTP first (fastest)
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
                print(f"  ✓ Downloaded (direct): {dest.name}")
                return True
    except Exception as e:
        print(f"  Direct download failed ({e}), trying Playwright...")

    # Playwright approach — capture download event
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=not visible)
        context = await browser.new_context(accept_downloads=True)
        page = await context.new_page()
        try:
            async with page.expect_download(timeout=20000) as dl_info:
                await page.goto(url, timeout=20000)
            download = await dl_info.value
            await download.save_as(str(dest))
            print(f"  ✓ Downloaded (Playwright): {dest.name}")
            await browser.close()
            return True
        except Exception as e:
            print(f"  ✗ Download failed: {e}")
            await browser.close()
            return False


# ── Main run logic ─────────────────────────────────────────────────────────────

async def run(list_only: bool = False, force: bool = False, visible: bool = False):
    db = SourceDiscoveryDB(DB_PATH)
    documents = await fetch_document_list(visible=visible)

    if not documents:
        print("[EEA Monitor] No documents found. The portal may require interactive login,")
        print("  or the page structure has changed. Try running with --visible to debug.")
        print(f"  Manual URL: {PORTAL_URL}")
        return

    if list_only:
        print(f"\n{'─'*60}")
        print(f"Documents for RTN {RTN}  ({len(documents)} total)")
        print(f"{'─'*60}")
        for d in documents:
            print(f"  [{d.get('date_filed','?')}]  {d['doc_type']:25s}  {d['title'][:60]}")
        return

    # ── Determine which docs are new ──────────────────────────────────────────
    new_docs = [d for d in documents if force or not db.has_document(d["url"])]
    print(f"\n[EEA Monitor] {len(documents)} total docs | {len(new_docs)} new/to-process")

    results = []
    for doc in new_docs:
        dest = PDF_DIR / doc["filename"]

        # Download
        if not dest.exists() or force:
            success = await download_pdf(doc["url"], dest, visible=visible)
            if not success:
                db.record_download_failure(doc)
                continue

        # Parse
        print(f"  Parsing: {doc['filename']}")
        parsed = parse_source_discovery_pdf(str(dest), doc)
        if parsed:
            db.upsert_report(parsed)
            results.append(parsed)
            print(f"  ✓ Parsed: {len(parsed.get('sample_locations', []))} sample locations found")
        else:
            print(f"  ⚠ Could not extract structured data from {doc['filename']}")
            db.record_unparsed(doc, str(dest))

    db.save()
    print(f"\n[EEA Monitor] Done. {len(results)} new reports added to {DB_PATH}")
    return results


# ── Helpers ───────────────────────────────────────────────────────────────────

def _infer_doc_type(text: str) -> str:
    t = text.lower()
    if "phase i"   in t:                                return "Phase I Site Assessment"
    if "phase ii"  in t:                                return "Phase II Site Assessment"
    if "sampling plan" in t:                            return "Sampling Plan"
    if "field activ" in t or "field inv" in t:          return "Field Activity Report"
    if "analytical" in t or "lab data" in t or "lab report" in t: return "Laboratory Results"
    if "groundwater" in t:                              return "Groundwater Sampling Report"
    if "soil" in t and "sampling" in t:                 return "Soil Sampling Report"
    if "sampling result" in t or "sample result" in t:  return "Sampling Results"
    if "tier" in t:                                      return "Tier Classification"
    if "rao" in t or "response action" in t:            return "RAO Statement"
    if "notification" in t:                             return "Notification"
    if "permit" in t:                                   return "Permit"
    if "transmittal" in t:                              return "Transmittal"
    if "well install" in t or "boring" in t:            return "Well Installation Report"
    if "ins-meet" in t or "inspection" in t or "meeting form" in t: return "Inspection / Meeting"
    if "document upload" in t:                          return "Document Upload"
    if "release amendment" in t or "bwsc102" in t:      return "Release Amendment"
    if "release log" in t or "bwsc101" in t:            return "Release Log"
    if "intake form" in t:                              return "Intake Form"
    if "sarss" in t or "data eval" in t:                return "Field Investigation Summary"
    if "private well" in t or "residential well" in t:  return "Private Well Sampling"
    if "geothermal" in t:                               return "Geothermal Sampling"
    if "pfas" in t and "sampling" in t:                 return "PFAS Sampling Report"
    return "Report"


def _extract_date_from_text(text: str) -> Optional[str]:
    """Extract a date from a filename or link text.

    Handles patterns like:
        MM-DD-YYYY, MM-DD-YY, YYYY-MM-DD, MM/DD/YYYY, MM/DD/YY
    Also handles filename-style dates: 12-16-2025, 04-09-25
    """
    if not text:
        return None
    # ISO format first: YYYY-MM-DD
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", text)
    if m:
        return m.group()
    # MM-DD-YYYY or MM-DD-YY (with - or /)
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
    # Try to get filename from URL
    path_part = url.split("?")[0].split("/")[-1]
    if path_part.lower().endswith(".pdf"):
        return re.sub(r"[^\w\-.]", "_", path_part)

    # Derive from title
    clean = re.sub(r"[^\w\s\-]", "", title)
    clean = re.sub(r"\s+", "_", clean.strip())
    return f"{clean[:80]}_{int(time.time())}.pdf"


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    list_only = "--list"    in sys.argv
    force     = "--force"   in sys.argv
    visible   = "--visible" in sys.argv
    asyncio.run(run(list_only=list_only, force=force, visible=visible))
