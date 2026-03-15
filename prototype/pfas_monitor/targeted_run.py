"""
Targeted download+parse of 6 high-value documents from the EEA portal.
"""
import asyncio
import json
from pathlib import Path
from eea_monitor import fetch_document_list, download_pdf, PDF_DIR, DB_PATH
from source_discovery_parser import parse_source_discovery_pdf
from source_discovery_db import SourceDiscoveryDB

# Substrings that uniquely match each target doc title
TARGETS = [
    "December 2024 PFAS Lab Report",
    "Lab Data. PFAS. Residential Wells",
    "Lab Data. PFAS. June - August 2025",
    "Nantucket BWSC PFAS Sampling 12-16-25",
    "Town Nantucket Soil and Groundwater Sampling Results",
    "SARSS Nantucket_Field Inv and Data Eval Summary",
]

async def main():
    documents = await fetch_document_list(visible=True)
    print(f"\nTotal docs on portal: {len(documents)}")

    # Filter to targets
    selected = []
    for doc in documents:
        for t in TARGETS:
            if t.lower() in doc["title"].lower():
                selected.append(doc)
                break

    print(f"Matched {len(selected)} target docs:\n")
    for d in selected:
        print(f"  [{d['date_filed'] or '?':>12s}]  {d['doc_type']:30s}  {d['title'][:70]}")

    if len(selected) != len(TARGETS):
        matched_targets = [t for t in TARGETS if any(t.lower() in d["title"].lower() for d in selected)]
        missed = [t for t in TARGETS if t not in matched_targets]
        print(f"\n⚠ Missing targets: {missed}")

    # Download and parse
    db = SourceDiscoveryDB(DB_PATH)
    results = []

    for doc in selected:
        dest = PDF_DIR / doc["filename"]
        print(f"\n{'─'*60}")
        print(f"Downloading: {doc['title'][:70]}")

        if not dest.exists():
            success = await download_pdf(doc["url"], dest, visible=True)
            if not success:
                print(f"  ✗ Download failed")
                continue
        else:
            print(f"  (already on disk: {dest.name})")

        print(f"  Parsing: {doc['filename']}")
        parsed = parse_source_discovery_pdf(str(dest), doc)
        if parsed:
            db.upsert_report(parsed)
            results.append(parsed)

            locs = parsed["sample_locations"]
            exceedances = [l for l in locs if l["status"] in ("HIGH-DETECT", "HAZARD")]
            print(f"  ✓ Format: {parsed['report_format']}")
            print(f"    Locations: {len(locs)} | Exceedances: {len(exceedances)}")
            print(f"    GW: {parsed['groundwater_locations_count']} | Soil: {parsed['soil_locations_count']}")
            if parsed.get("max_pfas6_gw") is not None:
                print(f"    Max PFAS6 (GW): {parsed['max_pfas6_gw']:.2f} ng/L")
            if exceedances:
                for e in exceedances:
                    print(f"    ⚠ {e['well_id']}: PFAS6={e.get('pfas6', '?')} ng/L → {e['status']}")
        else:
            print(f"  ✗ Parser returned None")

    db.save()

    # Summary
    print(f"\n{'═'*60}")
    print(f"SUMMARY: {len(results)}/{len(selected)} docs parsed successfully")
    print(f"{'═'*60}")
    total_locs = sum(len(r["sample_locations"]) for r in results)
    total_exc  = sum(1 for r in results for l in r["sample_locations"]
                     if l["status"] in ("HIGH-DETECT", "HAZARD"))
    print(f"  Total sample locations: {total_locs}")
    print(f"  Total exceedances: {total_exc}")
    for r in results:
        fmt = r["report_format"]
        locs = len(r["sample_locations"])
        exc = sum(1 for l in r["sample_locations"] if l["status"] in ("HIGH-DETECT", "HAZARD"))
        title = r["doc_title"][:55]
        print(f"  [{fmt:12s}]  {locs:2d} locs  {exc:2d} exc  {title}")

    print(f"\nResults saved to {DB_PATH}")

asyncio.run(main())
