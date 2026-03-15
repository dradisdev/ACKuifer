"""
test_source_discovery.py
------------------------
Tests for the Source Discovery pipeline.
Covers both Format A (lab cert) and Format B (field report).
"""

import json
import sys
import tempfile
from pathlib import Path

# ── Fixtures ──────────────────────────────────────────────────────────────────

# FORMAT B: Field investigation narrative report (Verdantas style)
FIXTURE_FIELD_REPORT = """
MassDEP Source Discovery Investigation — RTN 4-0029612
Nantucket, Massachusetts

Prepared by: Verdantas Environmental Engineering
Report Date: January 16, 2025
Site Address: 2 Fairgrounds Road, Nantucket, MA 02554

EXECUTIVE SUMMARY
MassDEP collected groundwater samples from 12 monitoring wells installed at
the Fairgrounds campus between November and December 2024. Laboratory results
indicate one location (VDT-4FG-4) exceeded the Massachusetts MCL of 20 ng/L
for PFAS6.

SAMPLE RESULTS — GROUNDWATER (ng/L)

Well ID         PFAS6    PFOS    PFOA    PFNA    PFHxS   PFHpS   PFDA
VDT-2FG-5       6.41     2.10    1.85    0.95    0.72    0.48    0.31
VDT-4FG-4      22.33     9.80    7.20    2.15    1.80    0.88    0.50
VDT-6FG-7       3.18     1.10    0.95    0.40    0.38    0.22    0.13
VDT-6FG-8       1.22     0.41    0.38    0.15    0.14    0.09    0.05
VDT-WAITT-12   18.75     7.20    5.90    1.95    1.60    1.41    0.69
VDT-OSR-1       0.88     0.30    0.25    0.10    0.09    0.06    0.08
VDT-OSR-2      ND        ND      ND      ND      ND      ND      ND
VDT-2FG-6       4.55     1.60    1.40    0.65    0.52    0.25    0.13
VDT-6FG-9       2.01     0.70    0.60    0.30    0.22    0.12    0.07
VDT-6FG-10      0.55     0.19    0.16    0.07    0.06    0.04    0.03
VDT-6FG-11      0.33     0.11    0.10    0.05    0.04    0.02    0.01
VDT-OSR-3      ND        ND      ND      ND      ND      ND      ND

GPS COORDINATES (NAD83)
VDT-2FG-5       41.2799  -70.0628
VDT-4FG-4       41.2801  -70.0623
VDT-6FG-7       41.2803  -70.0617
VDT-6FG-8       41.2806  -70.0612
VDT-WAITT-12    41.2845  -70.0598
VDT-OSR-1       41.2825  -70.0585
VDT-OSR-2       41.2828  -70.0582
VDT-2FG-6       41.2797  -70.0631
VDT-6FG-9       41.2808  -70.0609
VDT-6FG-10      41.2810  -70.0605
VDT-6FG-11      41.2812  -70.0601
VDT-OSR-3       41.2831  -70.0578

PFAS6 = sum of PFOS, PFOA, PFNA, PFHxS, PFHpS, PFDA
ND = Non-detect  MCL = 20 ng/L

Licensed Site Professional: Jane Smith, LSP #12345
Verdantas Environmental Engineering
"""

# FORMAT A: Lab analytical certificate (mirrors real Jan 2026 filing format)
# IMPORTANT:
#   Sample Location = site-level label, repeats across all samples → NOT the location key
#   Client ID       = monitoring well ID                           → IS the location key
FIXTURE_LAB_CERT = """
LABORATORY ANALYTICAL REPORT
Alpha Analytical Laboratories
Report Date: 01/23/2026
Project: RTN 4-0029612 — Nantucket PFAS Source Discovery

Client ID:        VDT-4FG-4
Lab Sample ID:    AA-2026-00441-01
Sample Location:  Fairgrounds Fire Department
Sample Date:      01/15/2026
Collected By:     TRC
Matrix:           Groundwater

Analyte                     Result    Units    MDL     MRL
PFOS                         9.80     ng/L    0.10    0.50
PFOA                         7.20     ng/L    0.10    0.50
PFNA                         2.15     ng/L    0.10    0.50
PFHxS                        1.80     ng/L    0.10    0.50
PFHpS                        0.88     ng/L    0.10    0.50
PFDA                         0.50     ng/L    0.10    0.50
PFHxA                        1.20     ng/L    0.10    0.50
PFBS                         ND       ng/L    0.10    0.50
PFBA                         ND       ng/L    0.10    0.50
HFPO-DA                      ND       ng/L    0.10    0.50
NEtFOSAA                     ND       ng/L    0.10    0.50
NMeFOSAA                     ND       ng/L    0.10    0.50
PFAS6: 22.33 ng/L

Client ID:        VDT-OSR-2
Lab Sample ID:    AA-2026-00441-02
Sample Location:  Fairgrounds Fire Department
Sample Date:      01/15/2026
Collected By:     TRC
Matrix:           Groundwater

Analyte                     Result    Units    MDL     MRL
PFOS                         ND       ng/L    0.10    0.50
PFOA                         ND       ng/L    0.10    0.50
PFNA                         ND       ng/L    0.10    0.50
PFHxS                        ND       ng/L    0.10    0.50
PFHpS                        ND       ng/L    0.10    0.50
PFDA                         ND       ng/L    0.10    0.50
PFAS6: ND ng/L

Client ID:        VDT-WAITT-12
Lab Sample ID:    AA-2026-00441-03
Sample Location:  Fairgrounds Fire Department
Sample Date:      01/15/2026
Collected By:     TRC
Matrix:           Groundwater

Analyte                     Result    Units    MDL     MRL
PFOS                         7.20     ng/L    0.10    0.50
PFOA                         5.90     ng/L    0.10    0.50
PFNA                         1.95     ng/L    0.10    0.50
PFHxS                        1.60     ng/L    0.10    0.50
PFHpS                        1.41     ng/L    0.10    0.50
PFDA                         0.69     ng/L    0.10    0.50
PFAS6: 18.75 ng/L
"""

# Keep FIXTURE_TEXT as an alias for the field report (legacy tests)
FIXTURE_TEXT = FIXTURE_FIELD_REPORT

# ── Test runner ───────────────────────────────────────────────────────────────

PASS = 0
FAIL = 0

def test(name, condition, detail=""):
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  ✓ PASS  {name}")
    else:
        FAIL += 1
        print(f"  ✗ FAIL  {name}" + (f"  →  {detail}" if detail else ""))


# ── Tests ─────────────────────────────────────────────────────────────────────

def test_format_detection():
    print("\n── Format detection ───────────────────────────────────")
    from source_discovery_parser import _detect_format

    test("lab cert detected as lab_cert",
         _detect_format(FIXTURE_LAB_CERT) == "lab_cert")
    test("field report detected as field_report",
         _detect_format(FIXTURE_FIELD_REPORT) == "field_report")
    test("text with Client ID + Sample Location = lab_cert",
         _detect_format("Client ID: VDT-1\nSample Location: Site A") == "lab_cert")
    test("text with GPS coords = field_report",
         _detect_format("VDT-4FG-4  41.2801  -70.0623") == "field_report")


def test_lab_cert_parser():
    print("\n── Format A: Lab certificate parser ───────────────────")
    from source_discovery_parser import (
        _parse_lab_cert,
        _extract_sample_location_label,
    )

    locs_by_id = {}
    _parse_lab_cert.__globals__  # just to confirm import OK

    # Run via the block splitter + block parser
    from source_discovery_parser import _split_into_sample_blocks, _parse_lab_cert_block
    blocks = _split_into_sample_blocks(FIXTURE_LAB_CERT, [FIXTURE_LAB_CERT])
    locs = {}
    for block in blocks:
        _parse_lab_cert_block(block, locs)

    # Keys are now (client_id, medium) tuples
    well_ids = {k[0] for k in locs.keys()}
    test("3 Client IDs found",              len(locs) == 3,
         f"found: {list(locs.keys())}")
    test("VDT-4FG-4 present",              "VDT-4FG-4" in well_ids)
    test("VDT-OSR-2 present",              "VDT-OSR-2" in well_ids)
    test("VDT-WAITT-12 present",           "VDT-WAITT-12" in well_ids)

    # Verify Client ID is used, not Sample Location
    test("no 'Fairgrounds Fire Dept' key", "Fairgrounds Fire Department" not in well_ids)

    vdt4 = next((v for k, v in locs.items() if k[0] == "VDT-4FG-4"), {})
    test("VDT-4FG-4 pfas6 = 22.33",       vdt4.get("pfas6") == 22.33,
         f"pfas6={vdt4.get('pfas6')}")
    test("VDT-4FG-4 PFOS = 9.80",         vdt4.get("compounds", {}).get("PFOS") == 9.80)

    osr2 = next((v for k, v in locs.items() if k[0] == "VDT-OSR-2"), {})
    test("VDT-OSR-2 pfas6 = 0 (ND)",      osr2.get("pfas6") == 0.0,
         f"pfas6={osr2.get('pfas6')}")

    waitt = next((v for k, v in locs.items() if k[0] == "VDT-WAITT-12"), {})
    test("VDT-WAITT-12 pfas6 = 18.75",    waitt.get("pfas6") == 18.75,
         f"pfas6={waitt.get('pfas6')}")

    # Sample Location label extraction (should be project-level string)
    label = _extract_sample_location_label(FIXTURE_LAB_CERT)
    test("Sample Location label extracted",label is not None)
    test("Label = Fairgrounds Fire Dept",  label and "Fairgrounds" in label,
         f"label={label!r}")


def test_field_report_parser():
    print("\n── Format B: Field report parser ──────────────────────")
    from source_discovery_parser import (
        _extract_report_date, _extract_firm, _extract_lsp,
        _extract_project_address, _parse_free_text_locations, _attach_coordinates,
        status_from_pfas6, STATUS_COLOR,
    )

    text = FIXTURE_FIELD_REPORT
    date = _extract_report_date(text)
    test("report_date extracted",          date is not None)
    test("report_date contains 2025",      date and "2025" in date)
    test("firm = Verdantas",               _extract_firm(text) == "Verdantas")
    test("LSP extracted",                  _extract_lsp(text) is not None)
    addr = _extract_project_address(text)
    test("address contains Fairgrounds",   addr and "Fairgrounds" in addr)

    locs = _parse_free_text_locations(text)
    _attach_coordinates(locs, text)
    test("locations found (>=8)",          len(locs) >= 8,     f"found: {len(locs)}")
    vdt4 = next((l for l in locs if "4FG-4" in l["well_id"]), None)
    test("VDT-4FG-4 found",               vdt4 is not None,
         f"wells: {[l['well_id'] for l in locs]}")
    if vdt4:
        test("VDT-4FG-4 pfas6 ≈ 22.33",  vdt4["pfas6"] is not None and abs(vdt4["pfas6"] - 22.33) < 1)
        test("VDT-4FG-4 has lat",         vdt4.get("lat") is not None)
        test("VDT-4FG-4 lat ~ Nantucket", vdt4.get("lat") and 41.0 < vdt4["lat"] < 42.0)

    nd_locs = [l for l in locs if l.get("pfas6") == 0.0]
    test("ND wells present",               len(nd_locs) >= 1)

    # Status thresholds
    test("pfas6=0 → NON-DETECT",          status_from_pfas6(0) == "NON-DETECT")
    test("pfas6=10 → DETECT",             status_from_pfas6(10) == "DETECT")
    test("pfas6=20 → DETECT",             status_from_pfas6(20) == "DETECT")
    test("pfas6=22.33 → HIGH-DETECT",     status_from_pfas6(22.33) == "HIGH-DETECT")
    test("pfas6=90 → HAZARD",             status_from_pfas6(90) == "HAZARD")
    test("pfas6=None → UNKNOWN",          status_from_pfas6(None) == "UNKNOWN")
    test("HIGH-DETECT → red",             STATUS_COLOR["HIGH-DETECT"] == "red")
    test("NON-DETECT → green",            STATUS_COLOR["NON-DETECT"] == "green")


def test_database():
    print("\n── Database ───────────────────────────────────────────")
    from source_discovery_db import SourceDiscoveryDB

    fake_report = {
        "rtn": "4-0029612",
        "source": "MassDEP Source Discovery",
        "doc_url": "https://example.com/test-report.pdf",
        "doc_type": "Laboratory Results",
        "doc_title": "Lab Analytical Certificate Jan 2026",
        "pdf_path": "/tmp/test.pdf",
        "date_filed": "2026-01-23",
        "date_parsed": "2026-01-24T00:00:00",
        "report_format": "lab_cert",
        "report_date": "01/23/2026",
        "consulting_firm": "TRC",
        "lsp": None,
        "project_address": "2 Fairgrounds Road, Nantucket, MA",
        "sample_location_label": "Fairgrounds Fire Department",
        "groundwater_locations_count": 2,
        "soil_locations_count": 0,
        "max_pfas6_gw": 22.33,
        "max_pfas6_soil": None,
        "worst_status": "HIGH-DETECT",
        "has_exceedance": True,
        "sample_locations": [
            {
                "well_id": "VDT-4FG-4",
                "medium": "groundwater",
                "depth_ft": None,
                "sample_date": "01/15/2026",
                "pfas6": 22.33,
                "compounds": {"PFOS": 9.80, "PFOA": 7.20, "PFNA": 2.15,
                              "PFHxS": 1.80, "PFHpS": 0.88, "PFDA": 0.50},
                "lat": 41.2801, "lng": -70.0623,
                "address": "4 Fairgrounds Road, Nantucket",
                "status": "HIGH-DETECT", "map_color": "red",
                "id_source": "client_id",
            },
            {
                "well_id": "VDT-OSR-2",
                "medium": "groundwater",
                "depth_ft": None,
                "sample_date": "01/15/2026",
                "pfas6": 0.0,
                "compounds": {},
                "lat": 41.2828, "lng": -70.0582,
                "address": "Old South Road, Nantucket",
                "status": "NON-DETECT", "map_color": "green",
                "id_source": "client_id",
            },
        ],
        "summary_text": FIXTURE_LAB_CERT[:500],
    }

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test_sd.json"
        db = SourceDiscoveryDB(db_path)

        test("has_document False before insert",
             not db.has_document("https://example.com/test-report.pdf"))
        db.upsert_report(fake_report)
        db.save()

        db2 = SourceDiscoveryDB(db_path)
        test("report persisted",           len(db2.all_reports()) == 1)
        test("has_document True after",    db2.has_document("https://example.com/test-report.pdf"))

        locs = db2.all_sample_locations()
        test("2 sample locations",         len(locs) == 2)

        exc = db2.exceedances()
        test("1 exceedance (VDT-4FG-4)",   len(exc) == 1)
        test("exceedance pfas6 = 22.33",   exc[0]["pfas6"] == 22.33)

        feats = db2.combined_map_features()
        test("2 map features",             len(feats) == 2)
        test("data_source = source_discovery",
             all(f["data_source"] == "source_discovery" for f in feats))
        test("map feature has popup_html", all("popup_html" in f for f in feats))

        gj = db2.geojson()
        test("GeoJSON FeatureCollection",  gj["type"] == "FeatureCollection")
        test("GeoJSON has 2 features",     len(gj["features"]) == 2)
        test("GeoJSON geometry Point",
             all(f["geometry"]["type"] == "Point" for f in gj["features"]))

        s = db2.summary()
        test("summary total_reports = 1",  s["total_reports"] == 1)
        test("summary exceedances = 1",    s["exceedances"] == 1)
        test("summary max_pfas6 = 22.33",  s["max_pfas6_gw"] == 22.33)


def test_geocoder():
    print("\n── Geocoder ───────────────────────────────────────────")
    from sd_geocoder import resolve_well, _derive_address_from_well_id

    result = resolve_well("VDT-4FG-4")
    test("VDT-4FG-4 in known table",       result.get("geocode_method") == "known_table")
    test("VDT-4FG-4 lat is float",         isinstance(result.get("lat"), float))
    test("VDT-4FG-4 lat near Nantucket",   41.0 < result["lat"] < 42.0)

    test("2FG → 2 Fairgrounds Road",  "2 Fairgrounds" in (_derive_address_from_well_id("VDT-2FG-5") or ""))
    test("4FG → 4 Fairgrounds Road",  "4 Fairgrounds" in (_derive_address_from_well_id("VDT-4FG-4") or ""))
    test("OSR → Old South Road",      "Old South" in (_derive_address_from_well_id("VDT-OSR-1") or ""))
    test("WAITT → Waitt Drive",       "Waitt" in (_derive_address_from_well_id("VDT-WAITT-12") or ""))
    test("TW → Tom's Way",            "Tom" in (_derive_address_from_well_id("VDT-TW-1") or ""))

    fallback = resolve_well("VDT-UNKNOWN-99")
    test("unknown well → centroid fallback", fallback.get("geocode_method") == "centroid_fallback")
    test("centroid lat in Nantucket",        41.0 < fallback["lat"] < 42.0)


def test_doc_type_inference():
    print("\n── Doc type inference ─────────────────────────────────")
    from eea_monitor import _infer_doc_type

    test("sampling plan",   _infer_doc_type("Sampling Plan and SOP") == "Sampling Plan")
    test("field activity",  _infer_doc_type("Field Activity Report December 2024") == "Field Activity Report")
    test("tier class",      _infer_doc_type("Tier Classification Submittal") == "Tier Classification")
    test("notification",    _infer_doc_type("Initial Notification Form") == "Notification")
    test("groundwater",     _infer_doc_type("Groundwater Sampling Results") == "Groundwater Sampling Report")
    test("lab results",     _infer_doc_type("Alpha Analytical Lab Results") == "Laboratory Results")


def test_helpers():
    print("\n── Helpers ────────────────────────────────────────────")
    from source_discovery_parser import (
        _parse_number, _normalise_medium, _worst_status, _normalise_compound_name
    )

    test("parse '22.33'",     _parse_number("22.33") == 22.33)
    test("parse '<ND'",       _parse_number("<ND") == 0.0)
    test("parse 'ND'",        _parse_number("ND") == 0.0)
    test("parse None",        _parse_number(None) is None)
    test("parse '<1.5'",      _parse_number("<1.5") == 1.5)
    test("parse '1.5E-01'",   _parse_number("1.5E-01") == 0.15)

    test("medium: soil text", _normalise_medium("Soil") == "soil")
    test("medium: GW text",   _normalise_medium("Groundwater") == "groundwater")
    test("medium: blank",     _normalise_medium("") == "groundwater")

    test("worst: HAZARD wins",      _worst_status([{"status":"DETECT"},{"status":"HAZARD"}]) == "HAZARD")
    test("worst: HIGH-DETECT wins", _worst_status([{"status":"DETECT"},{"status":"HIGH-DETECT"}]) == "HIGH-DETECT")
    test("worst: NON-DETECT alone", _worst_status([{"status":"NON-DETECT"}]) == "NON-DETECT")

    # Compound normalisation
    test("PFHXS → PFHxS",   _normalise_compound_name("PFHXS") == "PFHxS")
    test("pfos → PFOS",     _normalise_compound_name("pfos") == "PFOS")
    test("HFPO-DA stable",  _normalise_compound_name("HFPO-DA") == "HFPO-DA")


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("ACKuifer — Source Discovery Pipeline Test Suite")
    print("=" * 60)

    for fn in [test_helpers, test_format_detection, test_lab_cert_parser,
               test_field_report_parser, test_database, test_geocoder,
               test_doc_type_inference]:
        try:
            fn()
        except Exception as e:
            import traceback
            print(f"  ✗ EXCEPTION in {fn.__name__}: {e}")
            traceback.print_exc()

    print("\n" + "=" * 60)
    print(f"Results:  {PASS} passed  |  {FAIL} failed  |  {PASS+FAIL} total")
    print("=" * 60)
    sys.exit(0 if FAIL == 0 else 1)
