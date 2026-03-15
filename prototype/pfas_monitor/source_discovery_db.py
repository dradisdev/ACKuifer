"""
source_discovery_db.py
----------------------
Manages the persistent JSON store for MassDEP Source Discovery data.

Schema overview
───────────────
{
  "rtn":           "4-0029612",
  "last_checked":  "<ISO datetime>",
  "reports": [
    {
      "doc_url":      "https://...",
      "doc_type":     "Groundwater Sampling Report",
      "doc_title":    "Field Investigation...",
      "report_date":  "2025-01-16",
      "date_parsed":  "<ISO>",
      "project_address": "2 Fairgrounds Road, Nantucket, MA",
      "consulting_firm": "Verdantas",
      "lsp":          "Jane Smith",
      "worst_status": "HIGH-DETECT",
      "has_exceedance": true,
      "groundwater_locations_count": 12,
      "soil_locations_count": 8,
      "max_pfas6_gw": 22.33,
      "max_pfas6_soil": 1.1,
      "sample_locations": [
        {
          "well_id":     "VDT-4FG-4",
          "medium":      "groundwater",
          "depth_ft":    15,
          "sample_date": "2024-12-16",
          "pfas6":       22.33,
          "compounds": { "PFOS": 14.1, "PFOA": 5.2, ... },
          "lat":         41.2798,
          "lng":        -70.0621,
          "address":     null,
          "status":      "HIGH-DETECT",
          "map_color":   "red"
        }, ...
      ]
    }, ...
  ],
  "failed_downloads": [...],
  "unparsed_docs":    [...]
}

Compatibility with voluntary well programme
───────────────────────────────────────────
The existing prototype stores voluntary-programme data in pfas_reports.json
with a flat per-report structure.  Source Discovery data lives in its own
file (source_discovery.json) so the two can be kept independent and merged
at the map/alert layer.

The method combined_map_features() returns a list of GeoJSON-compatible
feature dicts that can be merged with the voluntary data for display.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Optional, Union

STATUS_COLOR = {
    "NON-DETECT":  "green",
    "DETECT":      "yellow",
    "HIGH-DETECT": "red",
    "HAZARD":      "purple",
    "UNKNOWN":     "gray",
}


class SourceDiscoveryDB:
    def __init__(self, db_path: Union[str, Path]):
        self.path = Path(db_path)
        self._data = self._load()

    # ── Persistence ───────────────────────────────────────────────────────────

    def _load(self) -> dict:
        if self.path.exists():
            try:
                return json.loads(self.path.read_text())
            except json.JSONDecodeError:
                print(f"[DB] Warning: corrupt JSON at {self.path}, starting fresh")
        return {
            "rtn":               "4-0029612",
            "project_name":      "MassDEP Nantucket PFAS Source Discovery",
            "project_address":   "2 Fairgrounds Road, Nantucket, MA",
            "last_checked":      None,
            "reports":           [],
            "failed_downloads":  [],
            "unparsed_docs":     [],
        }

    def save(self):
        self._data["last_checked"] = datetime.now().isoformat()
        self.path.write_text(json.dumps(self._data, indent=2, default=str))
        print(f"[DB] Saved → {self.path}  ({len(self._data['reports'])} reports)")

    # ── Report management ─────────────────────────────────────────────────────

    def has_document(self, url: str) -> bool:
        return any(r["doc_url"] == url for r in self._data["reports"])

    def upsert_report(self, parsed: dict):
        """Add or replace a report (matched by doc_url)."""
        url = parsed.get("doc_url", "")
        existing = [r for r in self._data["reports"] if r.get("doc_url") != url]
        existing.append(parsed)
        # Sort by report_date descending (most recent first)
        existing.sort(key=lambda r: r.get("report_date") or "", reverse=True)
        self._data["reports"] = existing

    def record_download_failure(self, doc_meta: dict):
        self._data["failed_downloads"].append({
            "url":        doc_meta.get("url"),
            "title":      doc_meta.get("title"),
            "failed_at":  datetime.now().isoformat(),
        })

    def record_unparsed(self, doc_meta: dict, pdf_path: str):
        self._data["unparsed_docs"].append({
            "url":       doc_meta.get("url"),
            "title":     doc_meta.get("title"),
            "pdf_path":  pdf_path,
            "noted_at":  datetime.now().isoformat(),
        })

    # ── Query helpers ─────────────────────────────────────────────────────────

    def all_reports(self) -> list[dict]:
        return self._data["reports"]

    def all_sample_locations(self) -> list[dict]:
        """
        Flatten all sample locations across all reports.
        Each location is augmented with its parent report's metadata.
        """
        locs = []
        for report in self._data["reports"]:
            for loc in report.get("sample_locations", []):
                enriched = {**loc}
                enriched["report_date"]    = report.get("report_date")
                enriched["doc_title"]      = report.get("doc_title")
                enriched["doc_url"]        = report.get("doc_url")
                enriched["consulting_firm"] = report.get("consulting_firm")
                enriched["rtn"]            = report.get("rtn", "4-0029612")
                enriched["data_source"]    = "source_discovery"
                locs.append(enriched)
        return locs

    def locations_with_coordinates(self) -> list[dict]:
        return [l for l in self.all_sample_locations()
                if l.get("lat") and l.get("lng")]

    def exceedances(self) -> list[dict]:
        """Return all groundwater locations exceeding the 20 ng/L MCL."""
        return [
            l for l in self.all_sample_locations()
            if l.get("medium") == "groundwater"
            and l.get("pfas6") is not None
            and l["pfas6"] > 20
        ]

    # ── Map output ────────────────────────────────────────────────────────────

    def combined_map_features(self) -> list[dict]:
        """
        Return a list of map-ready feature dicts for all located sample points.

        These are designed to be merged with the voluntary-programme features
        returned by the existing prototype's address_lookup / pfas_reports.json
        pipeline.  Both datasets use the same status/color convention.

        Each feature:
        {
          "id":          "sd-VDT-4FG-4",
          "lat":         41.2798,
          "lng":        -70.0621,
          "address":     null,
          "well_id":     "VDT-4FG-4",
          "pfas6":       22.33,
          "status":      "HIGH-DETECT",
          "map_color":   "red",
          "medium":      "groundwater",
          "report_date": "2025-01-16",
          "data_source": "source_discovery",   ← key discriminator
          "popup_html":  "..."
        }
        """
        features = []
        for loc in self.locations_with_coordinates():
            feat = {
                "id":           f"sd-{loc['well_id']}",
                "lat":          loc["lat"],
                "lng":          loc["lng"],
                "address":      loc.get("address"),
                "well_id":      loc["well_id"],
                "pfas6":        loc.get("pfas6"),
                "status":       loc.get("status", "UNKNOWN"),
                "map_color":    loc.get("map_color", "gray"),
                "medium":       loc.get("medium", "groundwater"),
                "report_date":  loc.get("report_date"),
                "data_source":  "source_discovery",
                "rtn":          loc.get("rtn", "4-0029612"),
                "doc_url":      loc.get("doc_url"),
                "popup_html":   _make_popup(loc),
            }
            features.append(feat)
        return features

    def geojson(self) -> dict:
        """Export all located points as a GeoJSON FeatureCollection."""
        features = []
        for f in self.combined_map_features():
            features.append({
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [f["lng"], f["lat"]],
                },
                "properties": {k: v for k, v in f.items()
                               if k not in ("lat", "lng", "popup_html")},
            })
        return {"type": "FeatureCollection", "features": features}

    def summary(self) -> dict:
        """Quick dashboard summary."""
        all_locs = self.all_sample_locations()
        gw = [l for l in all_locs if l.get("medium") == "groundwater"]
        soil = [l for l in all_locs if l.get("medium") == "soil"]
        return {
            "rtn":            self._data["rtn"],
            "project_name":   self._data["project_name"],
            "last_checked":   self._data["last_checked"],
            "total_reports":  len(self._data["reports"]),
            "total_gw_locs":  len(gw),
            "total_soil_locs": len(soil),
            "max_pfas6_gw":   max((l["pfas6"] for l in gw if l.get("pfas6") is not None), default=None),
            "exceedances":    len(self.exceedances()),
            "worst_status":   _overall_worst([l.get("status", "UNKNOWN") for l in gw]),
            "located_points": len(self.locations_with_coordinates()),
        }


# ── Standalone merge utility ──────────────────────────────────────────────────

def merge_with_voluntary(
    source_discovery_db_path: Union[str, Path],
    voluntary_db_path: Union[str, Path],
) -> list[dict]:
    """
    Merge Source Discovery map features with voluntary-programme features
    into a single list for the combined ACKuifer map.

    The voluntary features are expected to have:
      lat, lng, address, pfas6, status, map_color, data_source="voluntary"

    Both sets are returned together; callers can filter by data_source.
    """
    sd_db = SourceDiscoveryDB(source_discovery_db_path)
    sd_features = sd_db.combined_map_features()

    vol_features = []
    vol_path = Path(voluntary_db_path)
    if vol_path.exists():
        vol_data = json.loads(vol_path.read_text())
        reports = vol_data if isinstance(vol_data, list) else vol_data.get("reports", [])
        for r in reports:
            if not r.get("lat") or not r.get("lng"):
                continue
            vol_features.append({
                "id":          f"vol-{r.get('map_number', '')}-{r.get('folder', '')}",
                "lat":         r["lat"],
                "lng":         r["lng"],
                "address":     r.get("address"),
                "pfas6":       r.get("pfas6"),
                "status":      r.get("status", "UNKNOWN"),
                "map_color":   r.get("map_color", "gray"),
                "medium":      "groundwater",
                "report_date": r.get("sample_date"),
                "data_source": "voluntary",
                "popup_html":  _make_voluntary_popup(r),
            })

    return sd_features + vol_features


# ── Popup HTML generators ─────────────────────────────────────────────────────

def _make_popup(loc: dict) -> str:
    pfas6_str = f"{loc['pfas6']:.2f} ng/L" if loc.get("pfas6") is not None else "N/A"
    status = loc.get("status", "UNKNOWN")
    color  = loc.get("map_color", "gray")
    medium = loc.get("medium", "").capitalize()
    wid    = loc.get("well_id", "")
    date   = loc.get("report_date") or loc.get("sample_date") or ""
    source = loc.get("data_source", "source_discovery")
    label  = "MassDEP Source Discovery" if source == "source_discovery" else "Voluntary Well Testing"

    return f"""
<div class="ack-popup sd-popup">
  <div class="popup-badge" style="background:{color};color:#fff;padding:4px 8px;border-radius:4px;font-weight:bold">
    {status}
  </div>
  <h4 style="margin:8px 0 4px">{wid}</h4>
  <table style="font-size:13px;border-collapse:collapse">
    <tr><td><b>Program</b></td><td>{label}</td></tr>
    <tr><td><b>Medium</b></td><td>{medium}</td></tr>
    <tr><td><b>PFAS6</b></td><td>{pfas6_str}</td></tr>
    <tr><td><b>Date</b></td><td>{date}</td></tr>
    <tr><td><b>RTN</b></td><td>4-0029612</td></tr>
  </table>
</div>"""


def _make_voluntary_popup(r: dict) -> str:
    pfas6 = r.get("pfas6")
    pfas6_str = f"{pfas6:.2f} ng/L" if pfas6 is not None else "N/A"
    status = r.get("status", "UNKNOWN")
    color  = r.get("map_color", "gray")
    addr   = r.get("address", "Unknown")
    date   = r.get("sample_date", "")
    return f"""
<div class="ack-popup vol-popup">
  <div class="popup-badge" style="background:{color};color:#fff;padding:4px 8px;border-radius:4px;font-weight:bold">
    {status}
  </div>
  <h4 style="margin:8px 0 4px">{addr}</h4>
  <table style="font-size:13px;border-collapse:collapse">
    <tr><td><b>Program</b></td><td>Voluntary Well Testing</td></tr>
    <tr><td><b>PFAS6</b></td><td>{pfas6_str}</td></tr>
    <tr><td><b>Date</b></td><td>{date}</td></tr>
  </table>
</div>"""


def _overall_worst(statuses: list) -> str:
    order = ["NON-DETECT", "DETECT", "HIGH-DETECT", "HAZARD", "UNKNOWN"]
    for s in reversed(order):
        if s in statuses:
            return s
    return "UNKNOWN"


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    db_path = sys.argv[1] if len(sys.argv) > 1 else "source_discovery.json"
    db = SourceDiscoveryDB(db_path)
    print(json.dumps(db.summary(), indent=2, default=str))
    print(f"\nGeoJSON features: {len(db.combined_map_features())}")
