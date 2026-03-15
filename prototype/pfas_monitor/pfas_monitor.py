#!/usr/bin/env python3
"""
PFAS Report Monitor for Laserfiche Public Portal

This tool monitors a Laserfiche public portal for new PFAS-related reports
in property files, particularly in the Wells subfolder.

Usage:
    python pfas_monitor.py --check          # Check for new reports
    python pfas_monitor.py --list           # List all known reports
    python pfas_monitor.py --reset          # Clear the tracking database
    python pfas_monitor.py --map 21         # Check specific map (default: 21)
"""

import argparse
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass, asdict
from typing import Optional

# Try to import playwright - will provide installation instructions if missing
try:
    from playwright.sync_api import sync_playwright, Page, Browser, TimeoutError as PlaywrightTimeout
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False


@dataclass
class Report:
    """Represents a discovered report."""
    id: str
    name: str
    url: str
    path: str
    first_seen: str
    map_number: str
    folder: str


class ReportDatabase:
    """Simple JSON-based database for tracking seen reports."""
    
    def __init__(self, db_path: str = "pfas_reports.json"):
        self.db_path = Path(db_path)
        self.reports: dict[str, Report] = {}
        self.load()
    
    def load(self):
        """Load the database from disk."""
        if self.db_path.exists():
            with open(self.db_path, 'r') as f:
                data = json.load(f)
                self.reports = {
                    k: Report(**v) for k, v in data.get('reports', {}).items()
                }
    
    def save(self):
        """Save the database to disk."""
        data = {
            'reports': {k: asdict(v) for k, v in self.reports.items()},
            'last_updated': datetime.now().isoformat()
        }
        with open(self.db_path, 'w') as f:
            json.dump(data, f, indent=2)
    
    def add_report(self, report: Report) -> bool:
        """Add a report. Returns True if it's new."""
        if report.id in self.reports:
            return False
        self.reports[report.id] = report
        self.save()
        return True
    
    def get_all_reports(self) -> list[Report]:
        """Get all tracked reports."""
        return list(self.reports.values())
    
    def reset(self):
        """Clear all tracked reports."""
        self.reports = {}
        self.save()


class LaserfichePortalNavigator:
    """Navigate the Laserfiche public portal to find reports."""
    
    BASE_URL = "https://portal.laserfiche.com"
    REPO_ID = "r-ec7bdbfe"
    ROOT_FOLDER_ID = "145009"
    
    def __init__(self, headless: bool = True, timeout: int = 30000):
        self.headless = headless
        self.timeout = timeout
        self.browser: Optional[Browser] = None
        self.page: Optional[Page] = None
    
    def __enter__(self):
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(headless=self.headless)
        self.page = self.browser.new_page()
        self.page.set_default_timeout(self.timeout)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.browser:
            self.browser.close()
        if self.playwright:
            self.playwright.stop()
    
    def browse_url(self, folder_id: str) -> str:
        """Generate a browse URL for a folder."""
        return f"{self.BASE_URL}/Portal/Browse.aspx?id={folder_id}&repo={self.REPO_ID}"
    
    def doc_url(self, doc_id: str) -> str:
        """Generate a document URL."""
        return f"{self.BASE_URL}/Portal/DocView.aspx?id={doc_id}&repo={self.REPO_ID}"
    
    def navigate_to_folder(self, folder_id: str) -> bool:
        """Navigate to a specific folder and wait for it to load."""
        url = self.browse_url(folder_id)
        print(f"  Navigating to: {url}")
        self.page.goto(url)
        
        # Wait for the content to load (the "Loading..." should disappear)
        try:
            # Wait for folder listing to appear
            self.page.wait_for_selector('[class*="entry-list"], [class*="folder"], [class*="document"]', 
                                       timeout=self.timeout)
            # Give additional time for dynamic content
            self.page.wait_for_timeout(2000)
            return True
        except PlaywrightTimeout:
            print(f"  Warning: Timeout waiting for folder content to load")
            return False
    
    def get_folder_contents(self) -> list[dict]:
        """Get the contents of the current folder view."""
        entries = []
        
        # Try multiple selector strategies as Laserfiche portal structure may vary
        selectors = [
            '[class*="entry-row"]',
            '[class*="document-row"]',
            '[class*="folder-row"]',
            'tr[data-id]',
            '[data-entry-id]',
            '.lf-entry',
            '[class*="EntryListItem"]'
        ]
        
        for selector in selectors:
            try:
                elements = self.page.query_selector_all(selector)
                if elements:
                    for el in elements:
                        entry = self._parse_entry_element(el)
                        if entry:
                            entries.append(entry)
                    break
            except Exception as e:
                continue
        
        return entries
    
    def _parse_entry_element(self, element) -> Optional[dict]:
        """Parse an entry element to extract its data."""
        try:
            # Try to get entry ID from various possible attributes
            entry_id = (
                element.get_attribute('data-id') or 
                element.get_attribute('data-entry-id') or
                element.get_attribute('id')
            )
            
            # Get the entry name/title
            name = element.inner_text().strip().split('\n')[0]
            
            # Try to determine if it's a folder or document
            class_name = element.get_attribute('class') or ''
            is_folder = 'folder' in class_name.lower()
            
            if entry_id and name:
                return {
                    'id': entry_id,
                    'name': name,
                    'is_folder': is_folder
                }
        except Exception:
            pass
        return None
    
    def find_wells_folders(self, map_number: str = "21") -> list[dict]:
        """
        Find folders related to Wells for a specific map.
        
        This navigates through the portal structure to find:
        - Map {number} folder
        - Within that, the Wells subfolder
        """
        wells_reports = []
        
        # Start at root
        print(f"\nSearching for Map {map_number} Wells reports...")
        
        if not self.navigate_to_folder(self.ROOT_FOLDER_ID):
            print("Failed to load root folder")
            return []
        
        # Get page content for analysis
        content = self.page.content()
        
        # Look for links that might contain map references
        links = self.page.query_selector_all('a[href*="Browse"], a[href*="DocView"]')
        
        for link in links:
            try:
                href = link.get_attribute('href') or ''
                text = link.inner_text().strip()
                
                # Check if this links to our target map
                if f"Map {map_number}" in text or f"map{map_number}" in text.lower().replace(' ', ''):
                    # Extract the folder ID from the href
                    id_match = re.search(r'id=(\d+)', href)
                    if id_match:
                        folder_id = id_match.group(1)
                        print(f"  Found Map {map_number} folder: {text} (ID: {folder_id})")
                        
                        # Navigate into this folder to find Wells
                        wells_reports.extend(
                            self._search_folder_for_wells_reports(folder_id, map_number, text)
                        )
            except Exception as e:
                continue
        
        return wells_reports
    
    def _search_folder_for_wells_reports(self, folder_id: str, map_number: str, 
                                         parent_path: str) -> list[dict]:
        """Search a folder for Wells-related reports."""
        reports = []
        
        if not self.navigate_to_folder(folder_id):
            return reports
        
        links = self.page.query_selector_all('a[href*="Browse"], a[href*="DocView"]')
        
        for link in links:
            try:
                href = link.get_attribute('href') or ''
                text = link.inner_text().strip()
                
                # Check if this is a Wells folder or PFAS-related document
                is_wells = 'well' in text.lower()
                is_pfas = 'pfas' in text.lower()
                is_document = 'DocView' in href
                
                id_match = re.search(r'id=(\d+)', href)
                if not id_match:
                    continue
                    
                entry_id = id_match.group(1)
                
                if is_wells and not is_document:
                    # This is a Wells folder - search recursively
                    print(f"  Found Wells folder: {text}")
                    reports.extend(
                        self._search_folder_for_pfas_reports(
                            entry_id, map_number, f"{parent_path}/Wells"
                        )
                    )
                elif is_document and (is_pfas or is_wells):
                    # This is a PFAS or Wells-related document
                    reports.append({
                        'id': entry_id,
                        'name': text,
                        'url': self.doc_url(entry_id),
                        'path': parent_path,
                        'map_number': map_number,
                        'folder': 'Wells' if is_wells else parent_path
                    })
            except Exception:
                continue
        
        return reports
    
    def _search_folder_for_pfas_reports(self, folder_id: str, map_number: str,
                                        current_path: str) -> list[dict]:
        """Search a folder for PFAS-related documents."""
        reports = []
        
        if not self.navigate_to_folder(folder_id):
            return reports
        
        links = self.page.query_selector_all('a[href*="DocView"], a[href*="Browse"]')
        
        for link in links:
            try:
                href = link.get_attribute('href') or ''
                text = link.inner_text().strip()
                
                id_match = re.search(r'id=(\d+)', href)
                if not id_match:
                    continue
                
                entry_id = id_match.group(1)
                is_document = 'DocView' in href
                
                if is_document:
                    # Include all documents in the Wells folder
                    # You might want to filter more specifically
                    reports.append({
                        'id': entry_id,
                        'name': text,
                        'url': self.doc_url(entry_id),
                        'path': current_path,
                        'map_number': map_number,
                        'folder': 'Wells'
                    })
                else:
                    # It's a subfolder - optionally recurse
                    # Be careful not to recurse too deep
                    if current_path.count('/') < 5:
                        reports.extend(
                            self._search_folder_for_pfas_reports(
                                entry_id, map_number, f"{current_path}/{text}"
                            )
                        )
            except Exception:
                continue
        
        return reports
    
    def get_page_screenshot(self, path: str = "debug_screenshot.png"):
        """Take a screenshot for debugging."""
        self.page.screenshot(path=path)
        print(f"Screenshot saved to: {path}")
    
    def get_page_structure(self) -> str:
        """Get a simplified view of the page structure for debugging."""
        return self.page.content()


def check_for_new_reports(map_number: str = "21", headless: bool = True, 
                          db_path: str = "pfas_reports.json") -> list[Report]:
    """
    Check the portal for new PFAS reports.
    
    Returns a list of newly discovered reports.
    """
    if not PLAYWRIGHT_AVAILABLE:
        print("ERROR: Playwright is not installed.")
        print("Please install it with:")
        print("  pip install playwright")
        print("  playwright install chromium")
        sys.exit(1)
    
    db = ReportDatabase(db_path)
    new_reports = []
    
    print(f"Checking for new PFAS reports for Map {map_number}...")
    print(f"Tracking database: {db_path}")
    
    try:
        with LaserfichePortalNavigator(headless=headless) as navigator:
            found_reports = navigator.find_wells_folders(map_number)
            
            print(f"\nFound {len(found_reports)} reports in portal")
            
            for report_data in found_reports:
                report = Report(
                    id=report_data['id'],
                    name=report_data['name'],
                    url=report_data['url'],
                    path=report_data['path'],
                    first_seen=datetime.now().isoformat(),
                    map_number=report_data['map_number'],
                    folder=report_data['folder']
                )
                
                if db.add_report(report):
                    new_reports.append(report)
                    print(f"  NEW: {report.name}")
                    print(f"       URL: {report.url}")
    
    except Exception as e:
        print(f"Error during portal navigation: {e}")
        raise
    
    return new_reports


def list_all_reports(db_path: str = "pfas_reports.json"):
    """List all tracked reports."""
    db = ReportDatabase(db_path)
    reports = db.get_all_reports()
    
    if not reports:
        print("No reports tracked yet. Run with --check to scan for reports.")
        return
    
    print(f"\nTracked Reports ({len(reports)} total):\n")
    
    # Group by map
    by_map = {}
    for report in reports:
        map_num = report.map_number
        if map_num not in by_map:
            by_map[map_num] = []
        by_map[map_num].append(report)
    
    for map_num in sorted(by_map.keys()):
        print(f"Map {map_num}:")
        for report in by_map[map_num]:
            print(f"  - {report.name}")
            print(f"    Path: {report.path}")
            print(f"    URL: {report.url}")
            print(f"    First seen: {report.first_seen}")
        print()


def reset_database(db_path: str = "pfas_reports.json"):
    """Reset the tracking database."""
    db = ReportDatabase(db_path)
    db.reset()
    print(f"Database reset: {db_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Monitor Laserfiche portal for new PFAS well water reports"
    )
    
    parser.add_argument('--check', action='store_true',
                       help='Check for new reports')
    parser.add_argument('--list', action='store_true',
                       help='List all tracked reports')
    parser.add_argument('--reset', action='store_true',
                       help='Reset the tracking database')
    parser.add_argument('--map', type=str, default='21',
                       help='Map number to check (default: 21)')
    parser.add_argument('--db', type=str, default='pfas_reports.json',
                       help='Path to tracking database')
    parser.add_argument('--visible', action='store_true',
                       help='Run browser in visible mode (for debugging)')
    parser.add_argument('--debug', action='store_true',
                       help='Enable debug mode with screenshots')
    
    args = parser.parse_args()
    
    if not any([args.check, args.list, args.reset]):
        parser.print_help()
        print("\nExample usage:")
        print("  python pfas_monitor.py --check          # Check for new reports")
        print("  python pfas_monitor.py --check --map 22 # Check Map 22")
        print("  python pfas_monitor.py --list           # List tracked reports")
        return
    
    if args.reset:
        reset_database(args.db)
    
    if args.list:
        list_all_reports(args.db)
    
    if args.check:
        new_reports = check_for_new_reports(
            map_number=args.map,
            headless=not args.visible,
            db_path=args.db
        )
        
        if new_reports:
            print(f"\n{'='*50}")
            print(f"FOUND {len(new_reports)} NEW REPORT(S)!")
            print(f"{'='*50}")
            for report in new_reports:
                print(f"\n  {report.name}")
                print(f"  URL: {report.url}")
        else:
            print("\nNo new reports found.")


if __name__ == "__main__":
    main()
