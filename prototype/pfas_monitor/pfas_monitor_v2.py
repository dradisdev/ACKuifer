#!/usr/bin/env python3
"""
PFAS Report Monitor - Customized for Nantucket Property Files

Monitors Map 21 properties for new PFAS_Sampling documents and parses results.

Usage:
    python pfas_monitor_v2.py --check     # Check for new reports
    python pfas_monitor_v2.py --list      # List all tracked reports
    python pfas_monitor_v2.py --reset     # Clear tracking database
"""

import argparse
import json
import re
from datetime import datetime
from pathlib import Path
from playwright.sync_api import sync_playwright

# Configuration
CONFIG = {
    "base_url": "https://portal.laserfiche.com",
    "repo_id": "r-ec7bdbfe",
    "map_21_folder_id": "128490",
}

DB_FILE = "pfas_reports.json"

# All 18 PFAS compounds tested
PFAS_COMPOUNDS = [
    ('PFOS', 'PERFLUOROOCTANESULFONIC ACID', True),
    ('PFOA', 'PERFLUOROOCTANOIC ACID', True),
    ('PFHxS', 'PERFLUOROHEXANESULFONIC ACID', True),
    ('PFNA', 'PERFLUORONONANOIC ACID', True),
    ('PFHpA', 'PERFLUOROHEPTANOIC ACID', True),
    ('PFDA', 'PERFLUORODECANOIC ACID', True),
    ('PFBS', 'PERFLUOROBUTANESULFONIC ACID', False),
    ('PFDoA', 'PERFLUORODODECANOIC ACID', False),
    ('PFHxA', 'PERFLUOROHEXANOIC ACID', False),
    ('PFTA', 'PERFLUOROTETRADECANOIC ACID', False),
    ('PFTrDA', 'PERFLUOROTRIDECANOIC ACID', False),
    ('PFUnA', 'PERFLUOROUNDECANOIC ACID', False),
    ('NEtFOSAA', 'N-ETHYL PERFLUOROOCTANESULFONAMIDOACETIC ACID', False),
    ('NMeFOSAA', 'N-METHYL PERFLUOROOCTANESULFONAMIDOACETIC ACID', False),
    ('11Cl-PF3OUdS', '11-CHLOROEICOSAFLUORO-3-OXAUNDECANE-1-SULFONIC ACID', False),
    ('9Cl-PF3ONS', '9-CHLOROHEXADECAFLUORO-3-OXANONE-1-SULFONIC ACID', False),
    ('ADONA', '4,8-DIOXA-3H-PERFLUORONONANOIC ACID', False),
    ('HFPO-DA', 'HEXAFLUOROPROPYLENE OXIDE DIMER ACID', False),
]


def browse_url(folder_id: str) -> str:
    return f"{CONFIG['base_url']}/Portal/Browse.aspx?id={folder_id}&repo={CONFIG['repo_id']}"


def doc_url(doc_id: str) -> str:
    return f"{CONFIG['base_url']}/Portal/DocView.aspx?id={doc_id}&repo={CONFIG['repo_id']}"


def extract_all_links_with_scroll(page, max_scrolls: int = 50) -> list[dict]:
    """Scroll through the page to load all virtualized items, then extract links."""
    all_items = {}
    
    scroll_script = '''
    () => {
        const candidates = document.querySelectorAll('div, section, main, [class*="list"], [class*="content"], [class*="scroll"], [class*="grid"]');
        for (const el of candidates) {
            if (el.scrollHeight > el.clientHeight + 50) {
                el.scrollTop += 300;
                return true;
            }
        }
        return false;
    }
    '''
    
    for _ in range(max_scrolls):
        browse_links = page.query_selector_all('a[href*="Browse"]')
        doc_links = page.query_selector_all('a[href*="DocView"]')
        
        for link in browse_links:
            try:
                href = link.get_attribute('href') or ''
                name = link.inner_text().strip().split('\n')[0]
                match = re.search(r'id=(\d+)', href)
                if match and name:
                    fid = match.group(1)
                    if fid not in all_items:
                        all_items[fid] = {'type': 'folder', 'id': fid, 'name': name}
            except:
                pass
        
        for link in doc_links:
            try:
                href = link.get_attribute('href') or ''
                name = link.inner_text().strip().split('\n')[0]
                match = re.search(r'id=(\d+)', href)
                if match and name:
                    did = match.group(1)
                    if did not in all_items:
                        all_items[did] = {'type': 'document', 'id': did, 'name': name}
            except:
                pass
        
        page.evaluate(scroll_script)
        page.wait_for_timeout(300)
    
    return list(all_items.values())


def load_database() -> dict:
    """Load the tracking database."""
    if Path(DB_FILE).exists():
        with open(DB_FILE, 'r') as f:
            return json.load(f)
    return {"reports": {}, "last_checked": None}


def save_database(db: dict):
    """Save the tracking database."""
    db["last_checked"] = datetime.now().isoformat()
    with open(DB_FILE, 'w') as f:
        json.dump(db, f, indent=2)


def navigate_and_wait(page, folder_id: str) -> bool:
    """Navigate to a folder and wait for content to load."""
    try:
        page.goto(browse_url(folder_id), timeout=30000)
        page.wait_for_load_state('networkidle', timeout=15000)
        page.wait_for_timeout(2000)
        return True
    except Exception as e:
        print(f"    Error navigating to {folder_id}: {e}")
        return False


def extract_compound_value(content: str, short_name: str, long_name: str) -> float:
    """Extract a compound value from report content. Returns None if not found, 0 if ND."""
    
    pattern1a = rf'([\d.]+|ND)\s+ng/L\s+[\d.]+\s+[\d.]+\s+\d+{long_name[:10]}[^\n]*-?{short_name}'
    match = re.search(pattern1a, content, re.IGNORECASE)
    if match:
        val = match.group(1)
        return 0 if val == 'ND' else float(val)
    
    pattern1b = rf'([\d.]+|ND)\s*J?\s*ng/L[^\n]*ACID-{short_name}'
    match = re.search(pattern1b, content, re.IGNORECASE)
    if match:
        val = match.group(1)
        return 0 if val == 'ND' else float(val)
    
    pattern1c = rf'([\d.]+|ND)\s+ng/L[^\n]*{short_name}\b'
    match = re.search(pattern1c, content, re.IGNORECASE)
    if match:
        val = match.group(1)
        return 0 if val == 'ND' else float(val)
    
    pattern2 = rf'\({short_name}\)[^\d]*[\d.]+\s+[\d.]+(ND|[\d.]+)'
    match = re.search(pattern2, content, re.IGNORECASE)
    if match:
        val = match.group(1)
        return 0 if val == 'ND' else float(val)
    
    return None


def parse_report(page, doc_id: str) -> dict:
    """Parse a PFAS report and extract all data."""
    
    url = doc_url(doc_id)
    page.goto(url)
    page.wait_for_load_state('networkidle')
    page.wait_for_timeout(2000)
    
    # Click "Plain Text" link
    clicked = False
    selectors_to_try = [
        'text="Plain Text"',
        'text="plain text"',
        'text="Plain text"',
        'a:has-text("Plain Text")',
    ]
    
    for selector in selectors_to_try:
        try:
            el = page.query_selector(selector)
            if el and el.is_visible():
                el.click()
                clicked = True
                break
        except:
            continue
    
    if not clicked:
        links = page.query_selector_all('a, button')
        for link in links:
            try:
                text = link.inner_text().lower()
                if 'plain' in text:
                    link.click()
                    clicked = True
                    break
            except:
                continue
    
    if not clicked:
        return None
    
    page.wait_for_timeout(1500)
    
    # Detect total page count
    total_pages = 1
    body_text = page.inner_text('body')
    page_match = re.search(r'Page\s+\d+\s+of\s+(\d+)', body_text)
    if page_match:
        total_pages = int(page_match.group(1))
    
    # Collect text from all pages
    all_content = body_text
    
    for page_num in range(2, total_pages + 1):
        try:
            next_btn = page.query_selector('[aria-label*="next" i], [title*="next" i]')
            if next_btn and next_btn.is_visible():
                next_btn.click()
                page.wait_for_timeout(1000)
                content = page.inner_text('body')
                all_content += "\n" + content
        except:
            break
    
    content = all_content
    
    # Remove Laserfiche viewer UI garbage
    ui_garbage = [
        'Fit window', 'Fit width', 'Fit height',
        '400%', '200%', '100%', '75%', '50%', '25%',
        'View images', 'Text mode'
    ]
    for garbage in ui_garbage:
        content = content.replace(garbage, '')
    
    results = {
        'pfas6': None,
        'status': None,
        'sample_date': None,
        'sample_address': None,
        'compounds': {},
    }
    
    # Extract PFAS6 value
    pfas6_match = re.search(r'([\d.]+|ND)\s+ng/L[^\n]*PFAS6', content, re.IGNORECASE)
    if pfas6_match:
        val = pfas6_match.group(1)
        results['pfas6'] = 0 if val == 'ND' else float(val)
    else:
        pfas6_match = re.search(r'PFAS6[^=]+=\d+(ND|[\d.]+)', content, re.IGNORECASE)
        if pfas6_match:
            val = pfas6_match.group(1)
            results['pfas6'] = 0 if val == 'ND' else float(val)
    
    # Extract all 18 compounds
    for short_name, long_name, in_pfas6 in PFAS_COMPOUNDS:
        value = extract_compound_value(content, short_name, long_name)
        results['compounds'][short_name] = value
    
    # Determine pass/fail
    content_lower = content.lower()
    if 'does not meet' in content_lower:
        results['status'] = 'FAIL'
    elif 'suitable for drinking' in content_lower:
        results['status'] = 'PASS'
    elif results['pfas6'] == 0:
        results['status'] = 'PASS'
    elif results['pfas6'] is not None:
        results['status'] = 'FAIL' if results['pfas6'] > 20 else 'PASS'
    
    # Address
    addr_match = re.search(r'Collection Address[:\s]+([^,]+,\s*Nantucket,?\s*MA[^\n]*)', content)
    if addr_match:
        addr = addr_match.group(1).strip()
        addr = re.sub(r'\s+[A-Z]{2,3}$', '', addr)
        addr = re.sub(r',?\s*$', '', addr)
        results['sample_address'] = addr
    
    if not results['sample_address']:
        addr_match = re.search(r'(\d+\s+[A-Za-z][^,]+,\s*Nantucket)\s*[A-Z]{2}\d{2}/', content)
        if addr_match:
            results['sample_address'] = addr_match.group(1).strip()
    
    # Sample date
    date_match = re.search(r'Sampled[:\s]*([\d/]+)', content)
    if not date_match:
        date_match = re.search(r'Nantucket\s*[A-Z]{2}(\d{2}/\d{2}/\d{4})', content)
    if date_match:
        results['sample_date'] = date_match.group(1)
    
    return results


def find_pfas_reports(headless: bool = True, parse_reports: bool = True) -> list[dict]:
    """Navigate through Map 21 and find all PFAS_Sampling documents."""
    all_reports = []
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        page = browser.new_page()
        page.set_default_timeout(30000)
        
        # Start at Map 21
        print(f"\nNavigating to Map 21...")
        if not navigate_and_wait(page, CONFIG["map_21_folder_id"]):
            browser.close()
            return []
        
        # Load ALL property folders using scroll
        print("Loading all property folders...")
        all_links = extract_all_links_with_scroll(page)
        property_folders = [l for l in all_links if l['type'] == 'folder']
        print(f"Found {len(property_folders)} property folders")
        
        for i, prop in enumerate(property_folders):
            prop_name = prop['name']
            print(f"\n  [{i+1}/{len(property_folders)}] Checking: {prop_name}")
            
            if not navigate_and_wait(page, prop['id']):
                continue
            
            prop_links = extract_all_links_with_scroll(page, max_scrolls=10)
            well_folders = [l for l in prop_links if l['type'] == 'folder' 
                          and l['name'].lower() == 'well']
            
            if not well_folders:
                print(f"    No Well folder")
                continue
            
            if not navigate_and_wait(page, well_folders[0]['id']):
                continue
            
            well_links = extract_all_links_with_scroll(page, max_scrolls=10)
            reports_folders = [l for l in well_links if l['type'] == 'folder'
                             and 'report' in l['name'].lower()]
            
            if not reports_folders:
                print(f"    No Reports folder")
                continue
            
            if not navigate_and_wait(page, reports_folders[0]['id']):
                continue
            
            reports_links = extract_all_links_with_scroll(page, max_scrolls=10)
            year_folders = [l for l in reports_links if l['type'] == 'folder']
            
            for year_folder in year_folders:
                if not navigate_and_wait(page, year_folder['id']):
                    continue
                
                year_links = extract_all_links_with_scroll(page, max_scrolls=10)
                pfas_docs = [l for l in year_links if l['type'] == 'document'
                           and l['name'].upper().startswith(('PFAS_SAMPLING', 'PFAS_AND_WELL_SAMPLING'))]
                
                for doc in pfas_docs:
                    print(f"    Found: {doc['name']}")
                    
                    report = {
                        'id': doc['id'],
                        'name': doc['name'],
                        'url': doc_url(doc['id']),
                        'property': prop_name,
                        'year': year_folder['name'],
                        'path': f"Map 21 / {prop_name} / Well / Reports / {year_folder['name']}"
                    }
                    
                    # Parse the report to get PFAS data
                    if parse_reports:
                        print(f"      Parsing report...")
                        parsed = parse_report(page, doc['id'])
                        if parsed:
                            report.update(parsed)
                            status = report.get('status', 'Unknown')
                            pfas6 = report.get('pfas6')
                            if pfas6 == 0:
                                pfas6_str = "ND"
                            elif pfas6 is not None:
                                pfas6_str = f"{pfas6} ng/L"
                            else:
                                pfas6_str = "Unknown"
                            print(f"      PFAS6: {pfas6_str} | Status: {status}")
                    
                    all_reports.append(report)
        
        browser.close()
    
    return all_reports


def check_for_new_reports(headless: bool = True, parse_reports: bool = True) -> list[dict]:
    """Check for new PFAS reports and update the database."""
    db = load_database()
    new_reports = []
    
    print("=" * 50)
    print("PFAS Report Monitor - Nantucket Map 21")
    print("=" * 50)
    
    all_reports = find_pfas_reports(headless=headless, parse_reports=parse_reports)
    
    for report in all_reports:
        if report['id'] not in db['reports']:
            report['first_seen'] = datetime.now().isoformat()
            db['reports'][report['id']] = report
            new_reports.append(report)
    
    save_database(db)
    
    print("\n" + "=" * 50)
    if new_reports:
        print(f"FOUND {len(new_reports)} NEW REPORT(S)!")
        print("=" * 50)
        for r in new_reports:
            print(f"\n  {r['name']}")
            print(f"  Property: {r['property']}")
            print(f"  URL: {r['url']}")
            if 'status' in r:
                pfas6 = r.get('pfas6')
                if pfas6 == 0:
                    pfas6_str = "ND (Non-Detect)"
                elif pfas6 is not None:
                    pfas6_str = f"{pfas6} ng/L"
                else:
                    pfas6_str = "Unknown"
                print(f"  PFAS6: {pfas6_str}")
                print(f"  Status: {r['status']}")
    else:
        print("No new reports found.")
        print("=" * 50)
    
    print(f"\nTotal reports tracked: {len(db['reports'])}")
    
    return new_reports


def list_reports():
    """List all tracked reports."""
    db = load_database()
    
    if not db['reports']:
        print("No reports tracked yet. Run with --check first.")
        return
    
    print(f"\nTracked PFAS Reports ({len(db['reports'])} total):\n")
    
    # Group by property
    by_property = {}
    for report in db['reports'].values():
        prop = report.get('property', 'Unknown')
        if prop not in by_property:
            by_property[prop] = []
        by_property[prop].append(report)
    
    for prop in sorted(by_property.keys()):
        print(f"{prop}:")
        for r in sorted(by_property[prop], key=lambda x: x['name']):
            pfas6 = r.get('pfas6')
            if pfas6 == 0:
                pfas6_str = "ND"
            elif pfas6 is not None:
                pfas6_str = f"{pfas6} ng/L"
            else:
                pfas6_str = "?"
            
            status = r.get('status', '?')
            print(f"  - {r['name']} | PFAS6: {pfas6_str} | {status}")
            print(f"    {r['url']}")
        print()


def reset_database():
    """Reset the tracking database."""
    save_database({"reports": {}, "last_checked": None})
    print("Database reset.")


def main():
    parser = argparse.ArgumentParser(description="Monitor Nantucket Map 21 for PFAS reports")
    parser.add_argument('--check', action='store_true', help='Check for new reports')
    parser.add_argument('--list', action='store_true', help='List all tracked reports')
    parser.add_argument('--reset', action='store_true', help='Reset tracking database')
    parser.add_argument('--visible', action='store_true', help='Show browser window')
    parser.add_argument('--no-parse', action='store_true', help='Skip parsing report contents')
    
    args = parser.parse_args()
    
    if not any([args.check, args.list, args.reset]):
        parser.print_help()
        print("\nExamples:")
        print("  python pfas_monitor_v2.py --check           # Check for new reports")
        print("  python pfas_monitor_v2.py --check --visible # Check with visible browser")
        print("  python pfas_monitor_v2.py --list            # List tracked reports")
        return
    
    if args.reset:
        reset_database()
    
    if args.list:
        list_reports()
    
    if args.check:
        check_for_new_reports(headless=not args.visible, parse_reports=not args.no_parse)


if __name__ == "__main__":
    main()
