#!/usr/bin/env python3
"""
Test extracting PFAS data from a report - All 18 compounds.
"""

from playwright.sync_api import sync_playwright
import re

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


def parse_pfas_report(doc_id: str) -> dict:
    """Extract PFAS data from a report."""
    
    url = f"https://portal.laserfiche.com/Portal/DocView.aspx?id={doc_id}&repo=r-ec7bdbfe"
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        
        print(f"Opening report: {url}")
        page.goto(url)
        page.wait_for_load_state('networkidle')
        page.wait_for_timeout(3000)
        
        # Click "Plain Text" link
        print("Looking for Plain Text link...")
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
            input("  Please click Plain Text manually, then press Enter...")
        
        page.wait_for_timeout(1500)
        
        # Try to detect total page count from "Page X of Y" text
        total_pages = 1
        body_text = page.inner_text('body')
        page_match = re.search(r'Page\s+\d+\s+of\s+(\d+)', body_text)
        if page_match:
            total_pages = int(page_match.group(1))
            print(f"  Detected {total_pages} total pages")
        
        # Collect text from all pages
        all_content = body_text
        print(f"  Page 1: {len(body_text)} chars")
        
        for page_num in range(2, total_pages + 1):
            try:
                next_btn = page.query_selector('[aria-label*="next" i], [title*="next" i]')
                if next_btn and next_btn.is_visible():
                    next_btn.click()
                    page.wait_for_timeout(1000)
                    content = page.inner_text('body')
                    all_content += "\n" + content
                    print(f"  Page {page_num}: {len(content)} chars")
                else:
                    break
            except:
                break
        
        print(f"\nTotal: {len(all_content)} chars from {total_pages} page(s)")
        
        results = {
            'doc_id': doc_id,
            'pfas6': None,
            'status': None,
            'sample_date': None,
            'address': None,
            'compounds': {},
        }
        
        content = all_content
        
        # Remove Laserfiche viewer UI garbage
        ui_garbage = [
            'Fit window', 'Fit width', 'Fit height',
            '400%', '200%', '100%', '75%', '50%', '25%',
            'View images', 'Text mode'
        ]
        for garbage in ui_garbage:
            content = content.replace(garbage, '')
        
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
            results['compounds'][short_name] = {
                'value': value,
                'in_pfas6': in_pfas6,
            }
        
        # Determine status: NON-DETECT, DETECT, HIGH-DETECT, or HAZARD
        if results['pfas6'] is None:
            results['status'] = None
        elif results['pfas6'] == 0:
            results['status'] = 'NON-DETECT'
        elif results['pfas6'] <= 20:
            results['status'] = 'DETECT'
        elif results['pfas6'] <= 89.9:
            results['status'] = 'HIGH-DETECT'
        else:
            results['status'] = 'HAZARD'
        
        # Address
        addr_match = re.search(r'Collection Address[:\s]+([^,]+,\s*Nantucket,?\s*MA[^\n]*)', content)
        if addr_match:
            addr = addr_match.group(1).strip()
            addr = re.sub(r'\s+[A-Z]{2,3}$', '', addr)
            addr = re.sub(r',?\s*$', '', addr)
            results['address'] = addr
        
        if not results['address']:
            addr_match = re.search(r'(\d+\s+[A-Za-z][^,]+,\s*Nantucket)\s*[A-Z]{2}\d{2}/', content)
            if addr_match:
                results['address'] = addr_match.group(1).strip()
        
        # Sample date
        date_match = re.search(r'Sampled[:\s]*([\d/]+)', content)
        if not date_match:
            date_match = re.search(r'Nantucket\s*[A-Z]{2}(\d{2}/\d{2}/\d{4})', content)
        if date_match:
            results['sample_date'] = date_match.group(1)
        
        # Display results
        print("\n" + "="*60)
        print("PFAS REPORT RESULTS")
        print("="*60)
        
        pfas6_display = "ND (Non-Detect)" if results['pfas6'] == 0 else results['pfas6']
        print(f"\n  *** PFAS6: {pfas6_display} ng/L (MCL = 20 ng/L) ***")
        print(f"  *** STATUS: {results['status']} ***")
        
        print(f"\n  Address: {results['address']}")
        print(f"  Sample Date: {results['sample_date']}")
        
        print("\n  PFAS6 Compounds (regulated):")
        for short_name, long_name, in_pfas6 in PFAS_COMPOUNDS:
            if in_pfas6:
                val = results['compounds'][short_name]['value']
                if val is None:
                    display = "Not found"
                elif val == 0:
                    display = "ND"
                else:
                    display = f"{val} ng/L"
                print(f"    {short_name}: {display}")
        
        print("\n  Other Compounds:")
        for short_name, long_name, in_pfas6 in PFAS_COMPOUNDS:
            if not in_pfas6:
                val = results['compounds'][short_name]['value']
                if val is None:
                    display = "Not found"
                elif val == 0:
                    display = "ND"
                else:
                    display = f"{val} ng/L"
                print(f"    {short_name}: {display}")
        
        input("\nPress Enter to close browser...")
        browser.close()
        
        return results


if __name__ == "__main__":
    doc_id = input("Enter doc_id to test: ")
    parse_pfas_report(doc_id)
