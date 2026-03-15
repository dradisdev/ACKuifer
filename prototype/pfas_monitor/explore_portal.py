#!/usr/bin/env python3
"""
Explore the Laserfiche portal structure to understand how to navigate it.
"""

from playwright.sync_api import sync_playwright
import json
import re

BASE_URL = "https://portal.laserfiche.com"
REPO_ID = "r-ec7bdbfe"
ROOT_FOLDER_ID = "145009"

def explore_portal():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        url = f"{BASE_URL}/Portal/Browse.aspx?id={ROOT_FOLDER_ID}&repo={REPO_ID}"
        print(f"Navigating to: {url}\n")
        
        page.goto(url)
        
        # Wait for page to load
        page.wait_for_load_state('networkidle', timeout=30000)
        page.wait_for_timeout(3000)  # Extra time for JS rendering
        
        # Take a screenshot for reference
        page.screenshot(path="/home/claude/pfas_monitor/portal_screenshot.png")
        print("Screenshot saved to portal_screenshot.png\n")
        
        # Get all links on the page
        print("=== All Links Found ===")
        links = page.query_selector_all('a')
        interesting_links = []
        
        for link in links:
            try:
                href = link.get_attribute('href') or ''
                text = link.inner_text().strip()
                if text and ('Browse' in href or 'DocView' in href):
                    interesting_links.append({'text': text, 'href': href})
                    print(f"  {text[:60]:<60} -> {href}")
            except:
                pass
        
        # Look at network requests that were made
        print("\n=== Page Title ===")
        print(f"  {page.title()}")
        
        # Get HTML structure
        print("\n=== Main Content Area ===")
        content = page.content()
        
        # Save full HTML for analysis
        with open('/home/claude/pfas_monitor/portal_page.html', 'w') as f:
            f.write(content)
        print("Full HTML saved to portal_page.html\n")
        
        # Look for any API calls in the page
        print("=== Looking for API endpoints in page content ===")
        api_patterns = [
            r'api\.laserfiche',
            r'/api/',
            r'GetEntries',
            r'GetFolderChildren',
            r'entryId',
            r'folderId'
        ]
        
        for pattern in api_patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            if matches:
                print(f"  Found pattern '{pattern}': {len(matches)} occurrences")
        
        # Try to find any data attributes
        print("\n=== Elements with data attributes ===")
        data_elements = page.query_selector_all('[data-id], [data-entry-id], [data-folder-id]')
        for el in data_elements[:10]:  # First 10
            attrs = {}
            for attr in ['data-id', 'data-entry-id', 'data-folder-id']:
                val = el.get_attribute(attr)
                if val:
                    attrs[attr] = val
            if attrs:
                print(f"  {attrs}")
        
        browser.close()
        
        return interesting_links

if __name__ == "__main__":
    links = explore_portal()
    print(f"\n=== Summary: Found {len(links)} navigable links ===")
