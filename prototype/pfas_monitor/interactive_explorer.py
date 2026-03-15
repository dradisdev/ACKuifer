#!/usr/bin/env python3
"""
Interactive Portal Explorer

Run this first to understand the structure of your Laserfiche portal
and verify the navigation paths work correctly.

Usage:
    python interactive_explorer.py
"""

from playwright.sync_api import sync_playwright
import re
import json
from pathlib import Path

# Portal configuration - customize these for your portal
CONFIG = {
    "base_url": "https://portal.laserfiche.com",
    "repo_id": "r-ec7bdbfe",
    "root_folder_id": "145009",
    "target_map": "21",  # The map number you're interested in
}

def browse_url(folder_id: str) -> str:
    return f"{CONFIG['base_url']}/Portal/Browse.aspx?id={folder_id}&repo={CONFIG['repo_id']}"

def doc_url(doc_id: str) -> str:
    return f"{CONFIG['base_url']}/Portal/DocView.aspx?id={doc_id}&repo={CONFIG['repo_id']}"

def extract_links(page) -> list[dict]:
    """Extract all navigable links from the current page."""
    links = []
    elements = page.query_selector_all('a')
    
    for el in elements:
        try:
            href = el.get_attribute('href') or ''
            text = el.inner_text().strip()
            
            if not text or len(text) > 200:
                continue
                
            # Parse the link type and ID
            if 'Browse' in href:
                id_match = re.search(r'id=(\d+)', href)
                if id_match:
                    links.append({
                        'type': 'folder',
                        'id': id_match.group(1),
                        'name': text.split('\n')[0].strip(),
                        'href': href
                    })
            elif 'DocView' in href:
                id_match = re.search(r'id=(\d+)', href)
                if id_match:
                    links.append({
                        'type': 'document',
                        'id': id_match.group(1),
                        'name': text.split('\n')[0].strip(),
                        'href': href
                    })
        except:
            pass
    
    # Remove duplicates
    seen = set()
    unique_links = []
    for link in links:
        key = (link['type'], link['id'])
        if key not in seen:
            seen.add(key)
            unique_links.append(link)
    
    return unique_links

def interactive_explore():
    """Interactive exploration of the portal."""
    
    print("=" * 60)
    print("PFAS Report Portal Explorer")
    print("=" * 60)
    print(f"\nPortal: {CONFIG['base_url']}")
    print(f"Repository: {CONFIG['repo_id']}")
    print(f"Starting folder: {CONFIG['root_folder_id']}")
    print("\nStarting browser...\n")
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)  # Visible browser
        context = browser.new_context()
        page = context.new_page()
        
        current_folder = CONFIG['root_folder_id']
        navigation_history = []
        found_items = {'folders': [], 'documents': []}
        
        while True:
            url = browse_url(current_folder)
            print(f"\n{'='*60}")
            print(f"Navigating to: {url}")
            print("="*60)
            
            try:
                page.goto(url, timeout=30000)
                page.wait_for_load_state('networkidle', timeout=15000)
                page.wait_for_timeout(2000)  # Extra time for JS
            except Exception as e:
                print(f"Error loading page: {e}")
                continue
            
            # Get links
            links = extract_links(page)
            
            folders = [l for l in links if l['type'] == 'folder']
            documents = [l for l in links if l['type'] == 'document']
            
            # Display folders
            print(f"\nFOLDERS ({len(folders)}):")
            for i, folder in enumerate(folders):
                marker = " *" if CONFIG['target_map'] in folder['name'].lower() or 'map' in folder['name'].lower() else ""
                print(f"  [{i+1}] {folder['name']}{marker}")
            
            # Display documents
            print(f"\nDOCUMENTS ({len(documents)}):")
            for i, doc in enumerate(documents):
                marker = " *" if 'pfas' in doc['name'].lower() or 'well' in doc['name'].lower() else ""
                print(f"  [d{i+1}] {doc['name']}{marker}")
            
            # Show menu
            print("\n" + "-"*40)
            print("Commands:")
            print("  [number]  - Navigate to folder")
            print("  b         - Go back")
            print("  s         - Save found structure to file")
            print("  m         - Auto-find Map folders")
            print("  q         - Quit")
            print("-"*40)
            
            choice = input("\nEnter choice: ").strip().lower()
            
            if choice == 'q':
                break
            elif choice == 'b':
                if navigation_history:
                    current_folder = navigation_history.pop()
                else:
                    print("Already at root!")
            elif choice == 's':
                save_structure(found_items)
            elif choice == 'm':
                # Auto-find map folders
                map_folders = [f for f in folders if 'map' in f['name'].lower()]
                if map_folders:
                    print(f"\nFound {len(map_folders)} Map folders:")
                    for f in map_folders:
                        print(f"  - {f['name']} (ID: {f['id']})")
                        found_items['folders'].append(f)
                else:
                    print("No Map folders found at this level")
            elif choice.isdigit():
                idx = int(choice) - 1
                if 0 <= idx < len(folders):
                    navigation_history.append(current_folder)
                    current_folder = folders[idx]['id']
                    found_items['folders'].append(folders[idx])
                else:
                    print("Invalid folder number")
            elif choice.startswith('d') and choice[1:].isdigit():
                idx = int(choice[1:]) - 1
                if 0 <= idx < len(documents):
                    doc = documents[idx]
                    print(f"\nDocument: {doc['name']}")
                    print(f"URL: {doc_url(doc['id'])}")
                    found_items['documents'].append(doc)
                else:
                    print("Invalid document number")
        
        browser.close()
    
    return found_items

def save_structure(items: dict):
    """Save the discovered structure to a JSON file."""
    output = {
        'config': CONFIG,
        'discovered': items
    }
    
    filename = 'portal_structure.json'
    with open(filename, 'w') as f:
        json.dump(output, f, indent=2)
    print(f"\nStructure saved to {filename}")

def auto_discover():
    """
    Automatic discovery mode - finds all Wells folders under Map folders.
    """
    print("=" * 60)
    print("AUTO DISCOVERY MODE")
    print("=" * 60)
    print("\nThis will automatically navigate the portal to find:")
    print(f"  - All 'Map' folders")
    print(f"  - 'Wells' subfolders within them")
    print(f"  - Documents related to PFAS/well water")
    print("\nStarting...\n")
    
    all_reports = []
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.set_default_timeout(30000)
        
        # Start at root
        url = browse_url(CONFIG['root_folder_id'])
        print(f"Loading root folder: {url}")
        
        try:
            page.goto(url)
            page.wait_for_load_state('networkidle', timeout=15000)
            page.wait_for_timeout(2000)
        except Exception as e:
            print(f"Failed to load root: {e}")
            browser.close()
            return []
        
        # Find Map folders
        links = extract_links(page)
        map_folders = [l for l in links if l['type'] == 'folder' and 
                      ('map' in l['name'].lower() or re.search(r'map\s*\d+', l['name'], re.I))]
        
        print(f"\nFound {len(map_folders)} Map folder(s)")
        
        for map_folder in map_folders:
            print(f"\n--- Exploring: {map_folder['name']} ---")
            
            # Navigate to map folder
            try:
                page.goto(browse_url(map_folder['id']))
                page.wait_for_load_state('networkidle', timeout=15000)
                page.wait_for_timeout(1500)
            except:
                continue
            
            # Find Wells folder
            map_links = extract_links(page)
            wells_folders = [l for l in map_links if l['type'] == 'folder' and 
                           'well' in l['name'].lower()]
            
            for wells_folder in wells_folders:
                print(f"  Found Wells folder: {wells_folder['name']}")
                
                # Navigate to Wells folder
                try:
                    page.goto(browse_url(wells_folder['id']))
                    page.wait_for_load_state('networkidle', timeout=15000)
                    page.wait_for_timeout(1500)
                except:
                    continue
                
                # Get documents
                wells_links = extract_links(page)
                documents = [l for l in wells_links if l['type'] == 'document']
                
                for doc in documents:
                    report = {
                        'id': doc['id'],
                        'name': doc['name'],
                        'url': doc_url(doc['id']),
                        'map': map_folder['name'],
                        'folder': wells_folder['name'],
                        'path': f"{map_folder['name']}/{wells_folder['name']}"
                    }
                    all_reports.append(report)
                    print(f"    Document: {doc['name']}")
        
        browser.close()
    
    # Save results
    if all_reports:
        with open('discovered_reports.json', 'w') as f:
            json.dump(all_reports, f, indent=2)
        print(f"\n{'='*60}")
        print(f"Discovered {len(all_reports)} report(s)")
        print(f"Results saved to: discovered_reports.json")
        print(f"{'='*60}")
    else:
        print("\nNo reports found. Try running interactive mode to explore manually.")
    
    return all_reports

if __name__ == "__main__":
    print("\nPFAS Portal Explorer")
    print("1. Interactive mode (explore manually)")
    print("2. Auto-discovery (find all Wells reports)")
    
    choice = input("\nSelect mode [1/2]: ").strip()
    
    if choice == '2':
        auto_discover()
    else:
        interactive_explore()
