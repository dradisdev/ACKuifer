#!/usr/bin/env python3
"""
Look up a Nantucket address and return its Map number.
"""

from playwright.sync_api import sync_playwright
import re

def lookup_map_number(address: str) -> str:
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        
        print(f"\nLooking up: {address}")
        print("\n" + "="*50)
        print("MANUAL STEPS REQUIRED:")
        print("1. Dismiss any popups (Close, Exit, etc.)")
        print("2. The address will be typed automatically")
        print("3. Click on the correct search result")
        print("4. Come back here and press Enter")
        print("="*50 + "\n")
        
        # Go to MapGeo
        page.goto("https://nantucketma.mapgeo.io/datasets/properties")
        page.wait_for_load_state('networkidle')
        
        input("Press Enter after you've dismissed the popups...")
        
        # Find and fill search box
        search_box = page.query_selector('input[type="search"], input[placeholder*="search" i], input')
        if search_box:
            search_box.click()
            search_box.fill(address)
            print(f"Typed: {address}")
        else:
            print("Could not find search box - please type the address manually")
        
        input("Press Enter after you've clicked the correct property...")
        
        # Look for Map number
        content = page.content()
        
        # Try to find ID like "21-80"
        match = re.search(r'>(\d{1,2})-\d+<', content)
        if match:
            map_number = match.group(1)
            print(f"\n✓ Found Map number: {map_number}")
        else:
            print("\nCould not find Map number automatically.")
            map_number = input("What Map number do you see? (e.g., 21): ").strip()
        
        browser.close()
        return map_number


if __name__ == "__main__":
    address = input("Enter a Nantucket address: ")
    result = lookup_map_number(address)
    print(f"\nResult: Map {result}")
