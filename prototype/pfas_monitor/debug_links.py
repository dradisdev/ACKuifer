from playwright.sync_api import sync_playwright

with sync_playwright() as p:
    browser = p.chromium.launch(headless=False)
    page = browser.new_page()
    
    page.goto("https://portal.laserfiche.com/Portal/Browse.aspx?id=128490&repo=r-ec7bdbfe")
    page.wait_for_load_state('networkidle')
    page.wait_for_timeout(3000)
    
    all_folders = {}
    
    # Try scrolling inside different containers
    scroll_script = '''
    () => {
        // Find elements that might be scrollable
        const candidates = document.querySelectorAll('div, section, main, [class*="list"], [class*="content"], [class*="scroll"], [class*="grid"]');
        for (const el of candidates) {
            if (el.scrollHeight > el.clientHeight + 50) {
                el.scrollTop += 300;
                return el.className || el.tagName;
            }
        }
        return null;
    }
    '''
    
    for scroll_attempt in range(50):
        # Get current links
        browse_links = page.query_selector_all('a[href*="Browse"]')
        
        for link in browse_links:
            try:
                href = link.get_attribute('href') or ''
                name = link.inner_text().strip().split('\n')[0]
                if 'id=' in href and name:
                    import re
                    match = re.search(r'id=(\d+)', href)
                    if match:
                        fid = match.group(1)
                        if fid not in all_folders:
                            all_folders[fid] = name
                            print(f"  [{len(all_folders)}] {name}")
            except:
                pass
        
        # Scroll
        scrolled = page.evaluate(scroll_script)
        if scroll_attempt == 0:
            print(f"\nScrolling in: {scrolled}\n")
        page.wait_for_timeout(300)
    
    print(f"\n{'='*50}")
    print(f"Total unique folders found: {len(all_folders)}")
    print(f"{'='*50}")
    
    input("\nPress Enter to close browser...")
    browser.close()
