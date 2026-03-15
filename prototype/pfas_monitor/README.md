# PFAS Report Monitor for Laserfiche Public Portal

A Python tool to monitor a Laserfiche public portal for new PFAS-related well water reports.

## Overview

This tool helps you:
- **Monitor** the portal for new reports related to PFAS contamination in well water
- **Track** which reports you've already seen
- **Alert** you when new reports are posted
- **Search** specific map folders (e.g., Map 21)

## Prerequisites

- Python 3.9 or higher
- Google Chrome or Chromium browser

## Installation

### 1. Clone or download this folder

### 2. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 3. Install Playwright browsers

```bash
playwright install chromium
```

This downloads a compatible version of Chromium for browser automation.

## Usage

### Check for new reports (primary use case)

```bash
# Check Map 21 (default)
python pfas_monitor.py --check

# Check a different map
python pfas_monitor.py --check --map 22

# Run with visible browser (for debugging)
python pfas_monitor.py --check --visible
```

### List all tracked reports

```bash
python pfas_monitor.py --list
```

### Reset tracking database

```bash
python pfas_monitor.py --reset
```

### Example workflow

```bash
# First run - discovers all existing reports
$ python pfas_monitor.py --check
Checking for new PFAS reports for Map 21...
Tracking database: pfas_reports.json
Found 5 reports in portal
  NEW: PFAS Well Water Testing Results - Q1 2024
       URL: https://portal.laserfiche.com/Portal/DocView.aspx?id=...
  NEW: PFAS Remediation Status Report
       URL: ...

# Later runs - only shows new reports
$ python pfas_monitor.py --check
Checking for new PFAS reports for Map 21...
No new reports found.
```

## Configuration

### Portal Configuration

The tool is pre-configured for your portal:
- **Base URL**: `https://portal.laserfiche.com`
- **Repository ID**: `r-ec7bdbfe`
- **Root Folder ID**: `145009`

To change these, edit the constants at the top of `pfas_monitor.py`:

```python
class LaserfichePortalNavigator:
    BASE_URL = "https://portal.laserfiche.com"
    REPO_ID = "r-ec7bdbfe"
    ROOT_FOLDER_ID = "145009"
```

### Search Paths

The tool searches for:
1. Folders matching "Map {number}"
2. Within those, subfolders containing "Wells"
3. All documents within the Wells folders

To customize what reports are captured, modify the `find_wells_folders()` method.

## Automating Checks

### Using cron (Linux/Mac)

Add to your crontab to check daily at 9 AM:

```bash
crontab -e
```

Add this line:
```
0 9 * * * cd /path/to/pfas_monitor && python pfas_monitor.py --check >> check.log 2>&1
```

### Using Task Scheduler (Windows)

1. Open Task Scheduler
2. Create Basic Task
3. Set trigger (e.g., daily)
4. Action: Start a program
5. Program: `python`
6. Arguments: `C:\path\to\pfas_monitor.py --check`

## Email Notifications (Optional)

You can extend the tool to send email notifications. Add this to `pfas_monitor.py`:

```python
import smtplib
from email.mime.text import MIMEText

def send_notification(new_reports: list[Report]):
    if not new_reports:
        return
    
    body = "New PFAS Reports Found:\n\n"
    for report in new_reports:
        body += f"- {report.name}\n  {report.url}\n\n"
    
    msg = MIMEText(body)
    msg['Subject'] = f'PFAS Monitor: {len(new_reports)} New Report(s)'
    msg['From'] = 'your-email@example.com'
    msg['To'] = 'your-email@example.com'
    
    # Configure with your SMTP settings
    with smtplib.SMTP('smtp.gmail.com', 587) as server:
        server.starttls()
        server.login('your-email@example.com', 'your-app-password')
        server.send_message(msg)
```

## Troubleshooting

### "Playwright not installed"

```bash
pip install playwright
playwright install chromium
```

### "Timeout waiting for folder content"

The portal may be slow or have changed its structure. Try:
1. Run with `--visible` to see what's happening
2. Increase timeout in the code (default 30 seconds)

### "No reports found"

The portal structure may have changed. Run the exploration script:

```bash
python explore_portal.py
```

This saves screenshots and HTML to help debug.

### Network errors

Ensure you have internet access and the portal is accessible in your browser.

## Data Storage

Reports are tracked in `pfas_reports.json`:

```json
{
  "reports": {
    "12345": {
      "id": "12345",
      "name": "PFAS Test Results Q1 2024",
      "url": "https://...",
      "path": "Map 21/Wells",
      "first_seen": "2024-01-15T10:30:00",
      "map_number": "21",
      "folder": "Wells"
    }
  },
  "last_updated": "2024-01-15T10:30:00"
}
```

## Extending the Tool

### Monitor multiple maps

```python
for map_num in ['21', '22', '23']:
    new_reports = check_for_new_reports(map_number=map_num)
    # handle new_reports
```

### Custom filters

Modify `_search_folder_for_pfas_reports()` to filter by:
- Filename patterns
- Date ranges
- Specific keywords

### Download new reports

Add PDF downloading using `requests`:

```python
import requests

def download_report(report: Report, output_dir: str = "downloads"):
    # Note: You may need to handle the Laserfiche download URL format
    response = requests.get(report.url)
    filename = f"{output_dir}/{report.name}.pdf"
    with open(filename, 'wb') as f:
        f.write(response.content)
```

## License

This tool is provided for personal/civic use to monitor public records.
