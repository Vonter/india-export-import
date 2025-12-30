#!/usr/bin/env python3
"""
Fetch script to download monthly export/import data from the Foreign Trade Data Dissemination Portal.
Downloads data going back in time from the previous month until both import and export have 5 consecutive failures.
"""

import os
import io
import logging
import requests
import time
import zipfile
from datetime import datetime, timedelta
from calendar import month_abbr
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def get_month_string(date):
    """Convert a date to the format used in the URL (e.g., 'Nov-2021')."""
    month_name = month_abbr[date.month]
    return f"{month_name}-{date.year}"


def fetch_month_data(year, month, data_type='import'):
    """
    Fetch data for a specific month.
    
    Args:
        year: Integer year
        month: Integer month (1-12)
        data_type: 'import' or 'export'
    
    Returns:
        bytes: The zip file content if successful, None otherwise
    """
    date = datetime(year, month, 1)
    month_str = get_month_string(date)
    
    # Set eximp parameter: I for import, E for export
    eximp_param = 'I' if data_type == 'import' else 'E'
    
    url = (
        "https://ftddp.dgciskol.gov.in/dgcis/freeuserDownload"
        f"?eximp={eximp_param}"
        f"&datepicker={month_str}"
        f"&datepicker1={month_str}"
        "&commodities=A"
        "&countries=A"
        "&type=10"
        "&ports=A"
        "&regions=undefined"
        "&sorted=Order%20By%20HS_CODE,CTY,Value%20DESC"
        "&currency=B"
        "&reg=2"
    )
    
    try:
        logger.info(f"Fetching {data_type} data for {month_str}...")
        response = requests.get(url, timeout=30)
        
        if response.status_code != 200:
            logger.warning(f"Failed to fetch {data_type} data for {month_str} (HTTP {response.status_code})")
            return None
        
        # Check if response is a valid zip file
        content = response.content
        if len(content) < 4:
            logger.warning(f"Failed to fetch {data_type} data for {month_str} (empty response)")
            return None
        
        # Check zip file magic number (PK\x03\x04)
        if content[:2] != b'PK':
            logger.warning(f"Failed to fetch {data_type} data for {month_str} (not a zip file)")
            return None
        
        # Try to validate it's a proper zip file
        try:
            zipfile.ZipFile(io.BytesIO(content))
            logger.info(f"Successfully fetched {data_type} data for {month_str}")
            return content
        except zipfile.BadZipFile:
            logger.warning(f"Failed to fetch {data_type} data for {month_str} (invalid zip file)")
            return None
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch {data_type} data for {month_str} (request error: {e})")
        return None
    except Exception as e:
        logger.error(f"Failed to fetch {data_type} data for {month_str} (unexpected error: {e})")
        return None


def save_zip_file(content, year, month, data_type='import'):
    """Save zip file content to raw/{data_type}/$year/$month.zip"""
    raw_dir = Path("raw") / data_type / str(year)
    raw_dir.mkdir(parents=True, exist_ok=True)
    
    file_path = raw_dir / f"{month:02d}.zip"
    with open(file_path, "wb") as f:
        f.write(content)
    
    logger.info(f"Saved to {file_path}")


def main():
    """Main function to fetch data going back in time."""
    
    # Start from the previous month
    today = datetime.now()
    if today.month == 1:
        current_date = datetime(today.year - 1, 12, 1)
    else:
        current_date = datetime(today.year, today.month - 1, 1)
    
    consecutive_failures = {'import': 0, 'export': 0}
    max_consecutive_failures = 5
    
    logger.info(f"Starting fetch from {get_month_string(current_date)} going back in time...")
    
    while min(consecutive_failures.values()) < max_consecutive_failures:
        year = current_date.year
        month = current_date.month
        month_str = get_month_string(current_date)
        
        # Track if we made any requests this iteration
        made_request = False
        
        # Fetch both import and export data
        for data_type in ['import', 'export']:
            # Check if file already exists - avoid refetching
            file_path = Path("raw") / data_type / str(year) / f"{month:02d}.zip"
            if file_path.exists():
                logger.info(f"Skipping {data_type} data for {month_str} (already exists at {file_path})")
                consecutive_failures[data_type] = 0  # Reset counter if we skip
            else:
                # File doesn't exist, fetch it
                made_request = True
                content = fetch_month_data(year, month, data_type)
                
                if content:
                    save_zip_file(content, year, month, data_type)
                    consecutive_failures[data_type] = 0  # Reset counter on success
                else:
                    consecutive_failures[data_type] += 1
                    logger.warning(f"Consecutive failures for {data_type}: {consecutive_failures[data_type]}/{max_consecutive_failures}")
        
        # Move to previous month
        if current_date.month == 1:
            current_date = datetime(current_date.year - 1, 12, 1)
        else:
            current_date = datetime(current_date.year, current_date.month - 1, 1)
        
        # Only wait if we made a request (to be respectful to the server)
        if made_request:
            logger.debug("Waiting for 10 seconds before next request...")
            time.sleep(10)
    
    logger.info(f"Stopped after {max_consecutive_failures} consecutive failures for both import and export.")


if __name__ == "__main__":
    main()
