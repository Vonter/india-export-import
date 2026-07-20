#!/usr/bin/env python3
"""
Fetch script to download monthly export/import data from the Foreign Trade Data Dissemination Portal.
Downloads data going back in time from the previous month until both import and export have 5 consecutive failures.
"""

import io
import logging
import time
import zipfile
from calendar import month_abbr
from datetime import datetime
from pathlib import Path

import polars as pl
import requests

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)

DATA_TYPES = ('import', 'export')
MAX_CONSECUTIVE_FAILURES = 5


def month_string(year, month):
    """Format a month as used in the URL, e.g. 'Nov-2021'."""
    return f"{month_abbr[month]}-{year}"


def shift_month(year, month, delta):
    """Return the (year, month) offset by delta months."""
    index = year * 12 + (month - 1) + delta
    return index // 12, index % 12 + 1


def get_latest_parquet_month(parquet_path):
    """Return the most recent (year, month) in the dataset, or None if absent."""
    if not parquet_path.exists():
        return None
    row = (
        pl.scan_parquet(parquet_path)
        .select('Year', 'Month')
        .sort('Year', 'Month')
        .last()
        .collect()
    )
    return int(row['Year'][0]), int(row['Month'][0])


def fetch_month_data(year, month, data_type):
    """Fetch a month's zip file, returning its bytes or None on any failure."""
    month_str = month_string(year, month)
    url = (
        "https://ftddp.dgciskol.gov.in/dgcis/freeuserDownload"
        f"?eximp={'I' if data_type == 'import' else 'E'}"
        f"&datepicker={month_str}&datepicker1={month_str}"
        "&commodities=A&countries=A&type=10&ports=A&regions=undefined"
        "&sorted=Order%20By%20HS_CODE,CTY,Value%20DESC&currency=B&reg=2"
    )

    def fail(reason):
        logger.warning(f"Failed to fetch {data_type} data for {month_str} ({reason})")

    try:
        logger.info(f"Fetching {data_type} data for {month_str}...")
        response = requests.get(url, timeout=30)
        if response.status_code != 200:
            return fail(f"HTTP {response.status_code}")

        content = response.content
        if content[:2] != b'PK':
            return fail("not a zip file")

        zipfile.ZipFile(io.BytesIO(content))  # validate
        logger.info(f"Successfully fetched {data_type} data for {month_str}")
        return content
    except zipfile.BadZipFile:
        return fail("invalid zip file")
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch {data_type} data for {month_str} (request error: {e})")
    except Exception as e:
        logger.error(f"Failed to fetch {data_type} data for {month_str} (unexpected error: {e})")


def zip_path(year, month, data_type):
    return Path("raw") / data_type / str(year) / f"{month:02d}.zip"


def process_month(year, month, consecutive_failures):
    """
    Fetch both types for one month, skipping files already on disk and updating
    the consecutive_failures counters in place. Returns True if a request was made.
    """
    month_str = month_string(year, month)
    made_request = False

    for data_type in DATA_TYPES:
        path = zip_path(year, month, data_type)
        if path.exists():
            logger.info(f"Skipping {data_type} data for {month_str} (already exists at {path})")
            consecutive_failures[data_type] = 0
            continue

        made_request = True
        content = fetch_month_data(year, month, data_type)
        if content:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(content)
            logger.info(f"Saved to {path}")
            consecutive_failures[data_type] = 0
        else:
            consecutive_failures[data_type] += 1
            logger.warning(
                f"Consecutive failures for {data_type}: "
                f"{consecutive_failures[data_type]}/{MAX_CONSECUTIVE_FAILURES}"
            )

    return made_request


def fetch_months(start, direction, keep_going):
    """
    Walk months from `start` by `direction` (+1/-1), fetching each while
    `keep_going(year, month, failures)` holds. Waits between real requests.
    """
    failures = {t: 0 for t in DATA_TYPES}
    year, month = start
    while keep_going(year, month, failures):
        made_request = process_month(year, month, failures)
        year, month = shift_month(year, month, direction)
        if made_request:
            logger.debug("Waiting for 10 seconds before next request...")
            time.sleep(10)


def main():
    """Fetch only the months following the most recent month in the dataset."""
    parquet_path = Path("data") / "export-import.parquet"

    # The last complete month is the previous month
    now = datetime.now()
    end = shift_month(now.year, now.month, -1)

    latest = get_latest_parquet_month(parquet_path)

    if latest is None:
        logger.info(f"No existing dataset found; bootstrapping backward from {end[0]}-{end[1]:02d}...")
        fetch_months(
            end, -1,
            lambda y, m, f: min(f.values()) < MAX_CONSECUTIVE_FAILURES,
        )
        logger.info(
            f"Stopped after {MAX_CONSECUTIVE_FAILURES} consecutive failures for both import and export."
        )
        return

    logger.info(f"Most recent month in dataset: {latest[0]}-{latest[1]:02d}")
    start = shift_month(*latest, 1)
    if start > end:
        logger.info("Dataset is already up to date; nothing to fetch.")
        return

    logger.info(f"Fetching months from {start[0]}-{start[1]:02d} to {end[0]}-{end[1]:02d}...")
    fetch_months(start, 1, lambda y, m, f: (y, m) <= end)


if __name__ == "__main__":
    main()
