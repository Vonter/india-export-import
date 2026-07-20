#!/usr/bin/env python3
"""
Parse script to process monthly export/import data from zip files.
Recursively reads zip files in raw/, extracts XLS files, and combines them into Parquet and CSV formats.
Uses Polars for high-performance data processing with parallel processing for optimal performance.
"""

import logging
import traceback
import zipfile
from io import BytesIO
from multiprocessing import Pool, cpu_count
from pathlib import Path

import pandas as pd
import polars as pl
from tqdm import tqdm

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)

# Keys used to keep the combined dataset sorted on disk
SORT_KEYS = ['Commodity', 'Country', 'Port', 'Year', 'Month', 'Type']

# Excel file magic numbers -> pandas engine
EXCEL_SIGNATURES = {
    b'PK': 'openpyxl',                              # xlsx (ZIP)
    b'\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1': 'xlrd',    # xls (OLE2)
}


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


def find_column_indices(header_row):
    """Map logical columns to their index in the header row."""
    indices = dict.fromkeys(['commodity', 'country', 'port', 'unit', 'qty', 'inr', 'usd'])

    def match(val):
        v = val.upper()
        if 'COMMODITY' in v:
            return 'commodity'
        if 'COUNTRY' in v:
            return 'country'
        if 'PORT' in v:
            return 'port'
        if v == 'UNIT':
            return 'unit'
        if v == 'QTY':
            return 'qty'
        if 'INR' in v:
            return 'inr'
        if 'US $' in v or 'USD' in v or 'VALUE(US' in v:
            return 'usd'
        return None

    for idx, val in enumerate(header_row):
        if isinstance(val, str):
            key = match(val)
            if key and indices[key] is None:
                indices[key] = idx

    return indices


def read_excel(xls_data):
    """Read an Excel workbook (xls or xlsx) as raw strings, with engine fallback."""
    engine = next((e for sig, e in EXCEL_SIGNATURES.items() if xls_data.startswith(sig)), None)
    kwargs = dict(
        io=BytesIO(xls_data),
        header=None,
        dtype=str,
        na_values=['', 'N/A', 'n/a', 'NULL', 'null'],
        keep_default_na=False,
    )
    try:
        return pd.read_excel(engine=engine, **kwargs)
    except Exception as e:
        if engine is None:
            raise
        logger.debug(f"Failed to read with {engine} engine, trying auto-detect: {e}")
        return pd.read_excel(**kwargs)


def parse_xls_file(xls_data, year, month, data_type):
    """Parse a single XLS/XLSX file into a Polars DataFrame using vectorized operations."""
    try:
        df = read_excel(xls_data)
        if len(df) < 2:
            return pl.DataFrame()

        # Row 0 is the title, row 1 the column headers; data starts at row 2
        indices = find_column_indices(df.iloc[1].tolist())
        if indices['commodity'] is None or indices['country'] is None:
            logger.warning(
                f"Could not find essential columns. Found: "
                f"commodity={indices['commodity']}, country={indices['country']}"
            )
            return pl.DataFrame()

        data = df.iloc[2:].copy()
        if data.empty:
            return pl.DataFrame()

        # Pad columns so every referenced index exists
        max_col = max(i for i in indices.values() if i is not None)
        for i in range(len(data.columns), max_col + 1):
            data[i] = None

        commodity = data.iloc[:, indices['commodity']].astype(str).str.strip()
        valid = commodity.notna() & ~commodity.str.upper().isin(['COMMODITY', 'NAN', 'NONE', ''])
        if not valid.any():
            return pl.DataFrame()
        data, commodity = data[valid], commodity[valid]

        def text(key, default=''):
            if indices[key] is None:
                return default
            col = data.iloc[:, indices[key]].astype(str).str.strip().replace('nan', default)
            return col.fillna(default)

        def numeric(key):
            if indices[key] is None:
                return pd.Series([None] * len(data), index=data.index, dtype='float64')
            return pd.to_numeric(data.iloc[:, indices[key]], errors='coerce')

        result = pd.DataFrame({
            'Commodity': commodity,
            'Country': text('country'),
            'Port': text('port'),
            'Year': year,
            'Month': month,
            'Type': 'Import' if data_type == 'import' else 'Export',
            'Quantity': numeric('qty'),
            'Unit': text('unit', 'N/A'),
            'INR Value': numeric('inr'),
            'USD Value': numeric('usd'),
        })

        return pl.from_pandas(result, schema_overrides={
            'Year': pl.Int32,
            'Month': pl.Int32,
            'Quantity': pl.Int64,
            'INR Value': pl.Int64,
            'USD Value': pl.Int64,
        })

    except Exception as e:
        logger.error(f"Error parsing XLS file: {e}")
        logger.debug(traceback.format_exc())
        return pl.DataFrame()


def extract_path_info(zip_path):
    """
    Extract (year, month, data_type) from a zip file path. Handles both
    raw/$year/$month.zip (assumed import) and raw/import|export/$year/$month.zip.
    """
    year = month = data_type = None
    for part in zip_path.parts:
        if part in ('import', 'export'):
            data_type = part
        elif part.isdigit() and len(part) == 4:
            year = int(part)
        elif part.endswith('.zip') and part[:-4].isdigit():
            month = int(part[:-4])

    if year is not None and month is not None and data_type is None:
        data_type = 'import'  # Old structure files are import data
        logger.debug(f"Old directory structure detected for {zip_path}, assuming import data")

    return year, month, data_type


def process_zip_file(zip_path):
    """Process a single zip file and return a Polars DataFrame of all its XLS data."""
    zip_path = Path(zip_path)
    year, month, data_type = extract_path_info(zip_path)
    if None in (year, month, data_type):
        logger.warning(f"Could not extract year/month/type from path: {zip_path}")
        return pl.DataFrame()

    frames = []
    try:
        with zipfile.ZipFile(zip_path, 'r') as z:
            for name in z.namelist():
                if not name.lower().endswith(('.xls', '.xlsx')):
                    continue
                try:
                    df = parse_xls_file(z.read(name), year, month, data_type)
                    if not df.is_empty():
                        frames.append(df)
                except Exception as e:
                    logger.error(f"Error processing {name} in {zip_path}: {e}")
    except Exception as e:
        logger.error(f"Error processing zip file {zip_path}: {e}")
        return pl.DataFrame()

    return pl.concat(frames) if frames else pl.DataFrame()


def clean_data(df):
    """Clean and standardize the combined DataFrame."""
    df = df.unique().with_columns(
        pl.col('Year').cast(pl.Int32, strict=False),
        pl.col('Month').cast(pl.Int32, strict=False),
        pl.col('Quantity').cast(pl.Int64, strict=False),
        pl.col('INR Value').cast(pl.Int64, strict=False),
        pl.col('USD Value').cast(pl.Int64, strict=False),
        pl.when(
            pl.col('Unit').is_null()
            | (pl.col('Unit').str.strip_chars() == '')
            | (pl.col('Unit').str.to_lowercase() == 'nan')
        )
        .then(pl.lit('N/A'))
        .otherwise(pl.col('Unit'))
        .alias('Unit'),
    )

    # Drop rows where Quantity, INR Value and USD Value are all zero/null
    df = df.filter(
        ~(
            (pl.col('Quantity').fill_null(0) == 0)
            & (pl.col('INR Value').fill_null(0) == 0)
            & (pl.col('USD Value').fill_null(0) == 0)
        )
    )

    return df.sort(SORT_KEYS)


def save_output_files(lf, data_dir):
    """
    Stream a sorted LazyFrame to Parquet and CSV, keeping the full dataset
    out of memory. Parquet is written to a temp file then atomically replaced,
    so a dataset scanned inside `lf` is never read and written at once.
    """
    parquet_path = data_dir / "export-import.parquet"
    tmp_parquet = parquet_path.with_name(parquet_path.name + ".tmp")
    logger.info(f"Streaming Parquet file to {parquet_path}...")
    lf.sink_parquet(tmp_parquet, compression='zstd', engine='streaming')
    tmp_parquet.replace(parquet_path)
    logger.info(f"Saved Parquet file: {parquet_path}")

    # Stream CSV from the freshly written (already sorted) Parquet, then compress
    csv_zip_path = data_dir / "export-import.csv.zip"
    tmp_csv = data_dir / "export-import.csv"
    logger.info(f"Streaming CSV and compressing to {csv_zip_path}...")
    pl.scan_parquet(parquet_path).sink_csv(tmp_csv, engine='streaming')
    try:
        with zipfile.ZipFile(csv_zip_path, 'w', zipfile.ZIP_DEFLATED, compresslevel=6) as z:
            z.write(tmp_csv, arcname="export-import.csv")
    finally:
        tmp_csv.unlink(missing_ok=True)
    logger.info(f"Saved CSV zip file: {csv_zip_path}")


def process_zip_file_wrapper(zip_file):
    """Wrapper for multiprocessing that never propagates exceptions."""
    try:
        return process_zip_file(zip_file)
    except Exception as e:
        logger.error(f"Error in process_zip_file_wrapper for {zip_file}: {e}")
        return pl.DataFrame()


def parse_zip_files(zip_files):
    """Process zip files (in parallel when worthwhile) with a progress bar."""
    num_workers = min(cpu_count(), len(zip_files), 8)
    desc = dict(desc="Processing zip files", unit="file")

    if num_workers > 1:
        with Pool(processes=num_workers) as pool:
            results = list(tqdm(pool.imap(process_zip_file_wrapper, zip_files),
                                total=len(zip_files), **desc))
    else:
        results = [process_zip_file(z) for z in tqdm(zip_files, **desc)]

    return [df for df in results if not df.is_empty()]


def main():
    """Process all new zip files and (re)build the Parquet and CSV outputs."""
    raw_dir = Path("raw")
    data_dir = Path("data")

    if not raw_dir.exists():
        logger.error(f"Raw directory {raw_dir} does not exist")
        return

    data_dir.mkdir(exist_ok=True)
    parquet_path = data_dir / "export-import.parquet"
    latest = get_latest_parquet_month(parquet_path)

    zip_files = sorted(raw_dir.rglob("*.zip"))
    if not zip_files:
        logger.warning("No zip files found in raw/ directory")
        return

    # Only process months following the most recent month already in the dataset
    if latest is not None:
        logger.info(f"Most recent month in dataset: {latest[0]}-{latest[1]:02d}")

        def is_new_month(zip_path):
            year, month, _ = extract_path_info(zip_path)
            return None not in (year, month) and (year, month) > latest

        zip_files = [z for z in zip_files if is_new_month(z)]
        if not zip_files:
            logger.info("No new zip files to process; dataset is already up to date")
            return
        logger.info(f"Processing {len(zip_files)} zip files for months after {latest[0]}-{latest[1]:02d}")

    all_dataframes = parse_zip_files(zip_files)
    if not all_dataframes:
        logger.error("No data was extracted from any zip files")
        return

    # Combine and clean the newly parsed data (bounded: only new months)
    new_df = clean_data(pl.concat(all_dataframes))
    logger.info(f"Parsed {len(new_df)} new rows across {len(zip_files)} zip files")

    # Append new rows to any existing dataset and re-sort out-of-core via streaming
    if latest is not None and parquet_path.exists():
        output_lf = pl.concat([pl.scan_parquet(parquet_path), new_df.lazy()]).sort(SORT_KEYS)
    else:
        output_lf = new_df.lazy()

    save_output_files(output_lf, data_dir)

    # Report the final size cheaply from Parquet metadata (no full scan)
    total_rows = pl.scan_parquet(parquet_path).select(pl.len()).collect().item()
    logger.info(f"Final dataset: {total_rows} rows")
    logger.info("Parsing complete!")


if __name__ == "__main__":
    main()
