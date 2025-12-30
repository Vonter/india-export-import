#!/usr/bin/env python3
"""
Parse script to process monthly export/import data from zip files.
Recursively reads zip files in raw/, extracts XLS files, and combines them into Parquet and CSV formats.
Uses Polars for high-performance data processing with parallel processing for optimal performance.
"""

import logging
import zipfile
import polars as pl
import pandas as pd
from pathlib import Path
from io import BytesIO
from multiprocessing import Pool, cpu_count
from functools import partial
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def find_column_indices(header_row):
    """
    Find column indices from header row.
    
    Args:
        header_row: List of header values
    
    Returns:
        dict: Column indices for commodity, country, port, unit, qty, inr, usd
    """
    indices = {
        'commodity': None,
        'country': None,
        'port': None,
        'unit': None,
        'qty': None,
        'inr': None,
        'usd': None
    }
    
    for idx, val in enumerate(header_row):
        if val is not None and isinstance(val, str):
            val_upper = val.upper()
            if 'COMMODITY' in val_upper and indices['commodity'] is None:
                indices['commodity'] = idx
            elif 'COUNTRY' in val_upper and indices['country'] is None:
                indices['country'] = idx
            elif 'PORT' in val_upper and indices['port'] is None:
                indices['port'] = idx
            elif val_upper == 'UNIT' and indices['unit'] is None:
                indices['unit'] = idx
            elif val_upper == 'QTY' and indices['qty'] is None:
                indices['qty'] = idx
            elif 'INR' in val_upper or 'VALUE(INR)' in val_upper:
                if indices['inr'] is None:
                    indices['inr'] = idx
            elif 'US $' in val_upper or 'USD' in val_upper or 'VALUE(US' in val_upper:
                if indices['usd'] is None:
                    indices['usd'] = idx
    
    return indices


def parse_numeric_series(series):
    """Vectorized parsing of numeric values from a pandas Series."""
    return pd.to_numeric(series, errors='coerce')


def detect_excel_format(xls_data):
    """
    Detect Excel file format from file header bytes.
    
    Args:
        xls_data: Bytes content of the Excel file
    
    Returns:
        str: 'xlsx' or 'xls' or None if unknown
    """
    if len(xls_data) < 8:
        return None
    
    # XLSX files start with PK (ZIP signature)
    if xls_data[:2] == b'PK':
        return 'xlsx'
    # XLS files start with specific OLE2 signature
    elif xls_data[:8] == b'\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1':
        return 'xls'
    return None


def parse_xls_file(xls_data, year, month, data_type='import'):
    """
    Parse a single XLS file and return a Polars DataFrame using vectorized operations.
    Handles both .xls (old format) and .xlsx (new format) files.
    
    Args:
        xls_data: Bytes content of the XLS file
        year: Year of the data
        month: Month of the data
        data_type: 'import' or 'export'
    
    Returns:
        Polars DataFrame with parsed data
    """
    try:
        # Detect file format and choose appropriate engine
        file_format = detect_excel_format(xls_data)
        
        # Try to read with appropriate engine, fallback to auto-detect
        read_kwargs = {
            'io': BytesIO(xls_data),
            'header': None,
            'dtype': str,
            'na_values': ['', 'N/A', 'n/a', 'NULL', 'null'],
            'keep_default_na': False
        }
        
        # Set engine based on detected format
        if file_format == 'xlsx':
            read_kwargs['engine'] = 'openpyxl'
        elif file_format == 'xls':
            # Use xlrd engine for .xls files (requires xlrd<2.0)
            # Check if xlrd is available
            try:
                import xlrd
                read_kwargs['engine'] = 'xlrd'
            except ImportError:
                logger.warning("xlrd not available for .xls files. Install with: pip install 'xlrd<2.0'")
                # Will try auto-detect below
        # If format is unknown, let pandas auto-detect (no engine specified)
        
        # Read Excel with optimized pandas settings
        # Try with detected engine first, fallback to auto-detect on error
        try:
            df_pandas = pd.read_excel(**read_kwargs)
        except Exception as e:
            # If engine-specific read fails, try without specifying engine (auto-detect)
            if 'engine' in read_kwargs:
                logger.debug(f"Failed to read with {read_kwargs.get('engine', 'unknown')} engine, trying auto-detect: {e}")
                read_kwargs.pop('engine', None)
                try:
                    df_pandas = pd.read_excel(**read_kwargs)
                except Exception as e2:
                    logger.error(f"Failed to read Excel file with auto-detect: {e2}")
                    raise
            else:
                raise
        
        if df_pandas.empty or len(df_pandas) < 2:
            return pl.DataFrame()
        
        # Row 0 is the title, Row 1 contains the actual column headers
        header_row = df_pandas.iloc[1].tolist()
        indices = find_column_indices(header_row)
        
        # Check essential columns
        if indices['commodity'] is None or indices['country'] is None:
            logger.warning(f"Could not find essential columns. Found: commodity={indices['commodity']}, country={indices['country']}")
            return pl.DataFrame()
        
        # Extract data starting from row 2 (skip title row 0 and header row 1)
        data_df = df_pandas.iloc[2:].copy()
        
        if data_df.empty:
            return pl.DataFrame()
        
        # Vectorized extraction of columns
        # Find maximum column index needed
        valid_indices = [i for i in indices.values() if i is not None]
        if valid_indices:
            max_col = max(valid_indices)
            if max_col >= len(data_df.columns):
                # Pad columns if needed
                for i in range(len(data_df.columns), max_col + 1):
                    data_df[i] = None
        
        # Extract commodity column and filter valid rows
        commodity_col = data_df.iloc[:, indices['commodity']].astype(str).str.strip()
        valid_mask = (
            commodity_col.notna() & 
            (commodity_col != '') & 
            (~commodity_col.str.upper().isin(['COMMODITY', 'NAN', 'NONE', '']))
        )
        
        if not valid_mask.any():
            return pl.DataFrame()
        
        data_df = data_df[valid_mask].copy()
        commodity_col = commodity_col[valid_mask]
        
        # Vectorized extraction of string columns
        def safe_str_extract(col_idx, default=''):
            if col_idx is None:
                return pd.Series([default] * len(data_df), index=data_df.index)
            col = data_df.iloc[:, col_idx].astype(str).str.strip()
            # Replace 'nan' strings (from pandas conversion) with default
            col = col.replace('nan', default)
            return col.fillna(default)
        
        country_col = safe_str_extract(indices['country'], '')
        port_col = safe_str_extract(indices['port'], '')
        # For Unit, use 'N/A' as default instead of empty string
        unit_col = safe_str_extract(indices['unit'], 'N/A')
        
        # Vectorized parsing of numeric columns
        def safe_numeric_extract(col_idx):
            if col_idx is None:
                return pd.Series([None] * len(data_df), index=data_df.index, dtype='float64')
            col = data_df.iloc[:, col_idx]
            return parse_numeric_series(col)
        
        qty_col = safe_numeric_extract(indices['qty'])
        inr_col = safe_numeric_extract(indices['inr'])
        usd_col = safe_numeric_extract(indices['usd'])
        
        # Convert data_type to proper format
        import_export = 'Import' if data_type == 'import' else 'Export'
        type_col = pd.Series([import_export] * len(data_df), index=data_df.index)
        year_col = pd.Series([year] * len(data_df), index=data_df.index, dtype='int32')
        month_col = pd.Series([month] * len(data_df), index=data_df.index, dtype='int32')
        
        # Create DataFrame directly from Series (much faster than row-by-row)
        result_df = pd.DataFrame({
            'Commodity': commodity_col,
            'Country': country_col,
            'Port': port_col,
            'Year': year_col,
            'Month': month_col,
            'Type': type_col,
            'Quantity': qty_col,
            'Unit': unit_col,
            'INR Value': inr_col,
            'USD Value': usd_col
        })
        
        # Convert to Polars with proper schema
        return pl.from_pandas(result_df, schema_overrides={
            'Year': pl.Int32,
            'Month': pl.Int32,
            'Quantity': pl.Int64,
            'INR Value': pl.Int64,
            'USD Value': pl.Int64
        })
    
    except Exception as e:
        logger.error(f"Error parsing XLS file: {e}")
        import traceback
        logger.debug(traceback.format_exc())
        return pl.DataFrame()


def extract_path_info(zip_path):
    """
    Extract year, month, and data type from zip file path.
    Handles both old structure (raw/$year/$month.zip) and new structure (raw/import|export/$year/$month.zip).
    
    Args:
        zip_path: Path to the zip file
    
    Returns:
        tuple: (year, month, data_type) or (None, None, None) if extraction fails
    """
    path_parts = zip_path.parts
    year = None
    month = None
    data_type = None
    
    for part in path_parts:
        if part in ['import', 'export']:
            data_type = part
        elif part.isdigit() and len(part) == 4:  # Year
            year = int(part)
        elif part.endswith('.zip'):
            month_str = part.replace('.zip', '')
            if month_str.isdigit():
                month = int(month_str)
    
    # Handle old directory structure: raw/$year/$month.zip (assume import)
    if year is not None and month is not None and data_type is None:
        data_type = 'import'  # Old structure files are import data
        logger.debug(f"Old directory structure detected for {zip_path}, assuming import data")
    
    return year, month, data_type


def process_zip_file(zip_path):
    """
    Process a single zip file and extract all XLS files.
    Optimized for performance with vectorized operations.
    
    Args:
        zip_path: Path to the zip file (can be Path object or string)
    
    Returns:
        Polars DataFrame with all data from the zip file
    """
    zip_path = Path(zip_path) if not isinstance(zip_path, Path) else zip_path
    year, month, data_type = extract_path_info(zip_path)
    
    if year is None or month is None or data_type is None:
        logger.warning(f"Could not extract year/month/type from path: {zip_path}")
        return pl.DataFrame()
    
    all_data = []
    
    try:
        with zipfile.ZipFile(zip_path, 'r') as z:
            # Filter XLS files more efficiently
            xls_files = [f for f in z.namelist() if f.lower().endswith(('.xls', '.xlsx'))]
            
            if not xls_files:
                return pl.DataFrame()
            
            for xls_file in xls_files:
                try:
                    xls_data = z.read(xls_file)
                    df = parse_xls_file(xls_data, year, month, data_type)
                    
                    if not df.is_empty():
                        all_data.append(df)
                
                except Exception as e:
                    logger.error(f"Error processing {xls_file} in {zip_path}: {e}")
                    continue
        
        if all_data:
            # Use Polars concat for efficient combining
            combined_df = pl.concat(all_data)
            return combined_df
        else:
            return pl.DataFrame()
    
    except Exception as e:
        logger.error(f"Error processing zip file {zip_path}: {e}")
        return pl.DataFrame()


def clean_data(df):
    """
    Clean and standardize the combined DataFrame using optimized Polars operations.
    
    Args:
        df: Polars DataFrame
    
    Returns:
        Polars DataFrame: Cleaned DataFrame
    """
    # Combine all cleaning operations in a single pass where possible
    # Remove duplicates first (most efficient to do early)
    df = df.unique()
    
    # Ensure proper data types and clean in one pass
    df = df.with_columns([
        pl.col('Year').cast(pl.Int32, strict=False),
        pl.col('Month').cast(pl.Int32, strict=False),
        pl.col('Quantity').cast(pl.Int64, strict=False),
        pl.col('INR Value').cast(pl.Int64, strict=False),
        pl.col('USD Value').cast(pl.Int64, strict=False),
        # Set blank Unit values to "N/A" in the same pass
        # Also handle 'nan' strings that might come from pandas conversion
        pl.when(
            pl.col('Unit').is_null() | 
            (pl.col('Unit').str.strip_chars() == '') |
            (pl.col('Unit').str.to_lowercase() == 'nan')
        )
        .then(pl.lit('N/A'))
        .otherwise(pl.col('Unit'))
        .alias('Unit')
    ])
    
    # Filter out rows where Quantity, INR Value, and USD Value are all 0
    df = df.filter(
        ~((pl.col('Quantity').fill_null(0) == 0) & 
          (pl.col('INR Value').fill_null(0) == 0) & 
          (pl.col('USD Value').fill_null(0) == 0))
    )
    
    # Sort by Commodity, Country, Port, Year, Month, Type
    df = df.sort(['Commodity', 'Country', 'Port', 'Year', 'Month', 'Type'])
    
    return df


def save_output_files(df, data_dir):
    """
    Save the DataFrame to Parquet and CSV formats.
    Optimized to write CSV directly to zip without intermediate file.
    
    Args:
        df: Polars DataFrame
        data_dir: Path to data directory
    """
    # Save as Parquet
    parquet_path = data_dir / "export-import.parquet"
    logger.info(f"Saving Parquet file to {parquet_path}...")
    df.write_parquet(parquet_path, compression='zstd')
    logger.info(f"Saved Parquet file: {parquet_path}")
    
    # Save CSV directly to zip file (more efficient)
    csv_zip_path = data_dir / "export-import.csv.zip"
    logger.info(f"Creating CSV zip file at {csv_zip_path}...")
    
    # Write CSV to BytesIO buffer, then add to zip
    csv_buffer = BytesIO()
    df.write_csv(csv_buffer)
    csv_buffer.seek(0)
    
    with zipfile.ZipFile(csv_zip_path, 'w', zipfile.ZIP_DEFLATED, compresslevel=6) as z:
        z.writestr("export-import.csv", csv_buffer.getvalue())
    
    logger.info(f"Saved CSV zip file: {csv_zip_path}")


def process_zip_file_wrapper(zip_file):
    """Wrapper function for multiprocessing."""
    try:
        return process_zip_file(zip_file)
    except Exception as e:
        logger.error(f"Error in process_zip_file_wrapper for {zip_file}: {e}")
        return pl.DataFrame()


def main():
    """Main function to process all zip files and create output files with parallel processing."""
    raw_dir = Path("raw")
    data_dir = Path("data")
    
    if not raw_dir.exists():
        logger.error(f"Raw directory {raw_dir} does not exist")
        return
    
    # Create data directory if it doesn't exist
    data_dir.mkdir(exist_ok=True)
    
    # Find all zip files recursively
    zip_files = sorted(raw_dir.rglob("*.zip"))
    
    if not zip_files:
        logger.warning("No zip files found in raw/ directory")
        return
    
    # Determine number of workers (use CPU count but cap at reasonable number)
    num_workers = min(cpu_count(), len(zip_files), 8)  # Cap at 8 to avoid too many processes
    
    if num_workers > 1:
        # Process zip files in parallel with progress bar
        with Pool(processes=num_workers) as pool:
            # Use imap for progress tracking in parallel processing
            results = list(tqdm(
                pool.imap(process_zip_file_wrapper, zip_files),
                total=len(zip_files),
                desc="Processing zip files",
                unit="file"
            ))
        
        # Filter out empty DataFrames
        all_dataframes = [df for df in results if not df.is_empty()]
    else:
        # Process sequentially if only one worker with progress bar
        all_dataframes = []
        for zip_file in tqdm(zip_files, desc="Processing zip files", unit="file"):
            df = process_zip_file(zip_file)
            if not df.is_empty():
                all_dataframes.append(df)
    
    if not all_dataframes:
        logger.error("No data was extracted from any zip files")
        return
    
    # Combine all data using Polars concat (very efficient)
    combined_df = pl.concat(all_dataframes)
    
    # Clean and standardize the data
    combined_df = clean_data(combined_df)
    
    logger.info(f"Final dataset: {len(combined_df)} rows, {len(combined_df.columns)} columns")
    
    # Get date range efficiently using a single aggregation
    date_stats = combined_df.select([
        pl.min('Year').alias('year_min'),
        pl.max('Year').alias('year_max')
    ]).row(0)
    
    year_min, year_max = date_stats
    month_min = combined_df.filter(pl.col('Year') == year_min)['Month'].min()
    month_max = combined_df.filter(pl.col('Year') == year_max)['Month'].max()
    logger.info(f"Date range: {year_min}-{month_min:02d} to {year_max}-{month_max:02d}")
    
    # Save output files
    save_output_files(combined_df, data_dir)
    
    logger.info("Parsing complete!")


if __name__ == "__main__":
    main()
