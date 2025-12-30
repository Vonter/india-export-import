#!/usr/bin/env python3
"""
Generate optimized Parquet and JSON files for visualization.
Creates mode-specific pre-aggregated files for faster loading.
JSON files for year-specific data (faster initial load).
Parquet file for base aggregated data (for filtered queries).
"""

import polars as pl
from pathlib import Path
import logging
import json

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Generate optimized aggregated Parquet files."""
    data_dir = Path("data")
    viz_data_dir = Path("viz/static/data")
    parquet_path = data_dir / "export-import.parquet"
    
    if not parquet_path.exists():
        logger.error(f"Source Parquet file not found: {parquet_path}")
        return
    
    # Create viz data directory if it doesn't exist
    viz_data_dir.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Reading source Parquet file: {parquet_path}")
    df = pl.read_parquet(parquet_path)
    
    logger.info(f"Source data: {len(df)} rows")
    
    # Get available years
    available_years = sorted(df['Year'].unique().to_list())
    logger.info(f"Available years: {available_years}")
    
    # Create base aggregated file (all combinations, including Year) - for filtered queries
    logger.info("Creating base aggregated file...")
    aggregated = df.group_by(["Country", "Commodity", "Port", "Type", "Year"]).agg(
        pl.sum("USD Value").alias("Total USD Value")
    ).sort("Total USD Value", descending=True)
    
    base_path = viz_data_dir / "export-import-aggregated.parquet"
    aggregated.write_parquet(base_path, compression='zstd', compression_level=9)
    logger.info(f"Saved base aggregated file: {base_path} ({base_path.stat().st_size / 1024 / 1024:.2f} MB)")
    
    # Create mode-specific pre-aggregated JSON files for each year (faster initial load)
    logger.info("Creating mode-specific aggregated JSON files per year...")
    
    total_json_files = 0
    total_json_size = 0
    for year in available_years:
        year_df = df.filter(pl.col("Year") == year)
        
        if year_df.is_empty():
            continue
        
        # Commodity mode: aggregate by Commodity, Type (for this year)
        commodity_agg = year_df.group_by(["Commodity", "Type"]).agg(
            pl.sum("USD Value").alias("Total USD Value")
        ).sort("Total USD Value", descending=True)
        
        commodity_path = viz_data_dir / f"commodity-aggregated-{year}.json"
        # Convert to dict for JSON serialization
        commodity_dict = commodity_agg.to_dicts()
        with open(commodity_path, 'w') as f:
            json.dump(commodity_dict, f, separators=(',', ':'))  # Compact JSON
        total_json_files += 1
        total_json_size += commodity_path.stat().st_size
        
        # Country mode: aggregate by Country, Type (for this year)
        country_agg = year_df.group_by(["Country", "Type"]).agg(
            pl.sum("USD Value").alias("Total USD Value")
        ).sort("Total USD Value", descending=True)
        
        country_path = viz_data_dir / f"country-aggregated-{year}.json"
        country_dict = country_agg.to_dicts()
        with open(country_path, 'w') as f:
            json.dump(country_dict, f, separators=(',', ':'))  # Compact JSON
        total_json_files += 1
        total_json_size += country_path.stat().st_size
        
        # Port mode: aggregate by Port, Type (for this year)
        port_agg = year_df.group_by(["Port", "Type"]).agg(
            pl.sum("USD Value").alias("Total USD Value")
        ).sort("Total USD Value", descending=True)
        
        port_path = viz_data_dir / f"port-aggregated-{year}.json"
        port_dict = port_agg.to_dicts()
        with open(port_path, 'w') as f:
            json.dump(port_dict, f, separators=(',', ':'))  # Compact JSON
        total_json_files += 1
        total_json_size += port_path.stat().st_size
        
        logger.info(f"Saved aggregated JSON files for year {year}")
    
    logger.info("File generation complete!")
    logger.info(f"Total files created: {total_json_files + 1} (1 base Parquet + {total_json_files} year-specific JSON)")
    logger.info(f"Base Parquet size: {base_path.stat().st_size / 1024 / 1024:.2f} MB")
    logger.info(f"Total JSON size: {total_json_size / 1024 / 1024:.2f} MB")
    logger.info(f"Total size: {(base_path.stat().st_size + total_json_size) / 1024 / 1024:.2f} MB")


if __name__ == "__main__":
    main()

