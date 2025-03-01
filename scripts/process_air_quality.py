import pandas as pd
import numpy as np
from pathlib import Path
import os
import pyarrow.parquet as pq

# Define directories
DATA_DIR = Path("./data")
RAW_AIR_QUALITY_DIR = DATA_DIR / "raw" / "air_quality"
PROCESSED_AIR_QUALITY_DIR = DATA_DIR / "processed" / "air_quality"

# Ensure processed directory exists
PROCESSED_AIR_QUALITY_DIR.mkdir(parents=True, exist_ok=True)

# Define output file paths
CLEANED_AIR_QUALITY_FILE = PROCESSED_AIR_QUALITY_DIR / "cleaned_air_quality.parquet"
CLEANED_METADATA_FILE = PROCESSED_AIR_QUALITY_DIR / "cleaned_metadata.parquet"

def merge_parquet_files():
    """Merge raw parquet files into a single DataFrame, skipping corrupt/empty ones."""
    if CLEANED_AIR_QUALITY_FILE.exists():
        print(f"✅ {CLEANED_AIR_QUALITY_FILE} already exists. Skipping merge.")
        return None

    parquet_files = [f for f in os.listdir(RAW_AIR_QUALITY_DIR) if f.endswith('.parquet')]

    if not parquet_files:
        print("⚠️ No parquet files found. Skipping merge.")
        return None

    valid_files = []
    for f in parquet_files:
        file_path = RAW_AIR_QUALITY_DIR / f
        try:
            if file_path.stat().st_size > 0:
                pq.ParquetFile(file_path)  # Validate structure
                valid_files.append(file_path)
            else:
                print(f"⚠️ Skipping empty file: {f}")
        except Exception as e:
            print(f"⚠️ Skipping corrupt file: {f}, Error: {e}")

    if not valid_files:
        print("⚠️ No valid parquet files found. Skipping merge.")
        return None

    print(f"📦 Merging {len(valid_files)} valid Parquet files in **batches**...")

    batch_size = 10
    merged_chunks = []

    for i in range(0, len(valid_files), batch_size):
        batch_files = valid_files[i : i + batch_size]
        batch_df = pd.concat([pd.read_parquet(f) for f in batch_files], ignore_index=True)
        merged_chunks.append(batch_df)

        if len(merged_chunks) > 5:
            merged_df = pd.concat(merged_chunks, ignore_index=True)
            merged_chunks = [merged_df]

    df = pd.concat(merged_chunks, ignore_index=True)
    print("✅ Parquet files merged.")
    
    return df

def optimize_air_quality_data(df):
    """Optimize air quality data to reduce memory usage."""
    print("🚀 Optimizing air quality data...")

    df["Samplingpoint"] = df["Samplingpoint"].str.split("/").str[-1]
    
    # Drop unnecessary columns
    df.drop(columns=["FkObservationLog", "End", "AggType", "ResultTime", "DataCapture"], inplace=True, errors="ignore")

    # Convert numerical columns to lower precision
    df["Pollutant"] = pd.to_numeric(df["Pollutant"], downcast="integer")
    df["Validity"] = pd.to_numeric(df["Validity"], downcast="integer")
    df["Verification"] = pd.to_numeric(df["Verification"], downcast="integer")
    df["Value"] = pd.to_numeric(df["Value"], downcast="float")

    print(df.info(memory_usage="deep"))

    return df

def clean_air_quality_data():
    """Load, optimize, and save air quality data."""
    if CLEANED_AIR_QUALITY_FILE.exists():
        print(f"✅ {CLEANED_AIR_QUALITY_FILE} already exists. Skipping cleaning.")
        return

    df = merge_parquet_files()
    if df is not None:
        df = optimize_air_quality_data(df)
        df.to_parquet(CLEANED_AIR_QUALITY_FILE, index=False, compression="snappy")
        print(f"✅ Cleaned air quality data saved to {CLEANED_AIR_QUALITY_FILE}")

def optimize_metadata():
    """Optimize metadata by converting columns to efficient data types."""
    print("🚀 Optimizing metadata...")

    metadata_path = DATA_DIR / "raw" / "metadata" / "metadata.csv"
    if not metadata_path.exists():
        print(f"⚠️ Metadata file not found: {metadata_path}")
        return None

    metadata = pd.read_csv(metadata_path)

    # Select required columns and make a deep copy
    metadata_filtered = metadata.loc[:, [
        "Sampling Point Id", "Air Quality Station Name", "Longitude", "Latitude",
        "Altitude", "Air Quality Station Area", "Air Quality Station Type",
        "Operational Activity Begin", "Operational Activity End", "Main Emission Sources"
    ]].copy()

    # Convert categorical columns
    for col in ["Air Quality Station Name", "Air Quality Station Area", "Air Quality Station Type", "Main Emission Sources"]:
        metadata_filtered[col] = metadata_filtered[col].astype("category")

    # Convert float64 → float32
    for col in ["Longitude", "Latitude", "Altitude"]:
        metadata_filtered[col] = metadata_filtered[col].astype("float32")

    print(metadata_filtered.info(memory_usage="deep"))
    
    return metadata_filtered

def clean_metadata():
    """Load, optimize, and save metadata."""
    if CLEANED_METADATA_FILE.exists():
        print(f"✅ {CLEANED_METADATA_FILE} already exists. Skipping cleaning.")
        return

    metadata_filtered = optimize_metadata()
    if metadata_filtered is not None:
        metadata_filtered.to_parquet(CLEANED_METADATA_FILE, index=False, compression="snappy")
        print(f"✅ Cleaned metadata saved to {CLEANED_METADATA_FILE}")

# Run the pipeline
if __name__ == "__main__":
    clean_air_quality_data()
    clean_metadata()
