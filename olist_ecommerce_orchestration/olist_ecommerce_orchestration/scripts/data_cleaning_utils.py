import pandas as pd
import os
import sys
from pathlib import Path

def clean_text_for_csv(
    df: pd.DataFrame,
    text_columns: list = None,
    null_placeholder: str = 'NULL',
    replace_delimiter: str = None
) -> pd.DataFrame:
    """
    Comprehensive text cleaning for CSV export.
    """
    df = df.copy()
    text_cols = text_columns if text_columns else df.select_dtypes(include=['object', 'string']).columns

    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].fillna(null_placeholder).astype(str)

            df[col] = (
                df[col]
                .str.encode('utf-8', errors='replace')
                .str.decode('utf-8')
            )

            if replace_delimiter:
                df[col] = df[col].str.replace(',', replace_delimiter)

            df[col] = (
                df[col]
                .str.replace('\r\n', ' ', regex=True)
                .str.replace('\n', ' ', regex=True)
                .str.replace('\r', ' ', regex=True)
                .str.replace('"', '""')
                .str.replace(r'[\x00-\x1F\x7F]', ' ', regex=True)
            )

    return df

def standardize_dates(
    df: pd.DataFrame, 
    date_columns: list = None,
    date_format: str = '%Y-%m-%d %H:%M:%S'
) -> pd.DataFrame:
    """
    Standardize date columns to consistent format.
    """
    df = df.copy()
    date_cols = date_columns or [c for c in df.columns if 'date' in c.lower() or 'timestamp' in c.lower()]

    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce', utc=True)

    return df

def prepare_dataframe_for_export(
    df: pd.DataFrame,
    null_placeholder: str = 'NULL',
    replace_delimiter: str = None
) -> pd.DataFrame:
    """
    Full cleaning pipeline with auto-deduplication.
    """
    df = df.copy()
    df = df.drop_duplicates()
    df = standardize_dates(df)
    df = clean_text_for_csv(
        df, 
        null_placeholder=null_placeholder,
        replace_delimiter=replace_delimiter
    )
    return df

def run_data_cleaning(input_dir: str) -> dict[str, pd.DataFrame]:
    """
    Reads and cleans all CSVs from input_dir.
    """
    input_path = Path(input_dir)
    if not input_path.is_dir():
        print(f"Error: Input directory '{input_dir}' does not exist.", file=sys.stderr)
        raise FileNotFoundError(f"Input directory '{input_dir}' does not exist.")

    cleaned_dfs = {}
    processed_count = 0
    skipped_count = 0

    print(f"Starting data cleaning from {input_dir}...")

    csv_files = list(input_path.glob("*.csv"))
    if not csv_files:
        print(f"Warning: No CSV files found in '{input_dir}'.", file=sys.stderr)
        return {}

    for file_path in csv_files:
        file_name = file_path.name
        print(f"  Cleaning {file_name}...")
        try:
            df = pd.read_csv(file_path, low_memory=False)
            cleaned_df = prepare_dataframe_for_export(df)
            cleaned_dfs[file_name] = cleaned_df
            processed_count += 1
            print(f"  Successfully cleaned {file_name}.")
        except Exception as e:
            skipped_count += 1
            print(f"  ❌ Error cleaning {file_name}: {e}. Skipping this file.", file=sys.stderr)

    if processed_count == 0 and skipped_count > 0:
        raise RuntimeError("No files were successfully processed during data cleaning.")

    print(f"Finished. Cleaned {processed_count} files, skipped {skipped_count}.")
    return cleaned_dfs
