import sys
import os
import json
import time
import traceback
from pathlib import Path 
from datetime import datetime, timedelta
import pandas as pd
from google.cloud import bigquery, storage
from google.cloud.exceptions import Conflict
from google.api_core.exceptions import GoogleAPICallError

# Configuration for retries
MAX_RETRIES = 3
RETRY_DELAY = 5

def log_message(message: str, level: str = "INFO"):
    """Log messages with timestamp to stdout or stderr."""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    if level == "ERROR":
        print(f"[{timestamp}] ERROR: {message}", file=sys.stderr)
    else:
        print(f"[{timestamp}] {level}: {message}")

def validate_dataframe(df: pd.DataFrame) -> bool:
    if df.empty:
        log_message("DataFrame is empty.", level="ERROR")
        raise ValueError("Empty DataFrame")
    if 'customer_id' in df.columns and df['customer_id'].isnull().any():
        log_message("Warning: Null values in 'customer_id' column found. This may cause issues if 'customer_id' is a required field.", level="INFO")
        raise ValueError("Null values in customer_id")
    return True

def add_load_timestamps(df: pd.DataFrame, timestamp_offset_hours: int) -> pd.DataFrame:
    """Add load timestamps with configurable timezone offset."""
    utc_now = datetime.utcnow()
    adjusted_time = utc_now + timedelta(hours=timestamp_offset_hours)
    df['load_timestamp'] = adjusted_time
    df['load_date'] = adjusted_time.date()
    log_message(f"Load timestamp added: {adjusted_time.strftime('%Y-%m-%d %H:%M:%S')} (offset by {timestamp_offset_hours}h)")
    return df

def upload_with_retry(
    client: bigquery.Client, 
    df: pd.DataFrame, 
    full_table_name: str, 
    gcs_uri: str, 
    bq_dataset_location: str, 
    local_csv_path: Path
) -> bool:
    """Upload DataFrame chunk with retry logic to BigQuery via GCS."""
    for attempt in range(MAX_RETRIES):
        try:
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1, 
                max_bad_records=1000,
                autodetect=True,
                write_disposition="WRITE_TRUNCATE",
                create_disposition="CREATE_IF_NEEDED"
            )
            
            log_message(f"Attempt {attempt + 1}/{MAX_RETRIES}: Loading {full_table_name} from {gcs_uri}")
            load_job = client.load_table_from_uri(
                gcs_uri,
                destination=full_table_name,
                job_config=job_config,
                location=bq_dataset_location
            )
            load_job.result()  # Wait for job to complete
            log_message(f"✅ Loaded {full_table_name} via batch load job successfully.")
            return True
        except GoogleAPICallError as e:
            log_message(f"Attempt {attempt + 1} failed for {full_table_name}: {str(e)}", level="ERROR")
            if attempt == MAX_RETRIES - 1:
                log_message(f"All {MAX_RETRIES} attempts failed for {full_table_name}.", level="ERROR")
                raise # Re-raise the exception after all retries
            time.sleep(RETRY_DELAY)
        except Exception as e: # Catch other potential errors during upload process
            log_message(f"Non-GoogleAPI error during upload for {full_table_name}: {str(e)}", level="ERROR")
            raise # Re-raise immediately for unhandled exceptions
    return False # Should not be reached if MAX_RETRIES > 0

def upload_to_bigquery_via_gcs(
    dfs: dict[str, pd.DataFrame],
    project_id: str,
    target_dataset_id: str,
    gcs_bucket_name: str,
    bq_dataset_location: str,
    add_load_timestamp: bool, # Flag to indicate if load_timestamp should be added
    timestamp_offset_hours: int # The numerical offset to apply
) -> list[str]:
    """
    Uploads a dictionary of DataFrames to BigQuery via GCS.
    This function replaces the logic from your original bq_upload_batch.py script,
    making it callable from other Python modules (like Dagster ops).
    """
    client = bigquery.Client(project=project_id)
    storage_client = storage.Client(project=project_id)
    
    # Dataset naming: "raw_{PROJECT_NAME}"
    full_dataset_ref = bigquery.DatasetReference(project_id, target_dataset_id)
    dataset = bigquery.Dataset(full_dataset_ref)
    dataset.location = bq_dataset_location # Use the passed location
    
    try:
        client.create_dataset(dataset)
        log_message(f"✅ Created dataset {target_dataset_id} in location {bq_dataset_location}")
    except Conflict:
        log_message(f"ℹ️ Dataset {target_dataset_id} already exists")
    
    uploaded_tables = []
    
    # Create a temporary directory for local CSVs if it doesn't exist
    temp_dir = Path("./_temp_csv_uploads_")
    temp_dir.mkdir(parents=True, exist_ok=True)
    log_message(f"Created temporary local directory: {temp_dir}")

    for name_with_ext, df_original in dfs.items():
        base_name = os.path.splitext(name_with_ext)[0]
        table_name = base_name.replace('-', '_').replace('.', '_') # Ensure BigQuery compliant table name
        full_table_name = f"{target_dataset_id}.{table_name}"
        
        filename = f"{table_name}.csv" # Use the cleaned table name for filename
        local_csv_path = temp_dir / filename
        gcs_uri = f"gs://{gcs_bucket_name}/{filename}"

        df = df_original.copy() # Work on a copy to avoid modifying original DataFrame in dfs
        if add_load_timestamp:
            df = add_load_timestamps(df, timestamp_offset_hours)
        
        try:
            validate_dataframe(df) # Validate the DataFrame before processing
        except ValueError as e:
            log_message(f"Skipping {name_with_ext} due to validation error: {e}", level="ERROR")
            continue # Skip to the next DataFrame if validation fails

        try:
            # Save to local CSV
            df.to_csv(local_csv_path, index=False)
            log_message(f"📄 Saved local CSV: {local_csv_path}")

            # Upload to GCS
            bucket = storage_client.bucket(gcs_bucket_name)
            blob = bucket.blob(filename)
            blob.upload_from_filename(str(local_csv_path)) # Ensure Path is converted to string for upload_from_filename
            log_message(f"☁️ Uploaded {filename} to {gcs_uri}")

            # Load to BigQuery with retry logic
            if upload_with_retry(client, df, full_table_name, gcs_uri, bq_dataset_location, local_csv_path):
                uploaded_tables.append(table_name)
            else:
                log_message(f"❌ Failed to load {full_table_name} after all retries.", level="ERROR")

        except Exception as e:
            log_message(f"❌ Error processing dataset '{name_with_ext}' (table: {table_name}): {str(e)}", level="ERROR")
            log_message(traceback.format_exc(), level="ERROR")
        finally:
            # Cleanup local and GCS files
            if local_csv_path.exists():
                os.remove(local_csv_path)
                log_message(f"🧹 Cleaned up local file {local_csv_path}")
            
            try:
                bucket = storage_client.bucket(gcs_bucket_name)
                blob = bucket.blob(filename)
                if blob.exists(): # Only try to delete if blob exists
                    blob.delete()
                    log_message(f"🧹 Cleaned up GCS file {gcs_uri}")
            except Exception as e:
                log_message(f"Warning: Failed to delete GCS blob {gcs_uri}: {e}", level="ERROR")

    # Clean up the temporary directory
    if temp_dir.exists():
        try:
            os.rmdir(temp_dir)
            log_message(f"🧹 Cleaned up temporary directory {temp_dir}")
        except OSError as e:
            log_message(f"Warning: Could not remove temporary directory {temp_dir}: {e}. It might not be empty.", level="ERROR")


    log_message(f"🎉 {len(uploaded_tables)}/{len(dfs)} datasets uploaded to BigQuery")
    if len(uploaded_tables) != len(dfs):
        # If not all tables uploaded, signal failure to Dagster
        raise RuntimeError(f"Only {len(uploaded_tables)} out of {len(dfs)} datasets were successfully uploaded to BigQuery. Check logs for failures.")

    return uploaded_tables