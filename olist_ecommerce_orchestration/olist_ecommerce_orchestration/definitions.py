import os
import subprocess
from pathlib import Path
import sys
import pandas as pd
import json

from dagster import Definitions, job, op, graph, AssetIn, AssetOut, Output, In, Out
from dagster_dbt import DbtCliResource, dbt_assets

from .scripts.load_kaggle_dataset import run_kaggle_download
from .scripts.data_cleaning_utils import run_data_cleaning
from .scripts.bq_upload_batch import upload_to_bigquery_via_gcs

# The directory where this definitions.py file resides
DAGSTER_CODE_DIR = Path(__file__).parent 

# The directory of the 'olist_ecommerce_orchestration' (outer) folder
OUTER_ORCHESTRATION_DIR = DAGSTER_CODE_DIR.parent

# The root directory (Module-2-Assignment-Project)
PROJECT_ROOT_DIR = OUTER_ORCHESTRATION_DIR.parent

# DBT project directory
DBT_PROJECT_DIR = OUTER_ORCHESTRATION_DIR / "olist_ecommerce"
DBT_PROFILES_DIR = DBT_PROJECT_DIR # profiles.yml is inside the dbt project directory

# Raw data directory - where Kaggle data will be downloaded and saved
RAW_DATA_DIR = OUTER_ORCHESTRATION_DIR / "raw_data"

# DBT Seeds directory
DBT_SEEDS_DIR = DBT_PROJECT_DIR / "seeds"

# Paths for DBT manifest
DBT_MANIFEST_PATH = DBT_PROJECT_DIR / "target" / "manifest.json"


dbt_resource = DbtCliResource(
    project_dir=str(DBT_PROJECT_DIR),
    profiles_dir=str(DBT_PROFILES_DIR),
)


@op
def load_kaggle_data():
    """
    Downloads the Olist dataset from Kaggle and saves raw CSVs to RAW_DATA_DIR,
    and the translation CSV to DBT_SEEDS_DIR.
    """
    print(f"Ensuring raw data directory exists: {RAW_DATA_DIR}")
    os.makedirs(RAW_DATA_DIR, exist_ok=True)
    print(f"Ensuring DBT seeds directory exists: {DBT_SEEDS_DIR}")
    os.makedirs(DBT_SEEDS_DIR, exist_ok=True)

    print("Executing Kaggle data download and distribution...")
    try:
        run_kaggle_download(str(RAW_DATA_DIR), str(DBT_SEEDS_DIR))
        print("Kaggle data loaded and distributed successfully.")
        return True
    except Exception as e:
        print(f"Error during Kaggle data load: {e}", file=sys.stderr)
        raise 

@op(ins={"start": In(bool)}, out=Out(dict))
def extract_and_clean_data(start: bool):
    """
    Reads all raw CSVs from RAW_DATA_DIR, cleans them using data_cleaning_utils,
    and returns them as a dictionary of Pandas DataFrames.
    """
    print(f"Starting data extraction and cleaning from {RAW_DATA_DIR}...")
    try:
        cleaned_dfs = run_data_cleaning(str(RAW_DATA_DIR))
        print(f"Data cleaning finished. {len(cleaned_dfs)} DataFrames processed.")
        return cleaned_dfs 
    except Exception as e:
        print(f"Error during data extraction and cleaning: {e}", file=sys.stderr)
        raise 

@op(ins={"dfs": In(dict)})
def load_cleaned_data_to_bq(dfs: dict):
    """
    Loads the processed DataFrames into BigQuery using your upload_to_bigquery_via_gcs utility.
    """
    print("Starting BigQuery upload using upload_to_bigquery_via_gcs function...")

    project_name = os.environ.get("PROJECT_NAME")
    gcp_project_id = os.environ.get("GCP_PROJECT_ID")
    gcs_bucket_name = os.environ.get("GCS_BUCKET_NAME")
    google_application_credentials = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    bq_dataset_location = os.environ.get("BQ_DATASET_LOCATION")
    load_timestamp_offset_hours_str = os.environ.get("LOAD_TIMESTAMP_OFFSET_HOURS")

    missing_vars = [
        var_name for var_name, var_value in {
            "PROJECT_NAME": project_name, "GCP_PROJECT_ID": gcp_project_id, 
            "GCS_BUCKET_NAME": gcs_bucket_name, "GOOGLE_APPLICATION_CREDENTIALS": google_application_credentials,
            "BQ_DATASET_LOCATION": bq_dataset_location, "LOAD_TIMESTAMP_OFFSET_HOURS": load_timestamp_offset_hours_str
        }.items() if var_value is None
    ]
    if missing_vars:
        error_msg = (f"🚨 CRITICAL ERROR: The following essential environment variables are missing or not set: {', '.join(missing_vars)}. "
                     "Please ensure they are properly configured before running the pipeline.")
        print(error_msg, file=sys.stderr)
        raise Exception(error_msg)

    try:
        load_timestamp_offset_hours = int(load_timestamp_offset_hours_str)
    except ValueError:
        print(f"Warning: LOAD_TIMESTAMP_OFFSET_HOURS '{load_timestamp_offset_hours_str}' is not an integer. Defaulting to 0.", file=sys.stderr)
        load_timestamp_offset_hours = 0
    
    add_load_timestamp_flag = (load_timestamp_offset_hours != 0)

    try:
        uploaded_tables_list = upload_to_bigquery_via_gcs(
            dfs=dfs,
            project_id=gcp_project_id,
            target_dataset_id=f"raw_{project_name}",
            gcs_bucket_name=gcs_bucket_name,
            bq_dataset_location=bq_dataset_location,
            add_load_timestamp=add_load_timestamp_flag,
            timestamp_offset_hours=load_timestamp_offset_hours
        )
        print(f"Successfully uploaded tables to BigQuery: {uploaded_tables_list}")
        return True
    except Exception as e:
        print(f"Error during BigQuery upload: {e}", file=sys.stderr)
        raise

@op(ins={"start": In(bool)})
def dbt_deps_op(start, dbt: DbtCliResource):
    """Runs dbt deps to install dbt package dependencies."""
    print("Running dbt deps...")
    dbt.cli(["deps"]).wait()
    print("dbt deps finished.")
    yield Output(True)

@op(ins={"start": In(bool)})
def dbt_seed_op(start, dbt: DbtCliResource):
    """
    Runs dbt seed. This op will run all seeds found in your dbt project's 'seeds' directory.
    """
    print("Running dbt seed (all seeds in project)...")
    dbt.cli(["seed"]).wait()
    print("dbt seed finished.")
    yield Output(True)

@op(ins={"start": In(bool)})
def dbt_run_staging_op(start, dbt: DbtCliResource):
    """Runs dbt run --select path:models/staging to build staging models only."""
    print("Running dbt run (staging models only)...")
    dbt.cli(["run", "--select", "path:models/staging"]).wait()
    print("dbt run (staging models) finished.")
    yield Output(True)

@op(ins={"start": In(bool)})
def dbt_snapshot_op(start, dbt: DbtCliResource):
    """
    Runs dbt snapshot. This op will execute all snapshots configured in your dbt project.
    """
    print("Running dbt snapshot...")
    dbt.cli(["snapshot"]).wait()
    print("dbt snapshot finished.")
    yield Output(True)

@op(ins={"start": In(bool)})
def dbt_run_all_models_op(start, dbt: DbtCliResource):
    """Runs dbt run to build all dbt models (including marts)."""
    print("Running dbt run (all models)...")
    dbt.cli(["run"]).wait() 
    print("dbt run (all models) finished.")
    yield Output(True)

@op(ins={"start": In(bool)})
def dbt_test_op(start, dbt: DbtCliResource):
    """Runs dbt test to execute all tests."""
    print("Running dbt test (all tests)...")
    dbt.cli(["test"]).wait() 
    print("dbt test finished.")
    yield Output(True) 


@job
def python_elt_job():
    """
    Orchestrates the data pipeline from Kaggle extraction, cleaning, loading to BQ, and full dbt execution.
    """
    # Load data from Kaggle and distribute to raw_data/ and dbt_seeds/
    kaggle_load_output = load_kaggle_data() 
    
    # Extract and clean data from raw_data/, passing DataFrames to the next step
    cleaned_dataframes = extract_and_clean_data(start=kaggle_load_output) 
    
    # Load the cleaned Dataframes to BigQuery via GCS (into raw_{PROJECT_NAME} dataset)
    bq_load_output = load_cleaned_data_to_bq(dfs=cleaned_dataframes) 

    # Run dbt commands sequentially
    deps_output = dbt_deps_op(bq_load_output)
    seed_output = dbt_seed_op(deps_output) 
    
    # Run only staging models first for snapshots
    run_staging_output = dbt_run_staging_op(seed_output) 
    snapshot_output = dbt_snapshot_op(run_staging_output) 
    
    # Run all remaining dbt models (marts, etc.) after snapshots
    run_all_models_output = dbt_run_all_models_op(snapshot_output) 
    
    # Run all dbt tests
    test_output = dbt_test_op(run_all_models_output) 


@dbt_assets(
    manifest=DBT_MANIFEST_PATH,
)
def olist_dbt_models(context, dbt: DbtCliResource):
    """
    Defines Dagster assets corresponding to dbt models.
    Running this will trigger dbt build to execute your dbt models.
    This is separate from the job's explicit steps but declares assets for the UI.
    """
    yield from dbt.cli(["build"]).stream()


defs = Definitions(
    assets=[olist_dbt_models],
    jobs=[python_elt_job],
    resources={
        "dbt": dbt_resource,
    },
) 