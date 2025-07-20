import os
import subprocess
from pathlib import Path
import sys
import pandas as pd
import json

import dagster as dg
from dagster_dbt import DbtCliResource, dbt_assets
from dagster_gcp_pandas import BigQueryPandasIOManager
from dagster import AssetOut, Output, multi_asset, AssetIn, AssetKey

from .scripts.load_kaggle_dataset import run_kaggle_download
from .scripts.data_cleaning_utils import run_data_cleaning
from .scripts.bq_upload_batch import upload_to_bigquery_via_gcs, add_load_timestamps

DAGSTER_CODE_DIR = Path(__file__).parent
OUTER_ORCHESTRATION_DIR = DAGSTER_CODE_DIR.parent
PROJECT_ROOT_DIR = OUTER_ORCHESTRATION_DIR.parent
DBT_PROJECT_DIR = OUTER_ORCHESTRATION_DIR / "olist_ecommerce"
DBT_PROFILES_DIR = DBT_PROJECT_DIR
RAW_DATA_DIR = OUTER_ORCHESTRATION_DIR / "raw_data"
DBT_SEEDS_DIR = DBT_PROJECT_DIR / "seeds"
DBT_MANIFEST_PATH = DBT_PROJECT_DIR / "target" / "manifest.json"

dbt_resource = DbtCliResource(
    project_dir=str(DBT_PROJECT_DIR),
    profiles_dir=str(DBT_PROFILES_DIR),
)

warehouse_io_manager = BigQueryPandasIOManager(
    project=os.getenv("GCP_PROJECT_ID"),
    dataset=f"raw_{os.getenv('PROJECT_NAME')}",
)

io_manager_for_dataframes = dg.FilesystemIOManager(
    base_dir=str(PROJECT_ROOT_DIR / "dagster_storage" / "intermediate_files")
)

@dg.asset(compute_kind="python")
def kaggle_raw_data(context: dg.AssetExecutionContext) -> bool:
    """
    Downloads the Olist dataset from Kaggle and saves raw CSVs to RAW_DATA_DIR,
    and the translation CSV to DBT_SEEDS_DIR.
    """
    context.log.info(f"Ensuring raw data directory exists: {RAW_DATA_DIR}")
    os.makedirs(RAW_DATA_DIR, exist_ok=True)
    context.log.info(f"Ensuring DBT seeds directory exists: {DBT_SEEDS_DIR}")
    os.makedirs(DBT_SEEDS_DIR, exist_ok=True)

    context.log.info("Executing Kaggle data download and distribution...")
    try:
        run_kaggle_download(str(RAW_DATA_DIR), str(DBT_SEEDS_DIR))
        context.log.info("Kaggle data loaded and distributed successfully.")
        return True
    except Exception as e:
        context.log.error(f"Error during Kaggle data load: {e}")
        raise

@multi_asset(
    deps=[kaggle_raw_data],
    compute_kind="python",
    outs={
        "customers_dataset": AssetOut(key=dg.AssetKey(["cleaned_dataframes", "customers_dataset"])),
        "orders_dataset": AssetOut(key=dg.AssetKey(["cleaned_dataframes", "orders_dataset"])),
        "order_items_dataset": AssetOut(key=dg.AssetKey(["cleaned_dataframes", "order_items_dataset"])),
        "products_dataset": AssetOut(key=dg.AssetKey(["cleaned_dataframes", "products_dataset"])),
        "sellers_dataset": AssetOut(key=dg.AssetKey(["cleaned_dataframes", "sellers_dataset"])),
        "order_payments_dataset": AssetOut(key=dg.AssetKey(["cleaned_dataframes", "order_payments_dataset"])),
        "order_reviews_dataset": AssetOut(key=dg.AssetKey(["cleaned_dataframes", "order_reviews_dataset"])),
        "geolocation_dataset": AssetOut(key=dg.AssetKey(["cleaned_dataframes", "geolocation_dataset"])),
    }
)
def cleaned_dataframes(context: dg.AssetExecutionContext):
    """
    Reads all raw CSVs from RAW_DATA_DIR, cleans them using data_cleaning_utils,
    and yields them as individual Pandas DataFrames.
    """
    context.log.info(f"Starting data extraction and cleaning from {RAW_DATA_DIR}...")
    try:
        cleaned_dfs = run_data_cleaning(str(RAW_DATA_DIR))
        context.log.info(f"Data cleaning finished. {len(cleaned_dfs)} DataFrames processed.")

        for df_key, df_data in cleaned_dfs.items():
            if isinstance(df_data, pd.DataFrame):
                cleaned_asset_name = df_key.replace('olist_', '').replace('.csv', '')
                yield Output(
                    value=df_data,
                    output_name=cleaned_asset_name,
                    metadata={
                        "rows": df_data.shape[0],
                        "cols": df_data.shape[1],
                        "columns": df_data.columns.tolist() if df_data.shape[1] > 0 else []
                    }
                )
            else:
                context.log.error(f"Value for '{df_key}' is NOT a DataFrame. Type: {type(df_data)}")

    except Exception as e:
        context.log.error(f"Error during data extraction and cleaning: {e}")
        raise

@dg.asset(
    ins={
        "customers_dataset": AssetIn(key=AssetKey(["cleaned_dataframes", "customers_dataset"]))
    },
    compute_kind="bigquery",
    key_prefix=["bigquery_raw_tables"],
    io_manager_key="warehouse_io_manager",
    name="olist_customers_dataset"
)
def customers(context: dg.AssetExecutionContext, customers_dataset: pd.DataFrame) -> pd.DataFrame:
    """Customers DataFrame loaded to BigQuery."""
    df_with_timestamps = add_load_timestamps(customers_dataset.copy(), timestamp_offset_hours=0)
    return df_with_timestamps

@dg.asset(
    ins={
        "orders_dataset": AssetIn(key=AssetKey(["cleaned_dataframes", "orders_dataset"]))
    },
    compute_kind="bigquery",
    key_prefix=["bigquery_raw_tables"],
    io_manager_key="warehouse_io_manager",
    name="olist_orders_dataset"
)
def orders(context: dg.AssetExecutionContext, orders_dataset: pd.DataFrame) -> pd.DataFrame:
    """Orders DataFrame loaded to BigQuery."""
    df_with_timestamps = add_load_timestamps(orders_dataset.copy(), timestamp_offset_hours=0)
    return df_with_timestamps

@dg.asset(
    ins={
        "order_items_dataset": AssetIn(key=AssetKey(["cleaned_dataframes", "order_items_dataset"]))
    },
    compute_kind="bigquery",
    key_prefix=["bigquery_raw_tables"],
    io_manager_key="warehouse_io_manager",
    name="olist_order_items_dataset"
)
def order_items(context: dg.AssetExecutionContext, order_items_dataset: pd.DataFrame) -> pd.DataFrame:
    """Order items DataFrame loaded to BigQuery."""
    df_with_timestamps = add_load_timestamps(order_items_dataset.copy(), timestamp_offset_hours=0)
    return df_with_timestamps

@dg.asset(
    ins={
        "products_dataset": AssetIn(key=AssetKey(["cleaned_dataframes", "products_dataset"]))
    },
    compute_kind="bigquery",
    key_prefix=["bigquery_raw_tables"],
    io_manager_key="warehouse_io_manager",
    name="olist_products_dataset"
)
def products(context: dg.AssetExecutionContext, products_dataset: pd.DataFrame) -> pd.DataFrame:
    """Products DataFrame loaded to BigQuery."""
    df_with_timestamps = add_load_timestamps(products_dataset.copy(), timestamp_offset_hours=0)
    return df_with_timestamps

@dg.asset(
    ins={
        "sellers_dataset": AssetIn(key=AssetKey(["cleaned_dataframes", "sellers_dataset"]))
    },
    compute_kind="bigquery",
    key_prefix=["bigquery_raw_tables"],
    io_manager_key="warehouse_io_manager",
    name="olist_sellers_dataset"
)
def sellers(context: dg.AssetExecutionContext, sellers_dataset: pd.DataFrame) -> pd.DataFrame:
    """Sellers DataFrame loaded to BigQuery."""
    df_with_timestamps = add_load_timestamps(sellers_dataset.copy(), timestamp_offset_hours=0)
    return df_with_timestamps

@dg.asset(
    ins={
        "order_payments_dataset": AssetIn(key=AssetKey(["cleaned_dataframes", "order_payments_dataset"]))
    },
    compute_kind="bigquery",
    key_prefix=["bigquery_raw_tables"],
    io_manager_key="warehouse_io_manager",
    name="olist_order_payments_dataset"
)
def order_payments(context: dg.AssetExecutionContext, order_payments_dataset: pd.DataFrame) -> pd.DataFrame:
    """Order payments DataFrame loaded to BigQuery."""
    df_with_timestamps = add_load_timestamps(order_payments_dataset.copy(), timestamp_offset_hours=0)
    return df_with_timestamps

@dg.asset(
    ins={
        "order_reviews_dataset": AssetIn(key=AssetKey(["cleaned_dataframes", "order_reviews_dataset"]))
    },
    compute_kind="bigquery",
    key_prefix=["bigquery_raw_tables"],
    io_manager_key="warehouse_io_manager",
    name="olist_order_reviews_dataset"
)
def order_reviews(context: dg.AssetExecutionContext, order_reviews_dataset: pd.DataFrame) -> pd.DataFrame:
    """Order reviews DataFrame loaded to BigQuery."""
    df_with_timestamps = add_load_timestamps(order_reviews_dataset.copy(), timestamp_offset_hours=0)
    return df_with_timestamps

@dg.asset(
    ins={
        "geolocation_dataset": AssetIn(key=AssetKey(["cleaned_dataframes", "geolocation_dataset"]))
    },
    compute_kind="bigquery",
    key_prefix=["bigquery_raw_tables"],
    io_manager_key="warehouse_io_manager",
    name="olist_geolocation_dataset"
)
def geolocation(context: dg.AssetExecutionContext, geolocation_dataset: pd.DataFrame) -> pd.DataFrame:
    """Geolocation DataFrame loaded to BigQuery."""
    df_with_timestamps = add_load_timestamps(geolocation_dataset.copy(), timestamp_offset_hours=0)
    return df_with_timestamps

@dbt_assets(
    manifest=DBT_MANIFEST_PATH,
    io_manager_key="warehouse_io_manager",
)
def olist_dbt_models(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    """
    This asset block loads dbt models into Dagster's asset graph.
    It first manually runs dbt deps, then performs the dbt build lifecycle.
    """
    context.log.info("Manually running dbt deps via subprocess...")
    deps_command = [
        "dbt",
        "deps",
        "--project-dir", str(DBT_PROJECT_DIR),
        "--profiles-dir", str(DBT_PROFILES_DIR)
    ]
    try:
        process = subprocess.run(
            deps_command,
            capture_output=True,
            text=True,
            check=True
        )
        context.log.info(f"dbt deps stdout:\n{process.stdout}")
        if process.stderr:
            context.log.warning(f"dbt deps stderr:\n{process.stderr}")
        context.log.info("dbt deps finished successfully.")
    except subprocess.CalledProcessError as e:
        context.log.error(f"dbt deps failed with exit code {e.returncode}.")
        context.log.error(f"dbt deps stdout:\n{e.stdout}")
        context.log.error(f"dbt deps stderr:\n{e.stderr}")
        raise

    context.log.info("Executing dbt build for all models, snapshots, and tests...")
    yield from dbt.cli(["build"], context=context).stream()
    context.log.info("dbt build finished for olist_dbt_models group.")

python_elt_job = dg.define_asset_job(
    "python_elt_job",
    selection=dg.AssetSelection.all()
)

defs = dg.Definitions(
    assets=[
        kaggle_raw_data,
        cleaned_dataframes,
        customers,
        orders,
        order_items,
        products,
        sellers,
        order_payments,
        order_reviews,
        geolocation,
        olist_dbt_models,
    ],
    jobs=[python_elt_job],
    resources={
        "dbt": dbt_resource,
        "warehouse_io_manager": warehouse_io_manager,
        "io_manager": io_manager_for_dataframes,
    },
)