import os
import subprocess
from pathlib import Path
import sys
import pandas as pd
import json
import numpy as np
import pickle
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from minisom import MiniSom
from sklearn.metrics import silhouette_score, davies_bouldin_score
from kneed import KneeLocator
from io import BytesIO
import base64
import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
import seaborn as sns

import dagster as dg
from dagster_dbt import DbtCliResource, dbt_assets
from dagster_gcp_pandas import BigQueryPandasIOManager
from dagster import AssetOut, Output, multi_asset, AssetIn, AssetKey, Config, JobDefinition, MetadataValue, SkipReason
from dagster_gcp import GCSResource, GCSPickleIOManager

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

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
VERTEX_AI_REGION = os.getenv("BQ_DATASET_LOCATION")
CONTAINER_IMAGE_URI = f"{VERTEX_AI_REGION}-docker.pkg.dev/{GCP_PROJECT_ID}/olist-repo/som-kmeans:latest"

raw_warehouse_io_manager = BigQueryPandasIOManager(
    project=os.getenv("GCP_PROJECT_ID"),
    dataset=f"raw_{os.getenv('PROJECT_NAME')}",
    method='insert_overwrite'
)

warehouse_io_manager = BigQueryPandasIOManager(
    project=os.getenv("GCP_PROJECT_ID"),
    dataset=f"analytics_{os.getenv('PROJECT_NAME')}",
    method='insert_overwrite'
)

io_manager_for_dataframes = dg.FilesystemIOManager(
    base_dir=str(PROJECT_ROOT_DIR / "dagster_storage" / "intermediate_files")
)

dbt_resource = DbtCliResource(
    project_dir=str(DBT_PROJECT_DIR),
    profiles_dir=str(DBT_PROFILES_DIR),
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
        "orders_dataset": AssetOut(key=AssetKey(["cleaned_dataframes", "orders_dataset"])),
        "order_items_dataset": AssetOut(key=AssetKey(["cleaned_dataframes", "order_items_dataset"])),
        "products_dataset": AssetOut(key=AssetKey(["cleaned_dataframes", "products_dataset"])),
        "sellers_dataset": AssetOut(key=AssetKey(["cleaned_dataframes", "sellers_dataset"])),
        "order_payments_dataset": AssetOut(key=AssetKey(["cleaned_dataframes", "order_payments_dataset"])),
        "order_reviews_dataset": AssetOut(key=AssetKey(["cleaned_dataframes", "order_reviews_dataset"])),
        "geolocation_dataset": AssetOut(key=AssetKey(["cleaned_dataframes", "geolocation_dataset"])),
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
    io_manager_key="raw_warehouse_io_manager",
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
    io_manager_key="raw_warehouse_io_manager",
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
    io_manager_key="raw_warehouse_io_manager",
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
    io_manager_key="raw_warehouse_io_manager",
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
    io_manager_key="raw_warehouse_io_manager",
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
    io_manager_key="raw_warehouse_io_manager",
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
    io_manager_key="raw_warehouse_io_manager",
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
    io_manager_key="raw_warehouse_io_manager",
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

class SomConfig(Config):
    m: int = 80
    n: int = 40
    sigma: float = 1.2
    learning_rate: float = 0.15
    num_iterations: int = 1_000_000
    random_seed: int = 42

def calculate_rfm(df):
    df['order_purchase_date'] = pd.to_datetime(df['order_purchase_date'])
    current_date = df['order_purchase_date'].max() + pd.Timedelta(days=1)
    rfm_df = df.groupby('customer_unique_id').agg(
        Recency=('order_purchase_date', lambda date: (current_date - date.max()).days),
        Frequency=('order_id', 'nunique'),
        Monetary=('price', 'sum')
    ).reset_index()
    return rfm_df

# Segment Names
SEGMENT_NAMES = {
    0: "The Vast Unengaged / Lapsed Base",
    1: "The Engaged & Growing Buyers",
    2: "The Recent, Highly Satisfied Explorers"
}

@dg.asset(
    ins={
        "dim_customer_bq": AssetIn(key=AssetKey("dim_customer")),
        "fact_orders_bq": AssetIn(key=AssetKey("fact_orders")),
        "fact_sales_bq": AssetIn(key=AssetKey("fact_sales")),
        "fact_payments_bq": AssetIn(key=AssetKey("fact_payments")),
        "fact_reviews_bq": AssetIn(key=AssetKey("fact_reviews")),
        "dim_product_bq": AssetIn(key=AssetKey("dim_product"))
    },
    compute_kind="python",
    group_name="ml_pipeline"
)
def customer_segmentation_input_df(
    context: dg.AssetExecutionContext,
    dim_customer_bq: pd.DataFrame,
    fact_orders_bq: pd.DataFrame,
    fact_sales_bq: pd.DataFrame,
    fact_payments_bq: pd.DataFrame,
    fact_reviews_bq: pd.DataFrame,
    dim_product_bq: pd.DataFrame
) -> pd.DataFrame:
    """
    Creates a comprehensive customer features DataFrame for segmentation,
    reading from BigQuery data marts (dbt models).
    """
    context.log.info("Starting feature engineering for customer segmentation...")

    customer_features_df = dim_customer_bq[['customer_unique_id']].drop_duplicates().copy()

    customer_orders_base = fact_orders_bq.merge(dim_customer_bq[['customer_id', 'customer_unique_id']], on='customer_id', how='left')
    customer_order_details = customer_orders_base.merge(
        fact_sales_bq[['order_id', 'product_id', 'price', 'order_item_id']],
        on='order_id',
        how='left'
    )
    
    rfm_df = calculate_rfm(customer_order_details)

    order_features = customer_order_details.groupby('customer_unique_id').agg(
        number_of_orders=('order_id', 'nunique'),
        avg_order_value=('price', 'mean'),
        total_order_items=('order_item_id', 'count')
    ).reset_index()

    order_customer_unique_linker = fact_orders_bq.merge(
        dim_customer_bq[['customer_id', 'customer_unique_id']],
        on='customer_id',
        how='left'
    )[['order_id', 'customer_unique_id']].drop_duplicates()

    payment_features = fact_payments_bq.merge(order_customer_unique_linker, on='order_id', how='left').groupby('customer_unique_id').agg(
        total_payment_value=('payment_value', 'sum'),
        avg_payment_installments=('payment_installments', 'mean')
    ).reset_index()

    customer_reviews_merged = fact_reviews_bq.merge(order_customer_unique_linker, on='order_id', how='left')
    
    if 'customer_id_y' in customer_reviews_merged.columns:
        customer_reviews_merged['customer_id'] = customer_reviews_merged['customer_id_y']
        customer_reviews_merged = customer_reviews_merged.drop(columns=['customer_id_x', 'customer_id_y'], errors='ignore')
    elif 'customer_id_x' in customer_reviews_merged.columns:
        customer_reviews_merged = customer_reviews_merged.rename(columns={'customer_id_x': 'customer_id'})

    review_features = customer_reviews_merged.groupby('customer_unique_id').agg(
        avg_review_score=('review_score', 'mean'),
        num_reviews=('review_id', 'count')
    ).reset_index()

    sales_with_products = fact_sales_bq.merge(dim_product_bq, on='product_id', how='left')
    product_features = sales_with_products.merge(order_customer_unique_linker, on='order_id', how='left').groupby('customer_unique_id').agg(
        avg_product_category_value=('price', 'mean'),
        num_unique_products=('product_id', 'nunique'),
        most_frequent_category=('product_category_name', lambda x: x.mode()[0] if not x.mode().empty else None)
    ).reset_index()

    dfs_to_merge = [
        rfm_df,
        order_features,
        payment_features,
        review_features,
        product_features
    ]

    for df in dfs_to_merge:
        customer_features_df = customer_features_df.merge(df, on='customer_unique_id', how='left')

    numerical_cols = customer_features_df.select_dtypes(include=np.number).columns
    for col in numerical_cols:
        customer_features_df[col] = customer_features_df[col].fillna(0)

    if 'most_frequent_category' in customer_features_df.columns:
        customer_features_df['most_frequent_category'] = customer_features_df['most_frequent_category'].fillna('Unknown')
    
    customer_features_df = customer_features_df.drop(columns=['most_frequent_category'], errors='ignore')
    customer_features_df = customer_features_df.drop(columns=['customer_id'], errors='ignore')


    context.log.info(f"Feature engineering complete. Shape: {customer_features_df.shape}")
    return customer_features_df

@dg.asset(compute_kind="python", group_name="ml_pipeline")
def scaled_customer_features(customer_segmentation_input_df: pd.DataFrame) -> pd.DataFrame:
    """
    Selects relevant features and scales them using StandardScaler.
    Scaling is crucial for distance-based algorithms like SOM and KMeans.
    The index of the returned DataFrame is customer_unique_id.
    """
    features_to_scale = customer_segmentation_input_df.drop(columns=['customer_unique_id'], errors='ignore')

    scaler = StandardScaler()
    scaled_features_array = scaler.fit_transform(features_to_scale)

    scaled_features_df = pd.DataFrame(
        scaled_features_array,
        columns=features_to_scale.columns,
        index=customer_segmentation_input_df['customer_unique_id']
    ).astype(np.float32)
    return scaled_features_df

@dg.asset(compute_kind="python", group_name="ml_pipeline")
def trained_som_model(context: dg.AssetExecutionContext, config: SomConfig, scaled_customer_features: pd.DataFrame) -> bytes:
    """
    Trains the Self-Organizing Map (SOM) model.
    Outputs the serialized SOM model.
    """
    X_scaled = scaled_customer_features.values

    som = MiniSom(
        x=config.m,
        y=config.n,
        input_len=X_scaled.shape[1],
        sigma=config.sigma,
        learning_rate=config.learning_rate,
        neighborhood_function='gaussian',
        random_seed=config.random_seed
    )
    som.random_weights_init(X_scaled)
    context.log.info(f"Training SOM with {config.num_iterations} iterations...")
    som.train_random(X_scaled, config.num_iterations, verbose=True)
    context.log.info("SOM training complete.")

    quant_error = som.quantization_error(X_scaled)
    topo_error = som.topographic_error(X_scaled)
    context.log.info(f"SOM Quantization Error: {quant_error:.4f}")
    context.log.info(f"SOM Topographic Error: {topo_error:.4f}")

    context.add_output_metadata({
        "quantization_error": MetadataValue.float(float(quant_error)), 
        "topographic_error": MetadataValue.float(float(topo_error)),  
        "som_map_dimensions": MetadataValue.text(f"{config.m}x{config.n}"),
        "num_iterations": MetadataValue.int(config.num_iterations)
    })

    return pickle.dumps(som)

@dg.asset(
    compute_kind="python",
    group_name="ml_pipeline",
    ins={"trained_som_model": AssetIn(key=AssetKey("trained_som_model"))}
)
def optimal_n_clusters(context: dg.AssetExecutionContext, trained_som_model: bytes) -> int:
    """
    Computes the optimal number of clusters for KMeans on SOM neurons
    using the Elbow Method and KneeLocator.
    """
    som_model = pickle.loads(trained_som_model)
    som_weights = som_model.get_weights().reshape(-1, som_model.get_weights().shape[2])

    sse = []
    k_range = range(2, 11)
    for k in k_range:
        kmeans_temp = KMeans(n_clusters=k, random_state=42, n_init='auto')
        kmeans_temp.fit(som_weights)
        sse.append(kmeans_temp.inertia_)

    kneedle = KneeLocator(k_range, sse, S=1.0, curve='convex', direction='decreasing')
    optimal_k = int(kneedle.elbow) if kneedle.elbow is not None else 3

    context.log.info(f"Programmatically determined optimal K-value: {optimal_k}")
    return optimal_k

@dg.asset(
    compute_kind="python", 
    group_name="ml_pipeline",
    ins={
        "trained_som_model": AssetIn(key=AssetKey("trained_som_model")),
        "optimal_k_value": AssetIn(key=AssetKey("optimal_n_clusters")) # New dependency
    }
)
def trained_kmeans_on_som_neurons_model(
    context: dg.AssetExecutionContext, 
    trained_som_model: bytes,
    optimal_k_value: int
) -> bytes:
    """
    Applies KMeans clustering to the SOM neuron weights to find customer segments,
    using the optimal k value determined by the Elbow Method.
    """
    som = pickle.loads(trained_som_model)
    som_weights = som.get_weights().reshape(-1, som.get_weights().shape[2])

    kmeans = KMeans(n_clusters=optimal_k_value, random_state=42, n_init='auto')
    kmeans.fit(som_weights)
    context.log.info(f"KMeans clustering on SOM neurons complete. Found {kmeans.n_clusters} clusters.")

    context.add_output_metadata({
        "n_clusters": MetadataValue.int(kmeans.n_clusters),
        "kmeans_inertia": MetadataValue.float(float(kmeans.inertia_))
    })

    return pickle.dumps(kmeans)

@dg.asset(compute_kind="python", group_name="ml_pipeline")
def customer_som_kmeans_segments(
    context: dg.AssetExecutionContext,
    customer_segmentation_input_df: pd.DataFrame,
    scaled_customer_features: pd.DataFrame,
    trained_som_model: bytes,
    trained_kmeans_on_som_neurons_model: bytes
) -> pd.DataFrame:
    """
    Assigns a segment ID to each customer (unique_id) using the trained SOM and KMeans models.
    """
    som = pickle.loads(trained_som_model)
    kmeans = pickle.loads(trained_kmeans_on_som_neurons_model)

    customer_bmus = np.array([som.winner(x) for x in scaled_customer_features.values])
    neuron_cluster_labels = kmeans.labels_

    som_x, som_y, _ = som.get_weights().shape
    neuron_to_segment_map = {}
    for i in range(som_x):
        for j in range(som_y):
            flattened_index = i * som_y + j
            neuron_to_segment_map[(i, j)] = neuron_cluster_labels[flattened_index]

    customer_segment_ids = [neuron_to_segment_map[tuple(bmu)] for bmu in customer_bmus]

    segmented_df = pd.DataFrame({
        'customer_unique_id': scaled_customer_features.index,
        'segment_id': customer_segment_ids
    })

    final_customer_dim_with_segments = customer_segmentation_input_df.merge(
        segmented_df, on='customer_unique_id', how='left'
    )
    final_customer_dim_with_segments['segment_id'] = final_customer_dim_with_segments['segment_id'].astype(int)

    context.log.info(f"Customer segments assigned. Shape: {final_customer_dim_with_segments.shape}")
    return final_customer_dim_with_segments

@dg.asset(compute_kind="python", group_name="ml_pipeline")
def model_validation_check(
    context: dg.AssetExecutionContext,
    scaled_customer_features: pd.DataFrame,
    trained_som_model: bytes,
    trained_kmeans_on_som_neurons_model: bytes,
    customer_som_kmeans_segments: pd.DataFrame
) -> Output[bool]:
    """
    Performs automated validation checks on the newly trained SOM and KMeans models.
    Returns True if the model is acceptable for deployment, False otherwise.
    Logs all relevant metrics.
    """
    som = pickle.loads(trained_som_model)
    kmeans = pickle.loads(trained_kmeans_on_som_neurons_model)

    quant_error = som.quantization_error(scaled_customer_features.values)
    topo_error = som.topographic_error(scaled_customer_features.values)
    context.log.info(f"Validation - Quantization Error: {quant_error:.4f}")
    context.log.info(f"Validation - Topographic Error: {topo_error:.4f}")

    features_and_labels = pd.merge(
        scaled_customer_features.reset_index(),
        customer_som_kmeans_segments[['customer_unique_id', 'segment_id']],
        on='customer_unique_id',
        how='inner'
    )
    features_for_metrics = features_and_labels.drop(columns=['customer_unique_id', 'segment_id']).values
    labels_for_metrics = features_and_labels['segment_id'].values

    silhouette_avg = -1.0 # Default if not computable
    davies_bouldin_idx = -1.0 # Default if not computable

    if len(np.unique(labels_for_metrics)) > 1 and len(features_for_metrics) > 1:
        try:
            silhouette_avg = silhouette_score(features_for_metrics, labels_for_metrics)
            davies_bouldin_idx = davies_bouldin_score(features_for_metrics, labels_for_metrics)
            context.log.info(f"Validation - Silhouette Score: {silhouette_avg:.4f}")
            context.log.info(f"Validation - Davies-Bouldin Index: {davies_bouldin_idx:.4f}")
        except Exception as e:
            context.log.warning(f"Could not compute cluster validity metrics: {e}")
    else:
        context.log.warning("Not enough unique clusters or data points to compute Silhouette/Davies-Bouldin.")

    # Define Acceptance Criteria
    MIN_SILHOUETTE_SCORE = 0.6
    MAX_DAVIES_BOULDIN_INDEX = 0.75
    MAX_QUANTIZATION_ERROR = 0.26
    MAX_TOPOGRAPHIC_ERROR = 0.26

    is_acceptable = True
    reasons_for_rejection = []

    if silhouette_avg < MIN_SILHOUETTE_SCORE:
        is_acceptable = False
        reasons_for_rejection.append(f"Silhouette Score ({silhouette_avg:.4f}) below threshold ({MIN_SILHOUETTE_SCORE}).")
    if davies_bouldin_idx > MAX_DAVIES_BOULDIN_INDEX:
        is_acceptable = False
        reasons_for_rejection.append(f"Davies-Bouldin Index ({davies_bouldin_idx:.4f}) above threshold ({MAX_DAVIES_BOULDIN_INDEX}).")
    if quant_error > MAX_QUANTIZATION_ERROR:
        is_acceptable = False
        reasons_for_rejection.append(f"Quantization Error ({quant_error:.4f}) above threshold ({MAX_QUANTIZATION_ERROR}).")
    if topo_error > MAX_TOPOGRAPHIC_ERROR:
        is_acceptable = False
        reasons_for_rejection.append(f"Topographic Error ({topo_error:.4f}) above threshold ({MAX_TOPOGRAPHIC_ERROR}).")

    context.add_output_metadata({
        "validation_status": MetadataValue.text("Accepted" if is_acceptable else "Rejected"),
        "silhouette_score": MetadataValue.float(float(silhouette_avg)),       
        "davies_bouldin_index": MetadataValue.float(float(davies_bouldin_idx)),
        "quantization_error": MetadataValue.float(float(quant_error)),        
        "topographic_error": MetadataValue.float(float(topo_error)),           
        "reasons_for_rejection": MetadataValue.text(", ".join(reasons_for_rejection) if reasons_for_rejection else "None")
    })

    if is_acceptable:
        context.log.info("✅ Model passed all validation checks. Ready for deployment.")
        return Output(True)
    else:
        context.log.warning(f"❌ Model failed validation checks: {', '.join(reasons_for_rejection)}")
        return Output(False)

@dg.asset(
    key_prefix="dim_customer_segmented",
    io_manager_key="warehouse_io_manager",
    group_name="ml_pipeline",
    ins={"model_is_acceptable": AssetIn(key=AssetKey("model_validation_check"))}
)
def dim_customer_segmented(
    context: dg.AssetExecutionContext,
    customer_som_kmeans_segments: pd.DataFrame,
    model_is_acceptable: bool
) -> pd.DataFrame:
    """
    Materializes the dim_customer_segmented table in BigQuery,
    including the new segment_id column, ONLY if the model is acceptable.
    """
    if not model_is_acceptable:
        context.log.warning("Model not acceptable for deployment. Returning empty dim_customer_segmented DataFrame.")
        return pd.DataFrame(columns=['customer_unique_id', 'segment_id'])
    
    context.log.info(f"Materializing dim_customer_segmented with {len(customer_som_kmeans_segments)} rows to BigQuery.")
    return customer_som_kmeans_segments

@dg.asset(
    key_prefix="dim_segment",
    io_manager_key="warehouse_io_manager",
    group_name="ml_pipeline",
    ins={"model_is_acceptable": AssetIn(key=AssetKey("model_validation_check"))}
)
def dim_segment(
    context: dg.AssetExecutionContext,
    trained_kmeans_on_som_neurons_model: bytes,
    model_is_acceptable: bool
) -> pd.DataFrame:
    """
    Creates and materializes the dim_segment lookup table in BigQuery,
    ONLY if the model is acceptable.
    """
    if not model_is_acceptable:
        context.log.warning("Model not acceptable for deployment. Returning empty dim_segment DataFrame.")
        return pd.DataFrame(columns=['segment_id', 'segment_name', 'segment_description', 'created_at'])


    kmeans = pickle.loads(trained_kmeans_on_som_neurons_model)
    
    segment_centroids = kmeans.cluster_centers_

    segment_data = []
    for i, centroid in enumerate(segment_centroids):
        segment_name = SEGMENT_NAMES.get(i, f"Segment {i} (Unknown Name)") 
        segment_description = f"Description for Segment {i} based on centroid: {np.round(centroid, 2).tolist()}"
        
        segment_data.append({
            'segment_id': i,
            'segment_name': segment_name,
            'segment_description': segment_description,
            'created_at': pd.Timestamp.now()
        })
    
    df_segment = pd.DataFrame(segment_data)
    df_segment['segment_id'] = df_segment['segment_id'].astype(int)
    context.log.info(f"Materializing dim_segment with {len(df_segment)} rows to BigQuery.")
    return df_segment

@dg.asset(compute_kind="python", group_name="ml_pipeline")
def som_model_visualizations(
    context: dg.AssetExecutionContext,
    scaled_customer_features: pd.DataFrame,
    trained_som_model: bytes,
    trained_kmeans_on_som_neurons_model: bytes,
    customer_som_kmeans_segments: pd.DataFrame,
    customer_segmentation_input_df: pd.DataFrame,
    optimal_n_clusters: int
):
    """
    Generates and attaches visualizations (Elbow, U-Matrix, Component Planes, Segment Map, Profile Dashboard)
    as metadata to this asset.
    """
    som_model = pickle.loads(trained_som_model)
    kmeans_som_model = pickle.loads(trained_kmeans_on_som_neurons_model)

    metadata = {}
    
    def plot_to_base64(fig):
        buf = BytesIO()
        fig.savefig(buf, format='png', bbox_inches='tight')
        plt.close(fig)
        return base64.b64encode(buf.getvalue()).decode('utf-8')

    actual_n_clusters = kmeans_som_model.n_clusters

    display_segment_labels = []
    # Check if the number of actual clusters matches the hardcoded SEGMENT_NAMES length
    if actual_n_clusters == len(SEGMENT_NAMES):
        # If they match, use the defined names
        for i in range(actual_n_clusters):
            display_segment_labels.append(SEGMENT_NAMES.get(i, f"Segment {i} (Default)"))
    else:
        # If they do not match, default to generic names based on the actual_n_clusters
        context.log.warning(
            f"Number of actual clusters ({actual_n_clusters}) does not match "
            f"hardcoded SEGMENT_NAMES length ({len(SEGMENT_NAMES)}). "
            "Using generic segment labels."
        )
        for i in range(actual_n_clusters):
            display_segment_labels.append(f"Segment {i}")

    context.log.info("Generating Elbow Method plot...")

    som_weights = som_model.get_weights().reshape(-1, som_model.get_weights().shape[2])

    sse = []
    k_range = range(2, 11)
    for k in k_range:
        kmeans_temp = KMeans(n_clusters=k, random_state=42, n_init='auto') # Use temp KMeans for elbow
        kmeans_temp.fit(som_weights)
        sse.append(kmeans_temp.inertia_)

    optimal_k_for_plot = optimal_n_clusters
    context.log.info(f"Using optimal K-value for plot: {optimal_k_for_plot}")

    fig_elbow, ax_elbow = plt.subplots(figsize=(10, 6))
    ax_elbow.plot(k_range, sse, marker='o')
    ax_elbow.set_xlabel('Number of Clusters (k)')
    ax_elbow.set_ylabel('Sum of Squared Errors (SSE)')
    ax_elbow.set_title('Elbow Method for Optimal k on SOM Neurons (Kneed Automated)')
    ax_elbow.set_xticks(k_range)
    ax_elbow.grid(True)

    if optimal_k_for_plot is not None:
        ax_elbow.axvline(x=optimal_k_for_plot, color='r', linestyle='--', label=f'Optimal k = {optimal_k_for_plot}')
        ax_elbow.legend()

    metadata['elbow_plot'] = MetadataValue.md(
        f"### Elbow Method Plot\n\n![Elbow Plot](data:image/png;base64,{plot_to_base64(fig_elbow)})"
    )
    metadata['optimal_k_value_kneed'] = MetadataValue.int(optimal_k_for_plot)
    plt.close(fig_elbow)

    context.log.info("Generating U-Matrix plot...")
    u_matrix = som_model.distance_map()
    fig_u_matrix, ax_u_matrix = plt.subplots(figsize=(12, 10))
    c = ax_u_matrix.pcolor(u_matrix.T, cmap='bone_r', edgecolors='k', linewidths=0.5)
    fig_u_matrix.colorbar(c, ax=ax_u_matrix, label='Distance to Neighboring Neurons (U-Matrix Value)')
    ax_u_matrix.set_title('Self-Organizing Map (SOM) U-Matrix')
    ax_u_matrix.set_xticks([])
    ax_u_matrix.set_yticks([])
    metadata['u_matrix_plot'] = MetadataValue.md(
        f"### U-Matrix Plot\n\n![U-Matrix](data:image/png;base64,{plot_to_base64(fig_u_matrix)})"
    )

    context.log.info("Generating Component Planes plots...")
    feature_names = scaled_customer_features.columns.tolist()
    
    num_features = len(feature_names)
    grid_cols = int(np.ceil(np.sqrt(num_features)))
    grid_rows = int(np.ceil(num_features / grid_cols))

    fig_comp_planes, axes_comp_planes = plt.subplots(grid_rows, grid_cols, figsize=(grid_cols * 5, grid_rows * 4))
    axes_comp_planes = axes_comp_planes.flatten()
    
    for i, f_name in enumerate(feature_names):
        if i < len(axes_comp_planes):
            ax = axes_comp_planes[i]
            comp_plane = som_model.get_weights()[:, :, i]
            im = ax.imshow(comp_plane.T, cmap='viridis', origin='lower')
            ax.set_title(f_name)
            ax.set_xticks([])
            ax.set_yticks([])
            fig_comp_planes.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
    
    for i in range(num_features, len(axes_comp_planes)):
        fig_comp_planes.delaxes(axes_comp_planes[i])

    plt.tight_layout()
    metadata['component_planes_plot'] = MetadataValue.md(
        f"### SOM Component Planes\n\n![Component Planes](data:image/png;base64,{plot_to_base64(fig_comp_planes)})"
    )

    context.log.info("Generating Segment Map on U-Matrix plot...")
    
    neuron_cluster_labels = kmeans_som_model.labels_
    
    som_x_dim, som_y_dim, _ = som_model.get_weights().shape
    neuron_segments_grid = np.zeros((som_x_dim, som_y_dim))
    for i in range(som_x_dim):
        for j in range(som_y_dim):
            flattened_index = i * som_y_dim + j
            neuron_segments_grid[i, j] = neuron_cluster_labels[flattened_index]

    context.log.info(f"DEBUG (Segment Map): kmeans_som_model.n_clusters: {kmeans_som_model.n_clusters}")
    context.log.info(f"DEBUG (Segment Map): len(SEGMENT_NAMES): {len(SEGMENT_NAMES)}")
    context.log.info(f"DEBUG (Segment Map): Unique neuron_cluster_labels: {np.unique(neuron_cluster_labels)}")

    cmap = plt.cm.get_cmap('tab10', actual_n_clusters) 
    bounds = np.arange(actual_n_clusters + 1) 
    norm = mcolors.BoundaryNorm(bounds, cmap.N)

    fig_segment_map, ax_segment_map = plt.subplots(figsize=(12, 10))
    im_segment_map = ax_segment_map.imshow(neuron_segments_grid.T, cmap=cmap, origin='lower', norm=norm, interpolation='nearest')
    ax_segment_map.set_title('SOM Neurons Colored by K-Means Segment')
    ax_segment_map.set_xticks([])
    ax_segment_map.set_yticks([])

    cbar = fig_segment_map.colorbar(im_segment_map, ax=ax_segment_map, ticks=np.arange(actual_n_clusters) + 0.5, orientation='vertical') # Use actual_n_clusters here
    cbar.set_ticklabels(display_segment_labels)
    cbar.set_label('Customer Segment ID')

    ax_segment_map.pcolor(u_matrix.T, cmap='Greys', alpha=0.3, edgecolors='k', linewidths=0.5)

    metadata['segment_map_plot'] = MetadataValue.md(
        f"### SOM Segment Map\n\n![Segment Map](data:image/png;base64,{plot_to_base64(fig_segment_map)})"
    )

    context.log.info("Generating Segment Profile Dashboard...")
    
    segmented_customer_features = customer_segmentation_input_df.merge(
        customer_som_kmeans_segments[['customer_unique_id', 'segment_id']],
        on='customer_unique_id',
        how='left'
    )
    
    profile_df = segmented_customer_features.drop(columns=['customer_unique_id'], errors='ignore')

    numerical_cols_for_profiling = profile_df.select_dtypes(include=np.number).columns.tolist()
    if 'segment_id' in numerical_cols_for_profiling:
        numerical_cols_for_profiling.remove('segment_id')

    segment_profiles = profile_df.groupby('segment_id')[numerical_cols_for_profiling].mean().reset_index()

    context.log.info(f"DEBUG PROFILE: Unique segment_ids in segment_profiles: {segment_profiles['segment_id'].unique()}")
    context.log.info(f"DEBUG PROFILE: len(segment_profiles): {len(segment_profiles)}")
    context.log.info(f"DEBUG PROFILE: segment_profiles['segment_id'] values: {list(segment_profiles['segment_id'])}")
    context.log.info(f"DEBUG PROFILE: len(SEGMENT_NAMES) for labels: {len(SEGMENT_NAMES)}")

    num_features_profile = len(numerical_cols_for_profiling)
    profile_grid_cols = 3
    profile_grid_rows = int(np.ceil(num_features_profile / profile_grid_cols))

    fig_profile, axes_profile = plt.subplots(profile_grid_rows, profile_grid_cols, figsize=(profile_grid_cols * 6, profile_grid_rows * 5))
    axes_profile = axes_profile.flatten()

    for i, feature in enumerate(numerical_cols_for_profiling):
        if i < len(axes_profile):
            ax = axes_profile[i]
            sns.barplot(x='segment_id', y=feature, data=segment_profiles, ax=ax, palette='viridis')
            ax.set_title(f'Average {feature} by Segment', fontsize=10)
            ax.set_xlabel('Segment ID', fontsize=8)
            ax.set_ylabel('Average Value', fontsize=8)
            ax.tick_params(axis='both', which='major', labelsize=7)
            
            unique_segment_ids_in_profile = sorted(segment_profiles['segment_id'].unique())
            
            context.log.info(f"DEBUG PLOT: unique_segment_ids_in_profile for xticks: {unique_segment_ids_in_profile}")
            context.log.info(f"DEBUG PLOT: len(unique_segment_ids_in_profile) for xticks: {len(unique_segment_ids_in_profile)}")

            ax.set_xticks(range(len(unique_segment_ids_in_profile)))

            xtick_labels = [display_segment_labels[s_id] if s_id < len(display_segment_labels) else f'Seg {s_id}' for s_id in unique_segment_ids_in_profile]
            
            context.log.info(f"DEBUG PLOT: xtick_labels generated: {xtick_labels}")
            context.log.info(f"DEBUG PLOT: len(xtick_labels) generated: {len(xtick_labels)}")

            ax.set_xticklabels(
                xtick_labels,
                rotation=45,
                ha='right',
                fontsize=7
            )
    
    for i in range(num_features_profile, len(axes_profile)):
        fig_profile.delaxes(axes_profile[i])

    plt.tight_layout()
    metadata['segment_profile_dashboard'] = MetadataValue.md(
        f"### Customer Segment Profile Dashboard\n\n![Segment Profile](data:image/png;base64,{plot_to_base64(fig_profile)})"
    )

    context.add_output_metadata(metadata)

ml_pipeline_assets_selection = dg.AssetSelection.groups("ml_pipeline")

som_kmeans_segmentation_pipeline = dg.define_asset_job(
    "som_kmeans_segmentation_pipeline",
    selection=ml_pipeline_assets_selection,
    config={
        "ops": {
            "trained_som_model": {
                "config": {
                    "m": 80,
                    "n": 40,
                    "sigma": 1.2,
                    "learning_rate": 0.15,
                    "num_iterations": 1_000_000,
                    "random_seed": 42
                }
            }
        }
    }
)

elt_pipeline_assets_selection = dg.AssetSelection.all() - dg.AssetSelection.groups("ml_pipeline")

python_elt_job = dg.define_asset_job(
    "python_elt_job",
    selection=elt_pipeline_assets_selection
)

full_data_ml_pipeline = dg.define_asset_job(
    "full_data_ml_pipeline",
    selection=dg.AssetSelection.all()
)

all_assets = [
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

    customer_segmentation_input_df,
    scaled_customer_features,
    trained_som_model,
    optimal_n_clusters,
    trained_kmeans_on_som_neurons_model,
    customer_som_kmeans_segments,
    model_validation_check,
    dim_customer_segmented,
    dim_segment,
    som_model_visualizations
]

defs = dg.Definitions(
    assets=all_assets,
    jobs=[
        python_elt_job,
        som_kmeans_segmentation_pipeline,
        full_data_ml_pipeline 
    ],
    resources={
        "dbt": dbt_resource,
        "raw_warehouse_io_manager": raw_warehouse_io_manager,
        "warehouse_io_manager": warehouse_io_manager,
        "io_manager_for_dataframes": io_manager_for_dataframes
    }
)