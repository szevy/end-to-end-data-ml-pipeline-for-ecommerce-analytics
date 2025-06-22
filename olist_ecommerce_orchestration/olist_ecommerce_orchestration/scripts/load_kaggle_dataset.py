import os
import pandas as pd
import sys
import kaggle
import zipfile
from tqdm import tqdm
import shutil

def run_kaggle_download(output_dir: str, dbt_seeds_dir: str):
    """
    Downloads the Olist Brazilian E-commerce dataset from Kaggle,
    extracts all CSV files, and saves them to the specified output directory.
    The 'product_category_name_translation.csv' is saved directly to the dbt seeds directory.
    """
    dataset_name = "olistbr/brazilian-ecommerce"
    print(f"Starting data loading for dataset: {dataset_name}")

    try:
        kaggle.api.authenticate()
    except Exception as e:
        print(f"Error authenticating with Kaggle API. Make sure your kaggle.json is configured correctly: {e}")
        print("See Kaggle API documentation for setup: https://www.kaggle.com/docs/api")
        raise

    temp_download_dir = os.path.join(output_dir, "temp_kaggle_download")
    os.makedirs(temp_download_dir, exist_ok=True)

    try:
        print(f"Downloading dataset '{dataset_name}' to {temp_download_dir}...")
        kaggle.api.dataset_download_files(dataset_name, path=temp_download_dir, unzip=False)
        print("Dataset ZIP file downloaded.")
    except Exception as e:
        print(f"Error downloading dataset from Kaggle: {e}")
        raise

    zip_file_name = dataset_name.split('/')[-1] + '.zip'
    zip_file_path = os.path.join(temp_download_dir, zip_file_name)

    if not os.path.exists(zip_file_path):
        print(f"Error: Downloaded ZIP file not found at {zip_file_path}", file=sys.stderr)
        raise FileNotFoundError(f"Downloaded ZIP file not found at {zip_file_path}")

    try:
        print(f"Extracting CSVs from {zip_file_path}...")
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            csv_files_in_zip = [f for f in zip_ref.namelist() if f.endswith('.csv')]
            if not csv_files_in_zip:
                print("No CSV files found in the downloaded ZIP archive.", file=sys.stderr)
                raise ValueError("No CSV files found in downloaded ZIP.")

            for file_name in tqdm(csv_files_in_zip, desc="Extracting CSVs"):
                if file_name == "product_category_name_translation.csv":
                    destination_dir = dbt_seeds_dir
                else:
                    destination_dir = output_dir

                os.makedirs(destination_dir, exist_ok=True)

                extracted_path = zip_ref.extract(file_name, temp_download_dir)
                final_path = os.path.join(destination_dir, file_name)
                
                if os.path.exists(final_path):
                    os.remove(final_path)
                    print(f"Overwriting existing file: {final_path}")

                os.rename(extracted_path, final_path)

                print(f"Moved {file_name} to {final_path}")

        print("All CSVs extracted and distributed.")

    except Exception as e:
        print(f"Error extracting ZIP file {zip_file_path}: {e}", file=sys.stderr)
        raise
    finally:
        if os.path.exists(temp_download_dir):
            try:
                shutil.rmtree(temp_download_dir)
                print(f"Removed temporary download directory: {temp_download_dir}")
            except OSError as e:
                print(f"Warning: Could not remove temporary directory {temp_download_dir}: {e}", file=sys.stderr)


    print("load_kaggle_dataset.py finished successfully.")
