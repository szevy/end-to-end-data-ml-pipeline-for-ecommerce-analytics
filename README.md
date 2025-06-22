# Module 2 Assignment Project

## Overview  
**Advanced Professional Certificate in Data Science and AI**

This project is part of Module 2 in the Advanced Professional Certificate program. It focuses on designing and implementing an end-to-end data pipeline and analytics workflow for an e-commerce company.

**Brazilian E-Commerce Dataset by Olist** is used as the primary data source. The pipeline includes:

- Extracting raw CSV files from Kaggle  
- Ingesting the data in a cloud-based data warehouse (**Google BigQuery**)  
- Designing a **star schema** to support analytical use cases  
- Performing **ELT** using Python and **dbt**  
- Implementing **data quality checks** with **dbt tests, dbt-utils and dbt-expectations**
- Full pipeline orchestration with **Dagster**
- Visualizing key business metrics and trends with **Pandas** - refer to `notebooks/sales_dept_analysis_insights.ipynb`

This project demonstrates a complete data workflow—from raw data ingestion to actionable insights.


## Setup
1.  Create the conda environment: `conda env create --file environment.yml`

3.  Activate the environment: `conda activate ecp`

4.  Generate a Kaggle API key on Kaggle.com.

5.  Set up Google Cloud credentials to allow access to BigQuery. Obtain your Google service account key as a JSON file from the Google Cloud IAM & Admin console under your service account's "Manage keys" section.

6.  Move the Kaggle and GCP service account key files from `Downloads/` to a secure project directory. Then, add them to .gitignore and set restrictive file permissions to protect sensitive credentials:
    ```bash
    move_key_file(key_type='kaggle', filename='kaggle.json', source_dir=None)
    move_key_file(key_type='gcp', filename='gcp_service_account.json', source_dir=None)
    ```


## Usage
1.  Navigate to the Dagster orchestration project directory:
    ```bash
    cd ~/Module-2-Assignment-Project/olist_ecommerce_orchestration
    ```

2.  Export environment variables:
    ```bash
    export GCP_PROJECT_ID="your-gcp-project-id" \
           PROJECT_NAME="your-project-name" \
           GCS_BUCKET_NAME="your-gcs-bucket-name" \
           GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/service-account-key.json" \
           BQ_DATASET_LOCATION="your-dataset-location" \
           LOAD_TIMESTAMP_OFFSET_HOURS="load-timestamp-offset"
    ```

3.  Start the Dagster orchestration UI:
    ```bash
    dagster dev
    ```

4.  In the Dagster UI, click on the job and then click **Launchpad** to run it.
