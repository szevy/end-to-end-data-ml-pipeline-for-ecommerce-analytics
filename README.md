# End-to-End Data & ML Pipeline for E-commerce Analytics

---

## Table of Contents

* [Overview](#overview)
* [Project Architecture](#project-architecture)
    * [End-to-End Data & ML Pipeline](#end-to-end-data--ml-pipeline-full_data_ml_pipeline)
    * [ELT Data Pipeline](#elt-data-pipeline-python_elt_job)
    * [Customer Segmentation ML Pipeline](#customer-segmentation-ml-pipelinesom_kmeans_segmentation_pipeline)
* [Setup](#setup)
* [Usage](#usage)
* [License](#license)

---

## Overview

This project showcases the design and implementation of a complete end-to-end data pipeline and analytics workflow for an e-commerce company. 

Utilizing the **Brazilian E-Commerce Dataset by Olist**, this pipeline demonstrates a full data workflow - from raw data ingestion to actionable insights and advanced machine learning. Initially focused on **core ELT and data warehousing**, the project has been **extended to include a customer segmentation ML pipeline**, providing deeper analytical capabilities. Key components include:

* **Cloud Data Warehouse:** Google BigQuery for data ingestion and storage.
* **ELT & Data Quality:** Python and dbt for transformations, coupled with dbt tests, `dbt-utils`, and `dbt-expectations` for robust data quality checks.
* **Analytics & Machine Learning:** A customer segmentation model is developed using a two-step clustering approach (Self-Organizing Map and K-Means), leveraging the refined data from dbt mart models to provide actionable insights.
* **Orchestration:** Dagster for full pipeline orchestration and observable lineage, managing **two distinct yet interconnected jobs**: a core ELT pipeline and a customer segmentation ML pipeline.

---

## Project Architecture

The pipeline follows a modern **ELT (Extract, Load, Transform)** approach, leveraging Google BigQuery as the cloud data warehouse and dbt for robust data transformations. Dagster orchestrates the entire process, providing clear and observable lineage through **three primary jobs**: `python_elt_job` for core data warehousing, `som_kmeans_segmentation_pipeline` for machine learning, or `full_data_ml_pipeline` for complete end-to-end runs. 

### End-to-End Data & ML Pipeline (`full_data_ml_pipeline`)
Here's a high-level view of the data flow:

![Global Asset Lineage - Full](assets/Global%20Asset%20Lineage%20-%20Full.png)

### ELT Data Pipeline (`python_elt_job`)
Here's a high-level view of the data flow:

![Global Asset Lineage - ELT](assets/Global%20Asset%20Lineage%20-%20ELT.jpg)

### Customer Segmentation ML Pipeline(`som_kmeans_segmentation_pipeline`)
Here's a high-level view of the data flow:

![Global Asset Lineage - ML](assets/Global%20Asset%20Lineage%20-%20ML.png)

The key stages are:

* **Raw Data Ingestion:** CSV files from the Olist dataset are extracted and stored locally, then loaded into BigQuery as raw tables.
* **Data Cleaning:** Raw data undergoes comprehensive cleaning and standardization using Pandas before loading into BigQuery.
* **dbt Transformations:** Staging models prepare data for the final analytical layer, followed by the creation of **Mart Models (Star Schema)** for efficient business intelligence. Extensive dbt tests ensure data integrity.
* **Customer Segmentation ML:** Built upon the dbt mart models, this stage performs feature engineering, trains SOM and K-Means clustering models, validates the model, and materializes segmented customer data and segment definitions. The model identifies distinct customer groups based on their behavior, employing a two-step clustering approach:

    * **Self-Organizing Map (SOM):** Reduces high-dimensional customer features into a low-dimensional grid, preserving topological relationships.
    * **K-Means Clustering:** Applied to the SOM neurons to group similar neurons into distinct customer segments.


![SOM Map with Segments](assets/SOM%20Map%20with%20Segments.png)

This process culminates in an updated `dim_customer_segmented` table in BigQuery, complete with new `segment_id` assignments and a `dim_segment` lookup table providing human-readable segment names. Key visualizations like the SOM U-Matrix and Component Planes aid in interpretation.

For a comprehensive dive into the architecture and the detailed data model (Star Schema) for each mart, step-by-step walkthrough of the customer segmentation model, including comprehensive feature engineering, hyperparameter tuning, segment naming justifications, and in-depth visualization of the segments, you can find more information in these resources:

* **[Customer Segmentation Notebook](/notebooks/ml_model_customer_segmentation.ipynb)**
* **[Detailed Project Guide](docs/DETAILED_PROJECT_GUIDE.md)**

---


## Setup

To get this project up and running:

1.  **Create the conda environment:**
    ```bash
    conda env create --file environment.yml
    ```
2.  **Activate the environment:**
    ```bash
    conda activate ecp
    ```
3.  **Generate a Kaggle API key** on [Kaggle.com](https://www.kaggle.com/).

4.  **Set up Google Cloud credentials** to allow access to BigQuery. Obtain your Google service account key as a JSON file from the Google Cloud IAM & Admin console.

5.  **Navigate to the `scripts/` directory:**
    ```bash
    cd ~/end-to-end-data-ml-pipeline-for-ecommerce-analytics/olist_ecommerce_orchestration/olist_ecommerce_orchestration/scripts
    ```

6.  **Move the Kaggle and GCP service account key files** from Downloads/ to a secure project directory. Then, add them to `.gitignore` and set restrictive file permissions to protect sensitive credentials:
    ```bash
    move_key_file(key_type='kaggle', filename='kaggle.json', source_dir=None)
    move_key_file(key_type='gcp', filename='gcp_service_account.json', source_dir=None)
    ```

---

## Usage

Follow these steps to run the data pipelines and access the Dagster UI:

1.  **Export environment variables:**
    ```bash
    export GCP_PROJECT_ID="your-gcp-project-id" \
    export PROJECT_NAME="your-project-name" \
    export GCS_BUCKET_NAME="your-gcs-bucket-name" \
    export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/service-account-key.json" \
    export BQ_DATASET_LOCATION="your-dataset-location" \
    export LOAD_TIMESTAMP_OFFSET_HOURS="load-timestamp-offset"  # Enter "-3" for Brazilian time
    ```

2.  **Make `start_dagster.sh` Executable (if not already):**
    ```bash
    chmod +x ~/end-to-end-data-ml-pipeline-for-ecommerce-analytics/start_dagster.sh
    ```

3.  **Start the Data Pipeline and Dagster UI:** From the project's root directory, execute the wrapper script:
    ```bash
    ~/end-to-end-data-ml-pipeline-for-ecommerce-analytics/start_dagster.sh
    ```

    This script will:
    * Navigate to the dbt project, run `dbt deps`, and then `dbt parse` to generate `manifest.json`.
    * Navigate back to the correct directory for Dagster.
    * Export all required environment variables.
    * Start the Dagster UI (Dagit), typically opening in your web browser (usually at `http://localhost:3000`).

4.  **Launch the Dagster Jobs:** Once Dagit is loaded, click on **Jobs**, and you will see three main jobs available:

    * **`full_data_ml_pipeline` (End-to-End Data & ML Pipeline):**
        * **Purpose:** Orchestrates the entire data and machine learning workflow from raw data ingestion through to customer segmentation and materialization of final analytical tables. This job is designed for complete end-to-end runs.
        * **To Run:** Navigate to **Jobs** in the left sidebar, click on `full_data_ml_pipeline`, then click **Materialize all** (or **Launch Run** on the Launchpad). This is ideal for a full refresh of all data and models.

    * **`python_elt_job` (ELT Pipeline):**
        * **Purpose:** Responsible for initial data ingestion (Kaggle download), cleaning, loading raw data into BigQuery, and running all dbt transformations to build your core data marts.
        * **To Run:** Navigate to **Jobs** in the left sidebar, click on `python_elt_job`, then click **Materialize all** (or **Launch Run** on the Launchpad). It's recommended to run this job first to ensure your core data marts are populated.

    * **`som_kmeans_segmentation_pipeline` (Customer Segmentation ML Pipeline):**
        * **Purpose:** Executes the machine learning workflow, including feature engineering (from dbt marts), SOM and K-Means model training, model validation, and materializing the final segmented customer data and segment lookup tables in BigQuery.
        * **Dependency Handling:** This job automatically checks the freshness of its upstream dbt model dependencies. If the dbt models are stale or have never been run, Dagster will intelligently trigger the `olist_dbt_models` asset (which runs `dbt build`) as part of this job's execution to ensure the ML pipeline operates on the latest data.
        * **To Run:** Navigate to **Jobs** in the left sidebar, click on `som_kmeans_segmentation_pipeline`, then click **Materialize all** (or **Launch Run** on the Launchpad).
    
---


## License

This project is open-sourced under the MIT License. Please refer to **[LICENSE](/LICENSE.md)** for more information.
