#!/bin/bash

# Get the directory where this script itself is located.
PROJECT_ROOT_DIR=$(dirname "$0")

DBT_PROJECT_RELATIVE_PATH="olist_ecommerce_orchestration/olist_ecommerce"
DAGSTER_CODE_MODULE_PARENT_RELATIVE_PATH="olist_ecommerce_orchestration"
DAGSTER_CODE_MODULE_NAME="olist_ecommerce_orchestration.definitions" # The actual module to import

echo "--- Running dbt parse to generate manifest.json ---"
cd "$PROJECT_ROOT_DIR/$DBT_PROJECT_RELATIVE_PATH" || { echo "Error: Could not find DBT project path: $DBT_PROJECT_RELATIVE_PATH"; exit 1; }

dbt deps || { echo "Error: dbt deps failed."; exit 1; }
dbt parse || { echo "Error: dbt parse failed. Please check your dbt project for errors."; exit 1; }
echo "--- dbt parse completed successfully. ---"

echo "--- Starting Dagster dev UI ---"

cd "$PROJECT_ROOT_DIR/$DAGSTER_CODE_MODULE_PARENT_RELATIVE_PATH" || { echo "Error: Could not find Dagster orchestration path: $DAGSTER_CODE_MODULE_PARENT_RELATIVE_PATH"; exit 1; }

dagster dev -m "$DAGSTER_CODE_MODULE_NAME"