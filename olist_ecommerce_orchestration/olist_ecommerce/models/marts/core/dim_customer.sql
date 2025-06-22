SELECT
  customer_id,
  customer_unique_id,
  zip_code_prefix,
  city,
  state,
  load_timestamp
FROM {{ ref('dim_customer_snapshot') }}
WHERE dbt_valid_to IS NULL  