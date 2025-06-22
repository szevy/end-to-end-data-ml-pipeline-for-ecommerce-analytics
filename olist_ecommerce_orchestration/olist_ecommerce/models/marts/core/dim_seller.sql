select
  seller_id,
  zip_code_prefix,
  city,
  state,
  load_timestamp
from {{ ref('dim_seller_snapshot') }}
WHERE dbt_valid_to IS NULL  