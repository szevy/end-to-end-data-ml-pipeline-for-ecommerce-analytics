select
  product_id,
  product_category_english AS product_category_name,
  product_photos_qty,
  product_weight_g,
  product_length_cm,
  product_height_cm,
  product_width_cm,
  load_timestamp
from {{ ref('dim_product_snapshot') }}
WHERE dbt_valid_to IS NULL