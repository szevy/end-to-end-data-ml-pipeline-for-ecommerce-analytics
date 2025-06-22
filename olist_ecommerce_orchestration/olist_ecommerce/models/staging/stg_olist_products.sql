WITH products AS (
  SELECT * FROM {{ source('raw_' ~ env_var('PROJECT_NAME'), 'olist_products_dataset') }}
),

translations AS (
  SELECT * FROM {{ ref('product_category_name_translation') }}
)

SELECT
  p.*,
  COALESCE(t.product_category_name_english, p.product_category_name) AS product_category_english
FROM products p
LEFT JOIN translations t ON p.product_category_name = t.product_category_name