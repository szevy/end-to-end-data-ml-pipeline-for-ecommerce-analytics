WITH orders AS (
    SELECT
        order_id,
        customer_id,
        order_status,
        order_delivered_customer_date,
        order_approved_at,
        order_estimated_delivery_date,
        load_timestamp
    FROM {{ ref('stg_olist_orders') }}
),

order_items AS (
    SELECT
        order_id,
        order_item_id,
        product_id,
        seller_id,
        shipping_limit_date
    FROM {{ ref('stg_olist_order_items') }}
)

SELECT
    o.order_id,
    oi.order_item_id,
    o.customer_id,
    oi.product_id,
    oi.seller_id,
    o.order_status,
    o.order_approved_at,
    oi.shipping_limit_date,
    CAST(FORMAT_DATE('%Y%m%d', DATE(oi.shipping_limit_date)) AS INT64) AS shipping_limit_date_id,

    CASE
      WHEN o.order_delivered_customer_date IS NOT NULL
        AND o.order_approved_at IS NOT NULL
      THEN DATE_DIFF(DATE(o.order_delivered_customer_date), DATE(o.order_approved_at), DAY)
      ELSE NULL
    END AS actual_delivery_days,

    CASE
      WHEN o.order_estimated_delivery_date IS NOT NULL
        AND o.order_approved_at IS NOT NULL
      THEN DATE_DIFF(DATE(o.order_estimated_delivery_date), DATE(o.order_approved_at), DAY)
      ELSE NULL
    END AS estimated_delivery_days,

    CASE
      WHEN o.order_delivered_customer_date IS NOT NULL
        AND o.order_estimated_delivery_date IS NOT NULL
      THEN DATE_DIFF(DATE(o.order_delivered_customer_date), DATE(o.order_estimated_delivery_date), DAY)
      ELSE NULL
    END AS delivery_delay,

    CASE
      WHEN o.order_delivered_customer_date IS NOT NULL
        AND o.order_estimated_delivery_date IS NOT NULL
        AND DATE_DIFF(DATE(o.order_delivered_customer_date), DATE(o.order_estimated_delivery_date), DAY) > 0
      THEN 1
      ELSE 0
    END AS delivery_late_flag,
    o.load_timestamp

FROM orders o
JOIN order_items oi
  ON o.order_id = oi.order_id