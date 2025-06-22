SELECT
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp, 

    CASE
        WHEN order_approved_at = 'NULL' THEN NULL 
        ELSE PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', order_approved_at) 
    END AS order_approved_at,

    order_delivered_carrier_date,
    order_delivered_customer_date, 
    order_estimated_delivery_date, 
    load_timestamp 

FROM
    {{ source('raw_' ~ env_var('PROJECT_NAME'), 'olist_orders_dataset') }}