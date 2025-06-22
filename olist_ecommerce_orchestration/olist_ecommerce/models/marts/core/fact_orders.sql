with orders as (
    select
        order_id,
        customer_id,
        order_status,
        order_purchase_timestamp AS order_purchase_date,
        order_approved_at AS order_approved_date,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        order_estimated_delivery_date,
        -- New date_id columns
        CAST(FORMAT_DATE('%Y%m%d', EXTRACT(DATE FROM order_purchase_timestamp)) AS INT64) AS order_purchase_date_id,
        CAST(FORMAT_DATE('%Y%m%d', EXTRACT(DATE FROM order_approved_at)) AS INT64) AS order_approved_date_id,
        CAST(FORMAT_DATE('%Y%m%d', EXTRACT(DATE FROM order_delivered_carrier_date)) AS INT64) AS order_delivered_carrier_date_id,
        CAST(FORMAT_DATE('%Y%m%d', EXTRACT(DATE FROM order_delivered_customer_date)) AS INT64) AS order_delivered_customer_date_id,
        CAST(FORMAT_DATE('%Y%m%d', EXTRACT(DATE FROM order_estimated_delivery_date)) AS INT64) AS order_estimated_delivery_date_id,
        load_timestamp
    from {{ ref('stg_olist_orders') }}
)
select * from orders