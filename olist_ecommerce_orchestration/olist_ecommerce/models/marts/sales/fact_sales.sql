with orders as (
    select
        order_id,
        order_purchase_timestamp,
        customer_id,
        order_status,
        load_timestamp
    from {{ ref('stg_olist_orders') }}
),
order_items as (
    select
        order_id,
        order_item_id,
        product_id,
        seller_id,
        price,
        freight_value
    from {{ ref('stg_olist_order_items') }}
)
select
    o.order_id,
    oi.order_item_id,
    date(o.order_purchase_timestamp) as order_purchase_date,
    CAST(FORMAT_DATE('%Y%m%d', EXTRACT(DATE FROM o.order_purchase_timestamp)) AS INT64) AS order_purchase_date_id,
    o.customer_id,
    oi.seller_id,
    oi.product_id,
    o.order_status,
    oi.price,
    oi.freight_value,
    oi.price + oi.freight_value as total_item_value,
    1 as order_count,
    case when o.order_status = 'delivered' then 1 else 0 end as delivered_flag,
    case when o.order_status in ('delivered', 'invoiced') then oi.price + oi.freight_value else 0 end as recognized_revenue,
    load_timestamp
from orders o
join order_items oi
    on o.order_id = oi.order_id