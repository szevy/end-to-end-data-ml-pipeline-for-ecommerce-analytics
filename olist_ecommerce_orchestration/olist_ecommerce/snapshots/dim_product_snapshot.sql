{% snapshot dim_product_snapshot %}

{{
  config(
    strategy='timestamp',
    unique_key="product_id",
    updated_at='load_timestamp',
    target_schema='snapshots_olist_ecommerce'
  )
}}

select * from {{ ref('stg_olist_products') }}

{% endsnapshot %}