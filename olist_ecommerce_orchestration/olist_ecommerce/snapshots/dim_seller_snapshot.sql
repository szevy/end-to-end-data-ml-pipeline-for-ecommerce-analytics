{% snapshot dim_seller_snapshot %}

{{
  config(
    strategy='timestamp',
    unique_key="seller_id",
    updated_at='load_timestamp',
    target_schema='snapshots_olist_ecommerce'
  )
}}

select * from {{ ref('stg_olist_sellers') }}

{% endsnapshot %}