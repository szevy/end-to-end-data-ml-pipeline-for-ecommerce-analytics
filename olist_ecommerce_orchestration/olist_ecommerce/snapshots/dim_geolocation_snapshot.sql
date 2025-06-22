{% snapshot dim_geolocation_snapshot %}
{{
  config(
    strategy='check',
    unique_key='geo_key',
    check_cols=['latitude', 'longitude', 'city', 'state'],
    target_schema='snapshots_olist_ecommerce',
    invalidate_hard_deletes=True
  )
}}

SELECT
    zip_code_prefix,
    latitude,
    longitude,
    city,
    state,
    load_timestamp,
    is_imputed,
    geo_key
FROM {{ ref('stg_olist_geolocation') }}

{% endsnapshot %}