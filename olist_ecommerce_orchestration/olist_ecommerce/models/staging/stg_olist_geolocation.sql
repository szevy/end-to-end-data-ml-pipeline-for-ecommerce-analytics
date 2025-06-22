{{
  config(
    materialized = 'table',
    unique_key = 'geo_key'
  )
}}

WITH source AS (
    SELECT 
        geolocation_zip_code_prefix,
        SAFE_CAST(geolocation_lat AS FLOAT64) AS latitude,
        SAFE_CAST(geolocation_lng AS FLOAT64) AS longitude,
        geolocation_city,
        geolocation_state,
        load_timestamp
    FROM {{ source('raw_' ~ env_var('PROJECT_NAME'), 'olist_geolocation_dataset') }}
    WHERE geolocation_zip_code_prefix IS NOT NULL
),

-- Normalize text fields and calculate centroids
normalized AS (
    SELECT
        LPAD(CAST(geolocation_zip_code_prefix AS STRING), 5, '0') AS zip_code_prefix,
        INITCAP(TRIM(geolocation_city)) AS city,
        UPPER(TRIM(geolocation_state)) AS state,
        latitude,
        longitude,
        load_timestamp
    FROM source
),

-- Aggregate duplicates by calculating centroids
deduplicated AS (
    SELECT
        zip_code_prefix,
        city,
        state,
        -- Calculate centroid coordinates
        ROUND(AVG(latitude), 6) AS latitude,
        ROUND(AVG(longitude), 6) AS longitude,
        -- Keep the most recent load timestamp
        MAX(load_timestamp) AS load_timestamp,
        -- Count of merged points
        COUNT(*) AS source_points,
        -- Create consistent geo_key
        CONCAT(zip_code_prefix, '|', city, '|', state) AS geo_key
    FROM normalized
    GROUP BY 1, 2, 3
)

SELECT
    zip_code_prefix,
    latitude,
    longitude,
    city,
    state,
    load_timestamp,
    FALSE AS is_imputed,
    source_points > 1 AS was_merged,
    source_points,
    geo_key
FROM deduplicated