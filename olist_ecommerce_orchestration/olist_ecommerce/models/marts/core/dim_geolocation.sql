SELECT
  zip_code_prefix,
  latitude,
  longitude,
  city,
  state,
  load_timestamp,
  geo_key,
  is_imputed,
  ST_GEOGPOINT(longitude, latitude) AS geo_point
FROM {{ ref('dim_geolocation_snapshot') }}