WITH source AS (
  SELECT * FROM {{ source('raw_' ~ env_var('PROJECT_NAME'), 'sellers') }}
),

cleaned AS (
  SELECT
    seller_id,

    LPAD(CAST(seller_zip_code_prefix AS STRING), 5, '0') AS zip_code_prefix,

    LOWER(TRIM(REGEXP_REPLACE(
      TRANSLATE(seller_city,
        'áàãâäéèêëíìîïóòõôöúùûüçÁÀÃÂÄÉÈÊËÍÌÎÏÓÒÕÔÖÚÙÛÜÇ',
        'aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC'),
      r'\s+', ' ')
    )) AS city,

    seller_state AS state,
    load_timestamp,

    COALESCE(
      CONCAT(
        LPAD(CAST(seller_zip_code_prefix AS STRING), 5, '0'), '||',
        LOWER(TRIM(REGEXP_REPLACE(
          TRANSLATE(seller_city,
            'áàãâäéèêëíìîïóòõôöúùûüçÁÀÃÂÄÉÈÊËÍÌÎÏÓÒÕÔÖÚÙÛÜÇ',
            'aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC'),
          r'\s+', ' ')
        )), '||',
        seller_state
      ),
      '00000||unknown||XX'
    ) AS location_key
  FROM source
)

SELECT * FROM cleaned