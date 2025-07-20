WITH source AS (
  SELECT * FROM {{ source('raw_' ~ env_var('PROJECT_NAME'), 'customers') }}
),

cleaned AS (
  SELECT
    customer_id,
    customer_unique_id,
    
    LPAD(CAST(customer_zip_code_prefix AS STRING), 5, '0') AS zip_code_prefix,

    LOWER(TRIM(REGEXP_REPLACE(
      TRANSLATE(customer_city,
        'áàãâäéèêëíìîïóòõôöúùûüçÁÀÃÂÄÉÈÊËÍÌÎÏÓÒÕÔÖÚÙÛÜÇ',
        'aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC'),
      r'\s+', ' ')
    )) AS city,

    customer_state AS state,
    load_timestamp,

    COALESCE(
      CONCAT(
        LPAD(CAST(customer_zip_code_prefix AS STRING), 5, '0'), '||',
        LOWER(TRIM(REGEXP_REPLACE(
          TRANSLATE(customer_city,
            'áàãâäéèêëíìîïóòõôöúùûüçÁÀÃÂÄÉÈÊËÍÌÎÏÓÒÕÔÖÚÙÛÜÇ',
            'aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC'),
          r'\s+', ' ')
        )), '||',
        customer_state
      ),
      '00000||unknown||XX'
    ) AS location_key
  FROM source
)

SELECT * FROM cleaned