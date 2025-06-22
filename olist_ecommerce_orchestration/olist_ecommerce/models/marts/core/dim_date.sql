WITH dates AS (
  SELECT
    DATE_ADD(DATE('2016-01-01'), INTERVAL day_offset DAY) AS date_day
  FROM UNNEST(GENERATE_ARRAY(
      0,
      DATE_DIFF(
          DATE_ADD(CURRENT_DATE(), INTERVAL 5 YEAR),
          DATE('2016-01-01'),                        
          DAY
      )
  )) AS day_offset
),

easter_sundays AS (
  SELECT
    EXTRACT(YEAR FROM d) AS year,
    DATE_FROM_UNIX_DATE(
      UNIX_DATE(DATE(EXTRACT(YEAR FROM d), 1, 1)) +
      (
        MOD(255 - 11 * MOD(EXTRACT(YEAR FROM d), 19), 30) + 21 +
        MOD(
          6 + MOD(
            EXTRACT(YEAR FROM d) +
            CAST(FLOOR(EXTRACT(YEAR FROM d) / 4) AS INT64) +
            CAST(FLOOR(
              (255 - 11 * MOD(EXTRACT(YEAR FROM d), 19)) / 30
            ) AS INT64), 7
          ), 7
        ) - 1
      )
    ) AS easter_sunday
  FROM (SELECT DISTINCT DATE(EXTRACT(YEAR FROM date_day), 1, 1) AS d FROM dates)
),

holidays_br AS (
  SELECT
    d.date_day,
    CASE
      WHEN FORMAT_DATE('%m-%d', d.date_day) = '01-01' THEN 'Ano Novo'
      WHEN FORMAT_DATE('%m-%d', d.date_day) = '04-21' THEN 'Tiradentes'
      WHEN FORMAT_DATE('%m-%d', d.date_day) = '05-01' THEN 'Dia do Trabalhador'
      WHEN FORMAT_DATE('%m-%d', d.date_day) = '09-07' THEN 'Independência do Brasil'
      WHEN FORMAT_DATE('%m-%d', d.date_day) = '10-12' THEN 'Nossa Senhora Aparecida'
      WHEN FORMAT_DATE('%m-%d', d.date_day) = '11-02' THEN 'Finados'
      WHEN FORMAT_DATE('%m-%d', d.date_day) = '11-15' THEN 'Proclamação da República'
      WHEN FORMAT_DATE('%m-%d', d.date_day) = '12-25' THEN 'Natal'
      WHEN d.date_day = e.easter_sunday - INTERVAL 48 DAY THEN 'Carnaval (Segunda-feira)'
      WHEN d.date_day = e.easter_sunday - INTERVAL 47 DAY THEN 'Carnaval (Terça-feira)'
      WHEN d.date_day = e.easter_sunday - INTERVAL 2 DAY THEN 'Sexta-feira Santa'
      WHEN d.date_day = e.easter_sunday THEN 'Páscoa'
      WHEN d.date_day = e.easter_sunday + INTERVAL 60 DAY THEN 'Corpus Christi'
      ELSE NULL
    END AS holiday_name
  FROM dates d
  LEFT JOIN easter_sundays e ON EXTRACT(YEAR FROM d.date_day) = e.year
)

SELECT
  CAST(FORMAT_DATE('%Y%m%d', d.date_day) AS INT64) AS date_id,
  d.date_day AS full_date,
  EXTRACT(YEAR FROM d.date_day) AS year,
  EXTRACT(MONTH FROM d.date_day) AS month,
  EXTRACT(DAY FROM d.date_day) AS day,
  EXTRACT(DAYOFWEEK FROM d.date_day) AS day_of_week,
  FORMAT_DATE('%A', d.date_day) AS day_name,
  FORMAT_DATE('%B', d.date_day) AS month_name,
  EXTRACT(WEEK FROM d.date_day) AS week_of_year,
  EXTRACT(QUARTER FROM d.date_day) AS quarter_of_year,
  FORMAT_DATE('%Y-%m', d.date_day) AS year_month,
  EXTRACT(DAYOFWEEK FROM d.date_day) IN (1, 7) AS is_weekend,
  hb.holiday_name IS NOT NULL AS is_holiday,
  hb.holiday_name,
  CASE
    WHEN EXTRACT(MONTH FROM d.date_day) IN (12, 1, 2) THEN 'Summer'
    WHEN EXTRACT(MONTH FROM d.date_day) IN (3, 4, 5) THEN 'Autumn'
    WHEN EXTRACT(MONTH FROM d.date_day) IN (6, 7, 8) THEN 'Winter'
    ELSE 'Spring'
  END AS season,
  EXTRACT(DAY FROM d.date_day) = 1 AS is_first_day_of_month,
  d.date_day = DATE_SUB(DATE_ADD(DATE_TRUNC(d.date_day, MONTH), INTERVAL 1 MONTH), INTERVAL 1 DAY) AS is_last_day_of_month
FROM dates d
LEFT JOIN holidays_br hb ON d.date_day = hb.date_day
ORDER BY d.date_day
