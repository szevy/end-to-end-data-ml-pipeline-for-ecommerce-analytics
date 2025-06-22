WITH payments AS (
  SELECT
    p.order_id,
    p.payment_sequential,
    p.payment_type,
    p.payment_installments,
    p.payment_value,
    CASE WHEN p.payment_installments > 1 THEN 1 ELSE 0 END AS is_installment,
    ROUND(p.payment_value / NULLIF(p.payment_installments, 0), 2) AS installment_value,
    CASE WHEN p.payment_type = 'credit_card' THEN 1 ELSE 0 END AS payment_type_flag_credit,
    p.load_timestamp
  FROM {{ ref('stg_olist_order_payments') }} p
),

orders AS (
  SELECT
    order_id,
    order_approved_at,
    customer_id
  FROM {{ ref('stg_olist_orders') }}
  WHERE order_approved_at IS NOT NULL 
),

payment_dates AS (
  SELECT
    pay.*,
    orders.order_approved_at,
    orders.customer_id,     
    DATE(orders.order_approved_at) AS payment_date
  FROM payments pay
  LEFT JOIN orders ON pay.order_id = orders.order_id
  WHERE orders.order_approved_at IS NOT NULL
)


SELECT
  order_id,
  customer_id,
  payment_sequential,
  payment_type,
  payment_installments,
  payment_value,
  is_installment,
  installment_value,
  payment_type_flag_credit,
  load_timestamp,
  CASE
    WHEN payment_date IS NULL THEN NULL 
    ELSE EXTRACT(YEAR FROM payment_date)*10000 + 
         EXTRACT(MONTH FROM payment_date)*100 + 
         EXTRACT(DAY FROM payment_date)
  END AS payment_date_id
FROM payment_dates