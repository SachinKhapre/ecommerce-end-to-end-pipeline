SELECT
  user_id,
  total_spent,
  CASE
    WHEN total_spent > 500 THEN 'high_value'
    WHEN total_spent > 100 THEN 'medium_value'
    ELSE 'low_value'
  END AS ltv_segment
FROM {{ ref('stg_customer_revenue') }}
