SELECT
  product_id,
  title,
  total_revenue,
  RANK() OVER (ORDER BY total_revenue DESC) AS product_rank
FROM {{ ref('stg_product_revenue') }}
