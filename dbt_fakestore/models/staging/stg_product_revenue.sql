SELECT
    product_id,
    title,
    total_revenue::numeric AS total_revenue
FROM {{ source('public', 'stg_product_revenue') }}