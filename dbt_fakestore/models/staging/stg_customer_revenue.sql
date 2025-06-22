SELECT
    user_id,
    total_spent::numeric AS total_spent
FROM {{ source('public', 'stg_customer_revenue') }}