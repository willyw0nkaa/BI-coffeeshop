SELECT
    order_hour,
    COUNT(order_id) AS total_transactions
FROM {{ ref('silver_transactions') }}
GROUP BY order_hour
ORDER BY total_transactions DESC