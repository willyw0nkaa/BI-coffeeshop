SELECT
    order_date,
    order_hour,
    COUNT(order_id) AS total_transactions
FROM {{ ref('silver_transactions') }}
GROUP BY order_date, order_hour
ORDER BY order_date, order_hour