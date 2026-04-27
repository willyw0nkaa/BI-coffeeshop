SELECT
    order_date,
    SUM(subtotal) AS total_revenue,
    COUNT(order_id) AS total_orders
FROM {{ ref('silver_transactions') }}
GROUP BY order_date