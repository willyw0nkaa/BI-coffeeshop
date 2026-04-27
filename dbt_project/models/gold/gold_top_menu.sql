SELECT
    product_name,
    SUM(qty) AS total_qty,
    SUM(subtotal) AS total_revenue
FROM {{ ref('silver_transactions') }}
GROUP BY product_name
ORDER BY total_qty DESC