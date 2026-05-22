WITH monthly_sales AS (
    SELECT
        toStartOfMonth(order_date) AS month,
        SUM(subtotal) AS revenue
    FROM {{ ref('silver_transactions') }}
    GROUP BY month
),

monthly_target AS (
    SELECT
        toStartOfMonth(parseDateTimeBestEffort(month)) AS month,
        MAX(target_revenue) AS target_revenue
    FROM {{ ref('silver_targets') }}
    GROUP BY month
)

SELECT
    s.month,
    s.revenue,
    t.target_revenue,
    (s.revenue / t.target_revenue) * 100 AS achievement_pct
FROM monthly_sales s
LEFT JOIN monthly_target t
ON s.month = t.month
ORDER BY s.month