SELECT
    toStartOfMonth(s.order_date) AS month,
    SUM(s.subtotal) AS revenue,
    t.target_revenue,
    (SUM(s.subtotal) / t.target_revenue) * 100 AS achievement_pct

FROM {{ ref('silver_transactions') }} s
LEFT JOIN {{ ref('silver_targets') }} t
ON formatDateTime(s.order_date, '%Y-%m') = t.month

GROUP BY
    month,
    t.target_revenue