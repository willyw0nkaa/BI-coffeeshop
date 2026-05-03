-- Silver Layer: Transaksi bersih & lengkap
-- Gabungan sale_order + sale_order_line + product + category

SELECT
    -- Order info
    toInt32(sol.order_id)                        AS order_id,
    toInt32(so.partner_id)                       AS store_id,
    so.name                                      AS order_name,
    toDateTime(so.date_order)                    AS order_datetime, -- proses sorting berdasarkan datetime
    toDate(so.date_order)                        AS order_date, -- untuk analisis harian
    toHour(toDateTime(so.date_order))            AS order_hour, -- untuk analisis jam sibuk
    toMonth(toDateTime(so.date_order))           AS order_month, -- untuk analisis bulanan
    toString(toYear(toDateTime(so.date_order)))  AS order_year, -- untuk analisis tahunan

    -- Product info
    toInt32(sol.product_id)                      AS product_id,
    p.name                                       AS product_name,
    p.product_type                               AS product_type,
    pc.name                                      AS category_name,

    -- Sales info
    toInt32(sol.product_uom_qty)                 AS qty,
    toFloat64(sol.price_unit)                    AS unit_price,
    toFloat64(sol.price_subtotal)                AS subtotal

FROM bronze.raw_sale_order_line AS sol
LEFT JOIN bronze.raw_sale_order AS so
    ON sol.order_id = so.id
LEFT JOIN bronze.raw_product AS p
    ON sol.product_id = p.id
LEFT JOIN bronze.raw_product_category AS pc
    ON p.categ_id = pc.id

WHERE so.state = 'done'