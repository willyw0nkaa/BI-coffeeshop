-- Silver Layer: Target penjualan bulanan per cabang

SELECT
    month                           AS month,
    toInt32(store_id)               AS store_id,
    store_location                  AS store_location,
    toFloat64(target_revenue)       AS target_revenue
FROM bronze.raw_targets