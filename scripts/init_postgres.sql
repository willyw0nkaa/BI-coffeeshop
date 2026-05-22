-- =============================================================
-- init_postgres.sql
-- Simulasi ERP Odoo untuk Dataset Maven Roasters Coffee Shop
-- Sumber: https://www.kaggle.com/datasets/ahmedabbas757/coffee-sales
-- =============================================================

-- Buat schema khusus untuk simulasi Odoo
-- Database: coffee_shop (sesuai docker-compose.yml)
CREATE SCHEMA IF NOT EXISTS odoo_sim;

-- =============================================================
-- 1. TABEL: res_partner (Cabang / Store sebagai "Partner/Customer")
--    Di Odoo, cabang bisa direpresentasikan sebagai partner
-- =============================================================
DROP TABLE IF EXISTS odoo_sim.res_partner CASCADE;
CREATE TABLE odoo_sim.res_partner (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(100) NOT NULL,
    street          VARCHAR(200),
    city            VARCHAR(100),
    active          BOOLEAN DEFAULT TRUE,
    write_date      TIMESTAMP DEFAULT NOW()
);

INSERT INTO odoo_sim.res_partner (id, name, city, street) VALUES
    (3, 'Astoria',         'New York', 'Astoria, Queens'),
    (5, 'Lower Manhattan', 'New York', 'Lower Manhattan'),
    (8, 'Hell''s Kitchen', 'New York', 'Hell''s Kitchen, Midtown');


-- =============================================================
-- 2. TABEL: product_category
--    Kategori produk (Coffee, Tea, Bakery, dll.)
-- =============================================================
DROP TABLE IF EXISTS odoo_sim.product_category CASCADE;
CREATE TABLE odoo_sim.product_category (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(100) NOT NULL,
    complete_name   VARCHAR(200),
    write_date      TIMESTAMP DEFAULT NOW()
);

INSERT INTO odoo_sim.product_category (id, name, complete_name) VALUES
    (1,  'Coffee',              'All / Coffee'),
    (2,  'Tea',                 'All / Tea'),
    (3,  'Drinking Chocolate',  'All / Drinking Chocolate'),
    (4,  'Bakery',              'All / Bakery'),
    (5,  'Flavours',            'All / Flavours'),
    (6,  'Loose Tea',           'All / Loose Tea'),
    (7,  'Coffee beans',        'All / Coffee beans'),
    (8,  'Packaged Chocolate',  'All / Packaged Chocolate'),
    (9,  'Branded',             'All / Branded');


-- =============================================================
-- 3. TABEL: product_template
--    Data master produk yang dijual di coffee shop
-- =============================================================
DROP TABLE IF EXISTS odoo_sim.product_template CASCADE;
CREATE TABLE odoo_sim.product_template (
    id              INTEGER PRIMARY KEY,   -- product_id dari dataset Kaggle
    name            VARCHAR(200) NOT NULL, -- product_detail
    product_type    VARCHAR(100),          -- product_type dari Kaggle
    categ_id        INTEGER REFERENCES odoo_sim.product_category(id),
    list_price      NUMERIC(10,2),         -- unit_price
    active          BOOLEAN DEFAULT TRUE,
    write_date      TIMESTAMP DEFAULT NOW()
);

-- Di-seed via Python dari dataset Kaggle (lihat seed_postgres.py)
-- Contoh data akan di-insert oleh script Python


-- =============================================================
-- 4. TABEL: sale_order
--    Header transaksi penjualan (1 baris = 1 transaksi)
--    Diambil dari: transaction_id, transaction_date, transaction_time, store_id
-- =============================================================
DROP TABLE IF EXISTS odoo_sim.sale_order CASCADE;
CREATE TABLE odoo_sim.sale_order (
    id              INTEGER PRIMARY KEY,       -- transaction_id dari Kaggle
    name            VARCHAR(50),               -- kode order, misal: SO/2023/00001
    date_order      TIMESTAMP NOT NULL,        -- gabungan transaction_date + transaction_time
    partner_id      INTEGER REFERENCES odoo_sim.res_partner(id), -- store_id
    state           VARCHAR(20) DEFAULT 'done',
    amount_total    NUMERIC(12,2),             -- dihitung dari sum(price_subtotal)
    write_date      TIMESTAMP DEFAULT NOW()
);


-- =============================================================
-- 5. TABEL: sale_order_line
--    Detail item per transaksi
--    Diambil dari: transaction_id, product_id, transaction_qty, unit_price
-- =============================================================
DROP TABLE IF EXISTS odoo_sim.sale_order_line CASCADE;
CREATE TABLE odoo_sim.sale_order_line (
    id              SERIAL PRIMARY KEY,
    order_id        INTEGER REFERENCES odoo_sim.sale_order(id),
    product_id      INTEGER REFERENCES odoo_sim.product_template(id),
    product_uom_qty INTEGER NOT NULL,          -- transaction_qty
    price_unit      NUMERIC(10,2) NOT NULL,    -- unit_price
    price_subtotal  NUMERIC(12,2) NOT NULL,    -- transaction_qty * unit_price
    write_date      TIMESTAMP DEFAULT NOW()
);


-- =============================================================
-- INDEX untuk performa query
-- =============================================================
CREATE INDEX IF NOT EXISTS idx_sale_order_date      ON odoo_sim.sale_order(date_order);
CREATE INDEX IF NOT EXISTS idx_sale_order_partner   ON odoo_sim.sale_order(partner_id);
CREATE INDEX IF NOT EXISTS idx_sale_order_line_ord  ON odoo_sim.sale_order_line(order_id);
CREATE INDEX IF NOT EXISTS idx_sale_order_line_prod ON odoo_sim.sale_order_line(product_id);
CREATE INDEX IF NOT EXISTS idx_product_categ        ON odoo_sim.product_template(categ_id);