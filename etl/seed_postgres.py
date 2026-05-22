"""
seed_postgres.py
================
Script untuk memindahkan dataset Kaggle (Maven Roasters Coffee Shop)
ke PostgreSQL dengan struktur tabel simulasi ERP Odoo.

Urutan eksekusi:
  1. Jalankan init_postgres.sql dulu (buat tabel)
  2. Jalankan script ini (isi data)

Requirement:
  pip install pandas psycopg2-binary openpyxl python-dotenv
"""

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import os

# =============================================================
# KONFIGURASI KONEKSI
# Sesuaikan dengan docker-compose.yml kalian
# =============================================================
DB_CONFIG = {
    "host":     os.getenv("PG_HOST", "localhost"),
    "port":     os.getenv("PG_PORT", "5432"),
    "dbname":   os.getenv("PG_DB",   "coffee_shop"),
    "user":     os.getenv("PG_USER", "admin"),
    "password": os.getenv("PG_PASS", "admin"),
}

DATASET_PATH = "data/Coffee_Shop_Sales.xlsx"

# =============================================================
# MAPPING: product_category name → id
# =============================================================
CATEGORY_MAP = {
    "Coffee":              1,
    "Tea":                 2,
    "Drinking Chocolate":  3,
    "Bakery":              4,
    "Flavours":            5,
    "Loose Tea":           6,
    "Coffee beans":        7,
    "Packaged Chocolate":  8,
    "Branded":             9,
}


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def load_dataset():
    print("📂 Membaca dataset Kaggle...")
    df = pd.read_excel(DATASET_PATH)
    df["transaction_date"] = pd.to_datetime(df["transaction_date"])
    df["transaction_time"] = df["transaction_time"].astype(str)
    df["datetime_order"] = pd.to_datetime(
        df["transaction_date"].dt.strftime("%Y-%m-%d") + " " + df["transaction_time"]
    )
    df["price_subtotal"] = df["transaction_qty"] * df["unit_price"]
    print(f"✅ Dataset loaded: {len(df):,} baris")
    return df


def seed_product_template(cur, df):
    """Seed tabel product_template dari kolom produk di dataset."""
    print("🔄 Seeding product_template...")

    products = (
        df[["product_id", "product_detail", "product_type", "product_category", "unit_price"]]
        .drop_duplicates(subset=["product_id"])
        .copy()
    )

    rows = []
    for _, row in products.iterrows():
        rows.append((
            int(row["product_id"]),
            str(row["product_detail"]),
            str(row["product_type"]),
            CATEGORY_MAP.get(str(row["product_category"]), 1),
            float(row["unit_price"]),
        ))

    execute_values(cur, """
        INSERT INTO odoo_sim.product_template
            (id, name, product_type, categ_id, list_price)
        VALUES %s
        ON CONFLICT (id) DO NOTHING
    """, rows)

    print(f"  ✅ {len(rows)} produk di-insert")


def seed_sale_order(cur, df):
    """Seed tabel sale_order (header transaksi)."""
    print("🔄 Seeding sale_order...")

    # Hitung amount_total per transaction_id
    totals = df.groupby("transaction_id")["price_subtotal"].sum().reset_index()
    totals.columns = ["transaction_id", "amount_total"]

    # Ambil satu baris per transaksi (untuk date_order dan store_id)
    orders = df.drop_duplicates(subset=["transaction_id"])[
        ["transaction_id", "datetime_order", "store_id"]
    ].merge(totals, on="transaction_id")

    rows = []
    for _, row in orders.iterrows():
        tid = int(row["transaction_id"])
        rows.append((
            tid,
            f"SO/2023/{tid:06d}",      # kode order ala Odoo
            row["datetime_order"],
            int(row["store_id"]),
            "done",
            float(row["amount_total"]),
        ))

    # Insert dalam batch 5000
    batch_size = 5000
    total = 0
    for i in range(0, len(rows), batch_size):
        execute_values(cur, """
            INSERT INTO odoo_sim.sale_order
                (id, name, date_order, partner_id, state, amount_total)
            VALUES %s
            ON CONFLICT (id) DO NOTHING
        """, rows[i:i+batch_size])
        total += len(rows[i:i+batch_size])
        print(f"  ... {total:,} order di-insert")

    print(f"  ✅ Total {total:,} sale_order di-insert")


def seed_sale_order_line(cur, df):
    """Seed tabel sale_order_line (detail item transaksi)."""
    print("🔄 Seeding sale_order_line...")

    rows = []
    for _, row in df.iterrows():
        rows.append((
            int(row["transaction_id"]),
            int(row["product_id"]),
            int(row["transaction_qty"]),
            float(row["unit_price"]),
            float(row["price_subtotal"]),
        ))

    batch_size = 5000
    total = 0
    for i in range(0, len(rows), batch_size):
        execute_values(cur, """
            INSERT INTO odoo_sim.sale_order_line
                (order_id, product_id, product_uom_qty, price_unit, price_subtotal)
            VALUES %s
        """, rows[i:i+batch_size])
        total += len(rows[i:i+batch_size])
        print(f"  ... {total:,} baris di-insert")

    print(f"  ✅ Total {total:,} sale_order_line di-insert")


def main():
    print("=" * 50)
    print("🚀 Seeding PostgreSQL (Simulasi ERP Odoo)")
    print("=" * 50)

    df = load_dataset()

    conn = get_conn()
    conn.autocommit = False
    cur = conn.cursor()

    try:
        seed_product_template(cur, df)
        seed_sale_order(cur, df)
        seed_sale_order_line(cur, df)
        conn.commit()
        print("\n✅ Semua data berhasil di-seed ke PostgreSQL!")
    except Exception as e:
        conn.rollback()
        print(f"\n❌ Error: {e}")
        raise
    finally:
        cur.close()
        conn.close()

    print("\n📊 Ringkasan tabel:")
    print(f"  product_template  : {df['product_id'].nunique()} produk")
    print(f"  sale_order        : {df['transaction_id'].nunique():,} transaksi")
    print(f"  sale_order_line   : {len(df):,} baris")
    print("\n✅ Selesai! PostgreSQL siap digunakan sebagai sumber ERP.")


if __name__ == "__main__":
    main()