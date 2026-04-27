"""
extract_to_bronze.py
====================
Script untuk ekstrak data dari PostgreSQL (simulasi ERP)
lalu memuatnya ke ClickHouse Bronze layer.

Jalankan SETELAH init_clickhouse.py

Requirement:
    pip install pandas psycopg2-binary clickhouse-connect
"""

import pandas as pd
import psycopg2
import clickhouse_connect
from datetime import datetime

# =============================================================
# KONFIGURASI KONEKSI
# =============================================================
PG_CONFIG = {
    "host":     "localhost",
    "port":     "5432",
    "dbname":   "coffee_shop",
    "user":     "admin",
    "password": "admin",
}

CH_CONFIG = {
    "host":     "localhost",
    "port":     8123,
    "username": "default",
    "password": "",
}

# Path file CSV target penjualan (dibuat manual oleh tim)
TARGETS_CSV = "data/sample_targets.csv"


# =============================================================
# KONEKSI
# =============================================================
def get_pg_conn():
    return psycopg2.connect(**PG_CONFIG)

def get_ch_client():
    return clickhouse_connect.get_client(**CH_CONFIG)


# =============================================================
# EXTRACT DARI POSTGRESQL
# =============================================================
def extract_table(conn, query):
    """Ekstrak data dari PostgreSQL sebagai DataFrame."""
    return pd.read_sql(query, conn)


def to_string_df(df):
    """Konversi semua kolom ke String (sesuai Bronze layer)."""
    return df.astype(str).replace("NaT", "").replace("None", "")


# =============================================================
# LOAD KE CLICKHOUSE BRONZE
# =============================================================
def load_to_bronze(client, df, table_name):
    """Load DataFrame ke tabel Bronze ClickHouse."""
    print(f"  📥 Loading ke bronze.{table_name}...")

    # Truncate dulu sebelum load (idempotent)
    client.command(f"TRUNCATE TABLE IF EXISTS bronze.{table_name}")

    # Insert data
    client.insert_df(f"bronze.{table_name}", df)
    print(f"  ✅ {len(df):,} baris berhasil dimuat ke bronze.{table_name}")


# =============================================================
# PIPELINE TIAP TABEL
# =============================================================
def extract_sale_order(conn, client):
    print("\n🔄 Ekstrak sale_order...")
    df = extract_table(conn, """
        SELECT
            id::text,
            name,
            date_order::text,
            partner_id::text,
            state,
            amount_total::text,
            write_date::text
        FROM odoo_sim.sale_order
    """)
    df = to_string_df(df)
    load_to_bronze(client, df, "raw_sale_order")


def extract_sale_order_line(conn, client):
    print("\n🔄 Ekstrak sale_order_line...")
    df = extract_table(conn, """
        SELECT
            id::text,
            order_id::text,
            product_id::text,
            product_uom_qty::text,
            price_unit::text,
            price_subtotal::text,
            write_date::text
        FROM odoo_sim.sale_order_line
    """)
    df = to_string_df(df)
    load_to_bronze(client, df, "raw_sale_order_line")


def extract_product(conn, client):
    print("\n🔄 Ekstrak product_template...")
    df = extract_table(conn, """
        SELECT
            id::text,
            name,
            product_type,
            categ_id::text,
            list_price::text,
            write_date::text
        FROM odoo_sim.product_template
    """)
    df = to_string_df(df)
    load_to_bronze(client, df, "raw_product")


def extract_product_category(conn, client):
    print("\n🔄 Ekstrak product_category...")
    df = extract_table(conn, """
        SELECT
            id::text,
            name,
            complete_name,
            write_date::text
        FROM odoo_sim.product_category
    """)
    df = to_string_df(df)
    load_to_bronze(client, df, "raw_product_category")


def extract_targets(client):
    """Load file CSV target penjualan ke Bronze."""
    print("\n🔄 Ekstrak targets dari CSV...")
    try:
        df = pd.read_csv(TARGETS_CSV)
        df = to_string_df(df)
        load_to_bronze(client, df, "raw_targets")
    except FileNotFoundError:
        print(f"  ⚠️  File {TARGETS_CSV} tidak ditemukan, skip.")
        print("       Buat file sample_targets.csv dulu di folder data/")


# =============================================================
# MAIN PIPELINE
# =============================================================
def main():
    print("=" * 50)
    print("🚀 Extract PostgreSQL → ClickHouse Bronze Layer")
    print(f"   Waktu: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)

    pg_conn = get_pg_conn()
    ch_client = get_ch_client()

    try:
        extract_sale_order(pg_conn, ch_client)
        extract_sale_order_line(pg_conn, ch_client)
        extract_product(pg_conn, ch_client)
        extract_product_category(pg_conn, ch_client)
        extract_targets(ch_client)

        print("\n" + "=" * 50)
        print("✅ Bronze layer berhasil diisi!")
        print("\n📊 Cek di DBeaver ClickHouse:")
        print("   bronze → raw_sale_order")
        print("   bronze → raw_sale_order_line")
        print("   bronze → raw_product")
        print("   bronze → raw_product_category")
        print("   bronze → raw_targets")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        raise
    finally:
        pg_conn.close()


if __name__ == "__main__":
    main()