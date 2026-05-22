"""
init_clickhouse.py
==================
Script untuk membuat database Bronze, Silver, Gold di ClickHouse.
Jalankan script ini SEKALI sebelum extract_to_bronze.py

Requirement:
    pip install clickhouse-connect
"""

import clickhouse_connect

# =============================================================
# KONFIGURASI KONEKSI CLICKHOUSE
# Sesuaikan dengan docker-compose.yml
# =============================================================
CH_CONFIG = {
    "host":     "localhost",
    "port":     8123,
    "username": "default",
    "password": "",
}


def get_client():
    return clickhouse_connect.get_client(**CH_CONFIG)


def init_databases(client):
    print("🔄 Membuat database Bronze, Silver, Gold...")

    client.command("CREATE DATABASE IF NOT EXISTS bronze")
    print("  ✅ Database 'bronze' siap")

    client.command("CREATE DATABASE IF NOT EXISTS silver")
    print("  ✅ Database 'silver' siap")

    client.command("CREATE DATABASE IF NOT EXISTS gold")
    print("  ✅ Database 'gold' siap")


def init_bronze_tables(client):
    print("\n🔄 Membuat tabel di Bronze layer...")

    # Semua kolom bertipe String (data mentah apa adanya)

    client.command("""
        CREATE TABLE IF NOT EXISTS bronze.raw_sale_order (
            id              String,
            name            String,
            date_order      String,
            partner_id      String,
            state           String,
            amount_total    String,
            write_date      String
        ) ENGINE = MergeTree()
        ORDER BY id
    """)
    print("  ✅ Tabel bronze.raw_sale_order siap")

    client.command("""
        CREATE TABLE IF NOT EXISTS bronze.raw_sale_order_line (
            id              String,
            order_id        String,
            product_id      String,
            product_uom_qty String,
            price_unit      String,
            price_subtotal  String,
            write_date      String
        ) ENGINE = MergeTree()
        ORDER BY id
    """)
    print("  ✅ Tabel bronze.raw_sale_order_line siap")

    client.command("""
        CREATE TABLE IF NOT EXISTS bronze.raw_product (
            id              String,
            name            String,
            product_type    String,
            categ_id        String,
            list_price      String,
            write_date      String
        ) ENGINE = MergeTree()
        ORDER BY id
    """)
    print("  ✅ Tabel bronze.raw_product siap")

    client.command("""
        CREATE TABLE IF NOT EXISTS bronze.raw_product_category (
            id              String,
            name            String,
            complete_name   String,
            write_date      String
        ) ENGINE = MergeTree()
        ORDER BY id
    """)
    print("  ✅ Tabel bronze.raw_product_category siap")

    client.command("""
        CREATE TABLE IF NOT EXISTS bronze.raw_targets (
            month           String,
            store_id        String,
            store_location  String,
            target_revenue  String
        ) ENGINE = MergeTree()
        ORDER BY (month, store_id)
    """)
    print("  ✅ Tabel bronze.raw_targets siap")


def main():
    print("=" * 50)
    print("🚀 Inisialisasi ClickHouse (Bronze/Silver/Gold)")
    print("=" * 50)

    client = get_client()
    init_databases(client)
    init_bronze_tables(client)

    print("\n✅ ClickHouse siap digunakan!")
    print("\n📊 Struktur yang dibuat:")
    print("  bronze → raw_sale_order, raw_sale_order_line,")
    print("           raw_product, raw_product_category, raw_targets")
    print("  silver → (akan diisi oleh dbt)")
    print("  gold   → (akan diisi oleh dbt)")


if __name__ == "__main__":
    main()