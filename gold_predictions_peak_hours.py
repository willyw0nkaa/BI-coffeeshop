import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import clickhouse_connect

# ============================================================
# 1. LOAD DATA DARI CLICKHOUSE
# ============================================================
client = clickhouse_connect.get_client(
    host='localhost',
    port=8123,
    username='default',
    password=''
)

df = client.query_df("""
    SELECT order_date, order_hour, total_transactions
    FROM gold.gold_peak_hours
    ORDER BY order_date, order_hour
""")

df['order_date'] = pd.to_datetime(df['order_date'])
print(f"Data loaded: {len(df)} baris ({df['order_date'].min().date()} s/d {df['order_date'].max().date()})")

# ============================================================
# 2. FEATURE ENGINEERING
# ============================================================
df['day']         = df['order_date'].dt.day
df['month']       = df['order_date'].dt.month
df['day_of_week'] = df['order_date'].dt.dayofweek
df['hour_squared'] = df['order_hour'] ** 2

# Kategori waktu
df['is_morning']   = ((df['order_hour'] >= 6)  & (df['order_hour'] <= 10)).astype(int)
df['is_lunch']     = ((df['order_hour'] >= 11) & (df['order_hour'] <= 13)).astype(int)
df['is_afternoon'] = ((df['order_hour'] >= 14) & (df['order_hour'] <= 17)).astype(int)
df['is_evening']   = ((df['order_hour'] >= 18) & (df['order_hour'] <= 21)).astype(int)

# Lag per jam
df = df.sort_values(['order_hour', 'order_date']).reset_index(drop=True)
df['lag_1_day'] = df.groupby('order_hour')['total_transactions'].shift(1)
df['lag_7_day'] = df.groupby('order_hour')['total_transactions'].shift(7)
df['rolling_7'] = df.groupby('order_hour')['total_transactions'].transform(
    lambda x: x.shift(1).rolling(7).mean()
)

df = df.dropna()
df = df.sort_values(['order_date', 'order_hour']).reset_index(drop=True)
print(f"Setelah dropna: {len(df)} baris, mulai dari {df['order_date'].min().date()}")

# ============================================================
# 3. EXPANDING WINDOW — predict Maret s/d Juni
# ============================================================
FEATURES = [
    'order_hour', 'hour_squared',
    'day', 'month', 'day_of_week',
    'is_morning', 'is_lunch', 'is_afternoon', 'is_evening',
    'lag_1_day', 'lag_7_day', 'rolling_7'
]

NAMA_BULAN = {3: 'Maret', 4: 'April', 5: 'Mei', 6: 'Juni'}

all_results = []

print("\n" + "="*65)
print("  EVALUASI MODEL PREDIKSI PEAK HOURS — PER BULAN")
print("="*65)

for test_month in range(3, 7):
    train = df[df['month'] < test_month].copy()
    test  = df[df['month'] == test_month].copy()

    model = LinearRegression()
    model.fit(train[FEATURES], train['total_transactions'])

    test = test.copy()
    test['prediction'] = model.predict(test[FEATURES]).round().clip(0).astype(int)

    mae  = mean_absolute_error(test['total_transactions'], test['prediction'])
    rmse = np.sqrt(mean_squared_error(test['total_transactions'], test['prediction']))
    r2   = r2_score(test['total_transactions'], test['prediction'])
    mape = (abs(test['total_transactions'] - test['prediction']) /
            test['total_transactions'].replace(0, np.nan)).mean() * 100

    # --- Interpretasi otomatis ---
    if mape < 10:
        kualitas = "SANGAT BAIK"
        catatan_mape = "Model sangat akurat. Prediksi layak digunakan untuk penjadwalan staf."
    elif mape < 20:
        kualitas = "BAIK"
        catatan_mape = "Model cukup andal. Kesalahan masih dalam batas wajar operasional."
    elif mape < 30:
        kualitas = "CUKUP"
        catatan_mape = "Model masih berguna namun perlu diperkuat dengan fitur tambahan (misal: hari libur)."
    else:
        kualitas = "PERLU PERBAIKAN"
        catatan_mape = "Error cukup tinggi. Pertimbangkan model non-linear atau tambahan data eksternal."

    if r2 >= 0.8:
        catatan_r2 = f"R²={r2:.3f}: Model menjelaskan {r2*100:.1f}% variansi transaksi. Sangat representatif."
    elif r2 >= 0.6:
        catatan_r2 = f"R²={r2:.3f}: Model cukup representatif ({r2*100:.1f}% variansi terjelaskan)."
    else:
        catatan_r2 = f"R²={r2:.3f}: Model kurang representatif. Terdapat faktor lain yang belum ditangkap."

    print(f"\n  Bulan: {NAMA_BULAN[test_month]} 2023  [Training: {len(train)} baris | Test: {len(test)} baris]")
    print(f"  {'─'*57}")
    print(f"  MAE   : {mae:.2f} transaksi  → rata-rata selisih prediksi vs aktual")
    print(f"  RMSE  : {rmse:.2f} transaksi  → penalti lebih besar untuk error besar")
    print(f"  R²    : {r2:.4f}            → {catatan_r2}")
    print(f"  MAPE  : {mape:.2f}%            → kualitas: {kualitas}")
    print(f"  Catatan: {catatan_mape}")

    all_results.append(test[['order_date', 'order_hour', 'total_transactions', 'prediction']])

# ============================================================
# 4. RINGKASAN AKHIR
# ============================================================
result = pd.concat(all_results).sort_values(['order_date', 'order_hour']).reset_index(drop=True)
result['order_date'] = result['order_date'].dt.date

print("\n" + "="*65)
print("  RINGKASAN RATA-RATA PREDIKSI VS AKTUAL PER JAM")
print("="*65)
summary = result.groupby('order_hour').agg(
    aktual=('total_transactions', 'mean'),
    prediksi=('prediction', 'mean')
).round(1)
summary['selisih'] = (summary['prediksi'] - summary['aktual']).round(1)
summary['error_%'] = ((summary['selisih'].abs() / summary['aktual']) * 100).round(1)
print(summary.to_string())

print(f"\n  Total baris hasil prediksi: {len(result)} baris (Maret–Juni 2023)")

# ============================================================
# 5. SIMPAN KE CSV
# ============================================================
result.to_csv("gold_predictions_peak_hours.csv", index=False)
print("\n  File gold_predictions_peak_hours.csv berhasil diperbarui!")

# ============================================================
# 7. PREDIKSI JULI 2023 (TANPA DATA AKTUAL)
# ============================================================
print("\n" + "="*65)
print("  PREDIKSI PEAK HOURS — JULI 2023 (FORECAST ONLY)")
print("="*65)

# Train dari seluruh data yang tersedia (Jan–Juni)
model_juli = LinearRegression()
model_juli.fit(df[FEATURES], df['total_transactions'])

# Buat tanggal Juli 2023 x jam (hanya jam yang ada di data historis)
juli_dates = pd.date_range('2023-07-01', '2023-07-31', freq='D')
juli_hours = sorted(df['order_hour'].unique())
juli_index = pd.MultiIndex.from_product([juli_dates, juli_hours], names=['order_date', 'order_hour'])
df_juli = pd.DataFrame(index=juli_index).reset_index()

df_juli['day']          = df_juli['order_date'].dt.day
df_juli['month']        = df_juli['order_date'].dt.month
df_juli['day_of_week']  = df_juli['order_date'].dt.dayofweek
df_juli['hour_squared'] = df_juli['order_hour'] ** 2
df_juli['is_morning']   = ((df_juli['order_hour'] >= 6)  & (df_juli['order_hour'] <= 10)).astype(int)
df_juli['is_lunch']     = ((df_juli['order_hour'] >= 11) & (df_juli['order_hour'] <= 13)).astype(int)
df_juli['is_afternoon'] = ((df_juli['order_hour'] >= 14) & (df_juli['order_hour'] <= 17)).astype(int)
df_juli['is_evening']   = ((df_juli['order_hour'] >= 18) & (df_juli['order_hour'] <= 21)).astype(int)

# Lag & rolling: ambil dari data historis per jam (nilai terakhir yang tersedia)
df_sorted = df.sort_values(['order_hour', 'order_date'])

lag1_map  = df_sorted.groupby('order_hour')['total_transactions'].last().to_dict()
lag7_map  = {
    hour: grp['total_transactions'].iloc[-7] if len(grp) >= 7 else grp['total_transactions'].mean()
    for hour, grp in df_sorted.groupby('order_hour')
}
roll7_map = {
    hour: grp['total_transactions'].tail(7).mean()
    for hour, grp in df_sorted.groupby('order_hour')
}

df_juli['lag_1_day'] = df_juli['order_hour'].map(lag1_map)
df_juli['lag_7_day'] = df_juli['order_hour'].map(lag7_map)
df_juli['rolling_7'] = df_juli['order_hour'].map(roll7_map)

df_juli['prediction'] = model_juli.predict(df_juli[FEATURES]).round().clip(0).astype(int)
df_juli['order_date'] = df_juli['order_date'].dt.date

# Ringkasan per jam
summary_juli = df_juli.groupby('order_hour').agg(
    prediksi_avg=('prediction', 'mean')
).round(1)
print(f"\n  Rata-rata prediksi transaksi per jam — Juli 2023:")
print(summary_juli.to_string())

print(f"\n  Total baris prediksi Juli: {len(df_juli)} baris (31 hari x {len(juli_hours)} jam aktif)")

# Simpan prediksi Juli ke CSV terpisah
result_juli = df_juli[['order_date', 'order_hour', 'prediction']]
result_juli.to_csv("gold_predictions_peak_hours_juli.csv", index=False)
print("  File gold_predictions_peak_hours_juli.csv berhasil dibuat!")

# Load ke ClickHouse
try:
    client.command('''
        CREATE TABLE IF NOT EXISTS gold.gold_predictions_peak_hours_juli (
            order_date  Date,
            order_hour  UInt8,
            prediction  Int64
        ) ENGINE = MergeTree()
        ORDER BY (order_date, order_hour)
    ''')
    client.command('TRUNCATE TABLE gold.gold_predictions_peak_hours_juli')
    client.insert_df('gold_predictions_peak_hours_juli', result_juli, database='gold')
    print("  Data berhasil masuk ke ClickHouse: gold.gold_predictions_peak_hours_juli")
except Exception as e:
    print(f"  Gagal connect ke ClickHouse: {e}")
    print("  Data tetap tersimpan di gold_predictions_peak_hours_juli.csv")

# ============================================================
# 6. LOAD KE CLICKHOUSE
# ============================================================
try:
    client.command('''
        CREATE TABLE IF NOT EXISTS gold.gold_predictions_peak_hours (
            order_date          Date,
            order_hour          UInt8,
            total_transactions  UInt64,
            prediction          Int64
        ) ENGINE = MergeTree()
        ORDER BY (order_date, order_hour)
    ''')
    client.command('TRUNCATE TABLE gold.gold_predictions_peak_hours')
    client.insert_df('gold_predictions_peak_hours', result, database='gold')
    print("  Data berhasil masuk ke ClickHouse: gold.gold_predictions_peak_hours")
except Exception as e:
    print(f"  Gagal connect ke ClickHouse: {e}")
    print("  Data tetap tersimpan di gold_predictions_peak_hours.csv")