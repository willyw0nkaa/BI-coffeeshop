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