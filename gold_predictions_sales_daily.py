import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import clickhouse_connect

# ============================================================
# 1. LOAD DATA
# ============================================================
client = clickhouse_connect.get_client(
    host='localhost',
    port=8123,
    username='default',
    password=''
)

df = client.query_df("""
    SELECT order_date, total_revenue AS revenue
    FROM gold.gold_sales_daily
    ORDER BY order_date
""")

df['date'] = pd.to_datetime(df['order_date'])
df = df.drop(columns=['order_date'])
df = df.sort_values('date').reset_index(drop=True)
print(f"Data loaded: {len(df)} baris ({df['date'].min().date()} s/d {df['date'].max().date()})")

# ============================================================
# 2. FEATURE ENGINEERING
# ============================================================
df['day']         = df['date'].dt.day
df['month']       = df['date'].dt.month
df['day_of_week'] = df['date'].dt.dayofweek

df['lag_1']      = df['revenue'].shift(1)
df['lag_7']      = df['revenue'].shift(7)
df['rolling_7']  = df['revenue'].rolling(7).mean()
df['rolling_14'] = df['revenue'].rolling(14).mean()

df = df.dropna()
print(f"Setelah dropna: {len(df)} baris, mulai dari {df['date'].min().date()}")

# ============================================================
# 3. EXPANDING WINDOW — predict Maret s/d Juni
# ============================================================
FEATURES = ['day', 'month', 'day_of_week', 'lag_1', 'lag_7', 'rolling_7', 'rolling_14']

NAMA_BULAN = {3: 'Maret', 4: 'April', 5: 'Mei', 6: 'Juni'}

all_results = []

print("\n" + "="*65)
print("  EVALUASI MODEL PREDIKSI REVENUE HARIAN — PER BULAN")
print("="*65)

for test_month in range(3, 7):
    train = df[df['month'] < test_month].copy()
    test  = df[df['month'] == test_month].copy()

    model = LinearRegression()
    model.fit(train[FEATURES], train['revenue'])

    test = test.copy()
    test['prediction'] = model.predict(test[FEATURES])

    mae  = mean_absolute_error(test['revenue'], test['prediction'])
    rmse = np.sqrt(mean_squared_error(test['revenue'], test['prediction']))
    r2   = r2_score(test['revenue'], test['prediction'])
    mape = (abs(test['revenue'] - test['prediction']) / test['revenue']).mean() * 100

    # --- Interpretasi otomatis ---
    if mape < 5:
        kualitas = "SANGAT BAIK"
        catatan_mape = "Prediksi sangat presisi. Cocok sebagai dasar target revenue & perencanaan anggaran."
    elif mape < 10:
        kualitas = "BAIK"
        catatan_mape = "Prediksi andal. Layak digunakan untuk perencanaan operasional harian."
    elif mape < 15:
        kualitas = "CUKUP"
        catatan_mape = "Model masih berguna namun ada ketidakpastian. Tambahkan fitur hari libur untuk memperbaiki."
    else:
        kualitas = "PERLU PERBAIKAN"
        catatan_mape = "Error relatif tinggi. Pertimbangkan model yang lebih kompleks (tree-based, dsb)."

    if r2 >= 0.8:
        catatan_r2 = f"R²={r2:.3f}: Model menjelaskan {r2*100:.1f}% variansi revenue. Sangat representatif."
    elif r2 >= 0.6:
        catatan_r2 = f"R²={r2:.3f}: Model cukup representatif ({r2*100:.1f}% variansi terjelaskan)."
    else:
        catatan_r2 = f"R²={r2:.3f}: Model kurang representatif. Fitur tambahan diperlukan."

    # Hitung rata-rata revenue aktual bulan itu untuk konteks
    avg_rev = test['revenue'].mean()
    print(f"\n  Bulan: {NAMA_BULAN[test_month]} 2023  [Training: {len(train)} hari | Test: {len(test)} hari]")
    print(f"  Rata-rata revenue aktual : USD {avg_rev:,.0f}/hari")
    print(f"  {'─'*57}")
    print(f"  MAE   : USD {mae:,.2f}  → rata-rata kesalahan prediksi per hari")
    print(f"  RMSE  : USD {rmse:,.2f}  → penalti lebih besar untuk selisih besar")
    print(f"  R²    : {r2:.4f}         → {catatan_r2}")
    print(f"  MAPE  : {mape:.2f}%         → kualitas: {kualitas}")
    print(f"  Catatan: {catatan_mape}")

    all_results.append(test[['date', 'revenue', 'prediction']])

# ============================================================
# 4. RINGKASAN AKHIR
# ============================================================
result = pd.concat(all_results).sort_values('date').reset_index(drop=True)
result['prediction'] = result['prediction'].round(2)
result['date'] = result['date'].dt.date

print("\n" + "="*65)
print("  RINGKASAN STATISTIK PREDIKSI VS AKTUAL")
print("="*65)
summary_all = result.copy()
summary_all['month'] = pd.to_datetime(result['date']).dt.month
for m, grp in summary_all.groupby('month'):
    avg_act = grp['revenue'].mean()
    avg_pred = grp['prediction'].mean()
    diff_pct = ((avg_pred - avg_act) / avg_act) * 100
    print(f"  {NAMA_BULAN[m]:6s} | Aktual avg: USD {avg_act:,.0f} | Prediksi avg: USD {avg_pred:,.0f} | Selisih: {diff_pct:+.1f}%")

print(f"\n  Total baris hasil prediksi: {len(result)} baris (Maret–Juni 2023)")

# ============================================================
# 5. SIMPAN KE CSV
# ============================================================
result.to_csv("gold_predictions.csv", index=False)
print("\n  File gold_predictions.csv berhasil diperbarui!")

# ============================================================
# 7. PREDIKSI JULI 2023 (TANPA DATA AKTUAL)
# ============================================================
print("\n" + "="*65)
print("  PREDIKSI REVENUE HARIAN — JULI 2023 (FORECAST ONLY)")
print("="*65)

# Train dari seluruh data yang tersedia (Jan–Juni)
model_juli = LinearRegression()
model_juli.fit(df[FEATURES], df['revenue'])

# Buat dataframe Juli 2023
df_juli = pd.DataFrame({'date': pd.date_range('2023-07-01', '2023-07-31', freq='D')})
df_juli['day']         = df_juli['date'].dt.day
df_juli['month']       = df_juli['date'].dt.month
df_juli['day_of_week'] = df_juli['date'].dt.dayofweek

# Lag & rolling: ambil dari data historis (nilai terakhir yang tersedia)
last_revenue = df['revenue'].values
df_juli['lag_1']      = last_revenue[-1]   # nilai terakhir hari ke-1, geser manual per baris di bawah
df_juli['lag_7']      = last_revenue[-7]
df_juli['rolling_7']  = last_revenue[-7:].mean()
df_juli['rolling_14'] = last_revenue[-14:].mean()

# Untuk lag_1: setiap baris Juli menggunakan nilai hari sebelumnya secara sekuensial
# Hari 1 Juli → lag_1 = 30 Juni (last_revenue[-1])
# Hari 2 Juli → lag_1 = prediksi 1 Juli, dst (iteratif)
predictions_juli = []
lag_buffer = list(last_revenue[-14:])  # simpan 14 hari terakhir historis

for i, row in df_juli.iterrows():
    lag_1     = lag_buffer[-1]
    lag_7     = lag_buffer[-7]
    rolling_7 = np.mean(lag_buffer[-7:])
    rolling_14 = np.mean(lag_buffer[-14:])

    X = pd.DataFrame([{
        'day': row['day'], 'month': row['month'], 'day_of_week': row['day_of_week'],
        'lag_1': lag_1, 'lag_7': lag_7, 'rolling_7': rolling_7, 'rolling_14': rolling_14
    }])
    pred = model_juli.predict(X[FEATURES])[0]
    predictions_juli.append(round(pred, 2))
    lag_buffer.append(pred)  # gunakan prediksi sebagai lag hari berikutnya

df_juli['prediction'] = predictions_juli
df_juli['date'] = df_juli['date'].dt.date

# Ringkasan
avg_pred_juli = df_juli['prediction'].mean()
print(f"\n  Rata-rata prediksi revenue Juli 2023 : USD {avg_pred_juli:,.0f}/hari")
print(f"\n  Preview prediksi harian Juli 2023:")
print(df_juli[['date', 'day_of_week', 'prediction']].to_string(index=False))
print(f"\n  Total baris prediksi Juli: {len(df_juli)} baris")

# Simpan prediksi Juli ke CSV terpisah
result_juli = df_juli[['date', 'prediction']]
result_juli.to_csv("gold_predictions_juli.csv", index=False)
print("  File gold_predictions_juli.csv berhasil dibuat!")

# Load ke ClickHouse
try:
    client.command('''
        CREATE TABLE IF NOT EXISTS gold.gold_predictions_juli (
            date        Date,
            prediction  Float64
        ) ENGINE = MergeTree()
        ORDER BY date
    ''')
    client.command('TRUNCATE TABLE gold.gold_predictions_juli')
    client.insert_df('gold.gold_predictions_juli', result_juli)
    print("  Data berhasil masuk ke ClickHouse: gold.gold_predictions_juli")
except Exception as e:
    print(f"  Gagal connect ke ClickHouse: {e}")
    print("  Data tetap tersimpan di gold_predictions_juli.csv")

# ============================================================
# 6. LOAD KE CLICKHOUSE
# ============================================================
try:
    client.command('''
        CREATE TABLE IF NOT EXISTS gold.gold_predictions (
            date        Date,
            revenue     Float64,
            prediction  Float64
        ) ENGINE = MergeTree()
        ORDER BY date
    ''')
    client.command('TRUNCATE TABLE gold.gold_predictions')
    client.insert_df('gold.gold_predictions', result)
    print("  Data berhasil masuk ke ClickHouse: gold.gold_predictions")
except Exception as e:
    print(f"  Gagal connect ke ClickHouse: {e}")
    print("  Data tetap tersimpan di gold_predictions.csv")