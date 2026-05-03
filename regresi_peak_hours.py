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
print(f"Data loaded: {len(df)} baris")

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

# Lag per jam (geser 1 hari untuk jam yang sama)
df = df.sort_values(['order_hour', 'order_date']).reset_index(drop=True)
df['lag_1_day'] = df.groupby('order_hour')['total_transactions'].shift(1)
df['lag_7_day'] = df.groupby('order_hour')['total_transactions'].shift(7)
df['rolling_7'] = df.groupby('order_hour')['total_transactions'].transform(
    lambda x: x.shift(1).rolling(7).mean()
)

df = df.dropna()
df = df.sort_values(['order_date', 'order_hour']).reset_index(drop=True)

# ============================================================
# 3. SPLIT TRAIN / TEST (by date)
# ============================================================
train = df[df['order_date'] < '2023-06-01'].copy()
test  = df[df['order_date'] >= '2023-06-01'].copy()

features = [
    'order_hour', 'hour_squared',
    'day', 'month', 'day_of_week',
    'is_morning', 'is_lunch', 'is_afternoon', 'is_evening',
    'lag_1_day', 'lag_7_day', 'rolling_7'
]

X_train = train[features]
y_train = train['total_transactions']
X_test  = test[features]
y_test  = test['total_transactions']

# ============================================================
# 4. MODEL & PREDICT
# ============================================================
model = LinearRegression()
model.fit(X_train, y_train)

test = test.copy()
test['prediction'] = model.predict(X_test).round().clip(0).astype(int)

# ============================================================
# 5. EVALUASI
# ============================================================
mae  = mean_absolute_error(y_test, test['prediction'])
rmse = np.sqrt(mean_squared_error(y_test, test['prediction']))
r2   = r2_score(y_test, test['prediction'])

print("\n===== Evaluasi Model Peak Hours =====")
print(f"MAE  : {mae:.2f}")
print(f"RMSE : {rmse:.2f}")
print(f"R²   : {r2:.4f}")

# ============================================================
# 6. SIMPAN KE CLICKHOUSE
# ============================================================
result = test[['order_date', 'order_hour', 'total_transactions', 'prediction']].copy()
result['order_date'] = result['order_date'].dt.date

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
    print("\nData berhasil masuk ke ClickHouse tabel gold.gold_predictions_peak_hours!")

except Exception as e:
    print(f"Gagal insert ke ClickHouse: {e}")

result.to_csv("gold_predictions_peak_hours.csv", index=False)
print("File gold_predictions_peak_hours.csv berhasil dibuat!")

# Ringkasan prediksi per jam
print("\n===== Rata-rata Prediksi vs Aktual Per Jam =====")
summary = result.groupby('order_hour').agg(
    aktual=('total_transactions', 'mean'),
    prediksi=('prediction', 'mean')
).round(1)
print(summary)