import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import numpy as np
import clickhouse_connect

# ============================================================
# 1. LOAD DATA
# ============================================================
df = pd.read_csv("gold_sales_daily.csv")

df.rename(columns={
    'order_date': 'date',
    'total_revenue': 'revenue'
}, inplace=True)

df['date'] = pd.to_datetime(df['date'])
df = df.sort_values('date').reset_index(drop=True)  # pastikan urut by date

# ============================================================
# 2. FEATURE ENGINEERING (sebelum split)
# ============================================================
df['day']         = df['date'].dt.day
df['month']       = df['date'].dt.month
df['day_of_week'] = df['date'].dt.dayofweek

# Lag features
df['lag_1'] = df['revenue'].shift(1)
df['lag_7'] = df['revenue'].shift(7)

# Rolling features (sesuai dokumen C3)
df['rolling_7']  = df['revenue'].rolling(7).mean()
df['rolling_30'] = df['revenue'].rolling(30).mean()

# Hapus null SEBELUM split (aman karena lag dihitung dari full df)
df = df.dropna()

# ============================================================
# 3. SPLIT TRAIN / TEST (by date, bukan random)
# ============================================================
train = df[df['date'] < '2023-06-01'].copy()
test  = df[df['date'] >= '2023-06-01'].copy()

features = ['day', 'month', 'day_of_week', 'lag_1', 'lag_7', 'rolling_7', 'rolling_30']

X_train = train[features]
y_train = train['revenue']

X_test = test[features]
y_test = test['revenue']

# ============================================================
# 4. MODEL & PREDICT
# ============================================================
model = LinearRegression()
model.fit(X_train, y_train)

test = test.copy()
test['prediction'] = model.predict(X_test)

# ============================================================
# 5. EVALUASI (MAE, RMSE, R² sesuai dokumen C3)
# ============================================================
mae  = mean_absolute_error(y_test, test['prediction'])
rmse = np.sqrt(mean_squared_error(y_test, test['prediction']))
r2   = r2_score(y_test, test['prediction'])

print("===== Evaluasi Model =====")
print(f"MAE  : {mae:.2f}")
print(f"RMSE : {rmse:.2f}")
print(f"R²   : {r2:.4f}")

# ============================================================
# 6. SIMPAN KE CSV (overwrite yang lama)
# ============================================================
result = test[['date', 'revenue', 'prediction']].copy()
result.to_csv("gold_predictions.csv", index=False)
print("\nFile gold_predictions.csv berhasil diperbarui!")

# ============================================================
# 7. LOAD KE CLICKHOUSE (sesuai arsitektur C2)
# ============================================================
try:
    client = clickhouse_connect.get_client(
        host='localhost',
        port=8123,
        username='default',
        password=''
    )

    # Buat tabel jika belum ada
    client.command('''
        CREATE TABLE IF NOT EXISTS gold.gold_predictions (
            date        Date,
            revenue     Float64,
            prediction  Float64
        ) ENGINE = MergeTree()
        ORDER BY date
    ''')

    # Truncate dulu supaya tidak duplikat saat run ulang
    client.command('TRUNCATE TABLE gold.gold_predictions')

    # Insert data
    client.insert_df('gold.gold_predictions', result)
    print("Data berhasil masuk ke ClickHouse tabel gold.gold_predictions!")

except Exception as e:
    print(f"Gagal connect ke ClickHouse: {e}")
    print("Data tetap tersimpan di gold_predictions.csv")