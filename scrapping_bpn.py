import requests
import csv
import os
from datetime import datetime
import pandas as pd

# URL API
url = "https://api-panelhargav2.badanpangan.go.id/api/front/harga-pangan-informasi"

# Parameter API
params = {
    "province_id": "12",  # Contoh: Jawa Barat
    "city_id": "184",     # Contoh: Kota Bandung
    "level_harga_id": "3" # Eceran
}

# Nama file CSV
csv_file_name = 'processed_beras_prices.csv'

# Format tanggal
today_date = datetime.today()
date_str = today_date.strftime('%Y-%m-%d')

# Fungsi untuk cek apakah data tanggal sudah ada
def is_date_already_in_csv(date, file_name):
    if not os.path.exists(file_name):
        return False
    with open(file_name, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        next(reader, None)  # skip header
        for row in reader:
            if row and row[0] == date:
                return True
    return False

# Tambahkan parameter tanggal
params['date'] = date_str

# Ambil data dari API
response = requests.get(url, params=params)

if response.status_code == 200:
    data = response.json()
    if data.get('status') == 'success':
        commodities = data.get('data', [])

        # Ambil harga today untuk Beras Medium dan Premium
        medium_price = None
        premium_price = None

        for item in commodities:
            if item.get('name') == 'Beras Medium':
                medium_price = item.get('today', None)
            elif item.get('name') == 'Beras Premium':
                premium_price = item.get('today', None)

        # Simpan jika belum ada
        if not is_date_already_in_csv(date_str, csv_file_name):
            try:
                with open(csv_file_name, 'a', newline='', encoding='utf-8') as file:
                    writer = csv.writer(file)

                    # Tulis header jika file kosong
                    if file.tell() == 0:
                        writer.writerow(['Date', 'Medium', 'Premium'])

                    writer.writerow([date_str, medium_price, premium_price])
                    print("✅ Data berhasil ditambahkan.")
            except Exception as e:
                print(f"❌ Gagal menulis ke file: {e}")
        else:
            print(f"ℹ️ Data untuk tanggal {date_str} sudah ada.")
    else:
        print("❌ Data tidak tersedia dari API.")
else:
    print(f"❌ Gagal request API: status code {response.status_code}")

# === Langkah Imputasi Data ===
import numpy as np

try:
    df = pd.read_csv(csv_file_name)

    # Bersihkan data dan konversi ke float
    df['Medium'] = pd.to_numeric(df['Medium'], errors='coerce')
    df['Premium'] = pd.to_numeric(df['Premium'], errors='coerce')

    # Ganti nilai 0 dengan NaN
    df[['Medium', 'Premium']] = df[['Medium', 'Premium']].replace(0, np.nan)

    # Interpolasi linear antar nilai
    df[['Medium', 'Premium']] = df[['Medium', 'Premium']].interpolate(method='linear', limit_direction='both')

    # Simpan kembali hasil imputasi
    df.to_csv(csv_file_name, index=False)
    print("🧹 Imputasi selesai. Nilai kosong dan 0 sudah diganti dengan interpolasi antar nilai.")
except Exception as e:
    print(f"❌ Gagal melakukan imputasi: {e}")

