import requests
import pandas as pd
from datetime import datetime

# Ambil tanggal sistem
current_date = datetime.now().strftime('%Y-%m-%d')

# Buat URL
url = f"https://svc-silinda.jabarprov.go.id//api/api/graphic_data/1/69/day/price/2021-03-28/{current_date}/0/market/-/eceran/null"

# Request ke API
response = requests.get(url)

# Cek apakah sukses
if response.status_code == 200:
    data = response.json()
    
    # Pastikan data yang diambil adalah list atau dict yang bisa diubah jadi DataFrame
    if isinstance(data, list):
        df = pd.DataFrame(data)
    elif isinstance(data, dict):
        # Jika berbentuk dict, ambil isi yang relevan
        if 'data' in data:
            df = pd.DataFrame(data['data'])
        else:
            df = pd.DataFrame([data])
    
    # Simpan ke CSV
    filename = f"data_silinda.csv"
    df.to_csv(filename, index=False)
    print(f"Data berhasil disimpan ke {filename}")
else:
    print(f"Request gagal dengan status code {response.status_code}")



