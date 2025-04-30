import requests
import pandas as pd
from datetime import datetime

# Ambil tanggal sistem
current_date = datetime.now().strftime('%Y-%m-%d')

# Daftar URL dan nama file yang ingin disimpan
urls = {
    "medium_cikurubuk": f"https://svc-silinda.jabarprov.go.id/api/api/graphic_data/1/69/day/price/2022-02-09/{current_date}/0/market/-/eceran/null",
    "medium_pancasila": f"https://svc-silinda.jabarprov.go.id/api/api/graphic_data/1/70/day/price/2022-02-09/{current_date}/0/market/-/eceran/null",
    "premium_cikurubuk": f"https://svc-silinda.jabarprov.go.id/api/api/graphic_data/24/69/day/price/2022-02-09/{current_date}/0/market/-/eceran/null",
    "premium_pancasila": f"https://svc-silinda.jabarprov.go.id/api/api/graphic_data/24/70/day/price/2022-02-09/{current_date}/0/market/-/eceran/null"
}

# Looping setiap URL
for name, url in urls.items():
    print(f"Memproses {name}...")
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            
            # Pastikan data bisa diubah ke DataFrame
            if isinstance(data, list):
                df = pd.DataFrame(data)
            elif isinstance(data, dict):
                if 'data' in data:
                    df = pd.DataFrame(data['data'])
                else:
                    df = pd.DataFrame([data])
            else:
                print(f"Format data tidak dikenali untuk {name}.")
                continue
            
            # Simpan ke file CSV
            filename = f"{name}.csv"
            df.to_csv(filename, index=False)
            print(f"Data {name} berhasil disimpan ke {filename}")
        else:
            print(f"Request untuk {name} gagal dengan status code {response.status_code}")
    except Exception as e:
        print(f"Terjadi error saat memproses {name}: {e}")