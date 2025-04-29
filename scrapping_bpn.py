import requests
import csv
from datetime import datetime

# Define the API URL
url = "https://api-panelhargav2.badanpangan.go.id/api/front/harga-pangan-informasi"

# Define the parameters for the API request
params = {
    "province_id": "12",  # ID provinsi
    "city_id": "184",     # ID kota
    "level_harga_id": "3" # ID tingkat harga (misalnya 3 = eceran)
}

# Nama file CSV untuk menyimpan data
csv_file_name = 'beras_premium_medium_prices.csv'

# Ambil tanggal hari ini
today_date = datetime.today()

# Format tanggal sesuai kebutuhan API dan penyimpanan
def get_date_string(date):
    return date.strftime('%Y-%m-%d')

# Fungsi untuk mengecek apakah data dengan tanggal dan nama komoditas sudah ada
def is_data_entry_already_in_csv(date, name, file_name):
    try:
        with open(file_name, 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            next(reader, None)  # Lewati header
            for row in reader:
                if row and row[0] == date and row[1] == name:
                    return True
    except FileNotFoundError:
        return False
    return False

# Siapkan list untuk menampung data hasil crawl
price_data = []

# Tambahkan parameter tanggal pada request
params['date'] = get_date_string(today_date)

# Lakukan request ke API
response = requests.get(url, params=params)

# Jika request sukses
if response.status_code == 200:
    data = response.json()

    # Jika status API sukses
    if data.get('status') == 'success':
        commodities = data.get('data', [])

        # Ambil hanya Beras Premium dan Beras Medium
        for item in commodities:
            if 'name' in item and item['name'] in ['Beras Premium', 'Beras Medium']:
                date_str = get_date_string(today_date)
                name = item['name']

                # Cek apakah data untuk komoditas dan tanggal tersebut sudah ada
                if not is_data_entry_already_in_csv(date_str, name, csv_file_name):
                    price_today = item['today']
                    price_yesterday = item['yesterday']
                    gap = item['gap']
                    gap_percentage = item['gap_percentage']
                    gap_change = item['gap_change']

                    price_data.append([
                        date_str, name, price_today, price_yesterday,
                        gap, gap_percentage, gap_change
                    ])
                else:
                    print(f"Data untuk {name} tanggal {date_str} sudah ada.")
    else:
        print(f"API tidak mengembalikan data yang diharapkan untuk {get_date_string(today_date)}")
else:
    print(f"Gagal mengambil data: status code {response.status_code}")

# Jika ada data baru yang siap disimpan
if price_data:
    try:
        with open(csv_file_name, 'a', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)

            # Jika file kosong, tulis header
            if file.tell() == 0:
                writer.writerow([
                    'Date', 'Commodity Name', 'Price Today (IDR)',
                    'Price Yesterday (IDR)', 'Price Change (IDR)',
                    'Price Change (%)', 'Change Direction'
                ])

            # Tulis data
            writer.writerows(price_data)
        print("Data berhasil ditambahkan ke file CSV.")
    except Exception as e:
        print(f"Gagal menulis ke file CSV: {e}")
else:
    print("Tidak ada data baru untuk ditambahkan.")
