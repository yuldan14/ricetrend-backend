import pandas as pd
import ast

# List semua file dan nama kolomnya
files = {
    'medium_cikurubuk': 'medium_cikurubuk.csv',
    'medium_pancasila': 'medium_pancasila.csv',
    'premium_cikurubuk': 'premium_cikurubuk.csv',
    'premium_pancasila': 'premium_pancasila.csv'
}

def extract_values_and_dates(df):
    """
    Ekstrak 'value' dan 'date' dari kolom 'result'
    """
    extracted_values = []
    extracted_dates = []

    if 'result' in df.columns:
        for idx, row in df.iterrows():
            result_data = row['result']
            if isinstance(result_data, str):
                try:
                    results_list = ast.literal_eval(result_data)
                    if isinstance(results_list, list):
                        # Ambil 'value' dan 'date' dari setiap item
                        for item in results_list:
                            value = item.get('value', None)
                            date = item.get('date', None)  # Mengambil 'date'
                            extracted_values.append(value)
                            extracted_dates.append(date)
                except (ValueError, SyntaxError) as e:
                    print(f"Error parsing row {idx}: {e}")
                    extracted_values.append(None)
                    extracted_dates.append(None)
            else:
                # Kalau bukan string (misal int atau NaN), isi None
                extracted_values.append(None)
                extracted_dates.append(None)
    
    return pd.Series([extracted_values, extracted_dates])

# Dictionary untuk menyimpan semua hasil
all_data = {}
dates = {}

# Proses semua file
for name, filename in files.items():
    print(f"Memproses {filename}...")
    try:
        df = pd.read_csv(filename)
        
        # Ekstrak nilai dan tanggal
        values_series, dates_series = extract_values_and_dates(df)

        # Pastikan semua isi jadi float untuk 'value' dan format tanggal yang benar
        values_series = pd.to_numeric(values_series, errors='coerce')

        # Simpan data per file dengan tanggal sebagai indeks
        temp_df = pd.DataFrame({
            'value': values_series,
            'date': pd.to_datetime(dates_series, errors='coerce')
        })

        # Set tanggal sebagai indeks
        temp_df.set_index('date', inplace=True)
        all_data[name] = temp_df['value']  # Simpan hanya kolom 'value'

    except Exception as e:
        print(f"Gagal memproses {filename}: {e}")

# Gabungkan semua data berdasarkan tanggal
combined_df = pd.concat(all_data, axis=1)

# Samakan tanggal berdasarkan indeks, dan isi NaN jika tidak ada data pada tanggal tersebut
combined_df = combined_df.reset_index()

# Hitung rata-rata untuk kolom medium dan premium jika ada data
combined_df['medium'] = combined_df[['medium_cikurubuk', 'medium_pancasila']].mean(axis=1, skipna=True)
combined_df['premium'] = combined_df[['premium_cikurubuk', 'premium_pancasila']].mean(axis=1, skipna=True)

combined_df = combined_df.drop(columns=['medium_cikurubuk', 'medium_pancasila', 'premium_cikurubuk', 'premium_pancasila'])
# Simpan hasil ke CSV
combined_filename = 'data_gabungan_dengan_rata2.csv'
combined_df.to_csv(combined_filename, index=False, encoding='utf-8-sig')
print(f"Data gabungan dengan kolom rata-rata dan tanggal berhasil disimpan ke {combined_filename}")
