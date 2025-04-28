import pandas as pd
import ast

# List semua file dan nama kolomnya
files = {
    'medium_cikurubuk': 'medium_cikurubuk.csv',
    'medium_pancasila': 'medium_pancasila.csv',
    'premium_cikurubuk': 'premium_cikurubuk.csv',
    'premium_pancasila': 'premium_pancasila.csv'
}

def extract_values_only(df):
    """
    Ekstrak hanya kolom 'value' dari kolom 'result'
    """
    extracted_values = []

    if 'result' in df.columns:
        for idx, row in df.iterrows():
            result_data = row['result']
            if isinstance(result_data, str):
                try:
                    results_list = ast.literal_eval(result_data)
                    if isinstance(results_list, list):
                        # Ambil semua 'value'
                        for item in results_list:
                            value = item.get('value', None)
                            extracted_values.append(value)
                except (ValueError, SyntaxError) as e:
                    print(f"Error parsing row {idx}: {e}")
                    extracted_values.append(None)
            else:
                # Kalau bukan string (misal int atau NaN), isi None
                extracted_values.append(None)
    return pd.Series(extracted_values)

# Dictionary untuk menyimpan semua hasil
all_data = {}

# Proses semua file
for name, filename in files.items():
    print(f"Memproses {filename}...")
    try:
        df = pd.read_csv(filename)
        values_series = extract_values_only(df)

        # Pastikan semua isi jadi float
        values_series = pd.to_numeric(values_series, errors='coerce')

        all_data[name] = values_series
    except Exception as e:
        print(f"Gagal memproses {filename}: {e}")

# Gabungkan semua menjadi satu DataFrame
combined_df = pd.DataFrame(all_data)

# Hitung rata-rata setelah konversi ke numerik
combined_df['medium'] = combined_df[['medium_cikurubuk', 'medium_pancasila']].mean(axis=1)
combined_df['premium'] = combined_df[['premium_cikurubuk', 'premium_pancasila']].mean(axis=1)

# Simpan hasil ke CSV
combined_filename = 'data_gabungan_dengan_rata2.csv'
combined_df.to_csv(combined_filename, index=False, encoding='utf-8-sig')
print(f"Data gabungan dengan kolom rata-rata berhasil disimpan ke {combined_filename}")
