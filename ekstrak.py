import pandas as pd
import ast
import numpy as np

# List semua file dan nama kolomnya
files = {
    'medium_cikurubuk': 'medium_cikurubuk.csv',
    'medium_pancasila': 'medium_pancasila.csv',
    'premium_cikurubuk': 'premium_cikurubuk.csv',
    'premium_pancasila': 'premium_pancasila.csv'
}

def extract_values_and_dates(df):
    extracted_values = []
    extracted_dates = []

    if 'result' in df.columns:
        for idx, row in df.iterrows():
            result_data = row['result']
            if isinstance(result_data, str):
                try:
                    results_list = ast.literal_eval(result_data)
                    if isinstance(results_list, list):
                        for item in results_list:
                            value = item.get('value', None)
                            date = item.get('date', None)
                            extracted_values.append(value)
                            extracted_dates.append(date)
                except (ValueError, SyntaxError) as e:
                    print(f"Error parsing row {idx}: {e}")
                    extracted_values.append(None)
                    extracted_dates.append(None)
            else:
                extracted_values.append(None)
                extracted_dates.append(None)
    
    return pd.Series([extracted_values, extracted_dates])

def impute_column(df, col):
    for i in range(len(df)):
        current_value = df.loc[i, col]
        if pd.isna(current_value) or current_value == 0:
            above = df.loc[:i-1, col][~df.loc[:i-1, col].isin([np.nan, 0])]
            below = df.loc[i+1:, col][~df.loc[i+1:, col].isin([np.nan, 0])]

            above_val = above.iloc[-1] if not above.empty else None
            below_val = below.iloc[0] if not below.empty else None

            if above_val is not None and below_val is not None:
                df.loc[i, col] = (above_val + below_val) / 2
            elif above_val is not None:
                df.loc[i, col] = above_val
            elif below_val is not None:
                df.loc[i, col] = below_val
            else:
                nearest = df[col][~df[col].isin([np.nan, 0])]
                if not nearest.empty:
                    df.loc[i, col] = nearest.mean()

# Dictionary untuk menyimpan hasil ekstraksi
all_data = {}

for name, filename in files.items():
    print(f"Memproses {filename}...")
    try:
        df = pd.read_csv(filename)
        values_series, dates_series = extract_values_and_dates(df)

        temp_df = pd.DataFrame({
            'value': pd.to_numeric(values_series, errors='coerce'),
            'date': pd.to_datetime(dates_series, errors='coerce')
        })

        temp_df.dropna(subset=['date'], inplace=True)
        temp_df.set_index('date', inplace=True)
        all_data[name] = temp_df['value']

    except Exception as e:
        print(f"Gagal memproses {filename}: {e}")

# Gabungkan semua data berdasarkan tanggal
combined_df = pd.concat(all_data, axis=1)

# Buat date range dari min ke max
full_date_range = pd.date_range(start=combined_df.index.min(), end=combined_df.index.max(), freq='D')

# Isi tanggal yang hilang
combined_df = combined_df.reindex(full_date_range)

# Hitung rata-rata untuk kolom medium dan premium
combined_df['medium'] = combined_df[['medium_cikurubuk', 'medium_pancasila']].mean(axis=1, skipna=True)
combined_df['premium'] = combined_df[['premium_cikurubuk', 'premium_pancasila']].mean(axis=1, skipna=True)

# Ambil hanya kolom final
combined_df = combined_df[['medium', 'premium']]
combined_df.reset_index(inplace=True)
combined_df.rename(columns={'index': 'date'}, inplace=True)

# Imputasi nilai kosong
impute_column(combined_df, 'medium')
impute_column(combined_df, 'premium')

# Simpan hasil
combined_filename = 'data_gabungan_dengan_rata2.csv'
combined_df.to_csv(combined_filename, index=False, encoding='utf-8-sig')
print(f"✅ Data gabungan lengkap disimpan ke {combined_filename}")
