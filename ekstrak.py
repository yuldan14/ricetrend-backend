import pandas as pd
import numpy as np

df =  pd.read_csv('data_silinda.csv')

import ast
from datetime import datetime

def extract_result_data(df):
    """
    Fungsi untuk mengekstrak dan membersihkan data dari kolom 'result'
    """
    try:
        # 1. Konversi string ke list of dicts
        if isinstance(df['result'], str):
            try:
                # Ganti tanda kutip tunggal dengan ganda untuk valid JSON
                json_str = df['result'].replace("'", '"')
                results_list = ast.literal_eval(json_str)
            except (SyntaxError, ValueError) as e:
                print(f"Error parsing JSON: {e}")
                return pd.DataFrame()  # Return empty DataFrame jika error
        
        # 2. Konversi ke DataFrame
        df_results = pd.DataFrame(results_list)
        
        if not df_results.empty:
            # 3. Pembersihan data
            # Konversi tipe data
            df_results['value'] = pd.to_numeric(df_results['value'], errors='coerce')
            
            # Handle multiple date formats
            df_results['date'] = pd.to_datetime(
                df_results['date'], 
                errors='coerce',
                format='mixed'  # Untuk handle berbagai format tanggal
            )
            
            # Drop baris dengan data hilang
            df_results.dropna(subset=['value', 'date'], inplace=True)
            
            # 4. Sort by date
            df_results.sort_values('date', inplace=True)
            
            return df_results
            
    except Exception as e:
        print(f"Unexpected error: {e}")
    
    return pd.DataFrame()

# Contoh penggunaan:
if not df.empty and 'result' in df.columns:
    # Ekstrak data untuk setiap baris (jika df memiliki multiple rows)
    all_results = pd.concat(
        [extract_result_data(row) for _, row in df.iterrows()],
        ignore_index=True
    )
    
    print("DataFrame Hasil Ekstraksi:")
    print(all_results.head())
    
    # Simpan ke CSV
    try:
        all_results.to_csv('hasil_ekstraksi.csv', index=False, encoding='utf-8-sig')
        print("Data berhasil disimpan ke hasil_ekstraksi.csv")
    except Exception as e:
        print(f"Gagal menyimpan file: {e}")
else:
    print("DataFrame input tidak valid atau kolom 'result' tidak ditemukan")
