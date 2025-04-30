import pandas as pd
import sqlite3

# Baca data CSV
df_bapanas = pd.read_csv('processed_beras_prices.csv')
df_silinda = pd.read_csv('data_gabungan_dengan_rata2.csv')

# Menstandarkan nama kolom untuk konsistensi (gunakan huruf kecil)
df_bapanas.columns = df_bapanas.columns.str.lower()  # ubah semua ke huruf kecil
df_silinda.columns = df_silinda.columns.str.lower()

# Pastikan kolom sesuai, misalnya 'date', 'medium', 'premium' di kedua dataset
df_bapanas = df_bapanas.rename(columns={
    'date': 'date',
    'medium': 'medium',
    'premium': 'premium'
})

df_silinda = df_silinda.rename(columns={
    'date': 'date',
    'medium': 'medium',
    'premium': 'premium'
})

# Menggabungkan kedua data berdasarkan 'date' menggunakan join outer
df_combined = pd.merge(df_silinda, df_bapanas, on='date', how='outer', suffixes=('_silinda', '_bapanas'))

# Koneksi ke SQLite
conn = sqlite3.connect('data_harga.db')
cursor = conn.cursor()

# Fungsi untuk membuat tabel jika belum ada
def create_table_if_not_exists(table_name, columns):
    columns_sql = ", ".join([f"{col} TEXT" for col in columns])
    create_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            {columns_sql},
            UNIQUE(date)  -- Hindari duplikat berdasarkan tanggal
        );
    """
    cursor.execute(create_query)

# Tentukan nama tabel untuk disimpan
table_name = 'gabungan_harga_beras'

# Buat tabel jika belum ada
create_table_if_not_exists(table_name, df_combined.columns)

# Insert data ke SQLite
inserted = 0
for _, row in df_combined.iterrows():
    placeholders = ", ".join(["?"] * len(row))
    columns = ", ".join(row.index)
    values = tuple(row.astype(str))  # Convert semua nilai ke string
    try:
        cursor.execute(f"INSERT OR IGNORE INTO {table_name} ({columns}) VALUES ({placeholders})", values)
        inserted += cursor.rowcount
    except Exception as e:
        print(f"Error saat insert baris: {e}")

# Commit perubahan dan tutup koneksi
conn.commit()
conn.close()

print(f"{inserted} baris baru berhasil ditambahkan ke tabel '{table_name}'.")
