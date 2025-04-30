import sqlite3

# Koneksi ke database SQLite
conn = sqlite3.connect('data_harga.db')
cursor = conn.cursor()

# Jalankan query untuk melihat tabel yang ada
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()
print("Tabel yang ada:", tables)

# Menampilkan data dari tabel tertentu
cursor.execute("SELECT * FROM gabungan_harga_beras LIMIT 5;")
rows = cursor.fetchall()
for row in rows:
    print(row)

# Menutup koneksi
conn.close()

