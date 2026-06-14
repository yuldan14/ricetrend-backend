import json
import math
import sqlite3
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / 'data_harga.db'
JSON_PATH = BASE_DIR / 'data_harga.json'
FRONTEND_JSON_PATH = BASE_DIR.parent / 'frontend' / 'public' / 'data_harga.json'
TABLE_NAME = 'gabungan_harga_beras'
PRICE_COLUMNS = {'medium_silinda', 'premium_silinda', 'medium_bapanas', 'premium_bapanas'}


def normalize_price(value):
    if value in (None, ''):
        return None

    try:
        number = float(value)
    except (TypeError, ValueError):
        return None

    return number if math.isfinite(number) else None


def normalize_id(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return value


def normalize_value(column, value):
    if column in PRICE_COLUMNS:
        return normalize_price(value)

    if column == 'id':
        return normalize_id(value)

    return value if value is not None else ''


def load_rows():
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type='table' AND name=?;
            """,
            (TABLE_NAME,),
        )

        if not cursor.fetchone():
            raise ValueError(f"Tabel '{TABLE_NAME}' tidak ditemukan di database.")

        cursor.execute(f'SELECT * FROM {TABLE_NAME} ORDER BY date')
        column_names = [description[0] for description in cursor.description]

        return [
            {
                column_names[index]: normalize_value(column_names[index], value)
                for index, value in enumerate(row)
            }
            for row in cursor.fetchall()
        ]


def write_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', encoding='utf-8') as json_file:
        json.dump(data, json_file, indent=4, allow_nan=False)
        json_file.write('\n')


try:
    data = load_rows()
    write_json(JSON_PATH, data)
    write_json(FRONTEND_JSON_PATH, data)
    print(f"Konversi berhasil! Data disimpan di {JSON_PATH} dan {FRONTEND_JSON_PATH}")
except sqlite3.Error as e:
    print(f"Terjadi kesalahan pada database: {e}")
except Exception as e:
    print(f"Terjadi kesalahan: {e}")
