import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd


BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "data_harga.db"
SILINDA_PATH = BASE_DIR / "data_gabungan_dengan_rata2.csv"
BAPANAS_PATH = BASE_DIR / "processed_beras_prices.csv"
TABLE_NAME = "gabungan_harga_beras"
PRICE_COLUMNS = [
    "medium_silinda",
    "premium_silinda",
    "medium_bapanas",
    "premium_bapanas",
]


def load_source(path):
    dataframe = pd.read_csv(path)
    dataframe.columns = dataframe.columns.str.lower()

    required_columns = {"date", "medium", "premium"}
    missing_columns = required_columns.difference(dataframe.columns)
    if missing_columns:
        raise ValueError(f"Kolom {path.name} tidak lengkap: {sorted(missing_columns)}")

    dataframe = dataframe[["date", "medium", "premium"]].copy()
    dataframe["date"] = pd.to_datetime(dataframe["date"], errors="coerce")
    dataframe = dataframe.dropna(subset=["date"])
    dataframe["date"] = dataframe["date"].dt.strftime("%Y-%m-%d")

    for column in ("medium", "premium"):
        dataframe[column] = pd.to_numeric(dataframe[column], errors="coerce")
        dataframe.loc[dataframe[column] <= 0, column] = np.nan

    return dataframe.drop_duplicates(subset=["date"], keep="last").sort_values("date")


def build_combined(silinda, bapanas):
    combined = pd.merge(
        silinda,
        bapanas,
        on="date",
        how="outer",
        suffixes=("_silinda", "_bapanas"),
    ).sort_values("date")

    for column in PRICE_COLUMNS:
        combined.loc[~np.isfinite(combined[column]), column] = np.nan

    return combined[["date", *PRICE_COLUMNS]]


def replace_table(dataframe):
    temporary_table = f"{TABLE_NAME}_new"

    with sqlite3.connect(DB_PATH) as connection:
        table_exists = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            (TABLE_NAME,),
        ).fetchone()
        existing_ids = {}
        if table_exists:
            existing_ids = dict(
                connection.execute(f"SELECT date, id FROM {TABLE_NAME}").fetchall()
            )

        next_id = max(existing_ids.values(), default=0) + 1
        rows = []
        for row in dataframe.itertuples(index=False, name=None):
            date = row[0]
            row_id = existing_ids.get(date)
            if row_id is None:
                row_id = next_id
                next_id += 1
            values = tuple(None if pd.isna(value) else value for value in row)
            rows.append((row_id, *values))

        connection.execute(f"DROP TABLE IF EXISTS {temporary_table}")
        connection.execute(
            f"""
            CREATE TABLE {temporary_table} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL UNIQUE,
                medium_silinda REAL,
                premium_silinda REAL,
                medium_bapanas REAL,
                premium_bapanas REAL
            )
            """
        )
        connection.executemany(
            f"""
            INSERT INTO {temporary_table} (
                id,
                date,
                medium_silinda,
                premium_silinda,
                medium_bapanas,
                premium_bapanas
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        connection.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
        connection.execute(f"ALTER TABLE {temporary_table} RENAME TO {TABLE_NAME}")


def main():
    silinda = load_source(SILINDA_PATH)
    bapanas = load_source(BAPANAS_PATH)
    combined = build_combined(silinda, bapanas)
    replace_table(combined)
    print(f"{len(combined)} baris disimpan ke tabel '{TABLE_NAME}'.")


if __name__ == "__main__":
    main()
