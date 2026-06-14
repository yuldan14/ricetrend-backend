import os
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


BASE_DIR = Path(__file__).resolve().parent
CSV_PATH = BASE_DIR / "processed_beras_prices.csv"
API_URL = "https://api-panelhargav2.badanpangan.go.id/api/front/harga-pangan-informasi"
TIMEZONE = ZoneInfo("Asia/Jakarta")
REQUEST_TIMEOUT = (10, 30)
PRICE_COLUMNS = ["Medium", "Premium"]


def current_date():
    override = os.environ.get("DATA_DATE")
    return override or datetime.now(TIMEZONE).strftime("%Y-%m-%d")


def create_session():
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    session = requests.Session()
    session.headers.update(
        {
            "Accept": "application/json",
            "User-Agent": "RiceTrend-data-refresh/1.0",
        }
    )

    api_key = os.environ.get("BAPANAS_API_KEY")
    if api_key:
        session.headers["X-API-Key"] = api_key

    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def parse_prices(payload):
    if payload.get("status") != "success":
        raise ValueError(payload.get("message") or "Bapanas tidak mengembalikan status success.")

    prices = {}
    commodity_columns = {
        "Beras Medium": "Medium",
        "Beras Premium": "Premium",
    }

    for item in payload.get("data", []):
        column = commodity_columns.get(item.get("name"))
        if column:
            prices[column] = pd.to_numeric(item.get("today"), errors="coerce")

    if set(prices) != set(PRICE_COLUMNS):
        raise ValueError("Harga Beras Medium atau Beras Premium tidak tersedia.")

    if any(pd.isna(value) or value <= 0 for value in prices.values()):
        raise ValueError("Harga Bapanas kosong, nol, atau bukan angka.")

    return prices


def fetch_prices(date):
    params = {
        "province_id": "12",
        "city_id": "184",
        "level_harga_id": "3",
        "date": date,
    }

    with create_session() as session:
        response = session.get(API_URL, params=params, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        return parse_prices(response.json())


def load_prices():
    dataframe = pd.read_csv(CSV_PATH)
    required_columns = {"Date", *PRICE_COLUMNS}
    missing_columns = required_columns.difference(dataframe.columns)
    if missing_columns:
        raise ValueError(f"Kolom CSV Bapanas tidak lengkap: {sorted(missing_columns)}")

    dataframe["Date"] = pd.to_datetime(dataframe["Date"], errors="coerce")
    dataframe = dataframe.dropna(subset=["Date"])
    dataframe["Date"] = dataframe["Date"].dt.strftime("%Y-%m-%d")

    for column in PRICE_COLUMNS:
        dataframe[column] = pd.to_numeric(dataframe[column], errors="coerce")
        dataframe.loc[dataframe[column] <= 0, column] = pd.NA

    dataframe = dataframe.drop_duplicates(subset=["Date"], keep="last").sort_values("Date")
    dataframe[PRICE_COLUMNS] = dataframe[PRICE_COLUMNS].interpolate(
        method="linear",
        limit_direction="both",
    )

    if dataframe[PRICE_COLUMNS].isna().any().any():
        raise ValueError("CSV Bapanas tidak memiliki harga valid untuk imputasi.")

    return dataframe


def write_prices(dataframe):
    temporary = CSV_PATH.with_suffix(".csv.tmp")
    try:
        dataframe.to_csv(temporary, index=False)
        temporary.replace(CSV_PATH)
    finally:
        temporary.unlink(missing_ok=True)


def main():
    date = current_date()
    dataframe = load_prices()

    try:
        prices = fetch_prices(date)
    except (requests.RequestException, ValueError) as error:
        print(f"Peringatan: pembaruan Bapanas dilewati: {error}")
        print("Data Bapanas sebelumnya dipertahankan.")
        write_prices(dataframe)
        return

    new_row = pd.DataFrame([{"Date": date, **prices}])
    dataframe = pd.concat([dataframe, new_row], ignore_index=True)
    dataframe = dataframe.drop_duplicates(subset=["Date"], keep="last").sort_values("Date")
    write_prices(dataframe)
    print(f"Data Bapanas {date} berhasil disimpan.")


if __name__ == "__main__":
    main()
