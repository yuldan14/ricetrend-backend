import os
from pathlib import Path
from zoneinfo import ZoneInfo
from datetime import datetime

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


BASE_DIR = Path(__file__).resolve().parent
TIMEZONE = ZoneInfo("Asia/Jakarta")
START_DATE = "2022-02-09"
REQUEST_TIMEOUT = (10, 60)
SOURCES = {
    "medium_cikurubuk": (1, 69),
    "medium_pancasila": (1, 70),
    "premium_cikurubuk": (24, 69),
    "premium_pancasila": (24, 70),
}


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
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def build_url(commodity_id, market_id, end_date):
    return (
        "https://svc-silinda.jabarprov.go.id/api/graphic_data/"
        f"{commodity_id}/{market_id}/day/price/{START_DATE}/{end_date}"
        "/0/market/-/eceran/null"
    )


def response_to_dataframe(payload, source_name):
    if isinstance(payload, list):
        rows = payload
    elif isinstance(payload, dict) and isinstance(payload.get("data"), list):
        rows = payload["data"]
    elif isinstance(payload, dict):
        rows = [payload]
    else:
        raise ValueError(f"Format data Silinda tidak dikenali untuk {source_name}.")

    dataframe = pd.DataFrame(rows)
    if dataframe.empty or "result" not in dataframe.columns:
        raise ValueError(
            f"Data Silinda kosong atau tidak memiliki result untuk {source_name}."
        )

    return dataframe


def fetch_all(end_date):
    fetched = {}

    with create_session() as session:
        for name, (commodity_id, market_id) in SOURCES.items():
            url = build_url(commodity_id, market_id, end_date)
            print(f"Memproses {name}...")
            response = session.get(url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            fetched[name] = response_to_dataframe(response.json(), name)

    return fetched


def write_all(fetched):
    temporary_paths = {}

    try:
        for name, dataframe in fetched.items():
            destination = BASE_DIR / f"{name}.csv"
            temporary = destination.with_suffix(".csv.tmp")
            dataframe.to_csv(temporary, index=False)
            temporary_paths[destination] = temporary

        for destination, temporary in temporary_paths.items():
            temporary.replace(destination)
            print(f"Data {destination.stem} disimpan ke {destination.name}")
    finally:
        for temporary in temporary_paths.values():
            temporary.unlink(missing_ok=True)


def main():
    end_date = current_date()

    try:
        fetched = fetch_all(end_date)
    except (requests.RequestException, ValueError) as error:
        print(f"Peringatan: pembaruan Silinda dilewati: {error}")
        print("File Silinda sebelumnya dipertahankan.")
        return

    write_all(fetched)


if __name__ == "__main__":
    main()
