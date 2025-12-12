import csv
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import os
import dotenv

import requests

dotenv.load_dotenv()
CMC_API_KEY = os.getenv("CMC_API_KEY", "123")
FILE_NAME = "output_cmc_llama.csv"

## Functions

def truncate_to_hour(dt: datetime) -> datetime:
    """ Truncate time down to the hour. """
    return dt.replace(minute=0, second=0, microsecond=0)


def ensure_csv_exists(path: Path) -> None:
    """Create CSV file if not exist yet."""
    if not path.exists():
        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "hour",                    # rounded to hour (local time)
                    "timestamp",               # exact timestamp script was run 
                    "ton_price",               # TON price (CoinMarketCap)
                    "ton_price_received_at",   # when TON request was made
                    "volume_usd_float",        # volume_usd as float (Ston.fi)
                    "volume_usd_received_at",  # when Ston.fi request was made
                ]
            )

def fetch_ton_price(started_at: datetime = None):
    """
    Call CoinMarketCap API to get TON price.
    Returns (price: float | None, received_at: datetime).
    If request fails, returns (None, received_at).
    """
    url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest" # v2/cryptocurrency/quotes/latest can be also used
    params = {
        "symbol": "TON",
        "convert": "USD",
    }
    headers = {
        "Accepts": "application/json",
        "X-CMC_PRO_API_KEY": CMC_API_KEY,
    }
    if started_at is None:
        started_at = datetime.now()

    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()

        data = response.json()
        status = data.get("status", {})
        error_code = status.get("error_code")

        if error_code not in (0, "0"):
            message = status.get("error_message")
            raise RuntimeError(f"CoinMarketCap error: {message}")

        price = float(data["data"]["TON"]["quote"]["USD"]["price"]) # in case of v2: data["data"]["TON"][0]["quote"]["USD"]["price"]
        ended_at = datetime.now()
        print(f"TON price fetched: {price} USD (request time: {ended_at - started_at})")
        return price, ended_at
    except Exception as exc:
        print(f"CoinMarketCap request failed: {exc}")
        return None, started_at

#--- Ignore this function ---
def fetch_dex_volume(started_at: datetime = None):
    """
    Call Ston.fi API to get DEX stats.
    Returns (volume_float: float | None, volume_str: str | None, received_at: datetime).
    If request fails, returns (None, None, received_at).
    """
    url = "https://api.ston.fi/v1/stats/dex"

    if started_at is None:
        started_at = datetime.now()

    try:
        response = requests.get(url)
        response.raise_for_status()

        data = response.json()
        volume_str = data["stats"]["volume_usd"]
        volume_float = float(volume_str)
        ended_at = datetime.now()
        print(f"DEX volume fetched: {volume_float} USD (request time: {ended_at - started_at})")   
        return volume_float, volume_str, ended_at

    except Exception as exc:
        print(f"Ston.fi request failed: {exc}")
        return None, None, started_at
    

def fetch_dex_volume_llama(started_at: datetime = None):
    """
    Call defillama API to get STON.fi stats.
    Returns (volume_float: float | None, received_at: datetime).
    If request fails, returns (None, received_at).
    """
    url = 'https://api.llama.fi/summary/dexs/ston.fi'

    if started_at is None:
        started_at = datetime.now()

    try:
        response = requests.get(url, params={'excludeTotalDataChart': 'true', 'excludeTotalDataChartBreakdown': 'true'})
        response.raise_for_status()

        data = response.json()
        volume_float = float(data['totalAllTime'])
        ended_at = datetime.now()
        print(f"DEX volume fetched: {volume_float} USD (request time: {ended_at - started_at})")   
        return volume_float, ended_at
    except Exception as exc:
        print(f"Ston.fi request failed: {exc}")
        return None,  started_at

## Main script

def main():
    if not CMC_API_KEY or CMC_API_KEY == "123":
        raise SystemExit(
            "Set your CoinMarketCap API key in the CMC_API_KEY environment variable."
        )

    csv_path = Path("./") / FILE_NAME
    ensure_csv_exists(csv_path)

    now = datetime.now()
    timestamp_hour = truncate_to_hour(now)

    # Run requests in parallel
    with ThreadPoolExecutor(max_workers=2) as pool:
        future_price = pool.submit(fetch_ton_price, started_at=now)
        future_volume = pool.submit(fetch_dex_volume_llama, started_at=now)

        ton_price, ton_price_ts = future_price.result()
        volume_float, volume_ts = future_volume.result()

    # Append row into CSV
    with csv_path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                timestamp_hour.isoformat(),
                now.isoformat(),
                ton_price,
                ton_price_ts.isoformat(),
                volume_float,
                volume_ts.isoformat(),
            ]
        )

    print(f"Row appended to {csv_path} for hour {timestamp_hour.isoformat()}")


if __name__ == "__main__":
    main()


# ------------------------------------------
# -- Extra code for Cloud Run environment --
# ------------------------------------------


def is_running_in_cloud_run() -> bool:
    """
    Detect if the code is running in Cloud Run (service or job).
    Cloud Run services set K_SERVICE, jobs set CLOUD_RUN_JOB by default. 
    """
    return bool(
        os.getenv("CLOUD_RUN_JOB")  # Cloud Run job
        or os.getenv("K_SERVICE")   # Cloud Run service
    )


def upload_last_row_to_gcs(local_csv: Path, bucket_name: str, object_name: str = FILE_NAME,) -> None:
    """
    Append the last row from local CSV into a CSV object in GCS bucket.
    If the object does not exist, create it with header + last row.
    """
    try:
        from google.cloud import storage
    except ImportError:
        print("google-cloud-storage is not installed; skipping GCS upload.")
        return

    if not local_csv.exists():
        print(f"Local CSV {local_csv} does not exist; nothing to upload.")
        return

    with local_csv.open("r", encoding="utf-8") as f:
        lines = [line for line in f.read().splitlines() if line.strip()]

    if len(lines) < 2:
        print("Local CSV has no data rows to upload.")
        return

    header_line = lines[0]
    last_line = lines[-1]

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)

    try:
        existing_text = blob.download_as_text(encoding="utf-8")
        if not existing_text.strip():
            new_content = header_line + "\n" + last_line + "\n"
        else:
            existing_lines = existing_text.splitlines()
            # Avoid duplicating the same row if it already exists
            if last_line in existing_lines:
                print("Last row already present in GCS object; skipping append.")
                return
            new_content = existing_text.rstrip("\n") + "\n" + last_line + "\n"
    except Exception:
        # Blob does not exist or cannot be downloaded -> create new file
        new_content = header_line + "\n" + last_line + "\n"

    blob.upload_from_string(new_content, content_type="text/csv")
    print(
        f"Uploaded latest row to gs://{bucket_name}/{object_name} "
        f"(length: {len(new_content)} bytes)"
    )


if is_running_in_cloud_run():
    csv_path = Path("./") / FILE_NAME
    bucket_name = "ton_info_hourly"
    upload_last_row_to_gcs(csv_path, bucket_name=bucket_name)
    print(f"Data uploaded to GCS bucket {bucket_name}.")
