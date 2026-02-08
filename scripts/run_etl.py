#!/usr/bin/env python3
"""Week 1 ETL: extract CSVs + ExchangeRate API, transform, and load to GCS."""

import argparse
import json
import logging
import os
import re
import time
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import requests

try:
    from google.cloud import storage
except Exception:  # pragma: no cover - optional dependency
    storage = None


LOGGER = logging.getLogger("storypoints_etl")
RAW_API_BASE = Path("data/raw/api_currency")
OUTPUT_BASE = Path("data/clean")
ENV_PATH = Path(".env")


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )


def load_env(path: Path = ENV_PATH) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


def get_env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    cleaned = []
    for col in df.columns:
        col = col.strip().lower()
        col = re.sub(r"[^0-9a-z]+", "_", col).strip("_")
        cleaned.append(col)
    df.columns = cleaned
    return df


def to_utc_iso(series: pd.Series) -> pd.Series:
    dt = pd.to_datetime(series, errors="coerce", utc=True)
    return dt.dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def fetch_exchange_rates(api_key: str, max_retries: int = 3, backoff: float = 1.6):
    url = f"https://v6.exchangerate-api.com/v6/{api_key}/latest/USD"
    session = requests.Session()

    for attempt in range(1, max_retries + 1):
        try:
            resp = session.get(url, timeout=15)
            data = resp.json()
            if resp.status_code == 200 and data.get("result") == "success":
                LOGGER.info("Fetched exchange rates from API.")
                return data
            LOGGER.warning(
                "API error (attempt %s/%s): status=%s result=%s",
                attempt,
                max_retries,
                resp.status_code,
                data.get("result"),
            )
        except Exception as exc:
            LOGGER.warning("API request failed (attempt %s/%s): %s", attempt, max_retries, exc)

        sleep_for = backoff ** attempt
        LOGGER.info("Retrying in %.1fs...", sleep_for)
        time.sleep(sleep_for)

    return None


def write_raw_api_json(
    data: dict,
    raw_base: Path,
    ingest_date: str,
    gcs_bucket: str | None,
    gcp_project: str | None,
) -> Path | None:
    if data is None:
        return None
    target_dir = raw_base / ingest_date
    target_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    target = target_dir / f"exchangerate_usd_{timestamp}.json"
    target.write_text(json.dumps(data, indent=2))
    LOGGER.info("Stored raw API response at %s", target)
    if gcs_bucket:
        upload_to_gcs(target, gcs_bucket, target.as_posix(), gcp_project)
    return target


def read_csv_in_chunks(path: Path, chunk_size: int, transform_fn) -> pd.DataFrame:
    chunks = []
    total_rows = 0

    for chunk in pd.read_csv(path, chunksize=chunk_size):
        total_rows += len(chunk)
        chunk = transform_fn(chunk)
        chunks.append(chunk)

    if total_rows == 0:
        return pd.DataFrame()

    LOGGER.info("Read %s rows from %s", total_rows, path)
    return pd.concat(chunks, ignore_index=True)


def transform_clickstream(df: pd.DataFrame) -> pd.DataFrame:
    df = standardize_columns(df)
    if "click_time" in df.columns:
        df["click_time"] = to_utc_iso(df["click_time"])
    return df


def transform_transactions(df: pd.DataFrame, rates: dict | None) -> pd.DataFrame:
    df = standardize_columns(df)
    if "txn_time" in df.columns:
        df["txn_time"] = to_utc_iso(df["txn_time"])

    if "amount" in df.columns:
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce")

    if rates is None:
        df["amount_in_usd"] = pd.NA
        return df

    rate_series = df.get("currency", pd.Series(index=df.index)).map(rates)
    df["amount_in_usd"] = df["amount"] / rate_series
    return df


def deduplicate(df: pd.DataFrame, subset: list[str] | None = None) -> tuple[pd.DataFrame, int]:
    before = len(df)
    df = df.drop_duplicates(subset=subset)
    removed = before - len(df)
    return df, removed


def write_partitioned(
    df: pd.DataFrame,
    dataset_name: str,
    ingest_date: str,
    output_base: Path,
    gcs_bucket: str | None,
    gcp_project: str | None,
) -> Path:
    target_dir = output_base / dataset_name / f"ingest_date={ingest_date}"
    target_dir.mkdir(parents=True, exist_ok=True)
    target_file = target_dir / f"{dataset_name}.csv"
    df.to_csv(target_file, index=False)
    LOGGER.info("Wrote %s records to %s", len(df), target_file)

    if gcs_bucket:
        upload_to_gcs(target_file, gcs_bucket, target_file.as_posix(), gcp_project)

    return target_file


def upload_to_gcs(
    local_path: Path,
    bucket_name: str,
    object_name: str,
    gcp_project: str | None = None,
):
    if storage is None:
        LOGGER.warning("google-cloud-storage not installed; skipping GCS upload.")
        return

    try:
        client = storage.Client(project=gcp_project) if gcp_project else storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(object_name)
        blob.upload_from_filename(str(local_path))
        LOGGER.info("Uploaded to gs://%s/%s", bucket_name, object_name)
    except Exception as exc:
        LOGGER.warning("Failed to upload to GCS: %s", exc)


def parse_args():
    parser = argparse.ArgumentParser(description="StoryPoints Week 1 ETL")
    parser.add_argument("--clickstream-path", default=os.getenv("CLICKSTREAM_PATH", "clickstream.csv"))
    parser.add_argument("--transactions-path", default=os.getenv("TRANSACTIONS_PATH", "transactions.csv"))
    parser.add_argument("--chunk-size", type=int, default=get_env_int("CHUNK_SIZE", 50000))
    parser.add_argument("--ingest-date", default=date.today().isoformat())
    parser.add_argument("--gcs-bucket", default=os.getenv("GCS_BUCKET"))
    parser.add_argument("--gcp-project", default=os.getenv("GCP_PROJECT_ID"))
    parser.add_argument("--api-key", default=os.getenv("EXCHANGE_API_KEY"))
    parser.add_argument("--skip-api", action="store_true", help="Skip API call and leave amount_in_usd null.")
    return parser.parse_args()


def main():
    setup_logging()
    load_env()
    args = parse_args()

    ingest_date = args.ingest_date
    output_base = OUTPUT_BASE
    raw_api_base = RAW_API_BASE

    rates = None
    api_payload = None

    if args.skip_api:
        LOGGER.warning("Skipping exchange-rate API call by user request.")
    elif not args.api_key:
        LOGGER.warning("EXCHANGE_API_KEY not set; skipping API call.")
    else:
        api_payload = fetch_exchange_rates(args.api_key)
        if api_payload:
            write_raw_api_json(api_payload, raw_api_base, ingest_date, args.gcs_bucket, args.gcp_project)
            rates = api_payload.get("conversion_rates")

    # Clickstream
    click_path = Path(args.clickstream_path)
    if not click_path.exists():
        LOGGER.warning("Clickstream input missing: %s", click_path)
    else:
        click_df = read_csv_in_chunks(click_path, args.chunk_size, transform_clickstream)
        click_df, removed = deduplicate(click_df)
        if removed:
            LOGGER.info("Removed %s duplicate clickstream rows.", removed)
        write_partitioned(
            click_df,
            "clickstream",
            ingest_date,
            output_base,
            args.gcs_bucket,
            args.gcp_project,
        )

    # Transactions
    txn_path = Path(args.transactions_path)
    if not txn_path.exists():
        LOGGER.warning("Transactions input missing: %s", txn_path)
    else:
        if rates is None:
            LOGGER.warning("No exchange rates available; amount_in_usd will be null.")

        def _transform_txn(chunk):
            return transform_transactions(chunk, rates)

        txn_df = read_csv_in_chunks(txn_path, args.chunk_size, _transform_txn)
        txn_df, removed = deduplicate(txn_df, subset=["txn_id"] if "txn_id" in txn_df.columns else None)
        if removed:
            LOGGER.info("Removed %s duplicate transaction rows.", removed)
        write_partitioned(
            txn_df,
            "transactions",
            ingest_date,
            output_base,
            args.gcs_bucket,
            args.gcp_project,
        )

    LOGGER.info("ETL complete.")


if __name__ == "__main__":
    main()
