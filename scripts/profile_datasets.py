#!/usr/bin/env python3
"""Profile datasets for README: schema, nulls, duplicates."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import pandas as pd


def profile_dataset(path: Path, key_cols=None) -> dict:
    df = pd.read_csv(path)
    profile = {
        "rows": len(df),
        "columns": df.columns.tolist(),
        "dtypes": df.dtypes.astype(str).to_dict(),
        "nulls": df.isna().sum().to_dict(),
        "duplicate_rows": int(df.duplicated().sum()),
    }
    if key_cols:
        profile["duplicate_keys"] = int(df.duplicated(subset=key_cols).sum())
    return profile


def main():
    load_env()
    parser = argparse.ArgumentParser()
    parser.add_argument("--clickstream", default=os.getenv("CLICKSTREAM_PATH", "clickstream.csv"))
    parser.add_argument("--transactions", default=os.getenv("TRANSACTIONS_PATH", "transactions.csv"))
    args = parser.parse_args()

    click = profile_dataset(Path(args.clickstream))
    txns = profile_dataset(Path(args.transactions), key_cols=["txn_id"])

    print("# Dataset Profile")
    print("\n## clickstream.csv")
    print(f"Rows: {click['rows']}")
    print(f"Columns: {click['columns']}")
    print(f"Dtypes: {click['dtypes']}")
    print(f"Nulls: {click['nulls']}")
    print(f"Duplicate rows: {click['duplicate_rows']}")
    print("\n## transactions.csv")
    print(f"Rows: {txns['rows']}")
    print(f"Columns: {txns['columns']}")
    print(f"Dtypes: {txns['dtypes']}")
    print(f"Nulls: {txns['nulls']}")
    print(f"Duplicate rows: {txns['duplicate_rows']}")
    print(f"Duplicate txn_id: {txns['duplicate_keys']}")


if __name__ == "__main__":
    main()
ENV_PATH = Path(".env")


def load_env(path: Path = ENV_PATH) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))
