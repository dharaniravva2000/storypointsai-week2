"""Validation helpers for the Week 2 Composer pipeline."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd


@dataclass
class ValidationOutcome:
    """Container for validation results."""

    is_valid: bool
    errors: list[str]
    details: dict[str, int]


def _normalize_codes(codes: Iterable[str]) -> set[str]:
    """Normalize currency codes for comparison."""

    return {str(code).strip().upper() for code in codes if str(code).strip()}


def load_currency_codes_from_api(api_json_path: Path) -> set[str]:
    """Load valid currency codes from a raw API payload."""

    if not api_json_path.exists():
        return set()

    payload = json.loads(api_json_path.read_text())
    # ExchangeRate-API returns conversion rates keyed by currency code.
    rates = payload.get("conversion_rates") or {}
    return _normalize_codes(rates.keys())


def parse_currency_codes(raw_codes: str | None) -> set[str]:
    """Parse a comma-separated list of currency codes."""

    if not raw_codes:
        return set()
    return _normalize_codes(raw_codes.split(","))


def _null_check(df: pd.DataFrame, required_columns: list[str], label: str) -> tuple[list[str], dict[str, int]]:
    """Validate that required columns are non-null."""

    errors: list[str] = []
    details: dict[str, int] = {}

    for col in required_columns:
        if col not in df.columns:
            errors.append(f"{label}: missing required column '{col}'")
            continue
        # Capture null counts so they can be logged alongside validation outcomes.
        nulls = int(df[col].isna().sum())
        details[f"null_{col}"] = nulls
        if nulls:
            errors.append(f"{label}: {nulls} null values in '{col}'")

    return errors, details


def _positive_amount_check(df: pd.DataFrame, amount_column: str) -> tuple[list[str], dict[str, int]]:
    """Validate that amount values are positive."""

    if amount_column not in df.columns:
        return [f"transactions: missing '{amount_column}' column"], {}

    invalid = int((df[amount_column] <= 0).sum())
    details = {"non_positive_amounts": invalid}
    if invalid:
        return [f"transactions: {invalid} non-positive amounts"], details
    return [], details


def _currency_code_check(
    df: pd.DataFrame,
    currency_column: str,
    valid_codes: set[str],
) -> tuple[list[str], dict[str, int]]:
    """Validate currency codes using a reference set."""

    if currency_column not in df.columns:
        return [f"transactions: missing '{currency_column}' column"], {}

    if not valid_codes:
        return ["transactions: no valid currency code reference available"], {}

    # Normalize observed codes before comparison.
    observed = df[currency_column].astype(str).str.upper().str.strip()
    invalid_mask = ~observed.isin(valid_codes)
    invalid = int(invalid_mask.sum())
    details = {"invalid_currency_codes": invalid}
    if invalid:
        return [f"transactions: {invalid} invalid currency codes"], details
    return [], details


def validate_clickstream(df: pd.DataFrame, required_columns: list[str]) -> ValidationOutcome:
    """Validate clickstream data quality rules."""

    errors, details = _null_check(df, required_columns, label="clickstream")
    return ValidationOutcome(is_valid=not errors, errors=errors, details=details)


def validate_transactions(
    df: pd.DataFrame,
    required_columns: list[str],
    amount_column: str,
    currency_column: str,
    valid_currency_codes: set[str],
) -> ValidationOutcome:
    """Validate transaction data quality rules."""

    errors: list[str] = []
    details: dict[str, int] = {}

    null_errors, null_details = _null_check(df, required_columns, label="transactions")
    errors.extend(null_errors)
    details.update(null_details)

    amount_errors, amount_details = _positive_amount_check(df, amount_column)
    errors.extend(amount_errors)
    details.update(amount_details)

    currency_errors, currency_details = _currency_code_check(
        df,
        currency_column,
        valid_currency_codes,
    )
    errors.extend(currency_errors)
    details.update(currency_details)

    return ValidationOutcome(is_valid=not errors, errors=errors, details=details)
