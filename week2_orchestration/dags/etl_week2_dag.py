"""Week 2 DAG: orchestrate ingestion, validation, and loading in GCP Composer."""

from __future__ import annotations

import csv
import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import pendulum
import requests
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

# Ensure Week 1 scripts and Week 2 modules can be imported in Composer.
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

from scripts.run_etl import (  # noqa: E402
    deduplicate,
    fetch_exchange_rates,
    read_csv_in_chunks,
    transform_clickstream,
    transform_transactions,
    upload_to_gcs,
    write_partitioned,
    write_raw_api_json,
)
from week2_orchestration.validation.validation import (  # noqa: E402
    load_currency_codes_from_api,
    parse_currency_codes,
    validate_clickstream,
    validate_transactions,
)


LOGGER = logging.getLogger("storypoints_week2")

# Defaults align to Week 1 schema and can be overridden via env/Variables.
DEFAULT_CLICKSTREAM_COLUMNS = ["user_id", "session_id", "page_url", "click_time", "device", "location"]
DEFAULT_TRANSACTION_COLUMNS = ["txn_id", "user_id", "amount", "currency", "txn_time"]


def _setup_logging() -> None:
    """Ensure logs include timestamps in UTC."""

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )


def _log_json(event: str, payload: dict[str, Any]) -> None:
    """Log structured JSON for observability."""

    LOGGER.info(json.dumps({"event": event, **payload}, sort_keys=True))


def _get_env_value(names: list[str]) -> str | None:
    """Return the first non-empty environment variable from a list."""

    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return None


def _split_csv(raw_value: str | None) -> list[str]:
    """Split comma-separated strings into a list."""

    if not raw_value:
        return []
    return [item.strip() for item in raw_value.split(",") if item.strip()]


def _get_config_value(
    var_name: str,
    env_names: list[str],
    required: bool = False,
    cast: type | None = None,
    default: Any | None = None,
) -> Any:
    """Load configuration from Airflow Variables or environment variables."""

    env_value = _get_env_value(env_names)
    value = Variable.get(var_name, default_var=env_value if env_value is not None else default)

    if required and (value is None or str(value).strip() == ""):
        raise AirflowFailException(f"Missing required config for {var_name} ({'/'.join(env_names)})")

    if cast and value not in (None, ""):
        try:
            return cast(value)
        except Exception as exc:  # pragma: no cover - defensive
            raise AirflowFailException(f"Invalid value for {var_name}: {value}") from exc

    return value


def _load_config() -> dict[str, Any]:
    """Load all runtime configuration for the pipeline."""

    clickstream_path = _get_config_value(
        "week2_clickstream_path",
        ["WEEK2_CLICKSTREAM_PATH", "CLICKSTREAM_PATH"],
        required=True,
    )
    transactions_path = _get_config_value(
        "week2_transactions_path",
        ["WEEK2_TRANSACTIONS_PATH", "TRANSACTIONS_PATH"],
        required=True,
    )
    raw_dir = _get_config_value("week2_raw_dir", ["WEEK2_RAW_DIR"], required=True)
    clean_dir = _get_config_value("week2_clean_dir", ["WEEK2_CLEAN_DIR"], required=True)
    metadata_dir = _get_config_value("week2_metadata_dir", ["WEEK2_METADATA_DIR"], required=True)
    alerts_log_path = _get_config_value(
        "week2_alerts_log_path",
        ["WEEK2_ALERTS_LOG_PATH"],
        required=False,
    )

    chunk_size = _get_config_value(
        "week2_chunk_size",
        ["WEEK2_CHUNK_SIZE", "CHUNK_SIZE"],
        required=True,
        cast=int,
    )
    gcs_bucket = _get_config_value(
        "week2_gcs_bucket",
        ["WEEK2_GCS_BUCKET", "GCS_BUCKET"],
        required=False,
    )
    gcp_project = _get_config_value(
        "week2_gcp_project",
        ["WEEK2_GCP_PROJECT", "GCP_PROJECT_ID"],
        required=False,
    )
    api_key = _get_config_value(
        "week2_exchange_api_key",
        ["WEEK2_EXCHANGE_API_KEY", "EXCHANGE_API_KEY"],
        required=True,
    )

    required_clickstream_cols = _split_csv(
        _get_config_value(
            "week2_clickstream_required_columns",
            ["WEEK2_CLICKSTREAM_REQUIRED_COLUMNS", "CLICKSTREAM_REQUIRED_COLUMNS"],
        )
    )
    required_txn_cols = _split_csv(
        _get_config_value(
            "week2_transactions_required_columns",
            ["WEEK2_TRANSACTIONS_REQUIRED_COLUMNS", "TRANSACTIONS_REQUIRED_COLUMNS"],
        )
    )

    alert_emails = _split_csv(
        _get_config_value(
            "week2_alert_emails",
            ["WEEK2_ALERT_EMAILS", "ALERT_EMAILS"],
            required=False,
        )
    )
    slack_webhook = _get_config_value(
        "week2_slack_webhook",
        ["WEEK2_SLACK_WEBHOOK", "SLACK_WEBHOOK_URL"],
        required=False,
    )

    amount_column = _get_config_value(
        "week2_amount_column",
        ["WEEK2_AMOUNT_COLUMN"],
        required=False,
        default="amount",
    )
    currency_column = _get_config_value(
        "week2_currency_column",
        ["WEEK2_CURRENCY_COLUMN"],
        required=False,
        default="currency",
    )
    txn_id_column = _get_config_value(
        "week2_transaction_id_column",
        ["WEEK2_TRANSACTION_ID_COLUMN"],
        required=False,
        default="txn_id",
    )

    valid_currency_codes = parse_currency_codes(
        _get_config_value(
            "week2_valid_currency_codes",
            ["WEEK2_VALID_CURRENCY_CODES"],
            required=False,
        )
    )

    return {
        "clickstream_path": Path(clickstream_path),
        "transactions_path": Path(transactions_path),
        "raw_dir": Path(raw_dir),
        "clean_dir": Path(clean_dir),
        "metadata_dir": Path(metadata_dir),
        "alerts_log_path": Path(alerts_log_path) if alerts_log_path else None,
        "chunk_size": chunk_size,
        "gcs_bucket": gcs_bucket,
        "gcp_project": gcp_project,
        "api_key": api_key,
        "required_clickstream_cols": required_clickstream_cols or DEFAULT_CLICKSTREAM_COLUMNS,
        "required_txn_cols": required_txn_cols or DEFAULT_TRANSACTION_COLUMNS,
        "alert_emails": alert_emails,
        "slack_webhook": slack_webhook,
        "amount_column": amount_column,
        "currency_column": currency_column,
        "txn_id_column": txn_id_column,
        "valid_currency_codes": valid_currency_codes,
    }


def _stage_raw_csv(source_path: Path, target_path: Path, chunk_size: int) -> int:
    """Stage raw data with chunked ingestion to avoid memory spikes."""

    target_path.parent.mkdir(parents=True, exist_ok=True)
    if target_path.exists():
        target_path.unlink()

    total_rows = 0
    first_chunk = True

    for chunk in pd.read_csv(source_path, chunksize=chunk_size):
        mode = "w" if first_chunk else "a"
        chunk.to_csv(target_path, index=False, mode=mode, header=first_chunk)
        total_rows += len(chunk)
        first_chunk = False

    return total_rows


def _append_run_log(log_path: Path, row: dict[str, Any]) -> None:
    """Append metadata to a CSV run log."""

    log_path.parent.mkdir(parents=True, exist_ok=True)
    file_exists = log_path.exists()

    with log_path.open("a", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=row.keys())
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)


def _write_alert_log(alerts_log_path: Path, payload: dict[str, Any]) -> None:
    """Write alert details to a dedicated alerts log."""

    alerts_log_path.parent.mkdir(parents=True, exist_ok=True)
    with alerts_log_path.open("a") as handle:
        handle.write(json.dumps(payload, sort_keys=True) + "\n")


def _failure_alert_callback(context: dict[str, Any]) -> None:
    """Airflow callback for task failures."""

    _setup_logging()
    config = _load_config()

    exception = context.get("exception")
    task_instance = context.get("task_instance")
    dag_id = getattr(task_instance, "dag_id", "unknown")
    task_id = getattr(task_instance, "task_id", "unknown")
    run_id = context.get("run_id")

    payload = {
        "timestamp_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "dag_id": dag_id,
        "task_id": task_id,
        "run_id": run_id,
        "exception": str(exception) if exception else "unknown",
    }

    alerts_log_path = config["alerts_log_path"] or config["metadata_dir"] / "alerts.log"
    _write_alert_log(alerts_log_path, payload)

    webhook = config.get("slack_webhook")
    if webhook:
        try:
            requests.post(
                webhook,
                json={"text": f"Airflow failure in {dag_id}.{task_id}: {payload['exception']}"},
                timeout=10,
            )
        except Exception as exc:  # pragma: no cover - defensive
            LOGGER.warning("Slack webhook failed: %s", exc)


def _gcs_object_name(path: Path, base_dir: Path) -> str:
    """Derive a stable GCS object name relative to a base directory."""

    try:
        return path.relative_to(base_dir).as_posix()
    except ValueError:
        return path.as_posix()


def ingest_clickstream(ingest_date: str) -> dict[str, Any]:
    """Ingest clickstream data into the raw staging area."""

    _setup_logging()
    config = _load_config()

    source_path = config["clickstream_path"]
    if not source_path.exists():
        raise AirflowFailException(f"Clickstream source missing: {source_path}")

    target_path = config["raw_dir"] / "clickstream" / f"ingest_date={ingest_date}" / "clickstream.csv"
    rows = _stage_raw_csv(source_path, target_path, config["chunk_size"])

    _log_json(
        "ingest_clickstream",
        {"ingest_date": ingest_date, "rows": rows, "target": str(target_path)},
    )

    return {"rows_ingested": rows, "raw_path": str(target_path)}


def ingest_transactions(ingest_date: str) -> dict[str, Any]:
    """Ingest transactions data into the raw staging area."""

    _setup_logging()
    config = _load_config()

    source_path = config["transactions_path"]
    if not source_path.exists():
        raise AirflowFailException(f"Transactions source missing: {source_path}")

    target_path = config["raw_dir"] / "transactions" / f"ingest_date={ingest_date}" / "transactions.csv"
    rows = _stage_raw_csv(source_path, target_path, config["chunk_size"])

    _log_json(
        "ingest_transactions",
        {"ingest_date": ingest_date, "rows": rows, "target": str(target_path)},
    )

    return {"rows_ingested": rows, "raw_path": str(target_path)}


def ingest_currency_api(ingest_date: str) -> dict[str, Any]:
    """Fetch and stage currency exchange rates."""

    _setup_logging()
    config = _load_config()

    payload = fetch_exchange_rates(config["api_key"])
    if payload is None:
        raise AirflowFailException("Exchange rate API failed after retries")
    if not payload.get("conversion_rates"):
        raise AirflowFailException("Exchange rate API response missing conversion_rates")

    api_base = config["raw_dir"] / "api_currency"
    api_path = write_raw_api_json(payload, api_base, ingest_date, None, None)
    if not api_path:
        raise AirflowFailException("Failed to write raw API payload")

    _log_json(
        "ingest_currency_api",
        {"ingest_date": ingest_date, "target": str(api_path)},
    )

    return {"currency_api_status": "success", "api_raw_path": str(api_path)}


def transform_data(ingest_date: str, **context: Any) -> dict[str, Any]:
    """Transform and clean staged datasets."""

    _setup_logging()
    config = _load_config()
    ti = context["ti"]

    click_raw = Path(ti.xcom_pull(task_ids="ingest_clickstream")["raw_path"])
    txn_raw = Path(ti.xcom_pull(task_ids="ingest_transactions")["raw_path"])
    api_raw = Path(ti.xcom_pull(task_ids="ingest_currency_api")["api_raw_path"])

    if not api_raw.exists():
        raise AirflowFailException(f"Currency API payload missing: {api_raw}")

    payload = json.loads(api_raw.read_text())
    rates = payload.get("conversion_rates")

    click_df = read_csv_in_chunks(click_raw, config["chunk_size"], transform_clickstream)
    click_df, removed_click = deduplicate(click_df)

    txn_df = read_csv_in_chunks(
        txn_raw,
        config["chunk_size"],
        lambda chunk: transform_transactions(chunk, rates),
    )
    subset_cols = [config["txn_id_column"]] if config["txn_id_column"] in txn_df.columns else None
    txn_df, removed_txn = deduplicate(txn_df, subset=subset_cols)

    click_target = write_partitioned(
        click_df,
        "clickstream",
        ingest_date,
        config["clean_dir"],
        None,
        None,
    )
    txn_target = write_partitioned(
        txn_df,
        "transactions",
        ingest_date,
        config["clean_dir"],
        None,
        None,
    )

    _log_json(
        "transform",
        {
            "ingest_date": ingest_date,
            "clickstream_rows": len(click_df),
            "transactions_rows": len(txn_df),
            "clickstream_duplicates_removed": removed_click,
            "transactions_duplicates_removed": removed_txn,
        },
    )

    return {
        "clickstream_rows_transformed": len(click_df),
        "transactions_rows_transformed": len(txn_df),
        "clickstream_clean_path": str(click_target),
        "transactions_clean_path": str(txn_target),
    }


def validate_data(ingest_date: str, **context: Any) -> dict[str, Any]:
    """Validate cleaned datasets before loading to GCS."""

    _setup_logging()
    config = _load_config()
    ti = context["ti"]

    click_clean = Path(ti.xcom_pull(task_ids="transform")["clickstream_clean_path"])
    txn_clean = Path(ti.xcom_pull(task_ids="transform")["transactions_clean_path"])
    api_raw = Path(ti.xcom_pull(task_ids="ingest_currency_api")["api_raw_path"])

    if not click_clean.exists() or not txn_clean.exists():
        raise AirflowFailException("Cleaned datasets missing for validation")

    click_df = pd.read_csv(click_clean)
    txn_df = pd.read_csv(txn_clean)

    api_codes = load_currency_codes_from_api(api_raw)
    valid_codes = api_codes or config["valid_currency_codes"]

    click_result = validate_clickstream(click_df, config["required_clickstream_cols"])
    txn_result = validate_transactions(
        txn_df,
        config["required_txn_cols"],
        config["amount_column"],
        config["currency_column"],
        valid_codes,
    )

    errors = click_result.errors + txn_result.errors
    validation_status = "pass" if not errors else "fail"

    _log_json(
        "validation",
        {
            "ingest_date": ingest_date,
            "status": validation_status,
            "errors": errors,
            "clickstream_details": click_result.details,
            "transactions_details": txn_result.details,
        },
    )

    if errors:
        # Preserve validation details for downstream metadata logging.
        ti.xcom_push(key="validation_status", value=validation_status)
        ti.xcom_push(key="validation_errors", value="; ".join(errors))
        raise AirflowFailException("Validation failed: " + "; ".join(errors))

    return {"validation_status": validation_status, "validation_errors": "; ".join(errors)}


def load_to_gcs(ingest_date: str, **context: Any) -> dict[str, Any]:
    """Upload cleaned outputs to GCS."""

    _setup_logging()
    config = _load_config()
    ti = context["ti"]

    if not config["gcs_bucket"]:
        raise AirflowFailException("GCS bucket not configured for load step")

    click_clean = Path(ti.xcom_pull(task_ids="transform")["clickstream_clean_path"])
    txn_clean = Path(ti.xcom_pull(task_ids="transform")["transactions_clean_path"])
    api_raw = Path(ti.xcom_pull(task_ids="ingest_currency_api")["api_raw_path"])

    upload_to_gcs(
        click_clean,
        config["gcs_bucket"],
        _gcs_object_name(click_clean, config["clean_dir"]),
        config["gcp_project"],
    )
    upload_to_gcs(
        txn_clean,
        config["gcs_bucket"],
        _gcs_object_name(txn_clean, config["clean_dir"]),
        config["gcp_project"],
    )
    if api_raw.exists():
        upload_to_gcs(
            api_raw,
            config["gcs_bucket"],
            _gcs_object_name(api_raw, config["raw_dir"]),
            config["gcp_project"],
        )

    _log_json(
        "load_to_gcs",
        {"ingest_date": ingest_date, "bucket": config["gcs_bucket"]},
    )

    return {
        "clickstream_rows_loaded": int(pd.read_csv(click_clean).shape[0]),
        "transactions_rows_loaded": int(pd.read_csv(txn_clean).shape[0]),
    }


def record_metadata(ingest_date: str, **context: Any) -> None:
    """Persist run metadata for observability."""

    _setup_logging()
    config = _load_config()
    ti = context["ti"]

    def _pull(task_id: str, key: str, default: Any = None) -> Any:
        payload = ti.xcom_pull(task_ids=task_id)
        if isinstance(payload, dict) and key in payload:
            return payload.get(key, default)
        keyed_value = ti.xcom_pull(task_ids=task_id, key=key)
        if keyed_value is not None:
            return keyed_value
        return default

    row = {
        "run_id": context.get("run_id"),
        "dag_id": context.get("dag").dag_id if context.get("dag") else "unknown",
        "ingest_date": ingest_date,
        "currency_api_status": _pull("ingest_currency_api", "currency_api_status", "unknown"),
        "clickstream_rows_ingested": _pull("ingest_clickstream", "rows_ingested", 0),
        "transactions_rows_ingested": _pull("ingest_transactions", "rows_ingested", 0),
        "clickstream_rows_transformed": _pull("transform", "clickstream_rows_transformed", 0),
        "transactions_rows_transformed": _pull("transform", "transactions_rows_transformed", 0),
        "clickstream_rows_loaded": _pull("load_to_gcs", "clickstream_rows_loaded", 0),
        "transactions_rows_loaded": _pull("load_to_gcs", "transactions_rows_loaded", 0),
        "validation_status": _pull("validate", "validation_status", "unknown"),
        "validation_errors": _pull("validate", "validation_errors", ""),
        "timestamp_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    run_log_path = config["metadata_dir"] / "run_log.csv"
    _append_run_log(run_log_path, row)

    if config["gcs_bucket"]:
        upload_to_gcs(
            run_log_path,
            config["gcs_bucket"],
            _gcs_object_name(run_log_path, config["metadata_dir"]),
            config["gcp_project"],
        )

    _log_json("metadata_logged", {"ingest_date": ingest_date, "path": str(run_log_path)})


SCHEDULE = Variable.get("week2_schedule", default_var=os.getenv("WEEK2_SCHEDULE")) or None
START_DATE_RAW = Variable.get("week2_start_date", default_var=os.getenv("WEEK2_START_DATE"))
START_DATE = pendulum.parse(START_DATE_RAW) if START_DATE_RAW else pendulum.now("UTC").subtract(days=1)

ALERT_EMAILS_RAW = Variable.get(
    "week2_alert_emails",
    default_var=os.getenv("WEEK2_ALERT_EMAILS", os.getenv("ALERT_EMAILS")),
)
ALERT_EMAILS = _split_csv(ALERT_EMAILS_RAW)
RETRIES = int(Variable.get("week2_retries", default_var=os.getenv("WEEK2_RETRIES", "2")))
RETRY_DELAY_MIN = int(
    Variable.get("week2_retry_delay_min", default_var=os.getenv("WEEK2_RETRY_DELAY_MIN", "5"))
)

DEFAULT_ARGS = {
    "owner": os.getenv("AIRFLOW_OWNER", "airflow"),
    "depends_on_past": False,
    "retries": RETRIES,
    "retry_delay": timedelta(minutes=RETRY_DELAY_MIN),
    "email": ALERT_EMAILS,
    "email_on_failure": bool(ALERT_EMAILS),
    "email_on_retry": False,
    "on_failure_callback": _failure_alert_callback,
}

with DAG(
    dag_id="etl_week2_orchestration",
    default_args=DEFAULT_ARGS,
    start_date=START_DATE,
    schedule=SCHEDULE,
    catchup=False,
    tags=["storypoints", "week2"],
) as dag:
    ingest_clickstream_task = PythonOperator(
        task_id="ingest_clickstream",
        python_callable=ingest_clickstream,
        op_kwargs={"ingest_date": "{{ ds }}"},
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    ingest_transactions_task = PythonOperator(
        task_id="ingest_transactions",
        python_callable=ingest_transactions,
        op_kwargs={"ingest_date": "{{ ds }}"},
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    ingest_currency_task = PythonOperator(
        task_id="ingest_currency_api",
        python_callable=ingest_currency_api,
        op_kwargs={"ingest_date": "{{ ds }}"},
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform_data,
        op_kwargs={"ingest_date": "{{ ds }}"},
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    validate_task = PythonOperator(
        task_id="validate",
        python_callable=validate_data,
        op_kwargs={"ingest_date": "{{ ds }}"},
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    load_task = PythonOperator(
        task_id="load_to_gcs",
        python_callable=load_to_gcs,
        op_kwargs={"ingest_date": "{{ ds }}"},
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    metadata_task = PythonOperator(
        task_id="record_metadata",
        python_callable=record_metadata,
        op_kwargs={"ingest_date": "{{ ds }}"},
        trigger_rule=TriggerRule.ALL_DONE,
    )

    ingest_clickstream_task >> ingest_transactions_task >> ingest_currency_task
    ingest_currency_task >> transform_task >> validate_task >> load_task >> metadata_task
