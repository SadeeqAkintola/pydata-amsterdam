# cloud_function/main.py
"""
Google Cloud Functions (2nd Gen) event-driven trigger for Cloud Composer 2 & 3.
Listens to Google Cloud Storage events in gs://pydata-amsterdam-uploads/ and triggers
the target Apache Airflow DAG only when a threshold of at least 6 CSV files is reached.
"""

from __future__ import annotations

import datetime
import logging
import os
import uuid
from typing import Any

try:
    from cloudevents.http import CloudEvent
except ImportError:
    CloudEvent = Any  # type: ignore

try:
    import functions_framework
except ImportError:
    # Fallback decorator for local testing without functions_framework installed
    class _MockFunctionsFramework:
        @staticmethod
        def cloud_event(func):
            return func
    functions_framework = _MockFunctionsFramework()
from google.cloud import storage

import composer_to_airflow_rest_api

# Configure logging format
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# =====================================================================
# CONFIGURATION
# =====================================================================
# Airflow web server base URL (e.g., https://<tenant-id>-dot-<region>.composer.googleusercontent.com)
AIRFLOW_UI_URL = os.environ.get("AIRFLOW_UI_URL")

# Target DAG ID in Airflow to trigger
TARGET_DAG_ID = os.environ.get("TARGET_DAG_ID", "airflow_beam_dag")

# Minimum number of valid CSV files required in the uploads bucket before firing the trigger
TRIGGER_THRESHOLD = int(os.environ.get("TRIGGER_THRESHOLD", "6"))

# Expected source uploads bucket name
TARGET_UPLOADS_BUCKET = os.environ.get("RUNTIME_UPLOADS_BUCKET", "pydata-amsterdam-uploads")
# =====================================================================


def _validate_and_count_bucket_csvs(storage_client: storage.Client, bucket_name: str) -> tuple[int, list[str]]:
    """
    Inspects the GCS bucket and returns the count and list of valid CSV files.
    Applies strict checks to ignore folder markers, hidden files, and empty 0-byte objects.
    """
    bucket = storage_client.bucket(bucket_name)
    blobs = list(storage_client.list_blobs(bucket))

    valid_csvs: list[str] = []
    for blob in blobs:
        b_name = blob.name
        # Check 1: Must end with .csv (case-insensitive)
        if not b_name.lower().endswith(".csv"):
            continue

        # Check 2: Must not be a directory or placeholder marker
        if b_name.endswith("/") or b_name.endswith("/.keep"):
            continue

        # Check 3: Must not be a hidden file (e.g. .DS_Store, ._file.csv)
        base_name = os.path.basename(b_name)
        if base_name.startswith("."):
            continue

        # Check 4: Must not be an empty 0-byte file
        if blob.size is not None and blob.size <= 0:
            logger.warning(f"Ignoring 0-byte empty file in bucket: gs://{bucket_name}/{b_name}")
            continue

        valid_csvs.append(b_name)

    return len(valid_csvs), valid_csvs


@functions_framework.cloud_event
def trigger_dag_gcf(cloudevent: CloudEvent) -> str:
    """
    Cloud Functions (2nd Gen) entry point triggered by Cloud Storage Object Finalized events.

    Args:
        cloudevent: CloudEvent envelope containing GCS object creation details.

    Returns:
        A status string summarizing the action taken.
    """
    # -----------------------------------------------------------------
    # CHECK 1: Event Data Integrity
    # -----------------------------------------------------------------
    data: dict[str, Any] = cloudevent.data if hasattr(cloudevent, "data") else {}
    if not isinstance(data, dict):
        logger.error(f"Invalid CloudEvent payload format: expected dict, received {type(data)}.")
        return "Invalid event data format"

    bucket_name = data.get("bucket")
    file_name = data.get("name")
    raw_size = data.get("size")

    if not bucket_name or not file_name:
        logger.warning(f"Incomplete event payload: bucket='{bucket_name}', file='{file_name}'. Skipping.")
        return "Missing bucket or name in event"

    logger.info(f"Incoming GCS event received for: gs://{bucket_name}/{file_name}")

    # -----------------------------------------------------------------
    # CHECK 2: Directory and Placeholder Exclusion
    # -----------------------------------------------------------------
    if file_name.endswith("/") or file_name.endswith("/.keep"):
        logger.info(f"Skipping folder marker/placeholder object: gs://{bucket_name}/{file_name}")
        return "Ignored directory placeholder"

    # -----------------------------------------------------------------
    # CHECK 3: Hidden Files Exclusion (OS metadata, dotfiles)
    # -----------------------------------------------------------------
    base_name = os.path.basename(file_name)
    if base_name.startswith("."):
        logger.info(f"Skipping hidden/system file: gs://{bucket_name}/{file_name}")
        return "Ignored hidden file"

    # -----------------------------------------------------------------
    # CHECK 4: File Type Validation (Must be .csv)
    # -----------------------------------------------------------------
    if not file_name.lower().endswith(".csv"):
        logger.info(f"Skipping non-CSV file: gs://{bucket_name}/{file_name}")
        return "Ignored non-CSV file"

    # -----------------------------------------------------------------
    # CHECK 5: File Content Sanity (Non-zero size)
    # -----------------------------------------------------------------
    try:
        file_size = int(raw_size) if raw_size is not None else -1
    except (ValueError, TypeError):
        file_size = -1

    if file_size == 0:
        logger.warning(f"File gs://{bucket_name}/{file_name} is completely empty (0 bytes). Skipping trigger check.")
        return "Ignored 0-byte file"

    # -----------------------------------------------------------------
    # CHECK 6: Bucket Inventory & Threshold Verification
    # -----------------------------------------------------------------
    storage_client = storage.Client()
    csv_count, valid_csv_list = _validate_and_count_bucket_csvs(storage_client, bucket_name)

    logger.info(
        f"[THRESHOLD CHECK] Bucket 'gs://{bucket_name}' currently contains {csv_count} valid CSV file(s). "
        f"Required minimum threshold: {TRIGGER_THRESHOLD}."
    )

    # STRICT CONDITION: DO NOT trigger until count reaches or exceeds TRIGGER_THRESHOLD
    if csv_count < TRIGGER_THRESHOLD:
        remaining = TRIGGER_THRESHOLD - csv_count
        status_msg = (
            f"Threshold NOT met: {csv_count}/{TRIGGER_THRESHOLD} valid CSV files present. "
            f"Need {remaining} more file(s) before triggering Airflow. Standing by."
        )
        logger.info(status_msg)
        return status_msg

    # -----------------------------------------------------------------
    # CHECK 7: Threshold Reached -> Validate Composer Web Server Config
    # -----------------------------------------------------------------
    logger.info(
        f"🎯 [THRESHOLD MET] Threshold requirement ({TRIGGER_THRESHOLD} CSVs) satisfied! "
        f"Found {csv_count} CSV files in gs://{bucket_name}. Preparing to fire Airflow DAG '{TARGET_DAG_ID}'..."
    )

    if not AIRFLOW_UI_URL:
        err = (
            "AIRFLOW_UI_URL environment variable is missing or empty! "
            "Please configure the AIRFLOW_UI_URL on this Cloud Function with the Composer Web Server URI."
        )
        logger.error(err)
        raise RuntimeError(err)

    # -----------------------------------------------------------------
    # CHECK 8: Trigger Airflow DAG via REST API
    # -----------------------------------------------------------------
    timestamp_utc = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%S")
    unique_run_id = f"gcs_threshold_trigger__{timestamp_utc}_{uuid.uuid4().hex[:6]}"

    dag_conf = {
        "trigger_source": "gcs_threshold_cloud_function_2nd_gen",
        "source_bucket": bucket_name,
        "initiating_file": file_name,
        "threshold_required": TRIGGER_THRESHOLD,
        "csv_count_at_trigger": csv_count,
        "valid_csv_files": valid_csv_list,
        "triggered_at_utc": datetime.datetime.now(datetime.timezone.utc).isoformat()
    }

    try:
        response_text = composer_to_airflow_rest_api.trigger_dag(
            web_server_url=AIRFLOW_UI_URL,
            dag_id=TARGET_DAG_ID,
            data=dag_conf,
            dag_run_id=unique_run_id
        )
        success_msg = f"Airflow DAG '{TARGET_DAG_ID}' successfully initiated with run_id='{unique_run_id}'."
        logger.info(success_msg)
        return success_msg
    except Exception as exc:
        logger.error(f"Failed to trigger Airflow DAG '{TARGET_DAG_ID}': {exc}", exc_info=True)
        raise exc
