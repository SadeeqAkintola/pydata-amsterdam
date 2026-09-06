# cloud_function/main.py
# Cloud Function (2nd Gen) that acts as an event-driven trigger for Managed Airflow.
# Checks if the GCS bucket contains at least TRIGGER_THRESHOLD files before firing the DAG.

import os
import logging
from google.cloud import storage
import composer2_airflow_rest_api

# Configure logging
logging.basicConfig(level=logging.INFO)

# Load configuration from environment variables
AIRFLOW_UI_URL = os.environ.get("AIRFLOW_UI_URL")  # E.g., "https://xxx.composer.googleusercontent.com"
TARGET_DAG_ID = os.environ.get("TARGET_DAG_ID", "airflow_beam_dag")
TRIGGER_THRESHOLD = int(os.environ.get("TRIGGER_THRESHOLD", "5"))

def trigger_dag_gcf(event, context=None):
    """
    Background Cloud Function triggered by Cloud Storage event.
    
    Args:
        event (dict): Event payload containing file metadata.
        context (google.cloud.functions.Context): Metadata for the event (GCF 1st-Gen backward compatibility).
    """
    # GCF Gen 2 passes event data directly or inside a CloudEvent wrapper
    if hasattr(event, "data"):
        data = event.data
    else:
        data = event

    bucket_name = data.get("bucket")
    file_name = data.get("name")

    if not bucket_name or not file_name:
        logging.error("Invalid trigger payload: missing bucket or name.")
        return "Invalid trigger payload"

    # Only process CSV files
    if not file_name.lower().endswith(".csv"):
        logging.info(f"Skipping non-CSV file: {file_name}")
        return "Ignored non-CSV file"

    logging.info(f"New CSV detected: gs://{bucket_name}/{file_name}")

    # Count the number of CSV files currently in the uploads bucket
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = list(storage_client.list_blobs(bucket))

    # Filter files that end with .csv
    csv_files = [blob.name for blob in blobs if blob.name.lower().endswith(".csv")]
    current_count = len(csv_files)

    logging.info(f"Upload count: {current_count} CSV(s) in gs://{bucket_name} (Threshold: {TRIGGER_THRESHOLD})")

    # Check if threshold is met
    if current_count >= TRIGGER_THRESHOLD:
        logging.info(f"Threshold met! Triggering Apache Airflow DAG: '{TARGET_DAG_ID}'...")
        
        if not AIRFLOW_UI_URL:
            logging.error("AIRFLOW_UI_URL environment variable is not set. Cannot trigger DAG.")
            return "Missing AIRFLOW_UI_URL env var"

        dag_config = {
            "trigger_reason": "Threshold met via Cloud Function",
            "file_count": current_count,
            "initiated_by_file": file_name
        }

        try:
            response = composer2_airflow_rest_api.trigger_dag(
                web_server_url=AIRFLOW_UI_URL,
                dag_id=TARGET_DAG_ID,
                data=dag_config
            )
            logging.info(f"Airflow DAG triggered successfully. WebServer response: {response}")
            return "DAG triggered"
        except Exception as e:
            logging.error(f"Error triggering Airflow DAG: {e}")
            raise e
    else:
        logging.info(f"Waiting for more files. Currently {current_count}/{TRIGGER_THRESHOLD} uploaded.")
        return f"Threshold not met: {current_count}/{TRIGGER_THRESHOLD}"
