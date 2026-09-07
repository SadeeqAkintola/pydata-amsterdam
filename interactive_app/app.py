# interactive_app/app.py
# Modern Flask web application for PyData Amsterdam 2026.
# Captures participant details, compiles unique CSVs, and uploads to GCS uploads bucket.

from __future__ import annotations

import os
import re
import uuid
import datetime
import logging
from typing import Any
from flask import Flask, request, render_template, redirect, url_for, flash, session
from google.cloud import storage

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "pydata-amsterdam-workshop-2026-secret")

# Environment configurations
BUCKET_NAME = os.environ.get("RUNTIME_UPLOADS_BUCKET", "pydata-amsterdam-uploads")
TRIGGER_THRESHOLD = int(os.environ.get("TRIGGER_THRESHOLD", "6"))


def get_bucket_status() -> dict[str, Any]:
    """
    Inspects the GCS uploads bucket and returns statistics on valid CSV files.
    """
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)
        blobs = list(storage_client.list_blobs(bucket))

        valid_csvs = []
        for blob in blobs:
            name = blob.name
            # Must end with .csv
            if not name.lower().endswith(".csv"):
                continue
            # Ignore hidden files, placeholder markers, or locks
            base_name = os.path.basename(name)
            if base_name.startswith(".") or name.endswith("/") or name.endswith("/.keep"):
                continue
            # Ignore 0-byte objects
            if blob.size is not None and blob.size <= 0:
                continue
            valid_csvs.append(name)

        file_count = len(valid_csvs)
    except Exception as e:
        logger.error(f"Error listing GCS files in gs://{BUCKET_NAME}: {e}")
        valid_csvs = []
        file_count = 0

    required_files = max(0, TRIGGER_THRESHOLD - file_count)
    progress_percentage = min(100, int((file_count / TRIGGER_THRESHOLD) * 100)) if TRIGGER_THRESHOLD > 0 else 100
    threshold_met = file_count >= TRIGGER_THRESHOLD

    return {
        "files": valid_csvs,
        "file_count": file_count,
        "threshold": TRIGGER_THRESHOLD,
        "required_files": required_files,
        "progress_percentage": progress_percentage,
        "threshold_met": threshold_met,
        "bucket_name": BUCKET_NAME,
    }


EMAIL_REGEX = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")


def upload_csv_to_gcs(name: str, email: str, location: str) -> tuple[str, str]:
    """
    Generates a unique anonymized filename combining location, timestamp, and random hex,
    then writes the CSV record into gs://pydata-amsterdam-uploads/.
    Example format: Lisbon_20260906_225857_f5a74f.csv
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)

    # Sanitize location component for safe, readable GCS filename
    loc_slug = re.sub(r"[^a-zA-Z0-9_-]", "", location.strip())[:25] or "Location"
    timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d_%H%M%S")
    rand_hex = uuid.uuid4().hex[:6]

    filename = f"{loc_slug}_{timestamp}_{rand_hex}.csv"

    # Clean CSV values (escape quotes)
    name_clean = name.replace('"', '""')
    email_clean = email.replace('"', '""')
    location_clean = location.replace('"', '""')

    csv_content = f'name,email,location\n"{name_clean}","{email_clean}","{location_clean}"\n'

    blob = bucket.blob(filename)
    blob.upload_from_string(csv_content, content_type="text/csv")
    gcs_uri = f"gs://{BUCKET_NAME}/{filename}"
    logger.info(f"Uploaded participant CSV: {gcs_uri}")
    return filename, gcs_uri


@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        name = request.form.get("name", "").strip()
        email = request.form.get("email", "").strip()
        location = request.form.get("location", "").strip()

        if not name or not email or not location:
            flash("All fields (Name, Email, Location) are required!", "danger")
            return redirect(url_for("index"))

        if not EMAIL_REGEX.match(email):
            flash("Please enter a valid email address (e.g. name@example.com).", "danger")
            return redirect(url_for("index"))

        try:
            filename, gcs_uri = upload_csv_to_gcs(name, email, location)
            # Store submission info in session for success redirect
            session["last_submission"] = {
                "name": name,
                "email": email,
                "location": location,
                "filename": filename,
                "gcs_uri": gcs_uri,
                "submitted_at": datetime.datetime.now(datetime.timezone.utc).strftime("%H:%M:%S UTC"),
            }
            return redirect(url_for("success"))
        except Exception as e:
            logger.error(f"Error uploading to GCS: {e}", exc_info=True)
            flash(f"Error uploading to Google Cloud Storage: {str(e)}", "danger")
            return redirect(url_for("index"))

    # GET: Render form with live bucket status
    status = get_bucket_status()
    return render_template("index.html", **status)


@app.route("/success", methods=["GET"])
def success():
    submission = session.get("last_submission")
    status = get_bucket_status()
    return render_template("success.html", submission=submission, **status)


@app.route("/health", methods=["GET"])
def health():
    return {"status": "ok", "service": "pydata-interactive-app"}, 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
