# interactive_app/app.py
# Modern Flask web application designed for PyData Amsterdam 2026.
# Captures user details, compiles them into a CSV row, and uploads directly to GCS.

import os
import uuid
import datetime
import logging
from flask import Flask, request, render_template, redirect, url_for, flash
from google.cloud import storage

# Configure logging
logging.basicConfig(level=logging.INFO)

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "pydata-amsterdam-secret-2026")

# Environment configurations
BUCKET_NAME = os.environ.get("RUNTIME_UPLOADS_BUCKET", "pydata-amsterdam-uploads")
TRIGGER_THRESHOLD = int(os.environ.get("TRIGGER_THRESHOLD", "5"))

def upload_csv_to_gcs(name: str, email: str, location: str) -> str:
    """
    Formulates a CSV formatted string and uploads it as a file directly to the input GCS bucket.
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)

    # Clean inputs to avoid CSV injection or formatting errors
    name_clean = name.replace('"', '""')
    email_clean = email.replace('"', '""')
    location_clean = location.replace('"', '""')

    # Generate unique filename to avoid collisions
    timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d-%H%M%S")
    filename = f"registration_{timestamp}_{uuid.uuid4().hex[:6]}.csv"

    # CSV data format: header + data row
    csv_content = f'name,email,location\n"{name_clean}","{email_clean}","{location_clean}"\n'

    # Streaming upload to Cloud Storage
    blob = bucket.blob(filename)
    blob.upload_from_string(csv_content, content_type="text/csv")
    logging.info(f"Successfully uploaded CSV '{filename}' to bucket '{BUCKET_NAME}'")
    return filename

@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        name = request.form.get("name", "").strip()
        email = request.form.get("email", "").strip()
        location = request.form.get("location", "").strip()

        if not name or not email or not location:
            flash("All fields (Name, Email, Location) are required!", "danger")
            return redirect(url_for("index"))

        try:
            filename = upload_csv_to_gcs(name, email, location)
            flash(f"Registration successful! File '{filename}' uploaded.", "success")
        except Exception as e:
            logging.error(f"Error uploading to GCS: {e}")
            flash(f"Error uploading to GCS bucket: {str(e)}", "danger")

        return redirect(url_for("index"))

    # GET: Query Cloud Storage to count pending files
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)
        blobs = list(storage_client.list_blobs(bucket))
        # Filter files ending with .csv
        files = [blob.name for blob in blobs if blob.name.lower().endswith(".csv")]
    except Exception as e:
        logging.error(f"Error listing GCS files: {e}")
        files = []
        flash("GCP Authentication Warning: Unable to connect to GCS. Verify your credentials.", "warning")

    file_count = len(files)
    required_files = max(0, TRIGGER_THRESHOLD - file_count)
    progress_percentage = min(100, int((file_count / TRIGGER_THRESHOLD) * 100))

    return render_template(
        "index.html",
        files=files,
        file_count=file_count,
        required_files=required_files,
        progress_percentage=progress_percentage,
        threshold=TRIGGER_THRESHOLD,
        bucket_name=BUCKET_NAME
    )

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=True)
