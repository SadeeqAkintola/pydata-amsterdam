# airflow_beam_dag.py
# PyData Amsterdam 2026 Workshop Orchestration DAG

import os
import logging
from datetime import datetime, timedelta, timezone
import tempfile

from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowConfiguration
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowSkipException

# Optional libraries for Gemini enrichment and Email notifications
try:
    import base64
    import re
    import sendgrid
    from sendgrid.helpers.mail import (
        Mail, Email, Attachment, FileContent, FileName, FileType, Disposition, ContentId
    )
    import vertexai
    from vertexai.generative_models import GenerativeModel, GenerationConfig
    SENDGRID_VERTEX_INSTALLED = True
except ImportError:
    SENDGRID_VERTEX_INSTALLED = False
    logging.warning("SendGrid / VertexAI unavailable - email task will be skipped.")

# =====================================================================
# ------------ CONFIGURATION -----------------------------------------
# =====================================================================
GCP_PROJECT_ID     = "pydata-amsterdam-data-demo"
GCP_PROJECT_NUMBER = "773517017433"
GCP_REGION         = "europe-west4"  # Amsterdam region
DATAFLOW_NETWORK   = "default"
DATAFLOW_SUBNETWORK = f"regions/{GCP_REGION}/subnetworks/default"

# Keep the destination aligned with the table created in README.md.
BQ_DATASET         = "analytics_sessions"
BQ_TABLE           = "registrations"
BQ_TABLE_FQN       = f"`{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}`"
BIGQUERY_LOCATION  = "EU"

PIPELINE_RESOURCES_BUCKET = "pydata-amsterdam"
RUNTIME_UPLOADS_BUCKET    = "pydata-amsterdam-uploads"
BEAM_PYTHON_SCRIPT_PATH   = f"gs://{PIPELINE_RESOURCES_BUCKET}/beam_pipeline.py"

SENDGRID_API_KEY_VAR_NAME = "SENDGRID_API_KEY"
SENDER_EMAIL              = "hello@sadeeqakintola.com"
SENDER_NAME               = "Sadeeq @ PyData Amsterdam"
VERTEX_MODEL_ID           = "gemini-2.5-flash"

COMPUTE_SA = f"{GCP_PROJECT_NUMBER}-compute@developer.gserviceaccount.com"
# =====================================================================

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 1, 1),
    "retries": 0,
    "retry_delay": timedelta(minutes=3),
    "depends_on_past": False,
}

# ------------ TASKS ------------
@task(task_id="move_files_to_initiated_runs")
def move_files(src_bucket: str, dst_bucket: str, dst_prefix: str) -> list:
    """Moves newly uploaded CSV files from upload bucket to staging."""
    gcs = GCSHook()
    files = [f for f in (gcs.list(src_bucket) or []) if f.endswith(".csv")]
    if not files:
        raise AirflowSkipException("No files found in upload bucket to process.")
    
    moved = []
    for obj in files:
        dst_obj = os.path.join(dst_prefix, os.path.basename(obj))
        gcs.rewrite(src_bucket, obj, dst_bucket, dst_obj)
        gcs.delete(src_bucket, obj)
        moved.append(f"gs://{dst_bucket}/{dst_obj}")
    
    logging.info(f"Moved {len(moved)} files to staging: {moved}")
    return moved


def describe_location(location: str) -> str:
    """Invokes Gemini 2.5 Flash in europe-west4 to generate rich fun facts about a user's location."""
    try:
        vertexai.init(project=GCP_PROJECT_ID, location=GCP_REGION)
        model = GenerativeModel(VERTEX_MODEL_ID)
        prompt = f"""Provide a rich, deeply engaging, and comprehensive collection of fun facts about {location}, spanning both its deep historical roots and its modern, recent cultural or technological achievements.
Write 2 to 3 detailed, vivid paragraphs (or 4 to 5 well-developed bullet points with bold descriptive titles).
Include quirky trivia, hidden secrets, architectural lore, and recent milestones that make {location} truly extraordinary.
Do not limit your response—make it immersive, educational, and inspiring."""
        resp = model.generate_content(
            prompt,
            generation_config={"max_output_tokens": 2048, "temperature": 0.7},
        )
        return resp.text.strip()
    except Exception as e:
        logging.warning(f"Gemini generation unavailable for {location}: {e}")
        return f"{location} boasts a rich cultural history, iconic landmarks, and a thriving community of creators and innovators!"


def generate_travel_poster(location: str) -> tuple:
    """Generates an editorial travel poster using Gemini 2.5 Flash Image on Vertex AI."""
    try:
        from vertexai.generative_models import GenerativeModel, GenerationConfig
        from google.cloud import storage

        # Image generation model is available on Vertex AI in us-central1
        vertexai.init(project=GCP_PROJECT_ID, location="us-central1")
        model = GenerativeModel("gemini-2.5-flash-image")

        prompt = f"""Premium minimalist flat-vector travel poster for {location}, 3:4 vertical format.
Design an original, sophisticated travel poster that captures the authentic visual personality of {location}. The composition must feel purpose-built for this city rather than following a reusable template.

CITY IDENTITY
First interpret the unique visual character of {location} and build the scene around these five elements:
1. A recognizable landmark, architectural feature, or skyline element.
2. A distinctive local way of moving through the city.
3. One subtle everyday lifestyle moment.
4. A native plant, landscape, or environmental characteristic.
5. A composition and viewpoint that naturally belongs to {location}.

The overall scene structure must change from city to city. Do not repeatedly use the same object positions or visual formula.
Possible compositions include:
harbor viewpoint, narrow historic lane, riverside garden, coastal path, elevated city overlook, lively market passage, tram avenue, ferry terminal, botanical setting, heritage square, beach promenade, cultural plaza, or skyline terrace.
Choose whichever composition best represents {location}.

TYPOGRAPHY
Place "{location}" in the upper-left area with generous clean negative space.
Add one short, sophisticated English tagline inspired by the atmosphere of the city.
Typography should feel understated, editorial, spacious, and premium. Never allow text to dominate the artwork.

VISUAL HIERARCHY
Use one iconic landmark or architectural feature as the main focal point.
Support it with only 2–4 carefully selected local elements. Every added detail should strengthen the sense of place.
Avoid landmark collections or postcard-style collages. Keep the scene visually quiet and intentional.

PEOPLE
Include only 3–6 small-scale figures.
Give each person a believable activity connected to local life, such as:
walking through a historic lane, cycling, waiting for public transport, sketching architecture, boarding a ferry, reading outdoors, taking a quiet photograph, carrying beach equipment, browsing a market, or jogging beside the water.
Avoid crowds and avoid making any single person the main character. Figures should integrate naturally into the environment.

LOCAL CHARACTER
Use city-specific elements only when they genuinely fit {location}:
- local transportation
- architecture
- street furniture
- vegetation
- small signs or wayfinding
- local food references
- recreational activities
- pavement markings
- cultural details
Transport and signage should remain subtle. Instead of always using a large signboard, incorporate details naturally through a small transit marker, station clock, ferry information panel, bike symbol, painted road marking, beach flag, or understated metro icon.

ART DIRECTION
Japanese stationery-inspired aesthetic,
luxury sticker illustration,
premium commercial vector artwork,
modern editorial travel branding,
clean delicate outlines,
uniform line weight,
simple geometric forms,
flat-color illustration,
soft shapes,
balanced visual rhythm,
high-end minimalist postcard design.

COLOR SYSTEM
Build the atmosphere primarily with:
pale powder blue, soft sky blue, mist blue, and cool airy blues.
Balance these with:
warm ivory, cream, soft beige, muted sage, gray-green, and understated architectural neutrals.
Use dusty rose or muted blush only for tiny visual accents such as flowers, clothing details, small signs, awnings, or decorative objects.
Colors should remain soft, sophisticated, slightly desaturated, and cohesive.

MOOD
Fresh, airy, peaceful, refined, contemporary, elegant.
The final artwork should feel like a premium boutique travel postcard or luxury lifestyle brand illustration, with generous breathing room and a relaxed sense of place.

IMPORTANT QUALITY RULES
Every city must have its own visual identity.
Change the camera angle, scene structure, landmark placement, foreground treatment, and supporting details according to the city's character.
Do not simply replace {location} inside an existing composition.

NEGATIVE PROMPT
No photorealism.
No realism.
No watercolor.
No painterly brushwork.
No gradients.
No heavy shadows.
No dramatic cinematic lighting.
No paper texture.
No excessive detail.
No cluttered background.
No landmark collage.
No crowded streets.
No oversized characters.
No dominant hero character.
No repetitive café composition.
No fixed signboard position.
No identical foreground treatment.
No generic tourist-poster formula.
No copied layout from another city.
No unnecessary decorative elements.
"""

        logging.info(f"Generating travel poster for {location} using gemini-2.5-flash-image in us-central1...")
        response = model.generate_content(
            prompt,
            generation_config=GenerationConfig(response_modalities=["IMAGE", "TEXT"])
        )

        img_bytes = b""
        for candidate in response.candidates:
            for part in candidate.content.parts:
                if hasattr(part, "inline_data") and part.inline_data and part.inline_data.data:
                    img_bytes = part.inline_data.data
                    break
            if img_bytes:
                break

        if not img_bytes:
            logging.warning(f"No image bytes returned for {location}")
            return b"", ""

        client = storage.Client()
        bucket = client.bucket(PIPELINE_RESOURCES_BUCKET)
        loc_clean = "".join(c for c in location if c.isalnum() or c in ("-", "_")).strip() or "city"
        filename = f"posters/poster_{loc_clean}_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}.png"
        blob = bucket.blob(filename)
        blob.upload_from_string(img_bytes, content_type="image/png")
        logging.info(f"Saved poster to gs://{PIPELINE_RESOURCES_BUCKET}/{filename}")

        gcs_url = f"https://storage.googleapis.com/{PIPELINE_RESOURCES_BUCKET}/{filename}"
        return img_bytes, gcs_url
    except Exception as e:
        logging.warning(f"Unable to generate travel poster for {location}: {e}")
        return b"", ""


@task_group(group_id="process_registrations_and_notify")
def process_registrations_and_notify():

    @task(task_id="query_new_registrations")
    def query_new():
        sql = f"""
            SELECT DISTINCT name, email, location
            FROM {BQ_TABLE_FQN}
            WHERE (is_email_sent IS NULL OR is_email_sent = FALSE)
              AND email IS NOT NULL AND TRIM(email) <> ''
            LIMIT 1000
        """
        df = BigQueryHook(
            gcp_conn_id="google_cloud_default",
            project_id=GCP_PROJECT_ID,
            use_legacy_sql=False,
            location=BIGQUERY_LOCATION,
        ).get_pandas_df(sql=sql, dialect="standard", location=BIGQUERY_LOCATION)
        
        df = df.fillna("")
        recs = df.to_dict("records")
        if not recs:
            raise AirflowSkipException("No new pending registrations found in BigQuery.")
        return recs

    @task(task_id="send_personalized_emails")
    def send_emails(records: list):
        if not SENDGRID_VERTEX_INSTALLED:
            raise AirflowSkipException("SendGrid/VertexAI libraries missing. Skipping email dispatch.")

        try:
            api_key = Variable.get(SENDGRID_API_KEY_VAR_NAME, default_var=None)
        except Exception:
            api_key = None

        if not api_key or api_key.startswith("YOUR_"):
            raise AirflowSkipException(
                f"Airflow Variable '{SENDGRID_API_KEY_VAR_NAME}' is not configured. Skipping email dispatch."
            )

        sg = sendgrid.SendGridAPIClient(api_key)
        successes = []
        funfacts_cache = {}
        poster_cache = {}

        for r in records:
            name = str(r.get("name") or "").strip() or "Attendee"
            email = str(r.get("email") or "").strip()
            location = str(r.get("location") or "").strip() or "Amsterdam"

            if not email or "@" not in email:
                logging.warning(f"Skipping record with invalid email: {r}")
                continue

            loc_key = location.lower()
            if loc_key not in funfacts_cache:
                funfacts_cache[loc_key] = describe_location(location)
            funfacts = funfacts_cache[loc_key]

            if loc_key not in poster_cache:
                poster_cache[loc_key] = generate_travel_poster(location)
            img_bytes, poster_url = poster_cache[loc_key]

            # Format fun facts lines into styled paragraphs or bullets
            paragraphs = [p.strip() for p in funfacts.split("\n\n") if p.strip()]
            formatted_parts = []
            for p in paragraphs:
                p_clean = re.sub(r"\*\*(.*?)\*\*", r"<strong>\1</strong>", p)
                lines = [l.strip() for l in p_clean.splitlines() if l.strip()]
                if any(l.startswith(("*", "-", "•")) for l in lines):
                    bullet_items = []
                    for l in lines:
                        clean_item = l.lstrip("*-• ").strip()
                        if clean_item:
                            bullet_items.append(f"<li style='margin-bottom:12px; line-height:1.6;'>{clean_item}</li>")
                    formatted_parts.append(f"<ul style='margin:10px 0 14px 0; padding-left:20px; color:#4a5568;'>{''.join(bullet_items)}</ul>")
                else:
                    formatted_parts.append(f"<p style='color:#4a5568; line-height:1.6; margin:0 0 14px 0;'>{p_clean}</p>")

            funfacts_html = "".join(formatted_parts) or f"<p style='color:#4a5568; line-height:1.6; margin:0;'>{funfacts}</p>"

            loc_clean = "".join(c for c in location if c.isalnum() or c in ("-", "_")).strip() or "city"
            content_id = f"poster_{loc_clean}"

            if img_bytes:
                poster_html = f"""
                <div style="text-align:center; margin:28px 0; padding:20px; border:1px solid #e2e8f0; border-radius:10px; background-color:#fafbfc;">
                  <p style="font-size:14px; font-weight:600; color:#2d3748; margin:0 0 14px 0;">
                    🎨 A beautiful glimpse of <strong>{location}</strong> to inspire your next data stream!
                  </p>
                  <img src="cid:{content_id}" alt="Travel Poster for {location}" style="max-width:100%; width:340px; height:auto; border-radius:8px; box-shadow:0 4px 12px rgba(0,0,0,0.08); display:block; margin:0 auto;">
                  <p style="margin:12px 0 0 0; font-size:12px; color:#718096;">
                    <a href="{poster_url}" target="_blank" style="color:#0066cc; text-decoration:none; font-weight:500;">Open high-res poster on GCS ↗</a>
                  </p>
                </div>
                """
            else:
                poster_html = ""

            html_body = f"""
            <!DOCTYPE html>
            <html>
              <head>
                <meta charset="utf-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
              </head>
              <body style="margin:0; padding:0; background-color:#f4f6f8; font-family:-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; color:#2d3748;">
                <table width="100%" border="0" cellspacing="0" cellpadding="0" style="background-color:#f4f6f8; padding:30px 15px;">
                  <tr>
                    <td align="center">
                      <table width="100%" border="0" cellspacing="0" cellpadding="0" style="max-width:600px; background-color:#ffffff; border-radius:12px; overflow:hidden; box-shadow:0 4px 16px rgba(0,0,0,0.06); border:1px solid #e2e8f0;">
                        
                        <!-- Header Banner -->
                        <tr>
                          <td style="background: linear-gradient(135deg, #0d1b2a 0%, #1b263b 100%); padding:28px 32px; text-align:left;">
                            <span style="background-color:#00e676; color:#0d1b2a; font-size:11px; font-weight:700; text-transform:uppercase; letter-spacing:1px; padding:4px 8px; border-radius:4px; display:inline-block; margin-bottom:12px;">PyData Amsterdam 2026</span>
                            <h1 style="color:#ffffff; margin:0; font-size:22px; font-weight:700; letter-spacing:-0.5px;">Hey {name}! 👋</h1>
                          </td>
                        </tr>

                        <!-- Body Content -->
                        <tr>
                          <td style="padding:32px;">
                            <p style="font-size:15px; line-height:1.6; color:#4a5568; margin-top:0;">
                              Thanks for joining my <strong>PyData Amsterdam 2026</strong> workshop! Missed a part? Watch the replay <a href="https://amsterdam.pydata.org/program" style="color:#0066cc; text-decoration:underline; font-weight:600;">here</a>.
                            </p>
                            <p style="font-size:15px; line-height:1.6; color:#4a5568;">
                              Full demo source lives on <a href="https://github.com/SadeeqAkintola/pydata-amsterdam" style="color:#0066cc; text-decoration:underline; font-weight:600;">GitHub</a> – feel free to ⭐ and share.
                            </p>
                            
                            <!-- Status Badge -->
                            <div style="background-color:#f0fdf4; border:1px solid #bbf7d0; border-radius:8px; padding:14px 18px; margin:24px 0;">
                              <p style="margin:0; font-size:14px; color:#166534; font-weight:500;">
                                ✅ Your CSV containing <strong>{email}</strong> has been processed and loaded successfully!
                              </p>
                            </div>

                            <!-- Fun Facts Section -->
                            <div style="background-color:#f8fafc; border-left:4px solid #3b82f6; border-radius:0 8px 8px 0; padding:18px 20px; margin:24px 0;">
                              <h3 style="margin:0 0 12px 0; font-size:16px; color:#1e293b;">
                                ✨ Fun facts about {location}
                              </h3>
                              {funfacts_html}
                            </div>

                            <!-- Poster Section -->
                            {poster_html}

                            <!-- Sign-off -->
                            <div style="margin-top:32px; padding-top:20px; border-top:1px solid #edf2f7;">
                              <p style="font-size:14px; color:#4a5568; margin:0 0 16px 0;">
                                Stay in the loop → <a href="https://twitter.com/SadeeqAkintola" style="color:#0066cc; font-weight:600; text-decoration:none;">@SadeeqAkintola</a>
                              </p>
                              <p style="font-size:14px; color:#718096; margin:0; line-height:1.5;">
                                See you in the data streams,<br>
                                <a href="https://sadeeqakintola.com/" style="color:#1a202c; text-decoration:none; font-weight:700; font-size:15px;">Sadeeq</a>
                              </p>
                            </div>
                          </td>
                        </tr>

                        <!-- Footer -->
                        <tr>
                          <td style="background-color:#f8fafc; padding:18px 32px; text-align:center; border-top:1px solid #edf2f7;">
                            <p style="font-size:12px; color:#a0aec0; margin:0;">
                              Sent from <a href="mailto:hello@sadeeqakintola.com" style="color:#a0aec0; text-decoration:none;">hello@sadeeqakintola.com</a> • PyData Amsterdam 2026 Cloud Orchestration
                            </p>
                          </td>
                        </tr>

                      </table>
                    </td>
                  </tr>
                </table>
              </body>
            </html>
            """

            try:
                mail = Mail(
                    from_email=Email(SENDER_EMAIL, SENDER_NAME),
                    to_emails=email,
                    subject=f"🚀 PyData Amsterdam 2026 Workshop Follow-Up — {location}!",
                    plain_text_content=f"Hey {name}!\n\nThanks for joining my PyData Amsterdam 2026 workshop!\nYour record for {email} was processed and loaded successfully.\n\nFun facts about {location}:\n{funfacts}\n\nSee you in the data streams,\nSadeeq",
                    html_content=html_body,
                )

                if img_bytes:
                    encoded_img = base64.b64encode(img_bytes).decode("utf-8")
                    att = Attachment(
                        FileContent(encoded_img),
                        FileName(f"{loc_clean}_travel_poster.png"),
                        FileType("image/png"),
                        Disposition("inline"),
                        ContentId(content_id)
                    )
                    mail.add_attachment(att)

                resp = sg.send(mail)
                if 200 <= resp.status_code < 300:
                    successes.append(email)
                    logging.info(f"Email sent successfully to {email} (status: {resp.status_code})")
                else:
                    logging.warning(f"SendGrid returned status {resp.status_code} for {email}: {resp.body}")
            except Exception as e:
                logging.warning(f"SendGrid dispatch failed for {email}: {e}")

        logging.info(f"Total successful email deliveries: {len(successes)} out of {len(records)}")
        if not successes:
            raise AirflowSkipException("No emails were successfully sent.")

    send_emails(query_new())


UPDATE_SQL = f"""
UPDATE {BQ_TABLE_FQN}
SET is_email_sent = TRUE
WHERE (is_email_sent IS NULL OR is_email_sent = FALSE)
  AND email IS NOT NULL AND TRIM(email) <> ''
"""

# ------------ DAG DEFINITION ------------
@dag(
    dag_id="airflow_beam_dag",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["pydata", "beam", "dataflow"],
)
def airflow_beam_dag():

    start = EmptyOperator(task_id="start")

    moved = move_files(
        src_bucket=RUNTIME_UPLOADS_BUCKET,
        dst_bucket=PIPELINE_RESOURCES_BUCKET,
        dst_prefix="initiated-runs",
    )

    # Fixed: Full explicit pipeline_options with region and project
    run_df = BeamRunPythonPipelineOperator(
        task_id="run_beam_pipeline",
        py_file=BEAM_PYTHON_SCRIPT_PATH,
        runner="DataflowRunner",
        gcp_conn_id="google_cloud_default",
        deferrable=True,
        dataflow_config=DataflowConfiguration(
            project_id=GCP_PROJECT_ID,
            location=GCP_REGION,
        ),
        pipeline_options={
            "project": GCP_PROJECT_ID,
            "region": GCP_REGION,
            "job_name": f"pydata-workshop-{datetime.now(timezone.utc):%Y%m%d-%H%M%S}",
            "temp_location": f"gs://{PIPELINE_RESOURCES_BUCKET}/temp",
            "staging_location": f"gs://{PIPELINE_RESOURCES_BUCKET}/staging",
            "service_account_email": COMPUTE_SA,
            "network": DATAFLOW_NETWORK,
            "subnetwork": DATAFLOW_SUBNETWORK,
            "no_use_public_ips": True,
            "input_pattern": f"gs://{PIPELINE_RESOURCES_BUCKET}/initiated-runs/*.csv",
            "output_table": f"{GCP_PROJECT_ID}:{BQ_DATASET}.{BQ_TABLE}",
            "save_main_session": True,
        },
    )

    notify = process_registrations_and_notify()

    update_bigquery_email_flag = BigQueryInsertJobOperator(
        task_id="update_bigquery_email_flag",
        gcp_conn_id="google_cloud_default",
        configuration={
            "query": {
                "query": UPDATE_SQL,
                "useLegacySql": False,
            }
        },
        location=BIGQUERY_LOCATION,
    )

    @task(task_id="move_files_to_completed", trigger_rule=TriggerRule.NONE_FAILED)
    def move_to_completed(src: str, dst: str, src_prefix: str, dst_prefix: str):
        gcs = GCSHook()
        for obj in gcs.list(src, prefix=src_prefix) or []:
            if not obj.endswith(".csv"):
                continue
            dst_obj = os.path.join(dst_prefix, os.path.basename(obj))
            gcs.rewrite(src, obj, dst, dst_obj)
            gcs.delete(src, obj)
            logging.info(f"Moved completed run file: {obj} -> {dst_obj}")

    cleanup = move_to_completed(
        src=PIPELINE_RESOURCES_BUCKET,
        dst=PIPELINE_RESOURCES_BUCKET,
        src_prefix="initiated-runs", 
        dst_prefix="completed-runs"
    )

    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

    start >> moved >> run_df >> notify >> update_bigquery_email_flag >> cleanup >> end

airflow_beam_dag()
