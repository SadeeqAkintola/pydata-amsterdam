# Root Cause Analysis (RCA) Report: Cloud Function Trigger Failure for Airflow DAG

**Date**: September 6, 2026  
**Environment**: Google Cloud Platform  
**Project ID**: `pydata-amsterdam-data-demo` (`773517017433`)  
**Region**: `europe-west4` (Amsterdam)  
**Cloud Function**: `trigger-airflow-beam-dag` (2nd Gen)  
**Composer Environment**: `pydata-amsterdam` (`composer-3-airflow-3.3.1-build.1`)  
**Target DAG**: `airflow_beam_dag`  

---

## 1. Executive Summary

When CSV files were uploaded to the GCS bucket `gs://pydata-amsterdam-uploads/` (7 files present, exceeding the required threshold of 6), the event-driven Cloud Function `trigger-airflow-beam-dag` was executed via Cloud Storage `google.cloud.storage.object.v1.finalized` events, but **crashed with an uncaught `RuntimeError`**.

As a result, no trigger request reached Cloud Composer, and the Apache Airflow DAG `airflow_beam_dag` was never initiated.

Furthermore, forensic inspection of the codebase identified an additional latent bug: the Cloud Function helper code is written against **Airflow 1/2 REST API (`/api/v1`)**, whereas the Cloud Composer 3 environment is running **Airflow 3 (`composer-3-airflow-3.3.1-build.1`)**, where `/api/v1` has been removed and replaced by `/api/v2` with mandatory fields.

---

## 2. Evidence & Forensic Findings

### Finding 1: Empty Environment Variable on Cloud Function (Primary Blocker)
* **Configuration Inspection**:
  Inspection via `gcloud functions describe trigger-airflow-beam-dag --region=europe-west4 --gen2` revealed:
  ```yaml
  serviceConfig:
    environmentVariables:
      AIRFLOW_UI_URL: ''
      LOG_EXECUTION_ID: 'true'
      TARGET_DAG_ID: airflow_beam_dag
      TRIGGER_THRESHOLD: '6'
  ```
  `AIRFLOW_UI_URL` was deployed with an **empty string (`''`)**.

* **Execution Logs**:
  Inspection via `gcloud functions logs read trigger-airflow-beam-dag --region=europe-west4 --gen2` captured the exact failure at runtime:
  ```text
  AIRFLOW_UI_URL environment variable is missing or empty! Please configure the AIRFLOW_UI_URL on this Cloud Function with the Composer Web Server URI.
  Traceback (most recent call last):
    ...
    File "/workspace/main.py", line 187, in trigger_dag_gcf
      raise RuntimeError(err)
  RuntimeError: AIRFLOW_UI_URL environment variable is missing or empty! Please configure the AIRFLOW_UI_URL on this Cloud Function with the Composer Web Server URI.
  ```

* **Code Correlation**:
  In [`cloud_function/main.py`](file:///Users/sadeeq/workspace/pydata-amsterdam/cloud_function/main.py#L181-L188):
  ```python
  if not AIRFLOW_UI_URL:
      err = (
          "AIRFLOW_UI_URL environment variable is missing or empty! "
          "Please configure the AIRFLOW_UI_URL on this Cloud Function with the Composer Web Server URI."
      )
      logger.error(err)
      raise RuntimeError(err)
  ```
  The function successfully counted the files (7 files $\ge$ 6 threshold) and proceeded to step 7 ("Threshold Reached -> Validate Composer Web Server Config"). Because `AIRFLOW_UI_URL` was empty, it threw `RuntimeError` and crashed.

---

### Finding 2: Incompatibility with Airflow 3 REST API (`/api/v2`) (Secondary Blocker)

Even when `AIRFLOW_UI_URL` is set to the Composer Airflow Web Server URL (`https://5b089f1711a446ea921c50f7a674138e-dot-europe-west4.composer.googleusercontent.com`), triggering fails due to two breaking changes in Airflow 3:

1. **Endpoint Deprecated and Removed (`/api/v1` -> `/api/v2`)**:
   In [`cloud_function/composer_to_airflow_rest_api.py`](file:///Users/sadeeq/workspace/pydata-amsterdam/cloud_function/composer_to_airflow_rest_api.py#L68):
   ```python
   endpoint = f"api/v1/dags/{dag_id}/dagRuns"
   ```
   Testing against the live Composer 3 Web Server returned:
   ```json
   HTTP 404 Not Found
   {"error": "/api/v1 has been removed in Airflow 3, please use its upgraded version /api/v2 instead."}
   ```

2. **Mandatory Field `logical_date` in Airflow 3 POST Payload**:
   The OpenAPI specification on Airflow 3 (`/openapi.json`) defines `TriggerDAGRunPostBody` with:
   ```json
   "required": ["logical_date"]
   ```
   Currently, `composer_to_airflow_rest_api.py` only sends `{"conf": data, "dag_run_id": dag_run_id}`.
   Without `logical_date`, the Airflow 3 FastAPI router returns:
   ```json
   HTTP 422 Unprocessable Entity
   {"detail": [{"type": "missing", "loc": ["body", "logical_date"], "msg": "Field required", "input": {}}]}
   ```

---

## 3. Verification & Proof of Resolution

We executed a live verification using an authenticated session with the Airflow 3 REST API (`/api/v2/dags/airflow_beam_dag/dagRuns`) supplying:
```json
{
  "logical_date": "2026-09-06T19:43:28.037661+00:00",
  "dag_run_id": "test_trigger_1788723808",
  "conf": {"test": "true"}
}
```
* **Result**: `HTTP 200 OK`
* **Live DAG Run**: The run entered state `running`.
* **Tasks Executed**:
  - `start`: `success`
  - `move_files_to_initiated_runs`: `success` (moved the 7 CSV files to `gs://pydata-amsterdam/initiated-runs/`)
  - `run_beam_pipeline`: `deferred` (submitted Dataflow batch job `pydata-pipeline-triggered-at-20260906-194401` which is currently running in `europe-west4`).

This confirms that the DAG logic, permissions, and pipeline are completely functional when triggered via the Airflow 3 API.

---

## 4. Remediation Plan

To permanently fix the automatic trigger:

### Step 1: Update `cloud_function/composer_to_airflow_rest_api.py`
Update `trigger_dag` to use `/api/v2` and provide `logical_date`:

```python
import datetime

def trigger_dag(
    web_server_url: str,
    dag_id: str,
    data: dict,
    dag_run_id: Optional[str] = None
) -> str:
    clean_base_url = web_server_url.strip().rstrip("/")
    endpoint = f"api/v2/dags/{dag_id}/dagRuns"
    request_url = f"{clean_base_url}/{endpoint}"

    now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
    payload: dict[str, Any] = {
        "logical_date": now_iso,
        "conf": data,
    }
    if dag_run_id:
        payload["dag_run_id"] = dag_run_id

    response = make_composer_web_server_request(
        request_url,
        method="POST",
        json=payload,
        headers={"Content-Type": "application/json", "Accept": "application/json"}
    )
    ...
```

### Step 2: Redeploy `trigger-airflow-beam-dag` Cloud Function
Deploy with `AIRFLOW_UI_URL` populated from the Composer 3 environment:

```bash
cd cloud_function

export AIRFLOW_UI_URL="https://5b089f1711a446ea921c50f7a674138e-dot-europe-west4.composer.googleusercontent.com"

gcloud functions deploy trigger-airflow-beam-dag \
  --gen2 \
  --region=europe-west4 \
  --runtime=python311 \
  --source=. \
  --entry-point=trigger_dag_gcf \
  --trigger-bucket=pydata-amsterdam-uploads \
  --trigger-location=europe-west4 \
  --set-env-vars AIRFLOW_UI_URL="$AIRFLOW_UI_URL",TARGET_DAG_ID=airflow_beam_dag,TRIGGER_THRESHOLD=6 \
  --service-account=773517017433-compute@developer.gserviceaccount.com
```
