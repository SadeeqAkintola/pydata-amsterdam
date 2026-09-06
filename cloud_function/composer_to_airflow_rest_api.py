# cloud_function/composer_to_airflow_rest_api.py
"""
Helper module for Cloud Functions (2nd Gen) to authenticate and trigger
Apache Airflow DAG runs via the stable Airflow 2/3 REST API on Google Cloud Composer.
"""

from __future__ import annotations

import datetime
import logging
from typing import Any, Optional
import google.auth
from google.auth.transport.requests import AuthorizedSession
import requests

logger = logging.getLogger(__name__)

# Following Google Cloud best practices, credentials and the authorized session
# are initialized at cold start and reused across function invocations.
AUTH_SCOPE = "https://www.googleapis.com/auth/cloud-platform"
CREDENTIALS, _ = google.auth.default(scopes=[AUTH_SCOPE])
AUTHED_SESSION = AuthorizedSession(CREDENTIALS)


def make_composer_web_server_request(
    url: str, method: str = "GET", **kwargs: Any
) -> requests.Response:
    """
    Makes an authenticated HTTP request to the Cloud Composer Airflow web server.

    Args:
        url: The Airflow endpoint URL.
        method: HTTP method ('GET', 'POST', etc.). Default is 'GET'.
        **kwargs: Optional parameters passed to requests (e.g. json, timeout).

    Returns:
        The requests.Response object.
    """
    if "timeout" not in kwargs:
        kwargs["timeout"] = 90

    return AUTHED_SESSION.request(method, url, **kwargs)


def has_queued_dag_runs(web_server_url: str, dag_id: str) -> bool:
    """
    Checks if there are currently any QUEUED DAG runs for the given DAG in Airflow.
    If a run is already queued, Airflow will automatically process newly uploaded
    files once the currently active run completes.
    """
    clean_base_url = web_server_url.strip().rstrip("/")
    # Airflow 3 uses ?state=queued
    url_v2 = f"{clean_base_url}/api/v2/dags/{dag_id}/dagRuns?state=queued"
    try:
        resp = make_composer_web_server_request(url_v2, method="GET", timeout=10)
        if resp.status_code == 200:
            runs = resp.json().get("dag_runs", [])
            if runs:
                logger.info(f"Found {len(runs)} queued run(s) for DAG '{dag_id}'.")
                return True
            return False
        # Fallback to Airflow 2 endpoint if /api/v2 is not supported
        if resp.status_code == 404:
            url_v1 = f"{clean_base_url}/api/v1/dags/{dag_id}/dagRuns?state=queued"
            resp_v1 = make_composer_web_server_request(url_v1, method="GET", timeout=10)
            if resp_v1.status_code == 200:
                runs = resp_v1.json().get("dag_runs", [])
                return len(runs) > 0
    except Exception as e:
        logger.warning(f"Could not check queued DAG runs for '{dag_id}': {e}")
    return False


def trigger_dag(
    web_server_url: str,
    dag_id: str,
    data: dict,
    dag_run_id: Optional[str] = None
) -> str:
    """
    Triggers an Airflow DAG run using the stable Airflow 2/3 REST API.
    API Reference: https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html

    Args:
        web_server_url: The base URL of the Airflow web server
                        (e.g., https://<tenant-id>-dot-<region>.composer.googleusercontent.com).
        dag_id: The ID of the target Airflow DAG to trigger.
        data: Custom configuration dictionary passed to DAG run conf.
        dag_run_id: Optional unique identifier for this DAG run execution.

    Returns:
        The response body text from the Airflow API.

    Raises:
        requests.HTTPError: If the Airflow API returns an unhandled HTTP error status.
    """
    # Guard against stacking multiple queued runs when files are uploaded continuously
    if has_queued_dag_runs(web_server_url, dag_id):
        skip_msg = f"DAG '{dag_id}' already has a queued run waiting to execute. Skipping duplicate trigger."
        logger.info(skip_msg)
        return skip_msg

    clean_base_url = web_server_url.strip().rstrip("/")
    # Airflow 3 uses /api/v2, Airflow 2 uses /api/v1
    endpoint = f"api/v2/dags/{dag_id}/dagRuns"
    request_url = f"{clean_base_url}/{endpoint}"

    now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
    payload: dict[str, Any] = {
        "logical_date": now_iso,
        "conf": data
    }
    if dag_run_id:
        payload["dag_run_id"] = dag_run_id

    logger.info(f"Triggering DAG '{dag_id}' via Airflow REST API: {request_url}")
    logger.info(f"Payload configuration: {payload}")

    response = make_composer_web_server_request(
        request_url,
        method="POST",
        json=payload,
        headers={"Content-Type": "application/json", "Accept": "application/json"}
    )

    # Fallback to /api/v1 if running on Airflow 2 where /api/v2 endpoint is not present
    if response.status_code == 404 and "api/v2" in endpoint and "not found" in response.text.lower():
        v1_url = f"{clean_base_url}/api/v1/dags/{dag_id}/dagRuns"
        v1_payload: dict[str, Any] = {"conf": data}
        if dag_run_id:
            v1_payload["dag_run_id"] = dag_run_id
        logger.info(f"/api/v2 returned 404, falling back to Airflow 2 /api/v1 endpoint: {v1_url}")
        v1_response = make_composer_web_server_request(
            v1_url,
            method="POST",
            json=v1_payload,
            headers={"Content-Type": "application/json", "Accept": "application/json"}
        )
        if v1_response.status_code in (200, 201):
            logger.info(f"Successfully triggered DAG '{dag_id}' via /api/v1 (HTTP {v1_response.status_code}).")
            return v1_response.text
        if v1_response.status_code not in (404,):
            response = v1_response

    if response.status_code in (200, 201):
        logger.info(f"Successfully triggered DAG '{dag_id}' (HTTP {response.status_code}).")
        return response.text

    if response.status_code == 403:
        raise requests.HTTPError(
            f"Access Denied (HTTP 403) while triggering DAG '{dag_id}'. "
            "Verify that the Cloud Function's service account possesses the 'Composer User' "
            "(roles/composer.user) or 'Composer Administrator' role and appropriate Airflow RBAC permissions.\n"
            f"Server response: {response.text}"
        )

    if response.status_code == 404:
        raise requests.HTTPError(
            f"DAG '{dag_id}' was not found on Composer web server {clean_base_url} (HTTP 404). "
            "Ensure the DAG file is uploaded to the dags/ bucket, contains no syntax/import errors, "
            "and has been parsed by the Airflow scheduler.\n"
            f"Server response: {response.text}"
        )

    if response.status_code == 409:
        logger.warning(
            f"DAG Run conflict (HTTP 409) for DAG '{dag_id}'. A run with this execution timestamp "
            f"or run_id ('{dag_run_id}') already exists in Airflow.\n"
            f"Server response: {response.text}"
        )
        return response.text

    # Raise for any other 4xx/5xx responses
    response.raise_for_status()
    return response.text
