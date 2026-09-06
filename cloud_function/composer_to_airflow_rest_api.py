# cloud_function/composer_to_airflow_rest_api.py
"""
Helper module for Cloud Functions (2nd Gen) to authenticate and trigger
Apache Airflow DAG runs via the stable Airflow 2/3 REST API on Google Cloud Composer.
"""

from __future__ import annotations

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
    clean_base_url = web_server_url.strip().rstrip("/")
    endpoint = f"api/v1/dags/{dag_id}/dagRuns"
    request_url = f"{clean_base_url}/{endpoint}"

    payload: dict[str, Any] = {"conf": data}
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
