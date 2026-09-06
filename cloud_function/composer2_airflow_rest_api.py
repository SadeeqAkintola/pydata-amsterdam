# cloud_function/composer2_airflow_rest_api.py
# Helper module to authenticate and trigger Managed Airflow (Composer) REST API from Cloud Function.

from __future__ import annotations
from typing import Any
import google.auth
from google.auth.transport.requests import AuthorizedSession
import requests

# Construct authorization scopes and session
AUTH_SCOPE = "https://www.googleapis.com/auth/cloud-platform"
CREDENTIALS, _ = google.auth.default(scopes=[AUTH_SCOPE])

def make_composer2_web_server_request(
    url: str, method: str = "GET", **kwargs: Any
) -> google.auth.transport.Response:
    """
    Make an authorized HTTP request to the Managed Airflow web server.
    """
    authed_session = AuthorizedSession(CREDENTIALS)

    # Set default timeout of 90 seconds if not specified
    if "timeout" not in kwargs:
        kwargs["timeout"] = 90

    return authed_session.request(method, url, **kwargs)

def trigger_dag(web_server_url: str, dag_id: str, data: dict) -> str:
    """
    Triggers an Airflow DAG using the stable Airflow REST API.
    """
    # Remove trailing slash if present
    web_server_url = web_server_url.rstrip("/")
    endpoint = f"api/v1/dags/{dag_id}/dagRuns"
    request_url = f"{web_server_url}/{endpoint}"
    json_data = {"conf": data}

    print(f"Request URL: {request_url}")
    print(f"Payload: {json_data}")

    response = make_composer2_web_server_request(
        request_url, method="POST", json=json_data
    )

    if response.status_code == 403:
        raise requests.HTTPError(
            "Access denied (403). Verify that the Cloud Function service account has the necessary "
            "permissions (Composer User / Composer Administrator role) to trigger the Airflow API.\n"
            f"Details: {response.text}"
        )
    elif response.status_code not in (200, 201):
        response.raise_for_status()
    
    return response.text
