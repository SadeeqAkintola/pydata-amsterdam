#!/bin/bash
# push_to_gcp.sh
# Helper script to sync local DAG and Beam pipeline edits directly to Google Cloud.

# Exit on any error
set -e

# =====================================================================
# CONFIGURATION - UPDATE TO MATCH YOUR WORKSHOP RESOURCES
# =====================================================================
PROJECT_ID="pydata-amsterdam-data-demo"
RESOURCES_BUCKET="pydata-amsterdam"
# Replace with the GCS bucket corresponding to your Managed Airflow (Composer) DAG folder
DAGS_BUCKET="europe-west4-pydata-amsterd-3b7237b6-bucket/dags" 
ACCOUNT="<YOUR_ADMIN_EMAIL>"
# =====================================================================

echo "--------------------------------------------------"
echo " Syncing Workshop Files to Google Cloud Platform..."
echo " Account: $ACCOUNT"
echo " Project: $PROJECT_ID"
echo "--------------------------------------------------"

# Ensure the correct account and project are set
gcloud config set account "$ACCOUNT"
gcloud config set project "$PROJECT_ID"

# Upload Apache Beam script
echo "==> Pushing beam_pipeline.py to GCS resources..."
gcloud storage cp beam_pipeline.py "gs://$RESOURCES_BUCKET/beam_pipeline.py"

# Upload Airflow Orchestration DAG
echo "==> Pushing airflow_beam_dag.py to Managed Airflow DAG bucket..."
gcloud storage cp airflow_beam_dag.py "gs://$DAGS_BUCKET/airflow_beam_dag.py"

echo "--------------------------------------------------"
echo " SUCCESS: Local edits have been synced to Google Cloud!"
echo "--------------------------------------------------"
