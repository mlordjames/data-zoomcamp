# Airflow Phase 3 (Local)

This folder contains a Phase-3 Airflow DAG for the Open Payments ingestion engine.

## What it does
- Task 1: download (calls project code directly, no subprocess)
- Task 2: validate (checks manifest + output artifacts exist)
- Task 3: airflow marker (writes lineage breadcrumb into the run folder)

## Requirements
- Your repo must be mounted into the Airflow container at: /opt/project
- Your Airflow compose must include: - ..:/opt/project

## How to run
1) Start Airflow:
   docker compose up airflow-init
   docker compose up -d

2) Copy the DAG to airflow/dags:
   airflow/dags/openpayments_phase3_download_validate.py

3) Trigger the DAG from the UI
   - You can override parameters (year/max_files/etc.) via the UI "Trigger DAG" form.
