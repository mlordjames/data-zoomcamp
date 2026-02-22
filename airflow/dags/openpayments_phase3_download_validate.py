from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context

# Repo mount inside container (per docker-compose volume: - ..:/opt/project)
PROJECT_ROOT = Path("/opt/project")
OUT_ROOT = PROJECT_ROOT / "data" / "out"
TOTALS_DIR = PROJECT_ROOT / "data" / "totals"

DEFAULT_BUCKET = "openpayments-dezoomcamp2026-us-west-1-1f83ec"  # change if needed

with DAG(
    dag_id="openpayments_phase3_download_validate_upload",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["openpayments", "phase3"],
    params={
        # Download params
        "year": Param(2023, type="integer", minimum=2018, maximum=2035),
        "max_files": Param(0, type="integer", minimum=0, maximum=100000),
        "ensure_totals": Param(True, type="boolean"),
        "id_workers": Param(10, type="integer", minimum=1, maximum=50),
        "page_workers": Param(5, type="integer", minimum=1, maximum=10),
        "totals_workers": Param(2, type="integer", minimum=1, maximum=20),
        "totals_limit": Param(10, type="integer", minimum=1, maximum=5000),
        "totals_country": Param("UNITED STATES", type="string"),
        "resume": Param(True, type="boolean"),
        "verbose": Param(False, type="boolean"),

        # Upload params (EC2/role-driven)
        "upload_to_s3": Param(False, type="boolean"),
        "bucket": Param(DEFAULT_BUCKET, type="string"),
        "overwrite": Param(False, type="boolean"),
        "include_metadata": Param(True, type="boolean"),
        "include_latest_totals": Param(False, type="boolean"),
        "delete_local": Param(False, type="boolean"),
        "dry_run": Param(False, type="boolean"),
        "checksum_metadata": Param(False, type="boolean"),
    },
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    @task(retries=2)
    def download() -> str:
        """
        Runs downloader and returns manifest path.
        Uses Airflow run_id as the pipeline run_id (lineage).
        """
        ctx = get_current_context()
        params = ctx["params"]
        airflow_run_id = ctx["run_id"]

        sys.path.insert(0, str(PROJECT_ROOT))
        from scripts.download_general_payments import download_year  # noqa

        manifest_path = download_year(
            dataset="general-payments",
            year=int(params["year"]),
            out_root=OUT_ROOT,
            totals_dir=TOTALS_DIR,
            ensure_totals=bool(params["ensure_totals"]),
            rescrape_totals=False,
            max_files=int(params["max_files"]),
            id_workers=int(params["id_workers"]),
            page_workers=int(params["page_workers"]),
            totals_workers=int(params["totals_workers"]),
            totals_limit=int(params["totals_limit"]),
            totals_country=str(params["totals_country"]),
            slice_str=None,
            resume=bool(params["resume"]),
            verbose=bool(params["verbose"]),
            run_id=airflow_run_id,
            airflow_mode=True,
            no_progress=True,
        )
        return str(Path(manifest_path).resolve())

    @task
    def validate(manifest_path: str) -> str:
        """
        Validates manifest and key artifacts exist.
        """
        mp = Path(manifest_path)
        if not mp.exists():
            raise FileNotFoundError(f"Manifest not found: {mp}")

        manifest = json.loads(mp.read_text())
        status = manifest.get("status")
        if status not in {"completed", "completed_with_failures"}:
            raise ValueError(f"Unexpected manifest status: {status}")

        report_csv = Path(manifest["report_csv"])
        audits_jsonl = Path(manifest["audits_jsonl"])
        if not report_csv.exists():
            raise FileNotFoundError(f"report_csv missing: {report_csv}")
        if not audits_jsonl.exists():
            raise FileNotFoundError(f"audits_jsonl missing: {audits_jsonl}")

        if int(manifest.get("tasks_total", 0)) <= 0:
            raise ValueError("tasks_total <= 0; expected at least 1 task.")

        return manifest_path

    @task(retries=2)
    def upload_to_s3(manifest_path: str) -> str:
        """
        Optional upload step. Skips unless params['upload_to_s3'] is True.

        Runs the existing uploader script as a callable Click app.
        This should work on EC2 using the instance IAM role (no secrets in Git).
        """
        ctx = get_current_context()
        params = ctx["params"]

        if not bool(params["upload_to_s3"]):
            return "SKIPPED: upload_to_s3=false"

        sys.path.insert(0, str(PROJECT_ROOT))
        from scripts.upload_run_to_s3_full_files import main as upload_main  # noqa

        # Build argv exactly like CLI to keep Click validation behavior
        argv = [
            "upload_run_to_s3_full_files.py",
            "--bucket",
            str(params["bucket"]),
            "--out-root",
            str(OUT_ROOT),
            "--totals-dir",
            str(TOTALS_DIR),
            "--include-metadata" if bool(params["include_metadata"]) else "--no-include-metadata",
            "--include-latest-totals" if bool(params["include_latest_totals"]) else "--no-include-latest-totals",
            "--overwrite" if bool(params["overwrite"]) else "--no-overwrite",
        ]

        if bool(params["delete_local"]):
            argv.append("--delete-local")
        if bool(params["dry_run"]):
            argv.append("--dry-run")
        if bool(params["checksum_metadata"]):
            argv.append("--checksum-metadata")
        if bool(params["verbose"]):
            argv.append("--verbose")

        # Run uploader
        sys.argv = argv
        upload_main(standalone_mode=True)

        return f"OK: uploaded to s3://{params['bucket']}"

    @task
    def write_airflow_marker(manifest_path: str, upload_result: str) -> str:
        """
        Writes a lineage breadcrumb into the run directory.
        """
        ctx = get_current_context()
        airflow_run_id = ctx["run_id"]

        mp = Path(manifest_path)
        run_dir = mp.parent

        marker = {
            "airflow_run_id": airflow_run_id,
            "dag_id": ctx["dag"].dag_id,
            "manifest_path": str(mp),
            "upload_result": upload_result,
            "written_at_utc": datetime.utcnow().isoformat() + "Z",
        }

        out = run_dir / "airflow_marker.json"
        out.write_text(json.dumps(marker, indent=2))
        return str(out)

    mpath = download()
    validated = validate(mpath)
    upload_result = upload_to_s3(validated)
    marker = write_airflow_marker(validated, upload_result)

    start >> mpath >> validated >> upload_result >> marker >> end