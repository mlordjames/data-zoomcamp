from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.sdk import Param, get_current_context, task
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import ShortCircuitOperator
from docker.types import Mount

# Airflow container mounts your repo at /opt/project (compose: - ..:/opt/project)
PROJECT_ROOT = Path("/opt/project")
HOST_DATA_DIR = PROJECT_ROOT / "data"  # exists inside Airflow containers

# Container paths (inside your openpayments pipeline image)
CONTAINER_WORKDIR = "/app"
CONTAINER_DATA_DIR = "/app/data"
CONTAINER_OUT_ROOT = "/app/data/out"
CONTAINER_TOTALS_DIR = "/app/data/totals"

DEFAULT_IMAGE = "openpayments:latest"  # stable tag (recommended)
DEFAULT_BUCKET = "openpayments-dezoomcamp2026-us-west-1-1f83ec"

DATA_MOUNT = Mount(
    source=str(HOST_DATA_DIR),
    target=CONTAINER_DATA_DIR,
    type="bind",
)


def _bool_flag(name: str, val: bool) -> str:
    """
    Helper: returns CLI flag name if True else empty string.
    Used for optional click flags like --verbose.
    """
    return name if val else ""


with DAG(
    dag_id="openpayments_phase3_docker_download_validate_upload",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["openpayments", "phase3", "docker"],
    params={
        # runtime image
        "image": Param(DEFAULT_IMAGE, type="string"),

        # download params
        "year": Param(2023, type="integer", minimum=2018, maximum=2035),
        "max_files": Param(2, type="integer", minimum=0, maximum=100000),
        "ensure_totals": Param(True, type="boolean"),
        "id_workers": Param(10, type="integer", minimum=1, maximum=50),
        "page_workers": Param(5, type="integer", minimum=1, maximum=10),
        "totals_workers": Param(2, type="integer", minimum=1, maximum=20),
        "totals_limit": Param(10, type="integer", minimum=1, maximum=5000),
        "totals_country": Param("UNITED STATES", type="string"),
        "resume": Param(True, type="boolean"),
        "verbose": Param(False, type="boolean"),

        # upload params
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

    # -------------------------
    # 1) DOWNLOAD (container)
    # -------------------------
    download = DockerOperator(
        task_id="download_in_container",
        image="{{ params.image }}",
        docker_url="unix://var/run/docker.sock",
        api_version="auto",
        network_mode="bridge",
        working_dir=CONTAINER_WORKDIR,
        entrypoint="python",
        command=[
            "scripts/download_general_payments.py",
            "--dataset", "general-payments",
            "--year", "{{ params.year }}",
            "--out-root", CONTAINER_OUT_ROOT,
            "--totals-dir", CONTAINER_TOTALS_DIR,
            "{{ '--ensure-totals' if params.ensure_totals else '--no-ensure-totals' }}",
            "--max-files", "{{ params.max_files }}",
            "--id-workers", "{{ params.id_workers }}",
            "--page-workers", "{{ params.page_workers }}",
            "--totals-workers", "{{ params.totals_workers }}",
            "--totals-limit", "{{ params.totals_limit }}",
            "--totals-country", "{{ params.totals_country }}",
            "{{ '--resume' if params.resume else '--no-resume' }}",
            "{{ '--verbose' if params.verbose else '' }}",
            # lineage
            "--run-id", "{{ run_id }}",
            # airflow-friendly logs
            "--airflow-mode",
            "--no-progress",
        ],
        mounts=[DATA_MOUNT],
        auto_remove="success",  # Airflow 3: 'never' | 'success' | 'force'
    )

    # -------------------------
    # 2) VALIDATE (in Airflow)
    # -------------------------
    @task
    def validate_manifest() -> str:
        ctx = get_current_context()
        run_id = ctx["run_id"]

        manifest_path = (
            PROJECT_ROOT
            / "data"
            / "out"
            / "metadata"
            / "runs"
            / f"run_id={run_id}"
            / "manifest.json"
        )

        if not manifest_path.exists():
            raise FileNotFoundError(f"Manifest not found: {manifest_path}")

        manifest = json.loads(manifest_path.read_text())
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

        return str(manifest_path)

    validated_manifest = validate_manifest()
    validated_manifest.set_upstream(download)

    # -------------------------
    # 3) SHOULD UPLOAD?
    # -------------------------
    should_upload = ShortCircuitOperator(
        task_id="should_upload",
        python_callable=lambda **kwargs: bool(kwargs["params"]["upload_to_s3"]),
    )
    should_upload.set_upstream(validated_manifest)

    # -------------------------
    # 4) UPLOAD (container)
    # -------------------------
    upload = DockerOperator(
        task_id="upload_in_container",
        image="{{ params.image }}",
        docker_url="unix://var/run/docker.sock",
        api_version="auto",
        network_mode="bridge",
        working_dir=CONTAINER_WORKDIR,
        entrypoint="python",
        command=[
            "scripts/upload_run_to_s3_full_files.py",
            "--bucket", "{{ params.bucket }}",
            "--out-root", CONTAINER_OUT_ROOT,
            "--totals-dir", CONTAINER_TOTALS_DIR,
            "{{ '--include-metadata' if params.include_metadata else '--no-include-metadata' }}",
            "{{ '--include-latest-totals' if params.include_latest_totals else '--no-include-latest-totals' }}",
            "{{ '--overwrite' if params.overwrite else '--no-overwrite' }}",
            "{{ '--delete-local' if params.delete_local else '' }}",
            "{{ '--dry-run' if params.dry_run else '' }}",
            "{{ '--checksum-metadata' if params.checksum_metadata else '' }}",
            "{{ '--verbose' if params.verbose else '' }}",
        ],
        mounts=[DATA_MOUNT],
        auto_remove="success",  # Airflow 3 compliant
    )
    upload.set_upstream(should_upload)

    # -------------------------
    # 5) MARKER (in Airflow)
    # -------------------------
    @task
    def write_marker(manifest_path: str) -> str:
        ctx = get_current_context()
        run_id = ctx["run_id"]
        params = ctx["params"]

        mp = Path(manifest_path)
        run_dir = mp.parent

        marker = {
            "airflow_run_id": run_id,
            "dag_id": ctx["dag"].dag_id,
            "manifest_path": str(mp),
            "upload_to_s3": bool(params["upload_to_s3"]),
            "bucket": str(params["bucket"]),
            "written_at_utc": datetime.utcnow().isoformat() + "Z",
        }

        out = run_dir / "airflow_marker.json"
        out.write_text(json.dumps(marker, indent=2))
        return str(out)

    marker = write_marker(validated_manifest)
    # marker should run whether upload is skipped or run
    marker.set_upstream(should_upload)
    marker.set_upstream(upload)

    # graph
    start >> download >> validated_manifest >> should_upload
    should_upload >> upload
    marker >> end