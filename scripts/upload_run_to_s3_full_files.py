#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import logging
import mimetypes
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional, Tuple

import boto3
import click
from botocore.exceptions import ClientError


# ----------------------------
# Defaults (edit if needed)
# ----------------------------
DEFAULT_BUCKET = "openpayments-dezoomcamp2026-us-west-1-1f83ec"
TOTALS_PREFIX = "openpayments_companies_totals_by_year"


# ----------------------------
# Logging
# ----------------------------
def setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


# ----------------------------
# Helpers
# ----------------------------
def iter_files(root: Path) -> Iterable[Path]:
    for p in root.rglob("*"):
        if p.is_file():
            yield p


def should_skip(p: Path) -> bool:
    if p.name.endswith(".part"):
        return True
    if "_parts" in p.parts:
        return True
    return False


def guess_content_type(path: Path) -> Optional[str]:
    if path.suffix.lower() == ".csv":
        return "text/csv"
    if path.suffix.lower() == ".json":
        return "application/json"
    if path.suffix.lower() == ".jsonl":
        return "application/json"
    ct, _ = mimetypes.guess_type(str(path))
    return ct


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def find_latest_totals_file(totals_dir: Path) -> Optional[Path]:
    if not totals_dir.exists():
        return None
    candidates = sorted(
        totals_dir.glob(f"{TOTALS_PREFIX}_*.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return candidates[0] if candidates else None


def upload_file(
    s3_client,
    bucket: str,
    local_path: Path,
    key: str,
    *,
    add_checksum_metadata: bool,
) -> None:
    extra_args = {}
    ct = guess_content_type(local_path)
    if ct:
        extra_args["ContentType"] = ct

    if add_checksum_metadata:
        extra_args.setdefault("Metadata", {})
        extra_args["Metadata"]["sha256"] = sha256_file(local_path)

    # boto3 upload_file doesn't like ExtraArgs=None; pass only if present
    if extra_args:
        s3_client.upload_file(str(local_path), bucket, key, ExtraArgs=extra_args)
    else:
        s3_client.upload_file(str(local_path), bucket, key)


def build_raw_key(dataset_dir: Path, file_path: Path) -> str:
    """
    Local:
      data/out/dataset=general-payments/year=2024/company_id=...csv

    S3:
      raw/dataset=general-payments/year=2024/company_id=...csv
    """
    rel = file_path.relative_to(dataset_dir).as_posix()
    return f"raw/{dataset_dir.name}/{rel}"


def build_metadata_key(metadata_root: Path, file_path: Path) -> str:
    """
    Local:
      data/out/metadata/runs/run_id=.../manifest.json

    S3:
      metadata/runs/run_id=.../manifest.json
    """
    rel = file_path.relative_to(metadata_root).as_posix()
    return f"metadata/{rel}"


@dataclass
class UploadResult:
    local_path: Path
    s3_key: str
    ok: bool
    message: str


# ----------------------------
# Click CLI
# ----------------------------
@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.option("--bucket", default=DEFAULT_BUCKET, show_default=True, help="Target S3 bucket.")
@click.option(
    "--out-root",
    type=click.Path(path_type=Path),
    default=Path("data/out"),
    show_default=True,
    help="Local output root (contains dataset=* and metadata/).",
)
@click.option(
    "--include-metadata/--no-include-metadata",
    default=True,
    show_default=True,
    help="Upload data/out/metadata (runs + logs).",
)
@click.option(
    "--include-latest-totals/--no-include-latest-totals",
    default=False,
    show_default=True,
    help="Upload latest totals JSON to metadata/totals/.",
)
@click.option(
    "--totals-dir",
    type=click.Path(path_type=Path),
    default=Path("data/totals"),
    show_default=True,
    help="Where totals json files are stored locally.",
)
@click.option("--delete-local", is_flag=True, help="Delete local files after successful upload.")
@click.option("--dry-run", is_flag=True, help="Print planned uploads only.")
@click.option("--checksum-metadata", is_flag=True, help="Add sha256 checksum as S3 object metadata.")
@click.option("--verbose", is_flag=True, help="Verbose logging.")
def main(
    bucket: str,
    out_root: Path,
    include_metadata: bool,
    include_latest_totals: bool,
    totals_dir: Path,
    delete_local: bool,
    dry_run: bool,
    checksum_metadata: bool,
    verbose: bool,
) -> None:
    setup_logging(verbose)

    out_root = out_root.resolve()
    totals_dir = totals_dir.resolve()

    if not out_root.exists():
        raise SystemExit(f"out_root not found: {out_root}")

    s3 = boto3.client("s3")

    uploads: list[Tuple[Path, str]] = []

    # 1) RAW: every dataset=* folder under out_root
    dataset_dirs = sorted([p for p in out_root.iterdir() if p.is_dir() and p.name.startswith("dataset=")])
    if not dataset_dirs:
        logging.warning("No dataset=* folders found under %s", out_root)

    for ddir in dataset_dirs:
        for f in iter_files(ddir):
            if should_skip(f):
                continue
            uploads.append((f, build_raw_key(ddir, f)))

    # 2) METADATA: out_root/metadata/**
    metadata_root = out_root / "metadata"
    if include_metadata:
        if metadata_root.is_dir():
            for f in iter_files(metadata_root):
                if should_skip(f):
                    continue
                uploads.append((f, build_metadata_key(metadata_root, f)))
        else:
            logging.warning("metadata folder not found: %s", metadata_root)

    # 3) Totals (latest)
    if include_latest_totals:
        latest = find_latest_totals_file(totals_dir)
        if latest:
            uploads.append((latest, f"metadata/totals/{latest.name}"))
        else:
            logging.warning("No totals file found in: %s", totals_dir)

    if not uploads:
        logging.warning("Nothing to upload.")
        return

    logging.info("Bucket: %s", bucket)
    logging.info("Planned uploads: %d", len(uploads))

    results: list[UploadResult] = []

    for local_path, key in uploads:
        s3_uri = f"s3://{bucket}/{key}"

        if dry_run:
            logging.info("[DRY RUN] %s -> %s", local_path, s3_uri)
            results.append(UploadResult(local_path, key, True, "dry_run"))
            continue

        try:
            upload_file(
                s3,
                bucket,
                local_path,
                key,
                add_checksum_metadata=checksum_metadata,
            )
            results.append(UploadResult(local_path, key, True, "uploaded"))

            logging.info("[OK] %s -> %s", local_path, s3_uri)

            if delete_local:
                local_path.unlink()
                logging.info("[OK] deleted local: %s", local_path)

        except ClientError as e:
            results.append(UploadResult(local_path, key, False, f"client_error:{e}"))
            logging.error("[FAIL] %s -> %s", local_path, s3_uri)
            logging.exception(e)

        except Exception as e:
            results.append(UploadResult(local_path, key, False, f"error:{e}"))
            logging.error("[FAIL] %s -> %s", local_path, s3_uri)
            logging.exception(e)

    ok_count = sum(1 for r in results if r.ok)
    fail_count = len(results) - ok_count
    logging.info("Upload summary: ok=%d failed=%d", ok_count, fail_count)

    if fail_count:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
