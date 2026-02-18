#!/usr/bin/env python3

"""
usage
python upload_run_to_s3.py \
  --bucket openpayments-dezoomcamp2026-us-west-1-1f83ec \
  --out-root data/out \
  --dataset general-payments \
  --years 2024 \
  --include-metadata \
  --include-totals-latest

If you want it to delete local files after upload:
 --delete-local

"""
from __future__ import annotations

import argparse
import mimetypes
from pathlib import Path
from typing import Iterable, Tuple, Optional

import boto3
from botocore.exceptions import ClientError


TOTALS_PREFIX = "openpayments_companies_totals_by_year"


def iter_files(root: Path) -> Iterable[Path]:
    for p in root.rglob("*"):
        if p.is_file():
            yield p


def should_skip(local_path: Path) -> bool:
    # Skip temp/partial files and parts
    if local_path.name.endswith(".part"):
        return True
    if "_parts" in local_path.parts:
        return True
    return False


def guess_content_type(path: Path) -> Optional[str]:
    if path.suffix.lower() == ".csv":
        return "text/csv"
    if path.suffix.lower() == ".json":
        return "application/json"
    ct, _ = mimetypes.guess_type(str(path))
    return ct


def find_latest_totals_file(totals_dir: Path) -> Optional[Path]:
    if not totals_dir.exists():
        return None
    candidates = sorted(
        totals_dir.glob(f"{TOTALS_PREFIX}_*.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return candidates[0] if candidates else None


def upload_file(s3_client, bucket: str, local_path: Path, key: str) -> None:
    extra_args = {}
    ct = guess_content_type(local_path)
    if ct:
        extra_args["ContentType"] = ct

    s3_client.upload_file(
        Filename=str(local_path),
        Bucket=bucket,
        Key=key,
        ExtraArgs=extra_args if extra_args else None,
    )


def build_raw_key(dataset: str, year: str, year_dir: Path, local_file: Path) -> str:
    # Preserve relative layout inside year dir (e.g., company_id=...csv)
    rel = local_file.relative_to(year_dir).as_posix()
    return f"raw/dataset={dataset}/year={year}/{rel}"


def build_metadata_key(metadata_root: Path, local_file: Path) -> str:
    rel = local_file.relative_to(metadata_root).as_posix()
    return f"metadata/{rel}"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Upload Open Payments outputs to S3 using current folder structure."
    )
    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    parser.add_argument(
        "--out-root",
        default="data/out",
        help="Local output root (default: data/out)",
    )
    parser.add_argument(
        "--dataset",
        default="general-payments",
        help="Dataset name used in folder naming (default: general-payments)",
    )
    parser.add_argument(
        "--years",
        nargs="+",
        default=["2024"],
        help="Year(s) to upload (default: 2024)",
    )
    parser.add_argument(
        "--include-metadata",
        action="store_true",
        help="Upload data/out/metadata (runs + logs) to S3 under metadata/",
    )
    parser.add_argument(
        "--totals-dir",
        default="data/totals",
        help="Folder containing totals JSON files (default: data/totals)",
    )
    parser.add_argument(
        "--include-totals-latest",
        action="store_true",
        help="Upload the latest totals JSON file to S3 under metadata/totals/",
    )
    parser.add_argument(
        "--delete-local",
        action="store_true",
        help="Delete local files ONLY after successful upload",
    )
    parser.add_argument("--dry-run", action="store_true")

    args = parser.parse_args()

    bucket = args.bucket
    out_root = Path(args.out_root).resolve()
    dataset = args.dataset
    years = [str(y) for y in args.years]

    s3 = boto3.client("s3")

    uploads: list[Tuple[Path, str]] = []

    # RAW outputs: data/out/dataset=.../year=YYYY/...
    for year in years:
        year_dir = out_root / f"dataset={dataset}" / f"year={year}"
        if not year_dir.is_dir():
            raise SystemExit(f"Year dir not found: {year_dir}")

        for f in iter_files(year_dir):
            if should_skip(f):
                continue
            key = build_raw_key(dataset, year, year_dir, f)
            uploads.append((f, key))

    # METADATA: data/out/metadata/{runs,logs}/...
    if args.include_metadata:
        metadata_root = out_root / "metadata"
        if metadata_root.is_dir():
            for f in iter_files(metadata_root):
                if should_skip(f):
                    continue
                uploads.append((f, build_metadata_key(metadata_root, f)))
        else:
            print(f"[WARN] metadata folder not found: {metadata_root}")

    # Totals (latest)
    if args.include_totals_latest:
        totals_dir = Path(args.totals_dir).resolve()
        latest_totals = find_latest_totals_file(totals_dir)
        if not latest_totals:
            print(f"[WARN] No totals file found in: {totals_dir}")
        else:
            uploads.append((latest_totals, f"metadata/totals/{latest_totals.name}"))

    print(f"Local out_root: {out_root}")
    print(f"Bucket:         {bucket}")
    print(f"Uploads planned: {len(uploads)}")

    for local_path, key in uploads:
        s3_uri = f"s3://{bucket}/{key}"
        if args.dry_run:
            print(f"[DRY RUN] {local_path} -> {s3_uri}")
            continue

        try:
            upload_file(s3, bucket, local_path, key)
            print(f"[OK] Uploaded: {local_path} -> {s3_uri}")

            if args.delete_local:
                local_path.unlink()
                print(f"[OK] Deleted local: {local_path}")

        except ClientError as e:
            print(f"[ERROR] Upload failed, not deleting: {local_path}")
            print(e)

    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
