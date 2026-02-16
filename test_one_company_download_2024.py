#!/usr/bin/env python3
from __future__ import annotations

import csv
import os
import time
from pathlib import Path
from typing import Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ------------------------------------------------------------
# CONFIG (hardcoded for quick testing)
# ------------------------------------------------------------
YEAR = 2024

# From your totals file: openpayments_companies_totals_by_year_16-02-2026.json
# company_id=100000000066 has total_2024 ~ 534,793 (definitely > 10k)
COMPANY_ID = "100000010802"

# From your dataset_ids_cache.json for 2024
DATASET_ID_2024 = "9323b84e-cda3-5f6b-a501-b76926c7c035"

PAGE_LIMIT = 5000
MAX_PAGES = 3  # <-- change to 1, 2, 3 for quick tests (each page = up to 5000 rows)

CONNECT_TIMEOUT = 30
READ_TIMEOUT = 180

MAX_RETRIES = 4
BACKOFF_FACTOR = 0.8

MANUAL_ATTEMPTS = 5
MANUAL_BACKOFF_BASE = 2.0

EXPECTED_MIN_COLS = 80
MAX_VALIDATION_BYTES = 512 * 1024

DEFAULT_HEADERS = {
    "Referer": "https://openpaymentsdata.cms.gov/",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
    "Accept": "text/csv,application/csv,application/octet-stream;q=0.9,*/*;q=0.8",
}

OUT_DIR = Path("data_test")
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_FILE = OUT_DIR / f"test_general_payments_{YEAR}_{COMPANY_ID}_pages{MAX_PAGES}.csv"


# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------
def make_session() -> requests.Session:
    sess = requests.Session()

    retry = Retry(
        total=MAX_RETRIES,
        backoff_factor=BACKOFF_FACTOR,
        status_forcelist=(500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )

    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=20,
        pool_maxsize=20,
    )
    sess.mount("https://", adapter)
    sess.mount("http://", adapter)
    sess.headers.update(DEFAULT_HEADERS)
    return sess


def build_params(company_id: str, offset: int) -> dict:
    return {
        "conditions[0][property]": "applicable_manufacturer_or_applicable_gpo_making_payment_id",
        "conditions[0][operator]": "=",
        "conditions[0][value]": company_id,
        "format": "csv",
        "limit": PAGE_LIMIT,
        "offset": offset,
    }


def request_with_manual_backoff(session: requests.Session, url: str, params: dict) -> requests.Response:
    last_exc: Optional[Exception] = None
    for attempt in range(1, MANUAL_ATTEMPTS + 1):
        try:
            resp = session.get(url, params=params, stream=True, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
        except requests.exceptions.RequestException as e:
            last_exc = e
            sleep_s = MANUAL_BACKOFF_BASE * (attempt ** 1.3)
            print(f"[WARN] RequestException attempt={attempt}/{MANUAL_ATTEMPTS}: {e} | sleeping {sleep_s:.1f}s")
            time.sleep(sleep_s)
            continue

        if resp.status_code in (403, 429):
            sleep_s = MANUAL_BACKOFF_BASE * (attempt ** 1.6)
            print(f"[WARN] HTTP {resp.status_code} attempt={attempt}/{MANUAL_ATTEMPTS} | sleeping {sleep_s:.1f}s")
            time.sleep(sleep_s)
            continue

        return resp

    if last_exc:
        raise last_exc
    raise RuntimeError("request_with_manual_backoff exhausted without response")


def validate_header_min_cols(path: Path, expected_min_cols: int) -> Tuple[bool, str]:
    try:
        sample = path.read_bytes()[:MAX_VALIDATION_BYTES]
        header_line = None
        for enc in ("utf-8-sig", "utf-8", "cp1252"):
            try:
                text = sample.decode(enc, errors="replace")
                for line in text.splitlines():
                    if line.strip():
                        header_line = line
                        break
                if header_line:
                    break
            except Exception:
                continue

        if not header_line:
            return (False, "empty_or_binary_file")

        cols = next(csv.reader([header_line]))
        if len(cols) < expected_min_cols:
            return (False, f"too_few_columns({len(cols)}<{expected_min_cols})")
        return (True, "")
    except Exception as e:
        return (False, f"validation_error:{e}")


def truncate_trailing_partial_row(path: Path) -> None:
    try:
        if not path.exists() or path.stat().st_size == 0:
            return
        with path.open("rb+") as fh:
            fh.seek(-1, os.SEEK_END)
            if fh.read(1) == b"\n":
                return
            # find last newline
            block = 131072
            pos = fh.tell()
            buf = b""
            last_nl = -1
            while pos > 0:
                readpos = max(0, pos - block)
                fh.seek(readpos)
                chunk = fh.read(pos - readpos)
                buf = chunk + buf
                idx = buf.rfind(b"\n")
                if idx != -1:
                    last_nl = readpos + idx
                    break
                pos = readpos
            if last_nl == -1:
                fh.truncate(0)
                return
            fh.truncate(last_nl + 1)
    except Exception:
        return


# ------------------------------------------------------------
# Main test
# ------------------------------------------------------------
def main() -> None:
    base_url = f"https://openpaymentsdata.cms.gov/api/1/datastore/query/{DATASET_ID_2024}/download"
    print(f"[INFO] Testing download for YEAR={YEAR}")
    print(f"[INFO] dataset_id={DATASET_ID_2024}")
    print(f"[INFO] company_id={COMPANY_ID}")
    print(f"[INFO] base_url={base_url}")
    print(f"[INFO] output={OUT_FILE.resolve()}")
    print(f"[INFO] max_pages={MAX_PAGES} (limit={PAGE_LIMIT} per page)")

    session = make_session()

    tmp_path = OUT_FILE.with_suffix(".csv.part")
    if tmp_path.exists():
        tmp_path.unlink()

    total_data_rows = 0
    wrote_header = False

    for page_idx in range(MAX_PAGES):
        offset = page_idx * PAGE_LIMIT
        params = build_params(COMPANY_ID, offset)

        print(f"[INFO] Request page={page_idx+1}/{MAX_PAGES} offset={offset} ...")
        resp = request_with_manual_backoff(session, base_url, params)

        print(f"[INFO] HTTP {resp.status_code} | final_url={resp.url}")
        if resp.status_code != 200:
            print("[ERROR] Non-200 response. Stopping.")
            break

        mode = "w" if page_idx == 0 else "a"
        enc = "utf-8-sig" if page_idx == 0 else "utf-8"

        header_seen = False
        data_lines = 0

        with tmp_path.open(mode, encoding=enc, newline="") as fh:
            for line in resp.iter_lines(decode_unicode=True):
                if not line:
                    continue
                if not header_seen:
                    header_seen = True
                    if page_idx == 0:
                        fh.write(line + "\n")
                        wrote_header = True
                    # skip header line for subsequent pages
                    continue
                fh.write(line + "\n")
                data_lines += 1

        truncate_trailing_partial_row(tmp_path)

        print(f"[INFO] page={page_idx+1} wrote data rows={data_lines}")
        if data_lines == 0:
            print("[INFO] No more rows returned (data_lines=0). Stopping early.")
            break

        total_data_rows += data_lines
        time.sleep(0.2)

    if not wrote_header:
        print("[ERROR] No header written. Download likely failed.")
        return

    if total_data_rows == 0:
        print("[ERROR] Header-only file (0 data rows). Condition/company_id might be returning nothing.")
        return

    ok, reason = validate_header_min_cols(tmp_path, EXPECTED_MIN_COLS)
    if not ok:
        print(f"[ERROR] Validation failed: {reason}")
        print(f"[DEBUG] tmp file kept at: {tmp_path.resolve()}")
        return

    tmp_path.replace(OUT_FILE)
    print(f"[SUCCESS] Saved: {OUT_FILE.resolve()}")
    print(f"[SUCCESS] Total data rows written (approx): {total_data_rows}")
    print("[INFO] Next: open the CSV and confirm it looks correct.")


if __name__ == "__main__":
    main()
