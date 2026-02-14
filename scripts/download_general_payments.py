#!/usr/bin/env python3
from __future__ import annotations

import csv
import logging
import math
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Tuple

import click
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from urllib3.util.retry import Retry

# If your recordstotal.py is in the same folder, keep this import.
# It must expose: build_totals_json(out_path=..., min_year=..., max_year=..., verbose=..., workers=..., limit=..., country=...)
# (We are NOT forcing you to run totals every time; we will cache totals files.)
import recordstotal  # noqa: E402
import datasetid


# -------------------------------------------------------------------
# CONFIG
# -------------------------------------------------------------------

YEAR_TO_DATASET_ID = {
    int(k): v for k, v in datasetid.getdatasetids(cache_dir="./metadata").items()
}

PAGE_LIMIT = 5000

CONNECT_TIMEOUT = 60
READ_TIMEOUT = 180

MAX_RETRIES = 4
BACKOFF_FACTOR = 0.8

MANUAL_ATTEMPTS = 5
MANUAL_BACKOFF_BASE = 2.0

EXPECTED_MIN_COLS = 80
MAX_VALIDATION_BYTES = 512 * 1024

DEFAULT_HEADERS = {
    "Referer": "https://openpaymentsdata.cms.gov/",
    "User-Agent": "Mozilla/5.0",
    "Accept": "text/csv,application/csv,application/octet-stream;q=0.9,*/*;q=0.8",
}

TOTALS_PREFIX = "openpayments_companies_totals_by_year"
TOTALS_DATE_FMT = "%d-%m-%Y"
TOTALS_MAX_AGE_DAYS = 10


# -------------------------------------------------------------------
# LOGGING
# -------------------------------------------------------------------

def setup_logging(out_root: Path, year: int, verbose: bool) -> Path:
    logs_dir = out_root / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = logs_dir / f"openpayments_download_{year}_{ts}.log"

    level = logging.DEBUG if verbose else logging.INFO
    logger = logging.getLogger()
    logger.setLevel(level)
    logger.handlers.clear()
    logger.propagate = False

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(threadName)s | %(message)s")

    fh = logging.FileHandler(log_path, mode="w", encoding="utf-8")
    fh.setLevel(level)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    sh = logging.StreamHandler()
    sh.setLevel(level)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    logging.info("Logging to: %s", log_path.resolve())
    return log_path


# -------------------------------------------------------------------
# TOTALS CACHE MANAGEMENT
# -------------------------------------------------------------------

@dataclass
class TotalsFile:
    path: Path
    date: datetime


def _parse_totals_date_from_name(name: str) -> Optional[datetime]:
    """
    Expected:
      openpayments_companies_totals_by_year_dd-mm-yyyy.json
    """
    m = re.search(rf"{re.escape(TOTALS_PREFIX)}_(\d{{2}}-\d{{2}}-\d{{4}})\.json$", name)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), TOTALS_DATE_FMT)
    except Exception:
        return None


def find_latest_totals_file(totals_dir: Path) -> Optional[TotalsFile]:
    if not totals_dir.exists():
        return None

    candidates: List[TotalsFile] = []
    for p in totals_dir.glob(f"{TOTALS_PREFIX}_*.json"):
        dt = _parse_totals_date_from_name(p.name)
        if dt:
            candidates.append(TotalsFile(path=p, date=dt))

    if not candidates:
        return None

    candidates.sort(key=lambda x: x.date, reverse=True)
    return candidates[0]


def is_totals_fresh(totals_file: TotalsFile, max_age_days: int = TOTALS_MAX_AGE_DAYS) -> bool:
    age = datetime.now() - totals_file.date
    return age <= timedelta(days=max_age_days)


def totals_file_for_today(totals_dir: Path) -> Path:
    totals_dir.mkdir(parents=True, exist_ok=True)
    today = datetime.now().strftime(TOTALS_DATE_FMT)
    return totals_dir / f"{TOTALS_PREFIX}_{today}.json"


def load_company_totals_json(path: Path) -> pd.DataFrame:
    df = pd.read_json(path)
    df["company_id"] = df["company_id"].astype(str)
    return df


def save_totals_df(df: pd.DataFrame, out_path: Path) -> None:
    # Sort + stable output
    if "company_id" in df.columns:
        df = df.sort_values("company_id").reset_index(drop=True)
    df.to_json(out_path, orient="records", indent=2)
    logging.info("Totals JSON saved: %s", out_path.resolve())


def ensure_totals_for_year(
    year: int,
    totals_dir: Path,
    *,
    rescrape: bool,
    totals_workers: int,
    totals_limit: int,
    totals_country: str,
    verbose: bool,
) -> Path:
    """
    Rules you requested:
    - Totals file name contains dd-mm-yyyy
    - When running:
        - If we have a totals file <= 10 days old and it contains total_{year} => use it (unless rescrape)
        - If fresh file exists but missing total_{year} => scrape only that year and merge into same fresh file
        - If no fresh file => create today's file (and include at least the requested year)
    """
    totals_dir.mkdir(parents=True, exist_ok=True)
    latest = find_latest_totals_file(totals_dir)

    if latest and is_totals_fresh(latest) and (not rescrape):
        logging.info("Found fresh totals file: %s", latest.path.name)
        df_old = load_company_totals_json(latest.path)
        col = f"total_{year}"
        if col in df_old.columns:
            logging.info("Totals file already contains %s. Using it.", col)
            return latest.path

        # Fresh file exists but missing requested year -> scrape only that year and merge in.
        logging.info("Fresh totals file missing %s. Scraping that year and merging...", col)

        tmp_new = totals_dir / f"_tmp_totals_{year}_{int(time.time())}.json"
        recordstotal.build_totals_json(
            out_path=str(tmp_new),
            workers=totals_workers,
            limit=totals_limit,
            country=totals_country,
            min_year=year,
            max_year=year,
            verbose=verbose,
        )
        df_new = load_company_totals_json(tmp_new)

        # Keep only the new year column + company_id (+ error if exists)
        keep_cols = ["company_id"]
        if col in df_new.columns:
            keep_cols.append(col)
        if "error" in df_new.columns:
            keep_cols.append("error")

        df_new = df_new[keep_cols].copy()

        # Merge into old
        df_merged = df_old.merge(df_new, on="company_id", how="outer", suffixes=("", "_new"))

        # If old had no "error" but new has, keep it; if both, prefer old unless old is null.
        if "error_new" in df_merged.columns:
            if "error" in df_merged.columns:
                df_merged["error"] = df_merged["error"].fillna(df_merged["error_new"])
                df_merged.drop(columns=["error_new"], inplace=True)
            else:
                df_merged.rename(columns={"error_new": "error"}, inplace=True)

        save_totals_df(df_merged, latest.path)

        try:
            tmp_new.unlink(missing_ok=True)  # py3.8+; safe in 3.11
        except Exception:
            pass

        return latest.path

    # If we got here: either no fresh file, or rescrape=True, or no file.
    out_path = totals_file_for_today(totals_dir)
    logging.info(
        "Creating totals file (rescrape=%s). Output: %s",
        rescrape,
        out_path.name,
    )

    recordstotal.build_totals_json(
        out_path=str(out_path),
        workers=totals_workers,
        limit=totals_limit,
        country=totals_country,
        min_year=year,
        max_year=year,
        verbose=verbose,
    )
    return out_path


# -------------------------------------------------------------------
# HTTP + DOWNLOAD HELPERS
# -------------------------------------------------------------------

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def make_session(pool_size: int) -> requests.Session:
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
        pool_connections=pool_size,
        pool_maxsize=pool_size,
    )
    sess.mount("https://", adapter)
    sess.mount("http://", adapter)
    sess.headers.update(DEFAULT_HEADERS)
    return sess


def truncate_trailing_partial_row(path: Path) -> None:
    try:
        if not path.exists() or path.stat().st_size == 0:
            return
        with path.open("rb+") as fh:
            fh.seek(-1, os.SEEK_END)
            if fh.read(1) == b"\n":
                return
            block = 131072
            pos = fh.tell()
            buffer = b""
            last_nl = -1
            while pos > 0:
                readpos = max(0, pos - block)
                fh.seek(readpos)
                chunk = fh.read(pos - readpos)
                buffer = chunk + buffer
                idx = buffer.rfind(b"\n")
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


def count_lines_quick(path: Path, max_lines: int = 3) -> int:
    try:
        n = 0
        with path.open("rb") as f:
            for _ in f:
                n += 1
                if n >= max_lines:
                    break
        return n
    except Exception:
        return 0


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
            time.sleep(MANUAL_BACKOFF_BASE * (attempt ** 1.3))
            continue

        if resp.status_code in (403, 429):
            time.sleep(MANUAL_BACKOFF_BASE * (attempt ** 1.6))
            continue

        return resp

    if last_exc:
        raise last_exc
    raise RuntimeError("request_with_manual_backoff exhausted without response")


def download_small_sequential(session: requests.Session, url: str, company_id: str, year: int, year_dir: Path):
    final_path = year_dir / f"csv_{company_id}.csv"
    tmp_path = final_path.with_suffix(".csv.part")

    if tmp_path.exists():
        try:
            tmp_path.unlink()
        except Exception:
            pass

    offset = 0
    total_data_rows = 0
    wrote_header = False

    while True:
        resp = request_with_manual_backoff(session, url, build_params(company_id, offset))
        if resp.status_code != 200:
            try:
                tmp_path.unlink()
            except Exception:
                pass
            return (company_id, year, False, f"HTTP {resp.status_code} at offset {offset}")

        mode = "w" if offset == 0 else "a"
        enc = "utf-8-sig" if offset == 0 else "utf-8"

        header_seen = False
        data_lines = 0

        with tmp_path.open(mode, encoding=enc, newline="") as fh:
            for line in resp.iter_lines(decode_unicode=True):
                if not line:
                    continue
                if not header_seen:
                    header_seen = True
                    if offset == 0:
                        fh.write(line + "\n")
                        wrote_header = True
                    continue
                fh.write(line + "\n")
                data_lines += 1

        truncate_trailing_partial_row(tmp_path)

        if data_lines == 0:
            break

        total_data_rows += data_lines
        offset += PAGE_LIMIT
        time.sleep(0.1)

    if not wrote_header or total_data_rows == 0:
        try:
            tmp_path.unlink()
        except Exception:
            pass
        return (company_id, year, False, "no_results_header_only")

    ok, reason = validate_header_min_cols(tmp_path, EXPECTED_MIN_COLS)
    if not ok:
        try:
            tmp_path.unlink()
        except Exception:
            pass
        return (company_id, year, False, f"validation_failed:{reason}")

    tmp_path.replace(final_path)
    return (company_id, year, True, f"downloaded_small_ok_rows~{total_data_rows}")


def merge_parts(parts: List[Path], final_path: Path) -> Tuple[bool, str]:
    try:
        if final_path.exists():
            final_path.unlink()
        ensure_dir(final_path.parent)
        with final_path.open("wb") as out:
            for p in parts:
                if p.exists() and p.stat().st_size > 0:
                    out.write(p.read_bytes())

        if count_lines_quick(final_path, max_lines=3) < 2:
            try:
                final_path.unlink()
            except Exception:
                pass
            return (False, "no_results_header_only_after_merge")

        ok, reason = validate_header_min_cols(final_path, EXPECTED_MIN_COLS)
        if not ok:
            try:
                final_path.unlink()
            except Exception:
                pass
            return (False, f"validation_failed:{reason}")

        return (True, "merged_ok")
    except Exception as e:
        return (False, f"merge_error:{e}")


def fetch_page_to_partfile(session: requests.Session, url: str, company_id: str, offset: int, part_path: Path, is_first: bool):
    resp = request_with_manual_backoff(session, url, build_params(company_id, offset))
    if resp.status_code != 200:
        return (False, f"HTTP {resp.status_code}", 0)

    if part_path.exists():
        part_path.unlink()

    enc = "utf-8-sig" if is_first else "utf-8"
    first_nonempty_seen = False
    data_lines = 0

    with part_path.open("w", encoding=enc, newline="") as fh:
        for line in resp.iter_lines(decode_unicode=True):
            if not line:
                continue
            if not first_nonempty_seen:
                first_nonempty_seen = True
                if is_first:
                    fh.write(line + "\n")  # header
                continue
            fh.write(line + "\n")
            data_lines += 1

    truncate_trailing_partial_row(part_path)

    if data_lines == 0:
        try:
            part_path.unlink()
        except Exception:
            pass
        return (True, "empty", 0)

    return (True, "ok", data_lines)


def download_big_parallel_pages(session: requests.Session, url: str, company_id: str, expected_total: int, year: int, year_dir: Path, page_workers: int):
    pages = int(math.ceil(expected_total / PAGE_LIMIT))
    if pages <= 0:
        return (company_id, year, False, "bad_expected_total")

    final_path = year_dir / f"csv_{company_id}.csv"
    parts_root = year_dir / "_parts" / company_id
    ensure_dir(parts_root)

    part_paths = [parts_root / f"part_{i:06d}.csv" for i in range(pages)]
    offsets = [i * PAGE_LIMIT for i in range(pages)]

    total_data_rows = 0

    with ThreadPoolExecutor(max_workers=page_workers) as ex:
        futs = {
            ex.submit(fetch_page_to_partfile, session, url, company_id, offsets[i], part_paths[i], i == 0): i
            for i in range(pages)
        }
        for fut in as_completed(futs):
            ok, msg, data_lines = fut.result()
            if not ok:
                return (company_id, year, False, f"page_failed:{msg}")
            total_data_rows += data_lines

    if total_data_rows == 0:
        return (company_id, year, False, "no_results_all_pages_empty")

    ok, msg = merge_parts(part_paths, final_path)
    if not ok:
        return (company_id, year, False, msg)

    # cleanup best-effort
    try:
        for p in part_paths:
            if p.exists():
                p.unlink()
    except Exception:
        pass

    return (company_id, year, True, f"downloaded_big_ok_pages={pages}_rows~{total_data_rows}")


# -------------------------------------------------------------------
# CLICK CLI
# -------------------------------------------------------------------

@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.option("--year", type=int, required=True, help="Year to download (e.g., 2024).")
@click.option("--out-root", type=click.Path(path_type=Path), default=".", show_default=True, help="Output root directory.")
@click.option("--totals-dir", type=click.Path(path_type=Path), default=".", show_default=True, help="Directory to store totals JSON cache files.")
@click.option("--ensure-totals/--no-ensure-totals", default=True, show_default=True, help="Ensure totals JSON exists/is fresh before downloading.")
@click.option("--rescrape-totals", is_flag=True, help="Force re-scrape totals even if a fresh totals file exists.")
@click.option("--max-files", type=int, default=0, show_default=True, help="Limit number of company CSV files to download (0 = no limit).")
@click.option("--id-workers", type=int, default=10, show_default=True, help="Concurrency for company-level downloads.")
@click.option("--page-workers", type=int, default=10, show_default=True, help="Concurrency for page-level parallel downloads (big companies).")
@click.option("--totals-workers", type=int, default=10, show_default=True, help="Concurrency used when scraping totals JSON.")
@click.option("--totals-limit", type=int, default=100, show_default=True, help="How many company IDs to fetch in totals scraper (testing knob).")
@click.option("--totals-country", type=str, default="UNITED STATES", show_default=True, help="Country filter for totals scraper.")
@click.option("--slice", "slice_str", type=str, default=None, help="Optional slice like '0:20' or '20:40' or '20'.")
@click.option("--verbose", is_flag=True, help="Verbose logging.")
def cli(
    year: int,
    out_root: Path,
    totals_dir: Path,
    ensure_totals: bool,
    rescrape_totals: bool,
    max_files: int,
    id_workers: int,
    page_workers: int,
    totals_workers: int,
    totals_limit: int,
    totals_country: str,
    slice_str: Optional[str],
    verbose: bool,
) -> None:
    if year not in YEAR_TO_DATASET_ID:
        raise SystemExit(f"Unsupported year {year}. Supported: {sorted(YEAR_TO_DATASET_ID)}")

    dataset_id = YEAR_TO_DATASET_ID[year]
    base_url = f"https://openpaymentsdata.cms.gov/api/1/datastore/query/{dataset_id}/download"

    setup_logging(out_root=out_root, year=year, verbose=verbose)

    totals_path: Optional[Path] = None
    if ensure_totals:
        totals_path = ensure_totals_for_year(
            year=year,
            totals_dir=totals_dir,
            rescrape=rescrape_totals,
            totals_workers=totals_workers,
            totals_limit=totals_limit,
            totals_country=totals_country,
            verbose=verbose,
        )
    else:
        # If not ensuring totals, we still attempt to use latest totals file if present
        latest = find_latest_totals_file(totals_dir)
        if latest and latest.path.exists():
            totals_path = latest.path
            logging.info("Using latest totals file (ensure_totals disabled): %s", totals_path.name)

    if not totals_path or not totals_path.exists():
        raise FileNotFoundError(
            f"No totals JSON available in {totals_dir}. Enable --ensure-totals or place a totals file there."
        )

    df = load_company_totals_json(totals_path)

    year_dir = out_root / str(year)
    ensure_dir(year_dir)

    total_col = f"total_{year}"
    if total_col not in df.columns:
        raise SystemExit(
            f"Totals JSON ({totals_path.name}) missing required column: {total_col}. "
            f"Try --rescrape-totals or let it merge the year automatically."
        )

    temp = df[["company_id", total_col]].copy()
    temp[total_col] = temp[total_col].fillna(0).astype(int)
    temp = temp[temp[total_col] > 0].sort_values("company_id")

    tasks: List[Tuple[str, int]] = [(r["company_id"], int(r[total_col])) for _, r in temp.iterrows()]

    # Apply slice first
    if slice_str:
        sl = _parse_slice(slice_str)
        tasks = tasks[sl] if sl is not None else tasks
        logging.info("Applied slice=%s -> tasks=%d", slice_str, len(tasks))

    # Apply max_files (testing knob)
    if max_files and max_files > 0:
        tasks = tasks[:max_files]
        logging.info("Applied max_files=%d -> tasks=%d", max_files, len(tasks))

    if not tasks:
        logging.warning("No tasks to run.")
        return

    logging.info("Year=%s | Dataset=%s | Tasks=%d", year, dataset_id, len(tasks))
    logging.info("URL=%s", base_url)
    logging.info("Totals file used: %s", totals_path.resolve())

    pool_size = max(id_workers, page_workers) * 3
    session = make_session(pool_size=pool_size)

    results = []
    with ThreadPoolExecutor(max_workers=id_workers) as ex:
        futs = {}
        for company_id, expected_total in tasks:
            if expected_total <= PAGE_LIMIT:
                fut = ex.submit(download_small_sequential, session, base_url, company_id, year, year_dir)
            else:
                fut = ex.submit(download_big_parallel_pages, session, base_url, company_id, expected_total, year, year_dir, page_workers)
            futs[fut] = (company_id, expected_total)

        for fut in tqdm(as_completed(futs), total=len(futs), desc=f"Downloading ({year})", unit="job"):
            company_id, expected_total = futs[fut]
            try:
                cid, y, ok, msg = fut.result()
                results.append((cid, y, expected_total, ok, msg))
            except Exception as e:
                results.append((company_id, year, expected_total, False, f"executor_error:{e}"))

    report_path = out_root / f"download_report_{year}.csv"
    pd.DataFrame(results, columns=["company_id", "year", "expected_total", "ok", "message"]).to_csv(report_path, index=False)
    logging.info("Report saved: %s", report_path.resolve())


def _parse_slice(slice_str: Optional[str]) -> Optional[slice]:
    if not slice_str:
        return None
    s = slice_str.strip()
    if ":" not in s:
        n = int(s)
        return slice(0, n)

    left, right = s.split(":", 1)
    start = int(left) if left.strip() != "" else None

    right = right.strip()
    if right == "":
        end = None
    else:
        end_int = int(right)
        end = None if end_int == -1 else end_int

    return slice(start, end)


if __name__ == "__main__":
    cli()
