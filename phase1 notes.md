# Phase 1 Notes — Open Payments Ingestion System (AWS-Focused Zoomcamp)
*File name: `phase1 notes.md`*

> These notes document everything completed so far in Phase 1 (Weeks 1–2 focus), including the major bugs we hit, how we fixed them, what decisions we made, and what we’re doing next.
>
> Goal: **Build a repeatable, Dockerized ingestion system** for CMS Open Payments **General Payments** data — designed with production thinking (idempotency, caching, validation, observability), even while still in “testing mode”.

---

## 1) What we’re building (the system)
We are building a **data ingestion engine** around the CMS Open Payments dataset that:
- Discovers the correct dataset IDs for each year (dynamic, cached)
- Builds a **company totals** index (`total_YYYY`) used to plan downloads
- Downloads company-level CSV slices with:
  - Pagination (`limit/offset`)
  - Concurrency (threaded)
  - Validation (header checks, partial row truncation)
  - Reporting (per-run CSV report)
- Runs **repeatably** in Docker (WSL-compatible) with outputs mapped to local `./data/`

This is not “a script that downloads a file”. It’s a pipeline component you can evolve into:
- Raw zone in S3
- Metadata/audit zone
- Glue/Athena validation
- Orchestrated runs (Airflow later)

---

## 2) Key milestones achieved so far

### 2.1 Totals Engine: “company totals by year”
We refactored the original `openpayments_companies_totals_by_year.py` into a **library-first module** with a clean programmatic entrypoint:

- Fetches ALL company IDs via the datastore query (paginated)
- For each company ID:
  - Calls `https://openpaymentsdata.cms.gov/api/1/entities/companies/{id}`
  - Extracts `summaryByAvailableYear[].generalTransactions`
  - Writes columns like:
    - `total_2018 … total_2024` (or any requested years)
- Runs concurrently with `ThreadPoolExecutor`
- Captures per-company errors into an `error` column
- Writes JSON **atomically** (`.tmp` → rename) to avoid corruption

**What changed vs the original script**
- Converted “CLI-only” script into reusable functions, especially:
  - `build_totals_json(...)`
  - `fetch_all_company_ids(...)`
  - `fetch_company_totals_by_year(...)`
  - `extract_totals_by_year(...)`
- Added:
  - Optional `years=[...]` override
  - `return_df=True` to use the DF immediately in a downstream script

---

### 2.2 Totals caching strategy (so we don’t re-scrape constantly)
We decided we **should not regenerate totals on every run**.

New approach:
- When totals are built, save with date suffix:
  - `openpayments_companies_totals_by_year_DD-MM-YYYY.json`
- When downloader runs:
  1. Look for the most recent totals file
  2. If the date is **≤ 10 days old** and includes the requested year:
     - Reuse it
  3. If totals file is fresh but missing the requested year:
     - Add the new year (update/extend totals) rather than full rebuild
  4. If totals are stale or missing:
     - Rebuild totals

**Why this matters**
- Reduces upstream API load
- Makes runs faster and more deterministic
- Moves toward “metadata-driven ingestion”

---

### 2.3 General Payments downloader (core ingestion)
We built a year-based downloader that:
- Takes a single `--year` parameter (no more separate scripts per year)
- Uses `limit=5000` and `offset` pagination
- Decides between two strategies per company:
  - **Small** (≤ 5000): sequential offsets
  - **Big** (> 5000): page downloads in parallel + merge

**Reliability measures included**
- HTTP session pooling
- Retry policy via `urllib3 Retry` + manual retry backoff
- Partial row truncation at EOF
- Header validation (minimum number of columns)
- Empty file detection (header-only results)
- Per-run report CSV:
  - `download_report_{year}.csv`

This is already a “real ingestion engine” pattern:
- Plan work (via totals)
- Execute work concurrently
- Validate outputs
- Emit a run report

---

### 2.4 Dockerization (repeatable runtime)
We Dockerized the testing pipeline with:
- `Dockerfile` using `python:3.11-slim`
- **uv** for faster installs (`uv pip install --system -r requirements.txt`)
- `docker-compose.yml` mapping volumes:
  - `./data` → `/app/data`

We made sure:
- All outputs go into `/app/data/...` so they appear locally under `./data/...`
- The same command can be re-run consistently

**WSL note**
- Initially Docker failed in WSL with:
  - `failed to connect to the docker API at unix:///var/run/docker.sock`
- Fix:
  - Enabled Docker Desktop → WSL integration
  - Confirmed Docker works inside WSL

---

## 3) Major bugs we hit and how we fixed them

### Bug #1 — Compose env var warnings and broken CLI args
**Symptom**
- Compose warned variables were not set:
  - `WARN ... OP_YEAR not set. Defaulting to blank string.`
- Then Click failed:
  - `Error: Invalid value for '--year': '--out-root' is not a valid integer.`

**Root cause**
- Docker Compose `${OP_YEAR}` interpolation happens at *compose parse time*.
- Variables defined under `environment:` are container env vars and **do not** satisfy compose interpolation.

**Fix options**
1. **Use `.env`** file in compose directory (recommended)
2. Hardcode args directly in compose `command: [...]`
3. Use `entrypoint: ["sh","-lc"]` and expand env vars *inside* container

**What we learned**
- ENTRYPOINT/command handling matters for CLI tools
- Compose interpolation is different from container environment variables

---

### Bug #2 — Downloads failing (even outside Docker)
We created a standalone local test script and saw:

**Symptom**
- Download endpoint returned **404**:
  - `/api/1/datastore/query/<id>/download` → 404

**Root cause**
- We were using dataset IDs scraped from frontend `index.js`, but those IDs were **not the final datastore distribution IDs** required by `/download`.

---

### Bug #3 — Dataset ID discovery was wrong (critical)
**What we initially did**
- Scraped `index.js` for blocks like:
  - `PGYR2024:[...] type:"generalPayments", id:"<something>"`
- Assumed `id` from JS was the one used in:
  - `/api/1/datastore/query/{id}/download`

**Reality**
- JS “id” is **NOT** the final download-ready id.
- It must be resolved via:
  - `https://openpaymentsdata.cms.gov/api/1/datastore/query/{js_id}/0?results=false&count=true&schema=true`
- Then take:
  - `response.json()["query"]["resources"][0]["id"]`

**Proof**
- The website/curl example worked with a different ID:
  - `018556f0-3fda-5a15-b129-3a62493a87a7`
- But our cached JS id produced 404 when used directly.

**Fix**
- Rewrote `datasetid.py` to:
  1. Fetch frontend JS
  2. Extract *JS IDs* per year
  3. Resolve each JS ID to the final datastore `resources[0]["id"]`
  4. Cache resolved IDs in `dataset_ids_cache.json` (7-day TTL)

**Outcome**
- We now have a reliable mapping:
  - `YEAR -> FINAL_DATASET_ID_FOR_DOWNLOAD`

---

## 4) Current Phase 1 project assets (conceptual)
These are the core building blocks we now have (or are actively wiring together):

### 4.1 `datasetid.py` (dynamic dataset ID resolver)
- Fetches `index.js`
- Extracts per-year JS IDs (generalPayments)
- Resolves JS IDs to download-ready datastore IDs
- Caches results (`dataset_ids_cache.json`, TTL=7 days)

### 4.2 `recordstotal.py` (totals builder module)
- Fetch company IDs
- Fetch per-company totals by year
- Writes totals JSON atomically
- Designed for reuse by the downloader

### 4.3 General Payments downloader script
- Accepts `--year`
- Loads totals JSON (cached logic to avoid rebuilding)
- Uses dataset ID resolver for the correct download endpoint
- Downloads per company with concurrency + validation
- Writes:
  - CSV files to year directory
  - run report CSV

### 4.4 Docker runtime
- Dockerfile + requirements + compose volume mapping
- Repeatable runs from WSL

---

## 5) How to run (current operational commands)

### 5.1 Build image
```bash
docker compose build
```

### 5.2 Run downloader (container)
Example run (testing):
```bash
docker compose run --rm openpayments --year 2024 --max-files 20 --ensure-totals
```

> Note: exact flags depend on the current click-based script. The key pattern is:
> - outputs go to `/app/data/...`
> - local volume maps to `./data/...`

### 5.3 Local smoke test (outside Docker)
We built a “one-company test” to validate the download endpoint and identify dataset-ID issues.

This was crucial to prove:
- The code structure was fine
- The dataset ID was wrong

---

## 6) What’s next (the precise next steps)

### Next Step A — Confirm dataset resolution produces valid download IDs
- Run `datasetid.py`
- Pick the resolved ID for 2024
- Smoke-test with:
  - `/api/1/datastore/query/<final_id>/download?...`
- Confirm HTTP 200

**Success condition**
- Download endpoint returns 200 and returns CSV content.

---

### Next Step B — Wire dataset resolver into the main downloader permanently
Update downloader boot sequence:
1. Load cached dataset IDs
2. If missing year or stale, resolve again
3. Use `YEAR_TO_DATASET_ID[year]` to build download URL

**Success condition**
- The same year always resolves to a working ID (or fails loudly with clear logs).

---

### Next Step C — Complete the Click refactor of downloader CLI
We agreed to use **Click**, not argparse.
Add testing flags:
- `--max-files N` (download only first N companies, for fast testing)
- `--slice` support (optional)
- `--rescrape-totals` flag (force totals rebuild)
- `--add-year-if-missing` behavior for totals cache

**Success condition**
- One consistent CLI you can run locally or in Docker.

---

### Next Step D — Make the “Phase 1 runner” deterministic
Add a single “run” wrapper pattern:
- `make run YEAR=2024 MAX=20`
or
- `./run.sh --year 2024 --max-files 20`

**Success condition**
- New machine can run it with one command.

---

### Next Step E — Start AWS integration (Phase 2 transition)
Once downloads are stable:
- Upload raw outputs to S3 (raw zone)
- Write a manifest (run metadata)
- Validate row counts in Athena
- Begin separating zones:
  - `raw/`
  - `staging/`
  - `metadata/`

**Success condition**
- End-to-end: local or Docker run → S3 raw → Athena query works.

---

## 7) Lessons learned (Phase 1)
- **Frontend IDs are not always API IDs.** Validate endpoints using curl/standalone tests.
- **Compose interpolation vs container env** are different concepts.
- **Docker in WSL requires integration** (daemon isn’t automatically in Linux filesystem).
- “Working locally” is not enough — build:
  - validation
  - caching
  - observability
  - deterministic execution

---

## 8) Suggested LinkedIn progress post (optional)
**Title/Hook**
> Building an ingestion system ≠ downloading a CSV.

**Body**
Over the last phase of my Data Engineering Zoomcamp (AWS-focused), I’ve been building a repeatable ingestion engine around the CMS Open Payments dataset.

What’s shipped so far:
- concurrent company-level extraction
- totals-by-year index used for planning downloads
- pagination + parallel page fetching for large companies
- header validation + partial row protection
- dated caching to avoid re-scraping totals repeatedly
- Dockerized execution with WSL support + local volume mapping

One surprising bug I had to solve:
The dataset ID exposed in the frontend JS wasn’t the real download-ready datastore ID — it required a resolver call to the datastore API to get the final resource ID.

Next:
Hardening the CLI + integrating raw zone uploads into S3 (then Glue/Athena validation).

#DataEngineering #AWS #Docker #ETL #OpenData #Zoomcamp

---

## 9) Phase 1 Definition of Done (DoD)
Phase 1 is complete when:
- Dataset ID resolution is stable and cached
- Totals JSON caching works (10-day rule + add-year-if-missing)
- Downloader can:
  - download a test subset (`--max-files 20`)
  - produce valid CSV outputs
  - emit a report CSV
- Docker run outputs correctly map to local `./data/`
- The whole setup is runnable with a single command

---
