# 9-Week Data Engineering Zoomcamp (AWS-Focused) — Private Milestones Plan (Updated: dbt + Bruin)

This execution plan keeps the Open Payments capstone as the backbone, while incorporating:
- **dbt** (analytics engineering)
- **Bruin** (data platform pipeline orchestration)
without losing the AWS-first track.

Two checkpoints per week: one for learning depth, one for shipping system progress.

---

## WEEK 1 — Foundations Reframed (Docker, SQL, Systems Thinking)

### ✅ Checkpoint 1.1
**Zoomcamp Focus**
- Course orientation
- Docker basics
- Local Postgres

**Capstone Progress**
- Repo structure + Docker runtime
- Local validation layer (Postgres only for exploration)

**Outcome**
- One-command reproducible runtime
- Clear boundaries: raw vs processed

---

### ✅ Checkpoint 1.2
**Zoomcamp Focus**
- SQL refresh
- Ingestion concepts

**Capstone Progress**
- Inspect Open Payments schema variations
- Define “never mutate raw” rules

**Outcome**
- Clear ingestion contract + early validation approach

---

## WEEK 2 — Ingestion as a First-Class System (S3 Raw + Metadata)

### ✅ Checkpoint 2.1
**Focus**
- Deterministic ingestion runs
- Chunking, memory safety, retries

**Capstone Progress**
- Downloader hardened (restart-safe, auditable)
- Run manifest + file audits

---

### ✅ Checkpoint 2.2
**Focus**
- Data lake layout (GCS concepts → S3)

**Capstone Progress**
- Finalize S3 structure: raw/staging/curated/metadata
- Upload script: skip-safe + manifest upload

---

## WEEK 3 — Orchestration with Intent (Airflow)

### ✅ Checkpoint 3.1
**Focus**
- Airflow fundamentals
- DAG design as an operational contract

**Capstone Progress**
- Airflow docker-compose (local executor + Postgres)
- DAG wraps `run_pipeline.py` with params (`dataset`, `year`)

---

### ✅ Checkpoint 3.2
**Focus**
- Task boundaries, retries, backfills, SLAs

**Capstone Progress**
- Split DAG: ingestion vs validation
- Validation results written to metadata zone

---

## WEEK 4 — Analytics Engineering (dbt)

### ✅ Checkpoint 4.1
**Focus**
- dbt fundamentals
- Modeling workflow: sources → staging → marts

**Capstone Progress**
- Introduce dbt project
- Local target: DuckDB over curated/staged extracts (fast iteration)

---

### ✅ Checkpoint 4.2
**Focus**
- dbt tests + docs + deployment concepts

**Capstone Progress**
- Define core models:
  - payments fact
  - physicians dim
  - companies dim
- Add dbt tests (unique/not_null/relationships)

**Outcome**
- Analytics layer becomes testable and explainable

---

## WEEK 5 — Data Platforms (Bruin) + Batch Kickoff

### ✅ Checkpoint 5.1
**Focus**
- Bruin overview: pipeline-centric orchestration + quality hooks

**Capstone Progress**
- Build minimal Bruin pipeline that runs same steps:
  - ingestion → upload → (optional) validation
- Document comparison: Airflow vs Bruin

---

### ✅ Checkpoint 5.2
**Focus**
- Spark fundamentals (DataFrames + SQL)

**Capstone Progress**
- Start Spark transform job for:
  - schema normalization
  - writing curated partitions

---

## WEEK 6 — Batch Processing at Scale (Spark) + Cost/Partitioning

### ✅ Checkpoint 6.1
**Focus**
- Spark internals, partitions, shuffles

**Capstone Progress**
- Transform job hardening
- Quarantine invalid rows into staging/rejected

---

### ✅ Checkpoint 6.2
**Focus**
- Incremental processing patterns

**Capstone Progress**
- Incremental partition writes to S3 curated zone
- Add validation counters (rows in/out)

---

## WEEK 7 — Infrastructure as Code (Terraform) + Security

### ✅ Checkpoint 7.1
**Focus**
- Terraform modules, dependencies

**Capstone Progress**
- Terraform clean-up:
  - S3 + IAM + EC2 modules
  - instance profile for S3 access
  - reproducible networking

---

### ✅ Checkpoint 7.2
**Focus**
- IAM least privilege + security posture

**Capstone Progress**
- Tighten IAM:
  - specific actions needed for pipeline
  - optional bucket scoping strategy
- Runbook for access + rotation (no access keys on EC2)

---

## WEEK 8 — Platform Hardening (End-to-End + Observability)

### ✅ Checkpoint 8.1
**Focus**
- E2E runs across multiple years
- Failure simulations

**Capstone Progress**
- Run multi-year backfills via Airflow
- Ensure re-runs are safe and traceable

---

### ✅ Checkpoint 8.2
**Focus**
- Observability patterns

**Capstone Progress**
- Centralize run summaries:
  - manifest + audits + logs per run_id
- Optional: small dashboard or query-based checks (Athena)

---

## WEEK 9 — Finalization & Positioning

### ✅ Checkpoint 9.1
**Focus**
- Documentation, diagrams, setup

**Capstone Progress**
- Final README + architecture diagram
- “How to run locally / on EC2 / via Airflow”
- “Why these tools” section (Airflow vs Bruin, dbt benefits)

---

### ✅ Checkpoint 9.2
**Focus**
- Reflection + roadmap

**Capstone Progress**
- Final review + future extensions:
  - streaming mini-module (Kafka) if desired
  - warehouse enablement (Redshift) if budget allows
  - CI checks for dbt tests and pipeline linting

---

