# AWS Capstone Project — CMS Open Payments Data Platform (Updated: dbt + Bruin)

## Goal

Build a **production-style AWS data platform** that ingests, audits, transforms, and serves **CMS Open Payments** data for analytics.

This is not a demo. It’s an operational system with:
- deterministic runs
- audit-friendly metadata
- clear lifecycle zones
- repeatable infrastructure

---

## Why Open Payments

The dataset is ideal for real-world engineering because it:
- is large and multi-year
- arrives as CSV with practical edge cases
- contains schema drift and messy constraints
- benefits from strong auditing and testing
- maps well to analytics use cases (companies, physicians, payments)

---

## Architecture (Current → Target)

### Current (already working)
1. Download CSVs (company-level)
2. Write local run artifacts (manifest, audits, logs)
3. Upload to S3 raw zone (idempotent, skip-safe)

### Target (full platform)
1. **Raw ingestion** → S3 `raw/`
2. **Staging/validation** → S3 `staging/` + quarantine
3. **Transform** (Spark/Glue/EMR as needed) → S3 `curated/`
4. **Warehouse** (optional) → Redshift
5. **Analytics modeling** (dbt) → analytics-ready marts
6. **Orchestration** (Airflow primary; Bruin comparison) manages end-to-end runs
7. **Observability** via metadata + logs + run manifests

---

## Storage Design (S3)

S3 is the system of record with explicit lifecycle zones:

- `raw/` (immutable, never edited)
- `staging/` (validated, normalized, quarantine)
- `curated/` (analytics-ready parquet/csv outputs)
- `metadata/` (run manifests, audits, schema versions, logs)

Example (aligned to your current structure):
- `raw/dataset=general-payments/year=2024/company_id=...csv`
- `metadata/runs/run_id=.../manifest.json`
- `metadata/runs/run_id=.../file_audits_...jsonl`
- `metadata/logs/...log`

---

## Orchestration Strategy (Airflow + Bruin)

### Airflow (primary)
Airflow orchestrates:
- ingestion (download → upload)
- validation + data quality checks
- transformation jobs
- optional warehouse loads
- dbt runs/tests (later stage)

Key principles:
- idempotent tasks
- safe retries
- backfills
- parameterized DAGs (`dataset`, `year`, `run_id`)

### Bruin (comparison module)
Bruin is used to learn:
- pipeline-centric definitions
- built-in quality patterns
- deployment workflow

Outcome:
- Keep Airflow as the “capstone default”
- Document why/when Bruin might be preferred in other contexts

---

## Analytics Engineering (dbt)

dbt will manage the “analytics contract”:
- models (facts/dims/marts)
- tests (not_null, unique, relationships, accepted values)
- documentation

Targets:
- Local iteration: DuckDB (fast feedback loop)
- Cloud/warehouse: Redshift (when enabled)

---

## Batch Transformations (Spark)

Spark is used for:
- schema enforcement
- normalization
- incremental processing across large years
- writing curated partitions to S3

Execution options:
- local Spark for dev
- AWS Glue or EMR for scalable runs (later stage)

---

## Data Quality & Reliability

Quality is enforced through:
- file audits (size, checksum, status)
- row-count invariants and drift checks
- schema drift detection
- quarantine zone for rejected records
- dbt tests for warehouse-layer confidence

---

## Infrastructure as Code (Terraform)

Terraform provisions:
- VPC + subnet + routing
- EC2 runner instance
- S3 bucket(s)
- IAM roles/policies (EC2 instance profile)
- (later) supporting services (e.g., Redshift, managed Airflow alternatives if needed)

---

## Deliverables (Portfolio Standard)

- End-to-end runnable project (Docker + Airflow)
- Terraform-based infra reproduction
- Clear S3 layout and metadata contract
- dbt project with models/tests/docs (when warehouse is enabled)
- “Why” documentation: trade-offs, cost choices, failure modes
