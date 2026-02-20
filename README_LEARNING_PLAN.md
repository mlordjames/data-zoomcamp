# Data Engineering Zoomcamp (AWS-Focused) — Learning Plan (Updated: dbt + Bruin)

## Why I’m Taking This Course (Even Though I’m Not a Beginner)

I’m using DataTalksClub’s Data Engineering Zoomcamp as a **structured framework** to deepen production thinking and ship a real AWS system around **CMS Open Payments**.

I already work with:
- Python ingestion + scraping
- Large CSV datasets
- AWS (S3, IAM, EC2, Athena/Glue/Redshift as needed)
- Dockerized pipelines
- Logging, retries, and operational hardening

My goal isn’t to repeat beginner lessons.
My goal is to **build a coherent system** and sharpen **how and why** I use tools.

---

## How I Will Approach the Zoomcamp

I will:
- Translate GCP patterns into AWS equivalents (S3/Athena/Glue/Redshift, etc.)
- Treat each module as a **design problem**, not a tutorial
- Push for production concerns:
  - idempotency, restart safety, backfills
  - auditability + metadata as first-class outputs
  - cost-awareness and operability

The Zoomcamp is my **specification**, not my instructor.

---

## Updated Modules I’m Adding (dbt + Bruin) and Why They Matter

### Analytics Engineering (dbt)
**Why it’s essential:**
- Clean separation between **raw/staged** data engineering work and **analytics-ready** modeling
- Standard approach to:
  - transformations as code
  - tests + documentation
  - repeatable deployments

**How I’ll use it in this project:**
- dbt models for:
  - `payments_fact`
  - `physicians_dim`
  - `companies_dim`
- dbt tests for:
  - not_null / unique keys
  - accepted values (where appropriate)
  - relationship tests (fact ↔ dims)
- Targets:
  - **DuckDB** locally for rapid iteration
  - **Redshift** (or alternative) when warehouse is enabled

### Data Platform Orchestration (Bruin)
**Why it’s worth learning (even if Airflow is “the standard”):**
- Bruin focuses on **end-to-end pipeline definitions**, quality hooks, and deployment patterns
- It’s useful to compare “workflow-first” vs “pipeline-first” ergonomics

**How I’ll use it:**
- Keep Airflow as the baseline orchestrator (industry standard, open source)
- Add a minimal Bruin pipeline that runs the same steps so I can compare:
  - local dev experience
  - configuration, environments, secrets
  - quality checks integration
  - deployment story

**Decision goal:**
- Airflow remains the primary orchestrator in the capstone
- Bruin becomes a learning comparison artifact (documented trade-offs)

---

## How I Will Use Each Zoomcamp Module (Updated)

| Zoomcamp Area | How I’ll Apply It (AWS-Focused) |
|---|---|
| Docker & SQL | Reproducible runtime + local validation |
| Ingestion | Restart-safe downloads, audited and logged |
| Orchestration (Airflow) | Scheduling, retries, backfills, operational control |
| Analytics Engineering (dbt) | Modeling + tests + docs for curated analytics tables |
| Data Platforms (Bruin) | Comparison: pipeline-centric orchestration + quality patterns |
| Batch (Spark) | Large-scale transforms, schema enforcement, incremental patterns |
| Streaming (Kafka) | Optional extension; small demo pipeline if time allows |
| Terraform | Reproducible AWS infra (S3, IAM, EC2, networking) |
| Data Quality | Detect silent failures; enforce schema + row-count invariants |

---

## What Success Looks Like

By the end of the Zoomcamp, I will have:
- A real AWS capstone: **Open Payments data platform**
- A production-minded ingestion system with strong audit trails
- A curated analytics layer with **dbt models + tests**
- Orchestration implemented in **Airflow**, with a **Bruin comparison** documented
- A portfolio-ready README + architecture diagram + runbook

---

## Public Learning (Optional Output)

I’ll share **design decisions** and trade-offs, not “I learned X” posts:
- idempotency patterns, manifest strategy
- why modeling decisions were chosen
- orchestration trade-offs (Airflow vs Bruin)
- how tests changed confidence in data
