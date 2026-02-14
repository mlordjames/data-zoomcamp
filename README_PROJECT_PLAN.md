# AWS Capstone Project — CMS Open Payments Data Platform

## Why I Am Defining the Project First

I am starting this Zoomcamp by **designing my capstone upfront**.

I believe the fastest way to waste a strong course is to:
- Learn tools in isolation
- Build disconnected mini-projects
- Decide on a capstone at the end

Instead, I am **reverse-engineering my learning from a real project**.

Every concept I study in the Zoomcamp will directly support this single system.

---

## Project Goal

To design and implement a **production-style AWS data engineering platform** that ingests, validates, transforms, and serves **CMS Open Payments** data for analytics.

This project reflects:
- Real data volume
- Real schema inconsistency
- Real operational challenges
- Real architectural trade-offs

This is not a demo.
It is a system I should be able to explain, maintain, and evolve.

---

## Why CMS Open Payments Data

I chose this dataset intentionally because it:

- Is large and multi-year
- Comes in raw CSV form
- Has schema drift and inconsistencies
- Requires validation and auditing
- Represents a real analytics use case

This makes it ideal for practicing **real-world data engineering**, not toy pipelines.

---

## High-Level Architecture (Final State)

### Data Flow Overview

1. Raw CSV files downloaded or ingested
2. Stored in S3 as immutable raw data
3. Validated and audited
4. Transformed using Spark
5. Stored as curated datasets
6. Queried via Athena and Redshift
7. Orchestrated end-to-end with Airflow
8. Provisioned via Terraform

---

## Storage Design (S3)

S3 will act as the **single source of truth**.

├── raw/
│ ├── year=2019/
│ ├── year=2020/
│ └── year=2021/
├── staging/
│ ├── validated/
│ └── rejected/
├── curated/
│ ├── payments_fact/
│ ├── physicians_dim/
│ └── companies_dim/
└── metadata/
├── ingestion_audit/
└── schema_versions/


This structure supports:
- Replayability
- Auditability
- Cost control
- Clear data lifecycle boundaries

---

## Processing & Transformation

- Python for ingestion and validation
- Spark (AWS Glue or EMR) for batch transformations
- Schema enforcement and normalization
- Bad record quarantine
- Incremental processing where possible

---

## Orchestration Strategy

Airflow will manage:
- Year-based ingestion
- Validation steps
- Transformation jobs
- Warehouse loads

Key principles:
- Idempotent tasks
- Safe retries
- Backfills
- Parameterized DAGs

---

## Analytics Layer

- **Athena**
  - Exploratory queries
  - Validation checks
  - Cheap access to raw and staged data

- **Redshift**
  - Curated fact and dimension tables
  - Analytics-ready datasets
  - BI and reporting use cases

---

## Infrastructure as Code

Terraform will be used to provision:
- S3 buckets
- IAM roles and policies
- EC2 / Airflow runtime
- Redshift resources

This ensures:
- Reproducibility
- Clean teardown
- Environment separation (dev / prod)

---

## Data Quality & Monitoring

This project will include:
- File-level ingestion audits
- Row count checks
- Schema validation
- Detection of missing or corrupt files
- Clear separation of valid vs rejected data

Failures should be **visible and explainable**, not silent.

---

## How This Project Guides My Learning

Every Zoomcamp topic maps directly to this system:

- Docker → ingestion runtime
- SQL → validation and analytics
- Airflow → orchestration and recovery
- Spark → large-scale transformations
- Terraform → reproducible AWS infra
- Data quality → system reliability

This keeps my learning **focused, cumulative, and practical**.

---

## How I Will Share This Project Publicly

I plan to:
- Share design decisions, not just outcomes
- Document trade-offs and lessons learned
- Treat this project as a long-term reference system
- Use it as a portfolio anchor and interview discussion point

This project is how I ensure the Zoomcamp makes me **better at what I already do**, not just more familiar with tools.
