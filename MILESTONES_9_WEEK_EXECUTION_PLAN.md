# 9-Week Data Engineering Zoomcamp (AWS-Focused) — Private Milestones Plan

This document breaks the 9-week Zoomcamp into **two structured checkpoints per week**.
Each checkpoint is designed to:
- Advance my technical depth (not beginner-level)
- Move my AWS Open Payments capstone forward
- Produce at least one meaningful LinkedIn insight post

This is a **private execution guide**, not a public roadmap.

---

## WEEK 1 — Foundations Reframed (Docker, SQL, Thinking in Systems)

### ✅ Checkpoint 1.1
**Zoomcamp Focus**
- Course orientation
- Docker basics
- Local Postgres

**What I’m Learning (Advanced Lens)**
- Docker as a reproducible runtime, not a tutorial tool
- Data lifecycle thinking (raw vs processed)
- SQL as a validation and exploration layer

**Capstone Progress**
- Define repo structure
- Set up Docker-based ingestion runtime
- Local Postgres used for schema exploration only

**Level to Reach**
- I can spin up the environment in one command
- I understand where Postgres fits and where it does NOT

**LinkedIn Post Ideas**
1. “Why I still use local databases even when S3 is the destination”
2. “Docker isn’t about containers — it’s about repeatability”

---

### ✅ Checkpoint 1.2
**Zoomcamp Focus**
- SQL refresh
- Basic ingestion concepts

**What I’m Learning**
- Using SQL for sanity checks, not long-term storage
- Designing ingestion with failure in mind

**Capstone Progress**
- Inspect Open Payments schema variations
- Identify mandatory vs optional columns
- Decide raw storage format and naming conventions

**Level to Reach**
- I can explain my ingestion boundaries clearly
- I know what data I will NEVER mutate

**LinkedIn Post Ideas**
1. “Raw data should never be ‘clean’ — here’s why”
2. “The most important schema is the one you don’t touch”

---

## WEEK 2 — Ingestion as a First-Class System

### ✅ Checkpoint 2.1
**Zoomcamp Focus**
- Data ingestion pipelines
- Working with large datasets

**What I’m Learning**
- Chunking and memory safety
- Deterministic ingestion runs
- File-level metadata tracking

**Capstone Progress**
- Build Python ingestion script
- Upload yearly Open Payments data to S3 raw zone
- Generate ingestion audit logs

**Level to Reach**
- Ingestion is restart-safe
- Partial failures are detectable

**LinkedIn Post Ideas**
1. “Ingestion is where most pipelines quietly fail”
2. “Why I log metadata before I trust data”

---

### ✅ Checkpoint 2.2
**Zoomcamp Focus**
- Cloud storage concepts (GCS → S3)

**What I’m Learning**
- S3 as a data lake, not object dumping
- Partitioning for humans and machines

**Capstone Progress**
- Finalize S3 folder structure
- Implement raw / staging / metadata separation

**Level to Reach**
- I can replay any ingestion run confidently

**LinkedIn Post Ideas**
1. “Your S3 structure tells me how mature your pipeline is”
2. “Partitioning is not an optimization — it’s survival”

---

## WEEK 3 — Orchestration with Intent (Airflow)

### ✅ Checkpoint 3.1
**Zoomcamp Focus**
- Airflow fundamentals
- DAG concepts

**What I’m Learning**
- DAGs as operational contracts
- Idempotent task design
- Parameterized workflows

**Capstone Progress**
- Create ingestion DAG
- Year-based parameterization
- Retry and backfill logic

**Level to Reach**
- I can rerun one year without touching others

**LinkedIn Post Ideas**
1. “Airflow is not a scheduler — it’s a recovery tool”
2. “Why idempotency matters more than speed”

---

### ✅ Checkpoint 3.2
**Zoomcamp Focus**
- Airflow execution patterns

**What I’m Learning**
- Task boundaries
- Failure isolation
- SLA thinking

**Capstone Progress**
- Add validation DAG
- Separate ingestion from validation
- Store validation results in metadata zone

**Level to Reach**
- Failures are visible and explainable

**LinkedIn Post Ideas**
1. “Good DAGs fail loudly, not often”
2. “Why I split ingestion and validation”

---

## WEEK 4 — Warehousing & Analytics Thinking

### ✅ Checkpoint 4.1
**Zoomcamp Focus**
- BigQuery concepts (mapped to AWS)

**What I’m Learning**
- Athena vs Redshift decision-making
- Query cost awareness

**Capstone Progress**
- Query raw Open Payments data using Athena
- Validate row counts and distributions

**Level to Reach**
- I know when Athena is the right answer

**LinkedIn Post Ideas**
1. “Athena saved me money before Redshift saved me time”
2. “Not every dataset belongs in a warehouse”

---

### ✅ Checkpoint 4.2
**Zoomcamp Focus**
- Data modeling basics

**What I’m Learning**
- Fact vs dimension modeling
- Analytical query patterns

**Capstone Progress**
- Design payments fact table
- Define physician and company dimensions

**Level to Reach**
- I can justify my data model choices

**LinkedIn Post Ideas**
1. “Data modeling is about questions, not tables”
2. “Why analytics fail when modeling is rushed”

---

## WEEK 5 — Batch Processing with Spark

### ✅ Checkpoint 5.1
**Zoomcamp Focus**
- Spark fundamentals

**What I’m Learning**
- Schema enforcement
- Partition strategy
- Shuffle awareness

**Capstone Progress**
- Spark job for cleaning and normalization
- Reject invalid rows into quarantine zone

**Level to Reach**
- I trust my transformed data

**LinkedIn Post Ideas**
1. “Spark works… until your data doesn’t”
2. “Bad data should go somewhere — not disappear”

---

### ✅ Checkpoint 5.2
**Zoomcamp Focus**
- Spark optimization concepts

**What I’m Learning**
- Incremental processing
- Skew handling

**Capstone Progress**
- Optimize Spark transformations
- Write curated datasets to S3

**Level to Reach**
- Pipeline scales without rewriting logic

**LinkedIn Post Ideas**
1. “Scaling Spark is about data shape, not cluster size”
2. “Why incremental loads changed my pipeline design”

---

## WEEK 6 — Infrastructure as Code

### ✅ Checkpoint 6.1
**Zoomcamp Focus**
- Terraform basics

**What I’m Learning**
- Declarative infrastructure thinking
- Resource dependencies

**Capstone Progress**
- Terraform modules for S3 and IAM

**Level to Reach**
- Infra is reproducible from scratch

**LinkedIn Post Ideas**
1. “If you can’t recreate it, you don’t own it”
2. “Terraform taught me discipline, not syntax”

---

### ✅ Checkpoint 6.2
**Zoomcamp Focus**
- Advanced Terraform patterns

**What I’m Learning**
- Environment separation
- Least-privilege IAM

**Capstone Progress**
- Provision Redshift and Airflow runtime via Terraform

**Level to Reach**
- Clean teardown is possible

**LinkedIn Post Ideas**
1. “Infrastructure should be easy to delete”
2. “Security improves when infra is boring”

---

## WEEK 7 — Data Quality & Reliability

### ✅ Checkpoint 7.1
**Zoomcamp Focus**
- Data quality concepts

**What I’m Learning**
- Silent failure detection
- Quality as an engineering concern

**Capstone Progress**
- Row count checks
- Schema drift detection

**Level to Reach**
- I know when data is untrustworthy

**LinkedIn Post Ideas**
1. “Most data pipelines fail silently”
2. “Data quality is not a dashboard problem”

---

### ✅ Checkpoint 7.2
**Zoomcamp Focus**
- Observability patterns

**What I’m Learning**
- Metadata-driven monitoring

**Capstone Progress**
- Centralized audit tables
- Failure reporting

**Level to Reach**
- Debugging is fast and predictable

**LinkedIn Post Ideas**
1. “Metadata is the real product”
2. “Observability changed how I debug pipelines”

---

## WEEK 8 — Capstone Hardening

### ✅ Checkpoint 8.1
**Focus**
- End-to-end pipeline review

**Capstone Progress**
- Run full pipeline across multiple years
- Validate analytics outputs

**Level to Reach**
- System works without manual intervention

**LinkedIn Post Ideas**
1. “What broke when I ran everything together”
2. “End-to-end tests reveal uncomfortable truths”

---

### ✅ Checkpoint 8.2
**Focus**
- Performance and cost review

**Capstone Progress**
- Query optimization
- Storage cost review

**Level to Reach**
- I understand my system’s trade-offs

**LinkedIn Post Ideas**
1. “Every architecture has a cost profile”
2. “Optimizing later is only possible if you design well”

---

## WEEK 9 — Finalization & Positioning

### ✅ Checkpoint 9.1
**Focus**
- Documentation and cleanup

**Capstone Progress**
- Final README
- Architecture diagrams
- Clear setup instructions

**Level to Reach**
- Someone else can run this project

**LinkedIn Post Ideas**
1. “Documentation is part of engineering”
2. “A project isn’t done until it’s explainable”

---

### ✅ Checkpoint 9.2
**Focus**
- Reflection and positioning

**Capstone Progress**
- Final review
- Lessons learned
- Future extensions

**Level to Reach**
- I can defend every design decision

**LinkedIn Post Ideas**
1. “What this project taught me about data engineering”
2. “Why structured learning still matters at senior levels”

---

## Final Note

This plan ensures that:
- Every Zoomcamp module compounds into one system
- Learning stays senior-level
- Public sharing reinforces authority
- The capstone remains the central focus throughout
