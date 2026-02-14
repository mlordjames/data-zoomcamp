# 9-Week AWS Data Engineering Zoomcamp — Checklist Milestones (With Risk Buffers)

Purpose of this document:
- Act as a **daily execution checklist**
- Track **capstone progress**, not just course completion
- Anticipate **where delays usually happen**
- Keep learning senior-level and outcome-driven

Each week has **two checkpoints**.
Each checkpoint includes:
- Core checklist
- Capstone alignment
- Risk buffer notes
- LinkedIn post prompts (2 per checkpoint)

---

## WEEK 1 — Foundations Reframed (Low Risk)

### ☐ Checkpoint 1.1 — Environment & Framing
**Core Checklist**
- ☐ Course orientation reviewed
- ☐ Repo initialized (clear folder structure)
- ☐ Docker runtime set up
- ☐ Local Postgres running
- ☐ `.env` + config separation

**Capstone Alignment**
- ☐ Decide Postgres role (exploration only)
- ☐ Define immutable vs mutable data rules

**Risk Buffer**
- ⚠️ Low risk  
- Common delay: over-organizing repo too early  
- Buffer: **½ day max**

**LinkedIn Prompts**
- ☐ “Docker is about repeatability, not containers”
- ☐ “Why local databases still matter in cloud pipelines”

---

### ☐ Checkpoint 1.2 — Schema Familiarity
**Core Checklist**
- ☐ SQL refresh completed
- ☐ Sample Open Payments CSV inspected
- ☐ Column inconsistencies documented
- ☐ Mandatory vs optional fields identified

**Capstone Alignment**
- ☐ Raw data contract defined
- ☐ Decide what will NEVER be cleaned

**Risk Buffer**
- ⚠️ Medium risk  
- Common delay: underestimating schema chaos  
- Buffer: **1 day**

**LinkedIn Prompts**
- ☐ “Raw data should never be ‘clean’”
- ☐ “The most important schema is the one you don’t touch”

---

## WEEK 2 — Ingestion as a System (High Risk)

### ☐ Checkpoint 2.1 — Hardened Ingestion
**Core Checklist**
- ☐ Python ingestion script written
- ☐ Chunked CSV reading implemented
- ☐ Retry logic added
- ☐ Structured logging enabled

**Capstone Alignment**
- ☐ Year-based ingestion logic
- ☐ Deterministic run IDs

**Risk Buffer**
- ⚠️ High risk  
- Common delay: memory issues & partial failures  
- Buffer: **2 days**

**LinkedIn Prompts**
- ☐ “Ingestion is where pipelines quietly fail”
- ☐ “Why logging matters before transformations”

---

### ☐ Checkpoint 2.2 — S3 Data Lake Structure
**Core Checklist**
- ☐ S3 buckets created
- ☐ Raw / staging / metadata separation
- ☐ Partition strategy finalized
- ☐ Naming conventions locked

**Capstone Alignment**
- ☐ Replay strategy confirmed
- ☐ Audit trail location defined

**Risk Buffer**
- ⚠️ Medium risk  
- Common delay: restructuring S3 after first upload  
- Buffer: **1 day**

**LinkedIn Prompts**
- ☐ “Your S3 layout reveals your pipeline maturity”
- ☐ “Partitioning is survival, not optimization”

---

## WEEK 3 — Orchestration (Medium–High Risk)

### ☐ Checkpoint 3.1 — Airflow Ingestion DAG
**Core Checklist**
- ☐ Airflow installed (EC2 or local)
- ☐ DAG skeleton created
- ☐ Year parameterization added
- ☐ Idempotent tasks enforced

**Capstone Alignment**
- ☐ Ingestion fully orchestrated
- ☐ Backfill logic validated

**Risk Buffer**
- ⚠️ Medium–High risk  
- Common delay: Airflow config & permissions  
- Buffer: **1–2 days**

**LinkedIn Prompts**
- ☐ “Airflow is a recovery tool, not a scheduler”
- ☐ “Idempotency matters more than speed”

---

### ☐ Checkpoint 3.2 — Validation DAG
**Core Checklist**
- ☐ Separate validation DAG created
- ☐ Row count checks added
- ☐ Schema checks implemented
- ☐ Validation results stored

**Capstone Alignment**
- ☐ Clear valid vs rejected flow

**Risk Buffer**
- ⚠️ Medium risk  
- Common delay: defining validation boundaries  
- Buffer: **1 day**

**LinkedIn Prompts**
- ☐ “Good DAGs fail loudly”
- ☐ “Why ingestion and validation should never be merged”

---

## WEEK 4 — Warehousing & Modeling (Medium Risk)

### ☐ Checkpoint 4.1 — Athena Exploration
**Core Checklist**
- ☐ Athena external tables created
- ☐ Raw data queried
- ☐ Distribution anomalies identified

**Capstone Alignment**
- ☐ Data trust baseline established

**Risk Buffer**
- ⚠️ Medium risk  
- Common delay: IAM permissions for Athena  
- Buffer: **1 day**

**LinkedIn Prompts**
- ☐ “Athena saved me money before Redshift saved me time”
- ☐ “Not every dataset belongs in a warehouse”

---

### ☐ Checkpoint 4.2 — Data Modeling
**Core Checklist**
- ☐ Fact table defined
- ☐ Dimension tables defined
- ☐ Grain clearly documented

**Capstone Alignment**
- ☐ Analytics questions mapped to model

**Risk Buffer**
- ⚠️ Medium risk  
- Common delay: over-modeling too early  
- Buffer: **1 day**

**LinkedIn Prompts**
- ☐ “Data modeling is about questions”
- ☐ “Why analytics fail when modeling is rushed”

---

## WEEK 5 — Spark Batch Processing (High Risk)

### ☐ Checkpoint 5.1 — Spark Transformations
**Core Checklist**
- ☐ Spark job created
- ☐ Schema enforcement enabled
- ☐ Invalid records quarantined

**Capstone Alignment**
- ☐ Trusted curated dataset pipeline

**Risk Buffer**
- ⚠️ High risk  
- Common delay: Spark errors & memory tuning  
- Buffer: **2 days**

**LinkedIn Prompts**
- ☐ “Spark works until your data doesn’t”
- ☐ “Bad data must go somewhere”

---

### ☐ Checkpoint 5.2 — Optimization & Incremental Loads
**Core Checklist**
- ☐ Incremental logic added
- ☐ Partition strategy refined
- ☐ Performance validated

**Capstone Alignment**
- ☐ Scalable transformations achieved

**Risk Buffer**
- ⚠️ Medium–High risk  
- Common delay: data skew  
- Buffer: **1–2 days**

**LinkedIn Prompts**
- ☐ “Scaling Spark is about data shape”
- ☐ “Incremental loads changed my pipeline design”

---

## WEEK 6 — Infrastructure as Code (Medium Risk)

### ☐ Checkpoint 6.1 — Core Terraform Modules
**Core Checklist**
- ☐ Terraform initialized
- ☐ S3 modules written
- ☐ IAM roles defined

**Capstone Alignment**
- ☐ Infra reproducible

**Risk Buffer**
- ⚠️ Medium risk  
- Common delay: IAM complexity  
- Buffer: **1 day**

**LinkedIn Prompts**
- ☐ “If you can’t recreate it, you don’t own it”
- ☐ “Terraform teaches discipline, not syntax”

---

### ☐ Checkpoint 6.2 — Compute & Warehouse Infra
**Core Checklist**
- ☐ Redshift provisioned
- ☐ Airflow infra codified
- ☐ Dev/prod separation

**Capstone Alignment**
- ☐ Full infra defined as code

**Risk Buffer**
- ⚠️ Medium risk  
- Common delay: permissions & networking  
- Buffer: **1–2 days**

**LinkedIn Prompts**
- ☐ “Infrastructure should be easy to delete”
- ☐ “Security improves when infra is boring”

---

## WEEK 7 — Data Quality & Observability (Medium Risk)

### ☐ Checkpoint 7.1 — Quality Checks
**Core Checklist**
- ☐ Row count thresholds
- ☐ Null checks
- ☐ Schema drift detection

**Capstone Alignment**
- ☐ Trust signals established

**Risk Buffer**
- ⚠️ Medium risk  
- Common delay: deciding quality thresholds  
- Buffer: **1 day**

**LinkedIn Prompts**
- ☐ “Most pipelines fail silently”
- ☐ “Data quality is an engineering problem”

---

### ☐ Checkpoint 7.2 — Metadata & Auditing
**Core Checklist**
- ☐ Central audit tables
- ☐ Failure reporting
- ☐ Historical comparisons

**Capstone Alignment**
- ☐ Fast debugging enabled

**Risk Buffer**
- ⚠️ Low–Medium risk  
- Buffer: **½–1 day**

**LinkedIn Prompts**
- ☐ “Metadata is the real product”
- ☐ “Observability changed how I debug pipelines”

---

## WEEK 8 — System Hardening (High Risk)

### ☐ Checkpoint 8.1 — Full Pipeline Runs
**Core Checklist**
- ☐ Multi-year ingestion run
- ☐ Full transformation cycle
- ☐ Warehouse load verified

**Capstone Alignment**
- ☐ End-to-end system validated

**Risk Buffer**
- ⚠️ High risk  
- Common delay: edge cases appear only now  
- Buffer: **2 days**

**LinkedIn Prompts**
- ☐ “What broke when everything ran together”
- ☐ “End-to-end tests expose assumptions”

---

### ☐ Checkpoint 8.2 — Cost & Performance Review
**Core Checklist**
- ☐ Query performance reviewed
- ☐ Storage costs estimated
- ☐ Optimization decisions documented

**Capstone Alignment**
- ☐ Trade-offs understood

**Risk Buffer**
- ⚠️ Medium risk  
- Buffer: **1 day**

**LinkedIn Prompts**
- ☐ “Every architecture has a cost profile”
- ☐ “Good design enables later optimization”

---

## WEEK 9 — Finalization & Positioning (Low Risk)

### ☐ Checkpoint 9.1 — Documentation
**Core Checklist**
- ☐ Final README written
- ☐ Architecture diagram added
- ☐ Setup instructions validated

**Risk Buffer**
- ⚠️ Low risk  
- Buffer: **½–1 day**

**LinkedIn Prompts**
- ☐ “Documentation is engineering”
- ☐ “A project isn’t done until others can run it”

---

### ☐ Checkpoint 9.2 — Reflection & Future Scope
**Core Checklist**
- ☐ Lessons learned documented
- ☐ Future extensions listed
- ☐ Final repo cleanup

**Risk Buffer**
- ⚠️ Low risk

**LinkedIn Prompts**
- ☐ “What this project taught me about data engineering”
- ☐ “Why structured learning still matters at senior levels”

---

## Final Rule

If delayed:
- Do NOT rush checkpoints
- Skip LinkedIn posts if needed
- Preserve system quality over schedule

This plan prioritizes **depth, resilience, and long-term value**.
