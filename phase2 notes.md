
# Phase 2 Notes — AWS Integration, Idempotency, Bugs & System Hardening

Phase 2 is where the project transitioned from a functional ingestion engine 
to a cloud-aware, restart-safe, IAM-secured data pipeline.

This document captures:
- The architectural upgrades
- The bugs encountered
- The design decisions made
- Code-level adjustments
- Lessons learned

---

# 1️⃣ AWS IAM Integration (Security First)

## Challenge
Initially, uploads to S3 failed with:

AccessDenied: s3:PutObject is not authorized

Even though the EC2 instance was running and boto3 detected credentials.

## Root Cause
The IAM role attached to the EC2 instance only allowed:

- s3:ListBucket (partial)
- No s3:PutObject permission
- No bucket-agnostic scope

## Fix

We updated the IAM policy to allow:

{
  "Effect": "Allow",
  "Action": [
    "s3:PutObject",
    "s3:GetObject",
    "s3:ListBucket"
  ],
  "Resource": [
    "arn:aws:s3:::*",
    "arn:aws:s3:::*/*"
  ]
}

We removed restrictive conditions for now to unblock development.

## Lesson
IAM is strict. Even if boto3 detects credentials, permissions must explicitly allow the action.

---

# 2️⃣ Docker Compose & Buildx Issue

## Challenge
Running:

docker compose up --build

Resulted in:

compose build requires buildx 0.17.0 or later

## Root Cause
Amazon Linux 2023 installs Docker without the compose plugin properly configured.

## Fix

Manual installation of Docker Compose plugin:

sudo mkdir -p /usr/libexec/docker/cli-plugins
sudo curl -SL https://github.com/docker/compose/releases/download/v2.24.6/docker-compose-linux-x86_64   -o /usr/libexec/docker/cli-plugins/docker-compose
sudo chmod +x /usr/libexec/docker/cli-plugins/docker-compose

## Lesson
Cloud VMs often require explicit installation of CLI plugins.

---

# 3️⃣ Python Module Import Failure

## Challenge
Container error:

ModuleNotFoundError: No module named 'recordstotal'

## Root Cause
The scripts directory was not treated as a Python package.

We had:

import recordstotal

But the file lived under:

/app/scripts/recordstotal.py

## Fix

1. Added:

scripts/__init__.py

2. Updated imports:

from scripts import recordstotal

## Lesson
Container environments require explicit package structuring.

---

# 4️⃣ Idempotent Upload Logic

## Problem
Re-running the pipeline caused duplicate uploads or overwrite risks.

## Solution
Added skip-safe logic before upload:

def object_exists(s3_client, bucket, key):
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except:
        return False

if not overwrite and object_exists(client, bucket, key):
    logger.info("Skipping existing object")
else:
    s3_client.upload_file(...)

## Result
- Safe reruns
- No accidental overwrites
- Compatible with Airflow retries

---

# 5️⃣ Resume-Safe Download Strategy

## Problem
If the pipeline crashed mid-run, restarting would redownload everything.

## Solution
Before download:

if file_path.exists() and file_path.stat().st_size > 0:
    skip

## Result
Partial runs can resume safely.

---

# 6️⃣ Run Manifest Design

We introduced structured metadata per execution:

metadata/
  runs/run_id=.../
    manifest.json
    file_audits.jsonl

Example manifest structure:

{
  "run_id": "2026-02-18T17-20-33",
  "dataset": "general-payments",
  "year": 2024,
  "files_planned": 20,
  "files_downloaded": 18,
  "files_failed": 2,
  "status": "completed"
}

## Why It Matters

This becomes the contract for:
- Airflow task visibility
- Backfills
- Monitoring
- Future observability dashboards

---

# 7️⃣ Pipeline Abstraction

Instead of:

download.py
upload.py

We introduced:

run_pipeline.py

Flow:

download_year()
→ write manifest
→ conditional upload()
→ update manifest status

Airflow will call one clean interface.

---

# 8️⃣ Architecture Maturity Gains

Phase 2 improvements:

✔ Cloud-native authentication  
✔ Idempotent S3 writes  
✔ Resume-safe local processing  
✔ Deterministic run metadata  
✔ Clean raw zone partitioning  
✔ Dockerized reproducibility  

---

# 9️⃣ Phase 2 Definition of Done

- EC2 runs pipeline end-to-end
- IAM role handles auth
- S3 raw zone populated
- Metadata written per run
- Safe reruns possible
- Docker compose stable

---

# 🔟 What’s Next

Phase 3 introduces:

- Airflow orchestration
- dbt modeling
- Optional Bruin comparison
- Spark transformations

Phase 2 made the system cloud-ready.
Phase 3 makes it orchestrated and scalable.

---

# 0️⃣ Terraform: How AWS Resources Were Provisioned (Infrastructure-as-Code)

Before any “pipeline” work mattered, we needed repeatable infrastructure.

## What we provisioned with Terraform (us-west-1)

**Networking (own VPC)**
- Custom VPC
- Public subnet
- Internet Gateway
- Route table + association (public routing)

**Access**
- Security Group allowing SSH **only from my IP** (CIDR-based allowlist)
- Key pair SSH workflow (so I can get into the box quickly)

**Compute**
- EC2 Linux instance (Amazon Linux 2023)
- User-data bootstrap script (cloud-init) to install dependencies and prep the repo

**Storage**
- S3 bucket for Open Payments raw zone + metadata zone
- Bucket naming pattern: `openpayments-dezoomcamp2026-<region>-<suffix>`

## Challenge: “Whitelist any IP starting with 197.210.77…”

Terraform / AWS security groups work with **CIDR blocks**, not “string prefixes”.
So instead of “197.210.77.*” as a concept, you must express it as a CIDR, e.g.:

- `197.210.77.0/24`  ✅ (matches 197.210.77.x)
- `197.210.0.0/16`   ✅ (matches 197.210.*.*)

We used CIDR to do the intended allowlist safely.

## Challenge: User-data failed (cloud-init)

Cloud-init showed:

- `Failed to run module scripts-user`
- and in `/var/log/user-data.log` we saw package issues

Root causes we hit:
- `git` was not installed (so cloning couldn’t happen)
- `docker-compose-plugin` package wasn’t available via `dnf` on Amazon Linux 2023

Fix:
- install `git` explicitly
- install Docker compose via CLI plugin download (manual install path)

## Verification steps that became our “standard checks”

On EC2:
- `cloud-init status --long`
- `sudo cat /var/log/user-data.log`
- `sudo cat /var/log/cloud-init-output.log`
- `git --version`
- `docker --version`
- `docker compose version`

## Lesson
Terraform is not just “spinning up an EC2.”
It’s codifying:
- networking
- security boundaries
- bootstrapping behavior
- reproducible environments

This made the rest of Phase 2 possible.

