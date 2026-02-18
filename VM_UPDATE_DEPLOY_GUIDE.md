# VM Update & Deployment Guide

## 1. Initial Setup (Already Done by Terraform)

The EC2 instance: - Installs git, docker, curl - Installs Docker Compose
(v2 CLI plugin) - Clones repo to: /opt/data-zoomcamp - Does NOT start
docker automatically

------------------------------------------------------------------------

## 2. Local Development Workflow (Your Machine)

Whenever you make changes locally:

cd /path/to/data-zoomcamp git add . git commit -m "your update message"
git push origin main

------------------------------------------------------------------------

## 3. Pull Updates on the EC2 VM

SSH into your instance:

ssh -i /home/lordjames/.ssh/openpayments
ec2-user@`<EC2_PUBLIC_IP>`{=html}

Then run:

cd /opt/data-zoomcamp git pull origin main

------------------------------------------------------------------------

## 4. If Git Pull Fails (Local Changes on VM)

If the VM has accidental changes and pull fails:

cd /opt/data-zoomcamp git reset --hard git clean -fd git pull origin
main

WARNING: This deletes local changes on the VM.

------------------------------------------------------------------------

## 5. Running Docker Manually (When Ready)

Start docker:

sudo systemctl start docker

Verify:

docker --version docker compose version

Run your compose:

cd /opt/data-zoomcamp docker compose up --build

------------------------------------------------------------------------

End of guide.
