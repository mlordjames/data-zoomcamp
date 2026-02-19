# VM Update & Deployment Guide (EC2 Runner)

This guide covers:
- Pulling updates from GitHub on the VM
- Installing Docker + Docker Compose (the **working method**)
- Starting Docker and running `docker compose up --build`

---

## 1) Pull updates on the EC2 VM (every time you push)
SSH in:

```bash
ssh -i /home/lordjames/.ssh/openpayments ec2-user@ec2-54-219-26-50.us-west-1.compute.amazonaws.com
```

Go to the repo and pull:

```bash
cd /opt/data-zoomcamp
git pull origin main
```

### If `git pull` fails because of local changes on the VM
**Option A (discard VM changes, safest for you):**
```bash
cd /opt/data-zoomcamp
git reset --hard
git clean -fd
git pull origin main
```

**Option B (keep VM changes temporarily):**
```bash
cd /opt/data-zoomcamp
git stash push -m "temp vm changes"
git pull origin main
# later, if you need:
git stash pop
```

---

## 2) Install Docker + Git (Amazon Linux 2023)

```bash
sudo dnf update -y
sudo dnf install -y git docker curl
```

Enable + start Docker:

```bash
sudo systemctl enable docker
sudo systemctl start docker
```

Add your user to the docker group (so you can run docker without sudo):

```bash
sudo usermod -aG docker ec2-user
```

⚠️ **Important:** log out and SSH back in for group change to apply.

Verify:

```bash
git --version
docker --version
```

---

## 3) Install Docker Compose (WORKING METHOD)

This is the method that worked reliably on your VM.

```bash
sudo mkdir -p /usr/libexec/docker/cli-plugins
sudo curl -SL https://github.com/docker/compose/releases/download/v2.24.6/docker-compose-linux-x86_64 \
  -o /usr/libexec/docker/cli-plugins/docker-compose
sudo chmod +x /usr/libexec/docker/cli-plugins/docker-compose
```

Verify:

```bash
docker compose version
docker buildx version
```

Expected:
- Docker Compose should show **v2.x**
- buildx should show **v0.17.x or later**

---

## 4) Run your Docker Compose project

Go to your repo folder:

```bash
cd /opt/data-zoomcamp
```

Build + run:

```bash
docker compose up --build
```

Run in detached mode:

```bash
docker compose up -d --build
```

Stop:

```bash
docker compose down
```

---

## 5) Troubleshooting

### Compose says “requires buildx 0.17.0 or later”
Confirm buildx:

```bash
docker buildx version
```

If buildx is missing or too old, update docker:

```bash
sudo dnf update -y
sudo dnf reinstall -y docker
sudo systemctl restart docker
```

---

End of guide.
