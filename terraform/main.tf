terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = ">= 3.5"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# ----------------------------
# Random suffix for globally-unique S3 bucket name
# ----------------------------
resource "random_id" "bucket_suffix" {
  byte_length = 3
}

locals {
  name_prefix = "${var.project_name}-${var.aws_region}"
  bucket_name = "${var.s3_bucket_prefix}-${var.project_name}-${var.aws_region}-${random_id.bucket_suffix.hex}"

  common_tags = merge(var.tags, {
    Project = var.project_name
  })
}

# ----------------------------
# Networking: VPC + Public Subnet + IGW + Routes
# ----------------------------
resource "aws_vpc" "this" {
  cidr_block           = var.vpc_cidr
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = merge(local.common_tags, {
    Name = "${local.name_prefix}-vpc"
  })
}

resource "aws_internet_gateway" "this" {
  vpc_id = aws_vpc.this.id

  tags = merge(local.common_tags, {
    Name = "${local.name_prefix}-igw"
  })
}

resource "aws_subnet" "public" {
  vpc_id                  = aws_vpc.this.id
  cidr_block              = var.public_subnet_cidr
  availability_zone       = var.availability_zone
  map_public_ip_on_launch = true

  tags = merge(local.common_tags, {
    Name = "${local.name_prefix}-public-subnet"
  })
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.this.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.this.id
  }

  tags = merge(local.common_tags, {
    Name = "${local.name_prefix}-public-rt"
  })
}

resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}

# ----------------------------
# Security Group: SSH from your CIDR only
# ----------------------------
resource "aws_security_group" "ec2_sg" {
  name        = "${local.name_prefix}-ec2-sg"
  description = "Allow SSH from whitelisted CIDRs"
  vpc_id      = aws_vpc.this.id

  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = var.ssh_allowed_cidrs
  }

  egress {
    description = "All outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(local.common_tags, {
    Name = "${local.name_prefix}-ec2-sg"
  })
}

# ----------------------------
# Key Pair: create from your local public key file
# ----------------------------
resource "aws_key_pair" "this" {
  key_name   = var.key_pair_name
  public_key = file(var.ssh_public_key_path)

  tags = merge(local.common_tags, {
    Name = "${local.name_prefix}-keypair"
  })
}

# ----------------------------
# S3 Bucket: storage layer
# ----------------------------
resource "aws_s3_bucket" "this" {
  bucket = local.bucket_name

  tags = merge(local.common_tags, {
    Name = local.bucket_name
  })
}

resource "aws_s3_bucket_public_access_block" "this" {
  bucket = aws_s3_bucket.this.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "this" {
  bucket = aws_s3_bucket.this.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "this" {
  bucket = aws_s3_bucket.this.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# ----------------------------
# AMI: Amazon Linux 2023 (us-west-1 compatible)
# ----------------------------
data "aws_ami" "al2023" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-*-x86_64"]
  }

  filter {
    name   = "architecture"
    values = ["x86_64"]
  }

  filter {
    name   = "root-device-type"
    values = ["ebs"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

# ----------------------------
# EC2 Instance + user_data bootstrap
# ----------------------------
resource "aws_instance" "runner" {
  ami                    = data.aws_ami.al2023.id
  instance_type          = var.instance_type
  subnet_id              = aws_subnet.public.id
  vpc_security_group_ids = [aws_security_group.ec2_sg.id]
  key_name               = aws_key_pair.this.key_name

  root_block_device {
    volume_size = var.root_volume_gb
    volume_type = "gp3"
  }

# --- user_data (Amazon Linux 2023 safe) ---
user_data = <<-EOF
  #!/bin/bash
  set -euo pipefail

  exec > >(tee /var/log/user-data.log | logger -t user-data -s 2>/dev/console) 2>&1

  echo "[INFO] Updating packages..."
  dnf -y update

  echo "[INFO] Installing prerequisites..."
  dnf -y install git docker curl

  echo "[INFO] Installing Docker Compose (v2 CLI plugin binary)..."
  mkdir -p /usr/local/lib/docker/cli-plugins
  curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64" \
    -o /usr/local/lib/docker/cli-plugins/docker-compose
  chmod +x /usr/local/lib/docker/cli-plugins/docker-compose

  echo "[INFO] Cloning repo..."
  mkdir -p /opt
  if [ ! -d "/opt/data-zoomcamp/.git" ]; then
    git clone ${var.github_repo_url} /opt/data-zoomcamp
  else
    echo "[INFO] Repo already exists. Skipping clone."
  fi

  echo "[INFO] Setting ownership to ec2-user..."
  chown -R ec2-user:ec2-user /opt/data-zoomcamp || true

  echo "[INFO] Bootstrap complete."
  echo "[INFO] Docker installed but NOT started (as requested)."
EOF


  tags = merge(local.common_tags, {
    Name = "${local.name_prefix}-runner"
  })
}

# ----------------------------
# Outputs
# ----------------------------
output "s3_bucket_name" {
  value = aws_s3_bucket.this.bucket
}

output "ec2_public_ip" {
  value = aws_instance.runner.public_ip
}

output "ssh_command" {
  value = "ssh -i ${var.ssh_private_key_path} ec2-user@${aws_instance.runner.public_ip}"
}
