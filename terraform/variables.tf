variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-west-1"
}

variable "project_name" {
  description = "Project name used for tagging and naming"
  type        = string
  default     = "dezoomcamp2026"
}

variable "s3_bucket_prefix" {
  description = "Bucket prefix (will be combined with project, region, and a random suffix)"
  type        = string
  default     = "openpayments"
}

variable "vpc_cidr" {
  description = "CIDR block for the VPC"
  type        = string
  default     = "10.20.0.0/16"
}

variable "public_subnet_cidr" {
  description = "CIDR block for the public subnet"
  type        = string
  default     = "10.20.1.0/24"
}

variable "availability_zone" {
  description = "AZ for the public subnet (must be in us-west-1)"
  type        = string
  default     = "us-west-1a"
}

variable "instance_type" {
  description = "EC2 instance type for the runner"
  type        = string
  default     = "t3.small"
}

variable "root_volume_gb" {
  description = "Root EBS volume size in GB"
  type        = number
  default     = 60
}

variable "ssh_allowed_cidrs" {
  description = "CIDR blocks allowed to SSH into the instance"
  type        = list(string)
  default     = ["197.210.77.0/24"]
}

variable "key_pair_name" {
  description = "Name for the EC2 key pair"
  type        = string
  default     = "openpayments-keypair"
}

variable "ssh_public_key_path" {
  description = "Path to your local SSH public key (e.g., /home/lordjames/.ssh/openpayments.pub)"
  type        = string
  default     = "/home/lordjames/.ssh/openpayments.pub"
}

variable "ssh_private_key_path" {
  description = "Path to your local SSH private key (used only for output ssh command)"
  type        = string
  default     = "/home/lordjames/.ssh/openpayments"
}

variable "github_repo_url" {
  description = "GitHub repo to clone on instance bootstrap"
  type        = string
  default     = "https://github.com/mlordjames/data-zoomcamp"
}

variable "tags" {
  description = "Additional tags applied to all resources"
  type        = map(string)
  default = {
    Owner = "mlordjames"
  }
}
