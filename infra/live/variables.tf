variable "region" {
  description = "AWS region for everything except CloudFront, which is global."
  type        = string
  default     = "eu-west-1"
}

variable "project" {
  description = "Prefix for names, and the value of the project tag."
  type        = string
  default     = "livedemos"
}

variable "budget_email" {
  description = "Where budget alerts go. Set it in terraform.tfvars (gitignored)."
  type        = string
}

variable "monthly_budget_usd" {
  description = "Monthly cost budget for this account, in USD (AWS Budgets bills in USD)."
  type        = number
  default     = 10
}

variable "availability_zone" {
  description = "The one AZ the host runs in."
  type        = string
  default     = "eu-west-1a"
}

variable "instance_type" {
  description = "Host size. Changing it is a stop and start, not a rebuild."
  type        = string
  default     = "t4g.small"
}

variable "root_volume_gb" {
  description = "Root EBS volume (gp3), holding Docker images and ClickHouse data."
  type        = number
  default     = 25
}

variable "repo_url" {
  description = "Public repo the host checks out. Also the contact in Wikimedia's User-Agent."
  type        = string
  default     = "https://github.com/Divine-Hope/portfolio-live-demos"
}

variable "compose_version" {
  description = "Docker Compose plugin release installed on the host."
  type        = string
  default     = "v2.40.3"
}

variable "compose_sha256" {
  description = "SHA-256 of docker-compose-linux-aarch64 for compose_version, from its release page."
  type        = string
  default     = "d26373b19e89160546d15407516cc59f453030d9bc5b43ba7faf16f7b4980137"
}
