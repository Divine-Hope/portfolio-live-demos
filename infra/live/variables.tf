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
  description = "Where budget and host alarm emails go. Set it in terraform.tfvars (gitignored)."
  type        = string
  sensitive   = true

  validation {
    condition     = can(regex("^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$", var.budget_email))
    error_message = "budget_email must be an email address."
  }
}

variable "monthly_budget_usd" {
  description = "Monthly cost budget for this account, in USD (AWS Budgets bills in USD)."
  type        = number
  default     = 10
}

variable "availability_zones" {
  description = "Zones the host may run in. The first one keeps the subnet the single host used."
  type        = list(string)
  default     = ["eu-west-1a", "eu-west-1b", "eu-west-1c"]

  validation {
    condition     = length(var.availability_zones) > 0 && alltrue([for z in var.availability_zones : startswith(z, var.region)])
    error_message = "availability_zones must be zones of var.region."
  }
}

variable "instance_types" {
  description = "2 GB+ Graviton types the host may run on, in order of preference for on-demand. Spot picks by price and spare capacity."
  type        = list(string)
  default     = ["t4g.small", "c6g.medium", "c7g.medium"]

  validation {
    condition     = length(var.instance_types) > 0
    error_message = "instance_types needs at least one type."
  }
}

# On demand until the t4g.small free trial ends on 2026-12-31 (ADR 0010).
variable "on_demand" {
  description = "true: one on-demand host (the first instance type). false: Spot."
  type        = bool
  default     = true
}

variable "root_volume_gb" {
  description = "Root EBS volume (gp3), holding Docker images and ClickHouse data."
  type        = number
  default     = 16 # 5.5 GB used on 2026-10-07; the disk alert fires at 80%
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

  validation {
    condition     = can(regex("^[0-9a-f]{64}$", var.compose_sha256))
    error_message = "compose_sha256 must be 64 lowercase hex characters."
  }
}

variable "github_repo" {
  description = "owner/name of the repo whose workflows may deploy."
  type        = string
  default     = "Divine-Hope/portfolio-live-demos"

  validation {
    condition     = can(regex("^[A-Za-z0-9-]+/[A-Za-z0-9._-]+$", var.github_repo))
    error_message = "github_repo must be owner/name."
  }
}

variable "state_bucket" {
  description = "The Terraform state bucket (the one in backend.hcl), so CI's plan role can take the state lock. Its name holds the account id, so it lives in terraform.tfvars and the TF_STATE_BUCKET secret, never in git."
  type        = string
}

variable "github_repo_ids" {
  description = "Owner and repo ids, for the immutable OIDC subject: `gh api repos/OWNER/REPO/actions/oidc/customization/sub`."
  type        = object({ owner = number, repo = number })
  default     = { owner = 61884028, repo = 1404802454 }
}

variable "grafana_cloud" {
  description = "Grafana Cloud endpoints and user ids for Alloy (not secret; the token is set in SSM by hand). Set in terraform.tfvars and the TF_VAR_grafana_cloud repository variable."
  type = object({
    prom_url  = string # remote write, ending /api/prom/push
    prom_user = string
    loki_url  = string # push, ending /loki/api/v1/push
    loki_user = string
  })
}
