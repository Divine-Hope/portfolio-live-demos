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
