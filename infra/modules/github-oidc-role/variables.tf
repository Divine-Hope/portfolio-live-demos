variable "name" {
  description = "Role name."
  type        = string
}

variable "oidc_provider_arn" {
  description = "ARN of the GitHub Actions OIDC provider."
  type        = string
}

variable "subjects" {
  description = "OIDC `sub` claims allowed to assume the role, e.g. `repo:owner@1/name@2:environment:production`."
  type        = list(string)

  validation {
    condition     = length(var.subjects) > 0
    error_message = "At least one subject: a role any workflow could assume is never what you want."
  }
}

variable "managed_policy_arns" {
  description = "Managed policies attached to the role."
  type        = list(string)
  default     = []
}

variable "inline_policy_name" {
  description = "Name of the inline policy, or null for none. Set it with inline_policy_json."
  type        = string
  default     = null
}

variable "inline_policy_json" {
  description = "The inline policy document (JSON). Required when inline_policy_name is set."
  type        = string
  default     = null
}
