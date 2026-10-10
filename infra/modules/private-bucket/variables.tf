variable "name" {
  description = "Bucket name (global)."
  type        = string
}

variable "policy_documents" {
  description = "Extra IAM policy documents (JSON) merged into the bucket policy."
  type        = list(string)
  default     = []
}

variable "versioning" {
  description = "Keep replaced and deleted objects as noncurrent versions."
  type        = bool
  default     = false
}

variable "lifecycle_rules" {
  description = "Lifecycle rules, each over the whole bucket. Expiring noncurrent versions only makes sense with versioning."
  type = list(object({
    id = string
    transitions = optional(list(object({
      days          = number
      storage_class = string
    })), [])
    noncurrent_version_expiration_days     = optional(number)
    abort_incomplete_multipart_upload_days = optional(number)
  }))
  default = []
}
