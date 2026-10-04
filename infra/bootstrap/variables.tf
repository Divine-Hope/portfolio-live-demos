variable "region" {
  description = "AWS region for the state bucket. Same as everything else."
  type        = string
  default     = "eu-west-1"
}

variable "project" {
  description = "Prefix for names, and the value of the project tag."
  type        = string
  default     = "livedemos"
}
