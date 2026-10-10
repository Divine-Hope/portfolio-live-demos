# Credentials come from your SSO profile (AWS_PROFILE=livedemos), never from this code.
provider "aws" {
  region = var.region

  # Every resource gets these, which is what the budget and Cost Explorer group by.
  default_tags {
    tags = {
      project    = var.project
      managed-by = "terraform"
      repo       = "github.com/Divine-Hope/portfolio-live-demos"
      stack      = "live"
    }
  }
}

data "aws_caller_identity" "current" {}

data "aws_partition" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
  partition  = data.aws_partition.current.partition
}
