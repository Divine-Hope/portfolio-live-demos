# Credentials come from your SSO profile (AWS_PROFILE=livedemos), never from this code.
provider "aws" {
  region = var.region

  # Every resource gets these, which is what the budget and Cost Explorer group by.
  default_tags {
    tags = {
      project    = var.project
      managed-by = "terraform"
      repo       = "github.com/Divine-Hope/portfolio-live-demos"
    }
  }
}

data "aws_caller_identity" "current" {}
