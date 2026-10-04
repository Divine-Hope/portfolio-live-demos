terraform {
  required_version = ">= 1.11.0, < 2.0.0"

  # Bucket and region come from backend.hcl, written by infra/bootstrap.
  # `use_lockfile` locks with a .tflock object next to the state: no DynamoDB table.
  backend "s3" {
    key          = "live/terraform.tfstate"
    encrypt      = true
    use_lockfile = true
  }

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}
