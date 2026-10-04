output "state_bucket" {
  description = "S3 bucket holding Terraform state for infra/live."
  value       = aws_s3_bucket.state.id
}
