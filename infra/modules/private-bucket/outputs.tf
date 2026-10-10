output "id" {
  description = "Bucket name."
  value       = aws_s3_bucket.this.id
}

output "arn" {
  description = "Bucket ARN, for IAM and bucket policies."
  value       = aws_s3_bucket.this.arn
}

output "regional_domain_name" {
  description = "Regional endpoint, for a CloudFront origin."
  value       = aws_s3_bucket.this.bucket_regional_domain_name
}
