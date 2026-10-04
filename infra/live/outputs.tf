output "account_id" {
  description = "The AWS account this stack runs in. Should be the livedemos account."
  value       = data.aws_caller_identity.current.account_id
}

output "api_domain" {
  description = "CloudFront domain serving the data API."
  value       = aws_cloudfront_distribution.api.domain_name
}

output "live_url" {
  description = "The widget's data, through CloudFront."
  value       = "https://${aws_cloudfront_distribution.api.domain_name}/v1/wikipedia/live.json"
}

output "instance_id" {
  description = "For `aws ssm start-session --target <id>`."
  value       = aws_instance.host.id
}

output "snapshots_bucket" {
  description = "Where the api writes the fallback snapshot."
  value       = module.snapshots.id
}

output "deploy_role_arn" {
  description = "Set as the AWS_DEPLOY_ROLE_ARN variable on the GitHub production environment."
  value       = aws_iam_role.deploy.arn
}
