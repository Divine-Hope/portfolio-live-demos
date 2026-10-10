output "api_domain" {
  description = "CloudFront domain serving the data API."
  value       = aws_cloudfront_distribution.api.domain_name
}

output "live_url" {
  description = "The widget's data, through CloudFront."
  value       = "https://${aws_cloudfront_distribution.api.domain_name}/v1/wikipedia/live.json"
}

output "host_group" {
  description = "The host's Auto Scaling Group. `make host-id` finds its instance."
  value       = aws_autoscaling_group.host.name
}

output "snapshots_bucket" {
  description = "Where the api writes the fallback snapshot."
  value       = module.snapshots.id
}

output "deploy_role_arn" {
  description = "Set as the AWS_DEPLOY_ROLE_ARN secret on the GitHub production environment."
  value       = aws_iam_role.deploy.arn
}
