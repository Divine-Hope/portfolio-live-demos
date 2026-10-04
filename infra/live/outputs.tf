output "account_id" {
  description = "The AWS account this stack runs in. Should be the livedemos account."
  value       = data.aws_caller_identity.current.account_id
}
