output "arn" {
  description = "Role ARN, for the workflow's role-to-assume."
  value       = aws_iam_role.this.arn
}

output "name" {
  description = "Role name."
  value       = aws_iam_role.this.name
}
