# Terraform from GitHub Actions, keyless (same OIDC provider as deploy.tf).
#
#   pull request  -> plan, with a read-only role. Shows what a change would do.
#   merge to main -> plan, then apply in the `infra` environment, which needs your
#                    approval and only runs from main. Then the app deploy.
#
# Two roles because the two jobs need very different power. Apply manages IAM, so it is
# effectively admin; its trust is the guard: only the `infra` environment, which is
# restricted to main and protected by a required reviewer.
#
# Plan keeps ReadOnlyAccess on purpose, though that can read Terraform state and the
# generated secrets in it. Plan has to read state, so no narrower policy would keep them
# from it. The guard is the trust instead: only this repo's own workflows can assume the
# role (GitHub never gives OIDC tokens to pull requests from forks).

module "tf_plan_role" {
  source            = "../modules/github-oidc-role"
  name              = "${var.project}-tf-plan"
  oidc_provider_arn = aws_iam_openid_connect_provider.github.arn
  subjects = [
    "${local.github_sub}:pull_request",
    "${local.github_sub}:environment:infra-plan",
  ]
  managed_policy_arns = ["arn:${local.partition}:iam::aws:policy/ReadOnlyAccess"]
  inline_policy_name  = "terraform-state-lock"
  inline_policy_json  = data.aws_iam_policy_document.tf_plan_lock.json
}

# ReadOnlyAccess can read the state but not take Terraform's lock. Plans on main take it,
# so a plan never reads state in the middle of an apply.
data "aws_iam_policy_document" "tf_plan_lock" {
  statement {
    sid       = "StateLock"
    actions   = ["s3:PutObject", "s3:DeleteObject"]
    resources = ["arn:${local.partition}:s3:::${var.state_bucket}/live/terraform.tfstate.tflock"]
  }
}

module "tf_apply_role" {
  source              = "../modules/github-oidc-role"
  name                = "${var.project}-tf-apply"
  oidc_provider_arn   = aws_iam_openid_connect_provider.github.arn
  subjects            = ["${local.github_sub}:environment:infra"]
  managed_policy_arns = ["arn:${local.partition}:iam::aws:policy/AdministratorAccess"]
}

moved {
  from = aws_iam_role.tf_plan
  to   = module.tf_plan_role.aws_iam_role.this
}

moved {
  from = aws_iam_role_policy_attachment.tf_plan_read_only
  to   = module.tf_plan_role.aws_iam_role_policy_attachment.this["arn:aws:iam::aws:policy/ReadOnlyAccess"]
}

moved {
  from = aws_iam_role_policy.tf_plan_lock
  to   = module.tf_plan_role.aws_iam_role_policy.this[0]
}

moved {
  from = aws_iam_role.tf_apply
  to   = module.tf_apply_role.aws_iam_role.this
}

moved {
  from = aws_iam_role_policy_attachment.tf_apply_admin
  to   = module.tf_apply_role.aws_iam_role_policy_attachment.this["arn:aws:iam::aws:policy/AdministratorAccess"]
}

output "tf_plan_role_arn" {
  description = "Set as the AWS_TF_PLAN_ROLE_ARN repository secret."
  value       = module.tf_plan_role.arn
}

output "tf_apply_role_arn" {
  description = "Set as the AWS_TF_APPLY_ROLE_ARN secret on the GitHub infra environment."
  value       = module.tf_apply_role.arn
}
