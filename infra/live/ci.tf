# Terraform from GitHub Actions, keyless (same OIDC provider as deploy.tf).
#
#   pull request  -> plan, with a read-only role. Shows what a change would do.
#   merge to main -> plan, then apply in the `infra` environment, which needs your
#                    approval and only runs from main. Then the app deploy.
#
# Two roles because the two jobs need very different power. Plan can read everything,
# including Terraform state, which holds the generated secrets; only this repo's own
# workflows can assume it (GitHub never gives OIDC tokens to pull requests from forks).
# Apply manages IAM, so it is effectively admin; its trust is the guard: only the
# `infra` environment, which is restricted to main and protected by a required reviewer.

data "aws_iam_policy_document" "tf_plan_trust" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.github.arn]
    }

    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:sub"
      values = [
        "${local.github_sub}:pull_request",
        "${local.github_sub}:environment:infra-plan",
      ]
    }
  }
}

resource "aws_iam_role" "tf_plan" {
  name                 = "${var.project}-tf-plan"
  assume_role_policy   = data.aws_iam_policy_document.tf_plan_trust.json
  max_session_duration = 3600
}

resource "aws_iam_role_policy_attachment" "tf_plan_read_only" {
  role       = aws_iam_role.tf_plan.name
  policy_arn = "arn:aws:iam::aws:policy/ReadOnlyAccess"
}

# ReadOnlyAccess can read the state but not take Terraform's lock. Plans on main take it,
# so a plan never reads state in the middle of an apply.
data "aws_iam_policy_document" "tf_plan_lock" {
  statement {
    sid       = "StateLock"
    actions   = ["s3:PutObject", "s3:DeleteObject"]
    resources = ["arn:aws:s3:::${var.state_bucket}/live/terraform.tfstate.tflock"]
  }
}

resource "aws_iam_role_policy" "tf_plan_lock" {
  name   = "terraform-state-lock"
  role   = aws_iam_role.tf_plan.id
  policy = data.aws_iam_policy_document.tf_plan_lock.json
}

data "aws_iam_policy_document" "tf_apply_trust" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.github.arn]
    }

    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:sub"
      values   = ["${local.github_sub}:environment:infra"]
    }
  }
}

resource "aws_iam_role" "tf_apply" {
  name                 = "${var.project}-tf-apply"
  assume_role_policy   = data.aws_iam_policy_document.tf_apply_trust.json
  max_session_duration = 3600
}

resource "aws_iam_role_policy_attachment" "tf_apply_admin" {
  role       = aws_iam_role.tf_apply.name
  policy_arn = "arn:aws:iam::aws:policy/AdministratorAccess"
}

output "tf_plan_role_arn" {
  description = "Set as the AWS_TF_PLAN_ROLE_ARN repository secret."
  value       = aws_iam_role.tf_plan.arn
}

output "tf_apply_role_arn" {
  description = "Set as the AWS_TF_APPLY_ROLE_ARN secret on the GitHub infra environment."
  value       = aws_iam_role.tf_apply.arn
}
