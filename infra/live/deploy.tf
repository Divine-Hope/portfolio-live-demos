# Keyless deploys from GitHub Actions. The workflow gets a short-lived token from
# GitHub's OIDC provider and swaps it for this role. Nothing secret is stored in GitHub.

resource "aws_iam_openid_connect_provider" "github" {
  url            = "https://token.actions.githubusercontent.com"
  client_id_list = ["sts.amazonaws.com"]
}

# GitHub's OIDC subject for this repo, in its immutable form (owner and repo ids as well as
# names, enabled on the repo). A deleted-and-recreated repo with the same name gets new
# ids, so it can't inherit these roles.
locals {
  github_repo = split("/", var.github_repo)
  github_sub  = "repo:${local.github_repo[0]}@${var.github_repo_ids.owner}/${local.github_repo[1]}@${var.github_repo_ids.repo}"
}

# Only this repo's `production` environment can assume the role, so a fork or another
# branch's workflow can't deploy.
data "aws_iam_policy_document" "deploy_trust" {
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
      values   = ["${local.github_sub}:environment:production"]
    }
  }
}

resource "aws_iam_role" "deploy" {
  name                 = "${var.project}-deploy"
  assume_role_policy   = data.aws_iam_policy_document.deploy_trust.json
  max_session_duration = 3600
}

# Just enough to deploy: record the image tag, run the deploy script on the one host,
# and read the result.
data "aws_iam_policy_document" "deploy" {
  statement {
    sid       = "RecordImageTag"
    actions   = ["ssm:PutParameter"]
    resources = [aws_ssm_parameter.image_tag.arn]
  }

  statement {
    sid       = "ReadDeployTargets"
    actions   = ["ssm:GetParameter"]
    resources = [for p in aws_ssm_parameter.deploy : p.arn]
  }

  statement {
    sid     = "RunOnTheHostOnly"
    actions = ["ssm:SendCommand"]
    resources = [
      aws_instance.host.arn,
      "arn:aws:ssm:${var.region}::document/AWS-RunShellScript",
    ]
  }

  statement {
    sid       = "ReadCommandResults"
    actions   = ["ssm:GetCommandInvocation", "ssm:ListCommandInvocations"]
    resources = ["*"] # these actions don't support resource-level permissions
  }
}

resource "aws_iam_role_policy" "deploy" {
  name   = "deploy-to-host"
  role   = aws_iam_role.deploy.id
  policy = data.aws_iam_policy_document.deploy.json
}

# What the workflow deploys to. Kept outside the host's own prefix so it doesn't end up
# in the host's .env.
resource "aws_ssm_parameter" "deploy" {
  for_each = {
    "instance-id" = aws_instance.host.id
    "api-domain"  = aws_cloudfront_distribution.api.domain_name
  }
  name  = "/${var.project}-deploy/${each.key}"
  type  = "String"
  value = each.value
}
