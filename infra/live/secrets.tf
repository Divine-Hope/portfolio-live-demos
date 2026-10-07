# Everything the host needs at runtime, as SSM parameters under one prefix.
# deploy/host/render-env.sh turns each one into a variable in the host's .env:
#   /livedemos/clickhouse-api-password  ->  CLICKHOUSE_API_PASSWORD

locals {
  ssm_prefix = "/${var.project}"
  secret_names = [
    "clickhouse-admin-password",
    "clickhouse-migrator-password",
    "clickhouse-ingest-password",
    "clickhouse-api-password",
    "clickhouse-archiver-password",
    "api-origin-secret",
  ]
}

# Generated here, so nobody ever types or sees them. They also live in Terraform
# state, which is why the state bucket is private and encrypted.
resource "random_password" "secret" {
  for_each = toset(local.secret_names)
  length   = 32
  special  = false # they end up in an .env file; no quoting surprises
}

resource "aws_ssm_parameter" "secrets" {
  for_each = random_password.secret
  name     = "${local.ssm_prefix}/${each.key}"
  type     = "SecureString" # encrypted with the AWS-managed key: no KMS charge
  value    = each.value.result
}

resource "aws_ssm_parameter" "settings" {
  for_each = {
    "ingest-contact"      = var.repo_url
    "api-snapshot-bucket" = module.snapshots.id
    "archive-url"         = "https://${module.archive.id}.s3.${var.region}.amazonaws.com/wikipedia/edits"
    "aws-default-region"  = var.region
    "host-public-ip"      = aws_eip.host.public_ip # the archive runs only on the host holding it
    "grafana-prom-url"    = var.grafana_cloud.prom_url
    "grafana-prom-user"   = var.grafana_cloud.prom_user
    "grafana-loki-url"    = var.grafana_cloud.loki_url
    "grafana-loki-user"   = var.grafana_cloud.loki_user
  }
  name  = "${local.ssm_prefix}/${each.key}"
  type  = "String"
  value = each.value
}

# Fire drill switch for the API's 5xx alert: "true" makes every /v1/ request fail (docs/
# runbook.md, "Fire drill"). Set by hand during a drill; Terraform only creates it.
resource "aws_ssm_parameter" "api_drill_5xx" {
  name  = "${local.ssm_prefix}/api-drill-5xx"
  type  = "String"
  value = "false"

  lifecycle {
    ignore_changes = [value]
  }
}

# The commit SHA of the image to run. Each deploy writes it; Terraform only creates it.
resource "aws_ssm_parameter" "image_tag" {
  name  = "${local.ssm_prefix}/image-tag"
  type  = "String"
  value = "none"

  lifecycle {
    ignore_changes = [value]
  }
}

# Alloy's Grafana Cloud access policy token (metrics:write, logs:write). Created in Grafana
# Cloud, so it can't be generated here: Terraform makes the parameter with a placeholder,
# write-only so no value ever lands in state, and the token goes in by hand
# (docs/runbook.md, "Grafana Cloud"). Bumping the version would reset it to the placeholder.
resource "aws_ssm_parameter" "grafana_cloud_token" {
  name             = "${local.ssm_prefix}/grafana-cloud-token"
  type             = "SecureString"
  value_wo         = "unset"
  value_wo_version = 1
}
